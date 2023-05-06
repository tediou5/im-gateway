#![feature(
    let_chains,
    async_closure,
    iter_collect_into,
    iterator_try_collect,
    string_remove_matches,
    result_option_inspect
)]

mod axum_handler;
mod config;
mod conhash;
mod event_loop;
mod kafka;
mod linker;
mod raft;
mod redis;
mod socket_addr;

use once_cell::sync::OnceCell;

static AUTH_URL: OnceCell<String> = OnceCell::new();
static HTTP_CLIENT: OnceCell<reqwest::Client> = OnceCell::new();

static EVENT_LOOP: OnceCell<TokioSender<event_loop::Event>> = OnceCell::new();
static REDIS_CLIENT: OnceCell<redis::Client> = OnceCell::new();
static KAFKA_CLIENT: OnceCell<kafka::Client> = OnceCell::new();
type TokioSender<T> = tokio::sync::mpsc::UnboundedSender<T>;
// type TokioSender<T> = tokio::sync::mpsc::Sender<T>;
// type Sender = TokioSender<linker::Event>;
// type Sender = TokioSender<processor::TcpEvent>;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    // config path
    #[arg(short, long)]
    config: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let local_addr = socket_addr::ipv4::local_addr().await?;
    tracing::error!("local addr: {local_addr:?}");

    let args = <Args as clap::Parser>::parse();
    let config = config::Config::init(args.config);

    let tcp_listener = tokio::net::TcpListener::bind(config.get_tcp_addr_str()).await?;

    let client = reqwest::Client::new();
    AUTH_URL.set(config.tcp.auth).unwrap();
    HTTP_CLIENT.set(client).unwrap();

    let redis_client = redis::Client::new(local_addr.to_string(), config.redis.addrs).await?;
    REDIS_CLIENT.set(redis_client).unwrap();
    tracing::error!("starting redis client");

    let kafka_client =
        kafka::Client::new(local_addr.to_string().as_str(), config.kafka.clone()).await?;
    let client = kafka_client.clone();
    tracing::error!("starting kafka client");

    tokio::task::spawn(async {
        client
            .consume(async move |record, tx: TokioSender<event_loop::Event>| {
                axum_handler::KAFKA_CONSUME_COUNT
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let message: anyhow::Result<kafka::Message> = record
                    .record
                    .try_into()
                    .inspect_err(|e| tracing::error!("consumed record error: {e}"));

                if let Ok(message) = message {
                    tracing::error!("consume message: \n{message:?}\n------ end ------");
                    match message {
                        kafka::Message::Private(recv, message) => {
                            if let Err(_e) = tx.send(event_loop::Event::Send(recv, message)) {
                                // FIXME: handle error
                            };
                        }
                        kafka::Message::Group(chat, exclusions, additional, message) => {
                            if let Err(_e) = tx.send(event_loop::Event::SendBatch(
                                chat,
                                exclusions,
                                additional,
                                vec![message],
                            )) {
                                // FIXME: handle error
                            };
                        }
                        kafka::Message::Chat(kafka::Action::Join(chat, users)) => {
                            if let Err(_e) = tx.send(event_loop::Event::Join(chat, users)) {
                                // FIXME: handle error
                            };
                        }
                        kafka::Message::Chat(kafka::Action::Leave(chat, users)) => {
                            if let Err(_e) = tx.send(event_loop::Event::Leave(chat, users)) {
                                // FIXME: handle error
                            };
                        }
                    }
                };
            })
            .await;
    });

    KAFKA_CLIENT.set(kafka_client).unwrap();

    // TODO: init & run raft

    tokio::task::spawn(async {
        tracing::error!("running event loop");
        event_loop::run().await.unwrap();
    });

    tokio::task::spawn(async {
        tracing::error!("running http server");
        axum_handler::run(config.http).await;
    });

    tracing::error!("handle tcp connect");
    while let Ok((stream, _)) = tcp_listener.accept().await {
        // FIXME:
        // stream.set_nodelay(true)?;
        tokio::spawn(async move {
            use std::sync::atomic::Ordering::Relaxed;

            // Process each socket concurrently.
            axum_handler::LINK_COUNT.fetch_add(1, Relaxed);
            linker::tcp::process(stream).await;
            axum_handler::LINK_COUNT.fetch_sub(1, Relaxed);
        });
    }
    Ok(())
}
