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
mod kafka;
mod linker;
mod processor;
mod raft;
mod redis;
mod socket_addr;

use once_cell::sync::OnceCell;

static AUTH_URL: OnceCell<String> = OnceCell::new();
static HTTP_CLIENT: OnceCell<reqwest::Client> = OnceCell::new();

static DISPATCHER: OnceCell<tokio::sync::mpsc::Sender<processor::Event>> = OnceCell::new();
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    tracing::error!("version: 2023/5/12-14:13");

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

    let mut core_ids = core_affinity::get_core_ids().unwrap();
    let main_core_id = core_ids.pop();
    if let Some(core_id) = main_core_id {
        if core_affinity::set_for_current(core_id) {
            tracing::error!("setting main core [{}]", core_id.id);
        }
    };
    tokio::task::LocalSet::new()
        .run_until(async {
            // TODO: init & run raft

            let kafka_client =
            kafka::Client::new(local_addr.to_string().as_str(), config.kafka.clone()).await.unwrap();
            KAFKA_CLIENT.set(kafka_client.clone()).unwrap();

            let client = kafka_client;
            tracing::error!("starting kafka client");    

            tokio::task::spawn_local(async move {
                processor::run(core_ids).await.unwrap();
            });

            tokio::task::spawn_local(async {
                tracing::error!("running http server");
                axum_handler::run(config.http).await;
            });

            tokio::task::spawn_local(async move {
                if let Err(e) = client
                    .consume(
                        async move |record /* , tx: tokio::sync::mpsc::Sender<processor::Event> */| {
                            axum_handler::KAFKA_CONSUME_COUNT
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if let Some(event) = record.record.value &&
                            let Some(dispatcher) = DISPATCHER.get() &&
                            let Ok(event) = serde_json::from_slice(event.as_slice()) &&
                            let Ok(_) = dispatcher.send(event).await {
                                Ok(())
                            } else {
                                Err(anyhow::anyhow!("Kafka Consume Error"))
                            }
                        },
                    )
                    .await
                {
                    tracing::error!("Kafka Consume Error: {:?}", e)
                };
            });

            tracing::error!("handle tcp connect");
            while let Ok((stream, remote_addr)) = tcp_listener.accept().await {
                // stream.set_nodelay(true)?;
                tokio::task::spawn_local(async move {
                    // use std::sync::atomic::Ordering::Relaxed;

                    tracing::trace!("{remote_addr} connected");
                    // Process each socket concurrently.
                    // axum_handler::LINK_COUNT.fetch_add(1, Relaxed);
                    if let Err(e) = linker::tcp::auth(stream).await{
                        tracing::error!("tcp auth error: {e:?}");
                    } else {
                        tracing::trace!("{remote_addr} authed");
                    };

                    // axum_handler::LINK_COUNT.fetch_sub(1, Relaxed);
                    Ok::<(), anyhow::Error>(())
                });
            }
        })
        .await;

    Ok(())
}
