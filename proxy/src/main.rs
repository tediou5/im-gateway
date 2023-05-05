#![feature(async_closure)]

mod axum_handler;
mod config;
mod kafka;
mod protocol;
mod redis;
mod socket_addr;

// use once_cell::sync::OnceCell;

// static REDIS_CLIENT: OnceCell<redis::Client> = OnceCell::new();
// static KAFKA_CLIENT: OnceCell<kafka::Client> = OnceCell::new();
type TokioSender<T> = tokio::sync::mpsc::UnboundedSender<T>;
// type TokioSender<T> = tokio::sync::mpsc::Sender<T>;

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

    let kafka = kafka::Client::new(local_addr.to_string(), config.kafka).await?;

    tokio::task::spawn(async {
        kafka.consume(config.redis, async move |record, redis, kafka| {
            // FIXME: handle error
            if let Ok(proto) = record.record.try_into() {
                let linkers = match &proto {
                    protocol::LinkProtocol::Private(recvs, ..)
                    | protocol::LinkProtocol::Chat(protocol::chat::Action::Join(.., recvs)) => {
                        // FIXME: select the linker service by hashring.
                        if recvs.is_empty() {
                            None
                        } else {
                            redis.get_linkers().await.ok()
                        }
                    }
                    protocol::LinkProtocol::Group(chat, ..) /* FIXME: additional should send like Private Message */
                    | protocol::LinkProtocol::Chat(protocol::chat::Action::Leave(chat, ..)) => {
                        redis.get_router(chat.as_str()).await.ok()
                    }
                };

                if let Some(linkers) = linkers {
                    tracing::error!("produce into: {linkers:?}\nmessage: {proto:?}");

                    for linker in linkers {
                        let _ = kafka.produce(linker, proto.clone()).await;
                    }
                }
            }
        }).await;
    });

    tracing::error!("starting consume kafka");

    tracing::error!("running http server");
    axum_handler::run(config.http).await;
    tracing::error!("closed.");

    Ok(())
}
