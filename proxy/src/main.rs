mod axum_handler;
mod config;
mod kafka;
mod redis;

use once_cell::sync::OnceCell;

static REDIS_CLIENT: OnceCell<redis::Client> = OnceCell::new();
static KAFKA_CLIENT: OnceCell<kafka::Client> = OnceCell::new();
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
    let args = <Args as clap::Parser>::parse();
    let config = config::Config::init(args.config);

    let redis_client = redis::Client::new(config.redis.addrs).await?;
    REDIS_CLIENT.set(redis_client).unwrap();
    tracing::error!("starting redis client");

    let kafka_client = kafka::Client::new(config.kafka).await?;
    KAFKA_CLIENT.set(kafka_client).unwrap();
    tracing::error!("starting kafka client");

    tracing::error!("running http server");
    axum_handler::run(config.http).await;
    tracing::error!("closed.");

    Ok(())
}
