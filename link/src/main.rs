#![feature(
    let_chains,
    async_closure,
    const_trait_impl,
    iter_collect_into,
    iterator_try_collect,
    string_remove_matches,
    result_option_inspect,
    associated_type_bounds
)]

mod axum_handler;
mod config;
mod conhash;
mod kafka;
mod linker;
mod processor;
mod raft;
mod redis;
mod snowflake;
mod socket_addr;

use once_cell::sync::OnceCell;

static TCP_CONFIG: OnceCell<config::Tcp> = OnceCell::new();
static HTTP_CLIENT: OnceCell<reqwest::Client> = OnceCell::new();
static RETRY_CONFIG: OnceCell<config::Retry> = OnceCell::new();

static DISPATCHER: OnceCell<tokio::sync::mpsc::Sender<processor::Event>> = OnceCell::new();
static REDIS_CLIENT: OnceCell<redis::Client> = OnceCell::new();
static KAFKA_CLIENT: OnceCell<kafka::Client> = OnceCell::new();
type TokioSender<T> = tokio::sync::mpsc::UnboundedSender<T>;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    // config path
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("version: 2023/5/26-19:00");

    let _ = tokio::task::LocalSet::new().run_until(init()).await;

    Ok(())
}

async fn init() -> anyhow::Result<()> {
    let local_addr = socket_addr::ipv4::local_addr().await?;
    println!("local addr: {local_addr:?}");

    let args = <Args as clap::Parser>::parse();
    let config = config::Config::init(args.config.unwrap_or("./config.toml".to_string()));

    let client = reqwest::Client::new();
    let tcp_listener = tokio::net::TcpListener::bind(config.get_tcp_addr_str())
        .await
        .unwrap();
    println!("set tcp listener");
    let redis_client = redis::Client::new(local_addr.to_string(), config.redis)
        .await
        .unwrap();
    println!("set redis config");

    HTTP_CLIENT.set(client).unwrap();
    println!("set http config");
    RETRY_CONFIG.set(config.retry).unwrap();
    println!("set retry config");
    TCP_CONFIG.set(config.tcp).unwrap();
    println!("set tcp config");
    REDIS_CLIENT.set(redis_client).unwrap();
    println!("starting redis client");

    let mut core_ids = core_affinity::get_core_ids().unwrap();
    let main_core_id = core_ids.pop();
    if let Some(core_id) = main_core_id {
        if core_affinity::set_for_current(core_id) {
            println!("setting main core [{}]", core_id.id);
        }
    };

    // TODO: init & run raft

    let kafka_client = kafka::Client::new(local_addr.to_string().as_str(), config.kafka.clone())
        .await
        .unwrap();
    KAFKA_CLIENT.set(kafka_client.clone()).unwrap();

    let client = kafka_client;
    println!("starting kafka client");
    println!("
    ###################################################################################################################\n\n
      #########   #             #          #########   ##       #      ############    ##########     #
          #       ##           ##              #       # #      #          #           #         #    # #
          #       # #         # #              #       #  #     #          #           #         #    #   #
          #       #  #       #  #              #       #   #    #      ############    ###########    #######
          #       #   #     #   #              #       #    #   #          #           # #            #       #
          #       #    #   #    #              #       #     #  #          #           #    #         #         #
          #       #     # #     #              #       #      # #          #           #       #      #           #
      #########   #      #      #          #########   #       ##          #           #          #   #             #\n\n
    ###################################################################################################################
    ");

    tokio::task::spawn_local(async {
        println!("running http server");
        axum_handler::run(config.http).await;
    });

    tokio::task::spawn_local(async move {
        processor::run(core_ids).await.unwrap();
    });

    tokio::task::spawn_local(async move {
        if let Err(e) = client
            .consume(async move |record| {
                axum_handler::KAFKA_CONSUME_COUNT
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let dispatcher = match DISPATCHER.get() {
                    Some(dispatcher) => dispatcher,
                    None => {
                        return Err(anyhow::anyhow!(
                            "Kafka Consume Error: dispatcher not yet initialized"
                        ))
                    }
                };
                let event = match record.record.value {
                    Some(event) => event,
                    None => {
                        return Err(anyhow::anyhow!("Kafka Consume Error: record value is none"))
                    }
                };
                tracing::debug!("consume {} len message.", event.len());
                let event = match serde_json::from_slice(event.as_slice()) {
                    Ok(event) => event,
                    Err(e) => {
                        return Err(anyhow::anyhow!(
                            "Kafka Consume Error: deser event error: {e}"
                        ))
                    }
                };
                tracing::debug!("kafka Consume: ready for dispatch event: {event:?}");
                if (dispatcher.send(event).await).is_err() {
                    tracing::error!("Kafka Consume Dispatch error");
                    return Err(anyhow::anyhow!(
                        "Kafka Consume Error: dispatcher send error"
                    ));
                };

                Ok(())
            })
            .await
        {
            tracing::error!("Kafka Consume Error: {:?}", e)
        };
    });

    println!("handle tcp connect");
    while let Ok((stream, remote_addr)) = tcp_listener.accept().await {
        tokio::task::spawn_local(async move {
            tracing::info!("{remote_addr} connected");

            // set keepalive
            let stream = stream.into_std().unwrap();
            let socket = socket2::Socket::from(stream);
            let keep_alive = socket2::TcpKeepalive::new()
                .with_time(std::time::Duration::from_secs(20))
                .with_interval(std::time::Duration::from_secs(5));
            socket
                .set_tcp_keepalive(&keep_alive)
                .map_err(|e| format!("set keep alive error: {e}"))
                .unwrap();
            let stream = tokio::net::TcpStream::from_std(socket.into()).unwrap();

            // Process each socket concurrently.
            stream.set_nodelay(true).unwrap();
            if let Err(e) = linker::tcp::auth(stream).await {
                tracing::error!("{remote_addr} tcp auth error: {e:?}");
            } else {
                tracing::info!("{remote_addr} authed");
            };

            Ok::<(), anyhow::Error>(())
        });
    }
    Ok(())
}
