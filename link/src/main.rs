#![feature(let_chains, result_option_inspect, async_closure, const_trait_impl)]

use event_loop::Event;
mod axum_handler;
mod config;
mod conhash;
mod event_loop;
mod kafka;
mod processor;
mod raft;
mod redis;
mod socket_addr;

use once_cell::sync::OnceCell;

static EVENT_LOOP: OnceCell<TokioSender<Event>> = OnceCell::new();
static REDIS_CLIENT: OnceCell<redis::Client> = OnceCell::new();
static KAFKA_CLIENT: OnceCell<tokio::sync::Mutex<kafka::Client>> = OnceCell::new();
type TokioSender<T> = tokio::sync::mpsc::UnboundedSender<T>;
// type TokioSender<T> = tokio::sync::mpsc::Sender<T>;
type Sender = TokioSender<processor::TcpEvent>;
// type Sender = TokioSender<processor::TcpEvent>;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let local_addr = socket_addr::ipv4::local_addr().await?;
    tracing::error!("local addr: {local_addr:?}");

    let config = config::Config::init();

    let tcp_listener = tokio::net::TcpListener::bind(config.get_tcp_addr_str()).await?;

    let redis_client = redis::Client::new(local_addr.to_string(), config.redis.addrs).await?;
    REDIS_CLIENT.set(redis_client).unwrap();
    tracing::error!("starting redis client");

    let kafka_client =
        kafka::Client::new(local_addr.to_string().as_str(), config.kafka.clone()).await?;
    let client = kafka_client.clone();
    tracing::error!("starting kafka client");

    tokio::task::spawn(async {
        client
            .consume(
                async move |record, tx: tokio::sync::mpsc::UnboundedSender<Event>| {
                    axum_handler::KAFKA_CONSUME_COUNT
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let value_batch = record.record.value.unwrap_or_default();
                    let value_batch: Vec<Option<Vec<u8>>> =
                        serde_json::from_slice(&value_batch).unwrap_or_default();
                    let value_batch: Vec<event_loop::SendRequest> = value_batch
                        .into_iter()
                        .map(|value| {
                            serde_json::from_str(
                                String::from_utf8(value.unwrap()).unwrap().as_str(),
                            )
                            .unwrap()
                        })
                        .collect();

                    let mut chats_batch: ahash::AHashMap<String, Vec<event_loop::SendRequest>> =
                        ahash::AHashMap::new();

                    for value in value_batch {
                        chats_batch
                            .entry(value.chat.clone())
                            .and_modify(|batch| batch.push(value))
                            .or_default();
                    }

                    if let Err(_e) = tx.send(crate::Event::SendBatch(chats_batch)) {
                        // TODO: handle error
                    };

                    // if let Ok(send_request) = send_request {
                    //     // tracing::info!("consume send request: {send_request:?}");
                    //     if let Err(_e) = tx.send(crate::Event::Send(send_request)) {
                    //         // TODO: handle error
                    //     };
                    // }
                },
            )
            .await;
    });

    KAFKA_CLIENT
        .set(tokio::sync::Mutex::new(kafka_client))
        .unwrap();

    // TODO: init & run raft

    tokio::task::spawn(async {
        tracing::error!("running event loop");
        event_loop::run().await.unwrap();
    });

    tokio::task::spawn(async {
        tracing::error!("running http server");
        axum_handler::run().await;
    });

    tracing::error!("handle tcp connect");
    while let Ok((stream, _)) = tcp_listener.accept().await {
        // FIXME:
        // stream.set_nodelay(true)?;
        tokio::spawn(async move {
            use std::sync::atomic::Ordering::Relaxed;

            // Process each socket concurrently.
            axum_handler::TCP_COUNT.fetch_add(1, Relaxed);
            processor::process(stream).await;
            axum_handler::TCP_COUNT.fetch_sub(1, Relaxed);
        });
    }
    Ok(())
}
