use axum::{
    debug_handler,
    response::{IntoResponse, Response},
};
use once_cell::sync::Lazy;
use std::sync::atomic::AtomicU64;

pub(crate) static LINK_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static SEND_REQUEST_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static KAFKA_PRODUCE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static KAFKA_CONSUME_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LINK_SEND_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_LINK_SEND_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_KAFKA_PRODUCE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_KAFKA_CONSUME_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_COUNT_TIMESTAMP: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

#[debug_handler]
pub(crate) async fn get_count() -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let earlier = LAST_COUNT_TIMESTAMP.swap(now, Relaxed);

    let link_count = LINK_COUNT.load(Relaxed);
    let link_send_count = LINK_SEND_COUNT.load(Relaxed);
    let kafka_produce_count = KAFKA_PRODUCE_COUNT.load(Relaxed);
    let kafka_consume_count = KAFKA_CONSUME_COUNT.load(Relaxed);
    let last_link_send_count = LAST_LINK_SEND_COUNT.swap(link_send_count, Relaxed);
    let last_kafka_produce_count = LAST_KAFKA_PRODUCE_COUNT.swap(kafka_produce_count, Relaxed);
    let last_kafka_consume_count = LAST_KAFKA_CONSUME_COUNT.swap(kafka_consume_count, Relaxed);

    let mut time_interval = now - earlier;
    if time_interval == 0 {
        time_interval = 1
    };

    let link_send_interval = link_send_count - last_link_send_count;
    let kafka_produce_interval = kafka_produce_count - last_kafka_produce_count;
    let kafka_consume_interval = kafka_consume_count - last_kafka_consume_count;

    let send_per_secs = link_send_interval / time_interval;
    let kafka_produce_per_secs = kafka_produce_interval / time_interval;
    let kafka_consume_per_secs = kafka_consume_interval / time_interval;

    #[derive(serde_derive::Serialize)]
    struct SystemCount {
        #[serde(rename = "连接数")]
        link_count: u64,
        #[serde(rename = "每秒推送")]
        send_per_secs: u64,
        #[serde(rename = "每秒生产")]
        kafka_produce_per_secs: u64,
        #[serde(rename = "每秒消费")]
        kafka_consume_per_secs: u64,
    }

    let system_count = SystemCount {
        link_count,
        send_per_secs,
        kafka_produce_per_secs,
        kafka_consume_per_secs,
    };
    serde_json::to_string(&system_count)
        .unwrap()
        .into_response()
}

#[debug_handler]
pub(super) async fn clean_count() -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    LINK_COUNT.store(0, Relaxed);
    LINK_SEND_COUNT.store(0, Relaxed);
    SEND_REQUEST_COUNT.store(0, Relaxed);
    KAFKA_PRODUCE_COUNT.store(0, Relaxed);
    KAFKA_CONSUME_COUNT.store(0, Relaxed);
    LAST_LINK_SEND_COUNT.store(0, Relaxed);
    LAST_KAFKA_PRODUCE_COUNT.store(0, Relaxed);
    LAST_KAFKA_CONSUME_COUNT.store(0, Relaxed);
    LAST_COUNT_TIMESTAMP.store(now, Relaxed);
    ().into_response()
}

pub(super) async fn run(config: crate::config::Http) {
    use axum::{
        routing::{delete, get},
        Router,
    };
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    LAST_COUNT_TIMESTAMP.store(now, std::sync::atomic::Ordering::Relaxed);

    let app = Router::new()
        .route(
            config.websocket_router.as_str(),
            get(crate::linker::websocket::process),
        )
        .route("/count", get(get_count))
        .route("/count", delete(clean_count));

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.port));
    let server = axum::Server::bind(&addr).serve(app.into_make_service());
    if let Err(err) = server.await {
        panic!("server error: {}", err)
    }
}
