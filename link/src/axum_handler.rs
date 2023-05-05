use axum::{
    debug_handler,
    response::{IntoResponse, Response},
};
use once_cell::sync::Lazy;
use std::sync::atomic::AtomicU64;

pub(crate) static LINK_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static SEND_REQUEST_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static KAFKA_CONSUME_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LINK_SEND_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_REQUEST_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_LINK_SEND_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_KAFKA_CONSUME_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_COUNT_TIMESTAMP: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static REGISTER_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

#[debug_handler]
pub(crate) async fn get_count() -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let link_count = LINK_COUNT.load(Relaxed);
    let send_request_count = SEND_REQUEST_COUNT.load(Relaxed);
    let kafka_consume_count = KAFKA_CONSUME_COUNT.load(Relaxed);
    let link_send_count = LINK_SEND_COUNT.load(Relaxed);
    let register_count = REGISTER_COUNT.load(Relaxed);
    let last_request_count = LAST_REQUEST_COUNT.swap(send_request_count, Relaxed);
    let last_link_send_count = LAST_LINK_SEND_COUNT.swap(link_send_count, Relaxed);
    let last_kafka_consume_count = LAST_KAFKA_CONSUME_COUNT.swap(kafka_consume_count, Relaxed);
    let earlier = LAST_COUNT_TIMESTAMP.swap(now, Relaxed);

    let mut time_interval = now - earlier;
    if time_interval == 0 {
        time_interval = 1
    };
    let request_interval = send_request_count - last_request_count;
    let link_send_interval = link_send_count - last_link_send_count;
    let kafka_consume_interval = kafka_consume_count - last_kafka_consume_count;
    let request_per_secs = request_interval / time_interval;
    let send_per_secs = link_send_interval / time_interval;
    let kafka_per_secs = kafka_consume_interval / time_interval;

    #[derive(serde_derive::Serialize)]
    struct SystemCount {
        link_count: u64,
        link_send_count: u64,
        register_count: u64,
        send_request_count: u64,
        kafka_consume_count: u64,
        request_interval: u64,
        time_interval: u64,
        link_send_interval: u64,
        kafka_consume_interval: u64,
        request_per_secs: u64,
        send_per_secs: u64,
        kafka_per_secs: u64,
    }

    let system_count = SystemCount {
        link_count,
        send_request_count,
        kafka_consume_count,
        link_send_count,
        register_count,
        time_interval,
        request_interval,
        link_send_interval,
        kafka_consume_interval,
        request_per_secs,
        send_per_secs,
        kafka_per_secs,
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
    REGISTER_COUNT.store(0, Relaxed);
    SEND_REQUEST_COUNT.store(0, Relaxed);
    KAFKA_CONSUME_COUNT.store(0, Relaxed);
    LAST_LINK_SEND_COUNT.store(0, Relaxed);
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
