use axum::{
    debug_handler,
    response::{IntoResponse, Response},
    routing::delete,
};

use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub(crate) static CONSUME_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static PRODUCE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_COUNT_TIMESTAMP: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_CONSUME_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_PRODUCE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

#[debug_handler]
pub(crate) async fn get_count() -> Response {
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let consume_count = CONSUME_COUNT.load(Relaxed);
    let last_consume_count = LAST_CONSUME_COUNT.swap(consume_count, Relaxed);
    let produce_count = PRODUCE_COUNT.load(Relaxed);
    let last_produce_count = LAST_PRODUCE_COUNT.swap(produce_count, Relaxed);
    let earlier = LAST_COUNT_TIMESTAMP.swap(now, Relaxed);

    let mut time_interval = now - earlier;
    if time_interval == 0 {
        time_interval = 1
    };
    let consume_count_interval = consume_count - last_consume_count;
    let produce_count_interval = produce_count - last_produce_count;
    let consume_per_secs = consume_count_interval / time_interval;
    let produce_per_secs = produce_count_interval / time_interval;

    #[derive(serde_derive::Serialize)]
    struct SystemCount {
        consume_count: u64,
        produce_count: u64,
        time_interval: u64,
        consume_per_secs: u64,
        produce_per_secs: u64,
    }

    let system_count = SystemCount {
        consume_count,
        produce_count,
        time_interval,
        consume_per_secs,
        produce_per_secs,
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
    CONSUME_COUNT.store(0, Relaxed);
    PRODUCE_COUNT.store(0, Relaxed);
    LAST_COUNT_TIMESTAMP.store(now, Relaxed);
    LAST_CONSUME_COUNT.store(0, Relaxed);
    LAST_PRODUCE_COUNT.store(0, Relaxed);
    ().into_response()
}

pub(super) async fn run(config: crate::config::Http) {
    use axum::{routing::get, Router};

    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    LAST_COUNT_TIMESTAMP.store(now, std::sync::atomic::Ordering::Relaxed);

    let app = Router::new()
        .route("/count", get(get_count))
        .route("/count", delete(clean_count));

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.port));
    let server = axum::Server::bind(&addr).serve(app.into_make_service());
    if let Err(err) = server.await {
        panic!("server error: {}", err)
    }
}
