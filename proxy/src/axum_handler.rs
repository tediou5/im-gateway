use axum::{
    debug_handler,
    response::{IntoResponse, Response},
    routing::delete,
};

use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub(crate) static REQUEST_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static RESPONSE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_COUNT_TIMESTAMP: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_REQUEST_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub(crate) static LAST_RESPONSE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

#[debug_handler]
pub(crate) async fn get_count() -> Response {
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let request_count = REQUEST_COUNT.load(Relaxed);
    let last_request_count = LAST_REQUEST_COUNT.swap(request_count, Relaxed);
    let response_count = RESPONSE_COUNT.load(Relaxed);
    let last_response_count = LAST_RESPONSE_COUNT.swap(response_count, Relaxed);
    let earlier = LAST_COUNT_TIMESTAMP.swap(now, Relaxed);

    let mut time_interval = now - earlier;
    if time_interval == 0 {
        time_interval = 1
    };
    let request_count_interval = request_count - last_request_count;
    let response_count_interval = response_count - last_response_count;
    let request_per_secs = request_count_interval / time_interval;
    let response_per_secs = response_count_interval / time_interval;

    #[derive(serde_derive::Serialize)]
    struct SystemCount {
        request_count: u64,
        response_count: u64,
        time_interval: u64,
        request_per_secs: u64,
        response_per_secs: u64,
    }

    let system_count = SystemCount {
        request_count,
        response_count,
        time_interval,
        request_per_secs,
        response_per_secs,
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
    REQUEST_COUNT.store(0, Relaxed);
    RESPONSE_COUNT.store(0, Relaxed);
    LAST_COUNT_TIMESTAMP.store(now, Relaxed);
    LAST_REQUEST_COUNT.store(0, Relaxed);
    LAST_RESPONSE_COUNT.store(0, Relaxed);
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
