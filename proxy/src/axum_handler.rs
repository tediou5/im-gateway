use axum::{
    debug_handler,
    extract::Json,
    http::StatusCode,
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

#[derive(Debug, Clone, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
#[serde(untagged)]
pub(crate) enum LinkProtocol {
    Private(
        std::collections::HashSet<String>,                    /* recvs */
        std::collections::HashMap<String, serde_json::Value>, /* content */
    ),
    Group(
        String,                                               /* chat */
        std::collections::HashSet<String>,                    /* exclusions */
        std::collections::HashSet<String>,                    /* additional */
        std::collections::HashMap<String, serde_json::Value>, /* content */
    ),
    Chat(chat::Action),
}

impl From<LinkProtocol> for rskafka::record::Record {
    fn from(value: LinkProtocol) -> Self {
        use time::OffsetDateTime;

        Self {
            key: None,
            value: serde_json::to_vec(&value).ok(),
            headers: std::collections::BTreeMap::new(),
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}

// send message to link service
pub(crate) async fn send_message(Json(proto): Json<LinkProtocol>) -> Response {
    REQUEST_COUNT.fetch_add(1, Relaxed);

    let producer = match crate::KAFKA_CLIENT.get() {
        Some(producer) => producer,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "kafka is not ready".to_string(),
            )
                .into_response()
        }
    };

    let redis = match crate::REDIS_CLIENT.get() {
        Some(redis) => redis,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "redis is not ready".to_string(),
            )
                .into_response()
        }
    };

    let linkers = match &proto {
        LinkProtocol::Private(recvs, ..)
        | LinkProtocol::Chat(chat::Action::Join(.., recvs)) => {
            // FIXME: select the linker service by hashring.
            if recvs.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    "recvs must not empty".to_string(),
                )
                    .into_response();
            }
            redis.get_linkers().await
        }
        LinkProtocol::Group(chat, ..) /* FIXME: additional should send like Private Message */
        | LinkProtocol::Chat(chat::Action::Leave(chat, ..)) => {
            redis.get_router(chat.as_str()).await
        }
    };

    let linkers = match linkers {
        Ok(linkers) => linkers,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    tracing::debug!("produce into: {linkers:?}\nmessage: {proto:?}");

    for linker in linkers {
        if let Err(e) = producer.produce(linker, proto.clone()).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("kafka produce error: {e}"),
            )
                .into_response();
        };
    }

    RESPONSE_COUNT.fetch_add(1, Relaxed);

    ().into_response()
}

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
    use axum::{
        routing::{get, post},
        Router,
    };

    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    LAST_COUNT_TIMESTAMP.store(now, std::sync::atomic::Ordering::Relaxed);

    let app = Router::new()
        .route("/message", post(send_message))
        .route("/count", get(get_count))
        .route("/count", delete(clean_count));

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.port));
    let server = axum::Server::bind(&addr).serve(app.into_make_service());
    if let Err(err) = server.await {
        panic!("server error: {}", err)
    }
}

pub(crate) mod chat {
    #[derive(Debug, Clone, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Action {
        Join(String, std::collections::HashSet<String>),
        Leave(String, std::collections::HashSet<String>),
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::{chat::Action, LinkProtocol};

    #[test]
    fn proto_json() {
        let join = LinkProtocol::Chat(Action::Join(
            "cc_1".to_string(),
            HashSet::from_iter(["uu_1".to_string(), "uu_2".to_string()]),
        ));

        let join_from_json: LinkProtocol =
            serde_json::from_str(r#"{"join": ["cc_1", ["uu_1", "uu_2"]]}"#).unwrap();

        assert_eq!(join, join_from_json);

        let leave = LinkProtocol::Chat(Action::Leave(
            "cc_1".to_string(),
            HashSet::from_iter(["uu_1".to_string(), "uu_2".to_string()]),
        ));

        let leave_from_json: LinkProtocol =
            serde_json::from_str(r#"{"leave": ["cc_1", ["uu_1", "uu_2"]]}"#).unwrap();

        assert_eq!(leave, leave_from_json)
    }
}
