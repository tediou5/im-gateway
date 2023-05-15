use super::{Content, Message};

pub(crate) type Sender = tokio::sync::mpsc::UnboundedSender<Event>;

#[derive(Debug)]
pub(crate) enum Event {
    WriteBatch(std::rc::Rc<String>),
    // WriteBatch(std::sync::Arc<String>),
    Close,
}

pub(crate) async fn websocket(
    websocket: axum::extract::ws::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    websocket.on_upgrade(async move |mut socket| {
        let auth = match socket.recv().await {
            Some(Ok(axum::extract::ws::Message::Text(message))) => message,
            _ => return,
        };

        let content = match serde_json::from_str::<Content>(auth.as_str()) {
            Ok(content) => content,
            Err(_) => return,
        };

        let _ = content
            .handle_auth(async move |platform, message| {
                let content = message.content;
                let content = serde_json::to_string(&content)?;
                tracing::info!("platform connection: {platform:?} with baseinfo:\n{content:?}");
                if let Err(e) = socket.send(axum::extract::ws::Message::Text(content)).await {
                    return Err(anyhow::anyhow!("failed to write to socket; err = {e}"));
                }
                match platform.as_str() {
                    "web" => Ok(super::Platform::Web(socket)),
                    _ => Err(anyhow::anyhow!("unexpected platform")),
                }
            })
            .await;
    })
}

pub(crate) fn process(socket: axum::extract::ws::WebSocket, pin: std::rc::Rc<String>) -> Sender {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
    let mut write = handle(socket, tx.clone(), pin.clone());

    tokio::task::spawn_local(async move {
        use futures::SinkExt as _;
        use futures::StreamExt as _;

        while let Some(event) = rx.next().await {
            let content = match event {
                Event::Close => break,
                Event::WriteBatch(contents) => contents,
            };

            match write
                .send(axum::extract::ws::Message::Text(content.to_string()))
                .await
            {
                Ok(()) => {
                    tracing::info!("[{pin}] websocket send ok");
                    crate::axum_handler::LINK_SEND_COUNT
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::error!("[{pin}] websocket send error: {e:?}");
                    break;
                }
            };
        }

        tracing::info!("[{pin}] websocket closed");
        if let Some(redis) = crate::REDIS_CLIENT.get() {
            if let Err(e) = redis.del_heartbeat(pin.to_string()).await {
                tracing::error!("del [{pin}] heartbeat error: {}", e)
            };
        }
        let _ = write.close().await;
    });

    tx
}

fn handle(
    socket: axum::extract::ws::WebSocket,
    tx: Sender,
    pin: std::rc::Rc<String>,
) -> futures::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message> {
    use futures::StreamExt as _;

    let (sink, mut input) = socket.split();

    tokio::task::spawn_local(async move {
        while let Some(Ok(message)) = input.next().await {
            let pin = pin.clone();
            match message {
                axum::extract::ws::Message::Text(message) => {
                    tracing::info!("[{pin}] websocket received message: {message:?}");

                    tokio::task::spawn_local(async move {
                        if let Some(redis) = crate::REDIS_CLIENT.get() {
                            if let Err(e) = redis.heartbeat(pin.to_string()).await {
                                tracing::error!("update [{pin}] heartbeat error: {}", e)
                            };
                        }
                    });

                    let kafka = crate::KAFKA_CLIENT
                        .get()
                        .ok_or(anyhow::anyhow!("kafka is not available"))?;
                    let content = serde_json::from_str::<Content>(message.as_str())?;
                    let message: Message = content.into();
                    tracing::info!("websocket produce message: {message:?}");
                    kafka.produce(message).await?;
                }
                axum::extract::ws::Message::Close(_close) => {
                    let _ = tx.send(Event::Close);
                    break;
                }
                _ => continue,
            }
        }
        Ok::<(), anyhow::Error>(())
    });
    sink
}
