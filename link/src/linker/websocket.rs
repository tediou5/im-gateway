pub(crate) type Sender = local_sync::mpsc::unbounded::Tx<Event>;

#[derive(Debug)]
pub(crate) enum Event {
    WriteBatch(std::rc::Rc<String>),
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

        let content = match serde_json::from_str::<super::Content>(auth.as_str()) {
            Ok(content) => content,
            Err(_) => return,
        };

        let _ = content
            .handle_auth(async move |platform, message| match platform.as_str() {
                "web" => Ok(super::Login {
                    platform: super::Platform::Web(socket),
                    auth_message: message,
                }),
                _ => Err(anyhow::anyhow!("unexpected platform")),
            })
            .await;
    })
}

pub(crate) fn process(
    socket: axum::extract::ws::WebSocket,
    pin: std::rc::Rc<String>,
    auth_message: super::Message,
) -> Sender {
    let (tx, rx) = local_sync::mpsc::unbounded::channel::<Event>();
    let mut rx = local_sync::stream_wrappers::unbounded::ReceiverStream::new(rx);
    let mut write = handle(socket, tx.clone(), pin.clone());

    let auth_message = auth_message.content;
    // FIXME: maybe panic if auth_message serialize  error.
    let auth_message = serde_json::to_string(&auth_message).unwrap();
    let _ = tx.send(Event::WriteBatch(auth_message.into()));

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

                    let kafka = match crate::KAFKA_CLIENT
                        .get()
                        .ok_or(anyhow::anyhow!("kafka is not available"))
                    {
                        Ok(kafka) => kafka,
                        Err(e) => {
                            tracing::error!("Websocket Error: System Error: {e}");
                            break;
                        }
                    };

                    let message: super::Message =
                        match serde_json::from_str::<super::Content>(message.as_str()) {
                            Ok(content) => content.into(),
                            Err(e) => {
                                tracing::error!("Websocket Error: Serde Error: {e}");
                                break;
                            }
                        };

                    tracing::info!("websocket produce message: {message:?}");
                    if let Err(e) = kafka.produce(message).await {
                        tracing::error!("Websocket Error: Kafka Error: {e}");
                        break;
                    };
                }
                axum::extract::ws::Message::Close(_close) => {
                    break;
                }
                _ => continue,
            }
        }
        tracing::info!("websocket: [{pin}] read error.");
        let _ = tx.send(Event::Close);
        Ok::<(), anyhow::Error>(())
    });
    sink
}
