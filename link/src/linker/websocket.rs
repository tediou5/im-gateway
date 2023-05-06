use super::{Content, Message};

pub(crate) type Sender = tokio::sync::mpsc::UnboundedSender<Event>;

#[derive(Debug)]
pub(crate) enum Event {
    Write(std::sync::Arc<Message>),
    WriteBatch(std::sync::Arc<Vec<String>>),
    Close,
}

pub(crate) async fn process(
    ws: axum::extract::ws::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(async move |socket| {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
        let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let mut write = handle(socket, tx);

        use futures::SinkExt as _;
        use futures::StreamExt as _;

        while let Some(event) = rx.next().await {
            let contents = match event {
                Event::Close => break,
                Event::WriteBatch(contents) => {
                    contents
                    // FIXME:
                    // let len = messages.len() as u64;
                    // write.send(messages).await.map(|_| 1)
                }
                Event::Write(message) => {
                    let content = serde_json::to_string(&message.content).unwrap();
                    std::sync::Arc::new(vec![content])
                }
            };

            for content in contents.iter() {
                tracing::debug!("\nwebsocket wait for send: {content:?}\n++++++++++++++++++");
                match write
                    .send(axum::extract::ws::Message::Text(content.to_string()))
                    .await
                {
                    Ok(()) => {
                        tracing::debug!("websocket send ok");
                        crate::axum_handler::LINK_SEND_COUNT
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::error!("websocket send error: {e:?}");
                        break;
                    }
                };
            }
        }

        // TODO:
        // user_ws_rx stream will keep processing as long as the user stays
        // connected. Once they disconnect, then...
        // user_disconnected(&username, &uuid).await;
        tracing::info!("disconnecting");
    })
}

fn handle(
    socket: axum::extract::ws::WebSocket,
    tx: Sender,
) -> futures::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message> {
    use futures::StreamExt as _;

    let (sink, mut input) = socket.split();

    tokio::task::spawn(async move {
        while let Some(Ok(message)) = input.next().await {
            match message {
                axum::extract::ws::Message::Text(message) => {
                    tracing::debug!("received message: {message:?}");

                    // FIXME: is websocket send Message? or Content?:
                    // if let Ok(message) = serde_json::from_str::<Message>(message.as_str()) {
                    if let Ok(content) = serde_json::from_str::<Content>(message.as_str()) {
                        let message: Message = content.into();
                        let message = message
                            .handle(|platform| {
                                tracing::debug!("platform connection: {platform:?}");
                                match platform {
                                    "web" => Ok(super::Platform::Web(tx.clone())),
                                    _ => Err(anyhow::anyhow!("unexpected platform")),
                                }
                            })
                            .await;

                        tracing::debug!("handle message: {message:?}");

                        match (crate::KAFKA_CLIENT.get(), message) {
                            (Some(kafka), Ok(Some(message))) => {
                                let _ = kafka.produce(message).await;
                            }
                            (_, Ok(None)) => continue,
                            _ => {
                                // TODO: handle error: close connection and send error message to client
                                let _ = tx.send(Event::Close);
                                return;
                            }
                        }
                    };
                }
                axum::extract::ws::Message::Close(_close) => {
                    let _ = tx.send(Event::Close);
                    break;
                }
                _ => continue,
            }
        }
    });
    sink
}
