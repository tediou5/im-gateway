pub(crate) use message::{Content, Message, MessageCodec};

mod auth;
mod message;

#[derive(Debug)]
pub(crate) enum TcpEvent {
    // Write(std::sync::Arc<Message>),
    WriteBatch(std::sync::Arc<Vec<Message>>),
    Close,
}

pub(crate) async fn process(stream: tokio::net::TcpStream) {
    // Use an unbounded channel to handle buffering and flushing of messages
    // to the event source...
    // let (tx, rx) = tokio::sync::mpsc::channel::<TcpEvent>(10240);
    // let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<TcpEvent>();
    let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
    let mut write = handle(stream, tx);

    use futures::SinkExt as _;
    use tokio_stream::StreamExt as _;
    while let Some(text) = rx.next().await {
        let messages = match text {
            TcpEvent::Close => break,
            TcpEvent::WriteBatch(messages) => messages,
        };
        let len = messages.len() as u64;

        match write.send(messages).await {
            Ok(()) => {
                crate::axum_handler::TCP_SEND_COUNT
                    .fetch_add(len, std::sync::atomic::Ordering::Relaxed);
            }
            Err(e) => {
                tracing::error!("tcp send error: {e:?}");
                break;
            }
        };
    }
    let _ = write.close().await;
}

fn handle(
    stream: tokio::net::TcpStream,
    tx: crate::Sender,
) -> futures::stream::SplitSink<
    tokio_util::codec::Framed<tokio::net::TcpStream, MessageCodec>,
    std::sync::Arc<Vec<Message>>,
> {
    use futures::StreamExt as _;
    use tokio_util::codec::Decoder as _;

    let codec = MessageCodec;
    let (sink, mut input) = codec.framed(stream).split();
    // let (mut sink, mut input) = codec.framed(stream);
    // let (read, write) = stream.split();
    tokio::task::spawn(async move {
        while let Some(message) = input.next().await {
            if let Ok(message) = message {
                tracing::debug!("received message: {message:?}");

                let Message { content, .. } = &message;

                let message: Option<Message> = match content {
                    Content::Heart { .. } => {
                        // TODO:

                        None
                    }
                    Content::Connect {
                        app_id,
                        token,
                        platform,
                    } => {
                        // TODO:
                        if let Some(auth_url) = crate::AUTH_URL.get() &&
                        let Some(http) = crate::HTTP_CLIENT.get() {
                            let auth::Response {
                                code,
                                data,
                                message: _auth_message,
                            } = http.post(auth_url)
                            .json(&serde_json::json!({
                                "appId": app_id,
                                "token": token,
                                "platform": platform,
                            }))
                                .send()
                                .await
                                .inspect_err(|_e|{let _ = tx.send(TcpEvent::Close);})
                                .unwrap()
                            .json::<auth::Response>().await.inspect_err(|_e|{let _ = tx.send(TcpEvent::Close);}).unwrap();

                            let mut id = uuid::Uuid::new_v4().to_string();
                            id.remove_matches("-");
                            let id = id.as_bytes();
                            let mut id_bytes = [0u8;32];
                            id_bytes.copy_from_slice(&id[0..32]);

                            let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64;

                            match code.as_str() {
                                "0" => {
                                    if let Some(auth::Data {
                                        base_info,
                                        chats,
                                    }) = data {
                                        // auth pass, join into event loop.
                                        if let Some(redis_client) = crate::REDIS_CLIENT.get() {
                                            let _ = redis_client.regist(&chats).await;
                                        }

                                        if let Some(event_loop) = crate::EVENT_LOOP.get() {
                                            let _ = event_loop.send(crate::Event::Regist(base_info.pin.clone(), chats, tx.clone()));
                                        }

                                        let message_data = std::collections::HashMap::from([
                                            ("chatId".to_string(), serde_json::json!("")),
                                            ("msgFormat".to_string(), serde_json::json!("TEXT")),
                                            ("msgId".to_string(), serde_json::json!(String::from_utf8(id_bytes.to_vec()).unwrap_or_default())),
                                            ("noticeType".to_string(), serde_json::json!("USER_BASE_INFO")),
                                            ("body".to_string(), serde_json::json!(serde_json::to_string(&base_info).unwrap_or_default())),
                                            ("chatMsgType".to_string(), serde_json::json!("Notice")),
                                            ("fromId".to_string(), serde_json::json!(&base_info.pin)),
                                            ("appId".to_string(), serde_json::json!(app_id)),
                                            ("chatType".to_string(), serde_json::json!("Private")),
                                            ("timestamp".to_string(), serde_json::json!(timestamp)),
                                        ]);
                                        let auth_content = Content::Message { _ext: message_data };

                                        let auth_message = Message {
                                            length: 0,
                                            msg_id: id_bytes,
                                            timestamp,
                                            content: auth_content,
                                        };
                                        let _ = tx.send(TcpEvent::WriteBatch(std::sync::Arc::new(vec![auth_message])));
                                    };
                                },
                                _ => {
                                    // Authorization Error, close connection
                                    // FIXME: send error message to client and close connection
                                    let _ = tx.send(TcpEvent::Close);
                                    break;
                                }
                            }
                        }

                        None
                    }
                    Content::Message { _ext } => Some(message),
                    Content::Response { _ext } => Some(message),
                };

                // push into Kafka
                if let Some(kafka) = crate::KAFKA_CLIENT.get() &&
                let Some(message) = message {
                    let _ = kafka.produce(message).await;
                }
            }
        }
    });
    sink
}

// async fn _handle(request: &[u8], tx: &crate::Sender) -> anyhow::Result<()> {
//     let str = std::str::from_utf8(request)?;

// match serde_json::from_str(str)? {
//     Command::Registe { pin, chats } => {
//         crate::axum_handler::REGISTER_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

//         if let Some(redis_client) = crate::REDIS_CLIENT.get() {
//             // FIXME:
//             redis_client.regist(&chats).await?;
//         }

// if let Some(event_loop) = crate::EVENT_LOOP.get() {
//     event_loop.send(crate::Event::Regist(pin, chats, tx.clone()))?;
// }
//     }
//     Command::Send { pin, chat, content } => {
//         crate::axum_handler::SEND_REQUEST_COUNT
//             .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

//         let message = crate::event_loop::SendRequest {
//             pin,
//             chat: chat.clone(),
//             content,
//         };

//         if let Some(producer) = crate::KAFKA_CLIENT.get() &&
//         let Some(redis_client) = crate::REDIS_CLIENT.get() {
//             let routes = redis_client.get_router(chat.as_str()).await?;
//             let mut producer = producer.lock().await;
//             for route in routes {
//                 producer.produce(route, message.clone()).await?;
//             }
//         }
//     }
// }
//     Ok(())
// }
