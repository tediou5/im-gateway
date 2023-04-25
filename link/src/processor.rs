#[derive(serde_derive::Deserialize, serde_derive::Serialize)]
#[serde(untagged)]
pub(crate) enum Command {
    Registe {
        pin: String,
        chats: Vec<String>,
    },
    Send {
        pin: String,
        chat: String,
        content: String,
    },
}

#[derive(Debug)]
pub(crate) enum TcpEvent {
    Write(std::sync::Arc<String>),
    WriteBatch(std::sync::Arc<String>, u64),
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

    use tokio_stream::StreamExt as _;

    while let Some(text) = rx.next().await {
        let (text, len) = match text {
            TcpEvent::Write(text) => (text, 1),
            TcpEvent::Close => break,
            TcpEvent::WriteBatch(text, len) => (text, len),
        };

        if (write.writable().await).is_err() {
            break;
        };
        match write.try_write(text.as_bytes()) {
            Ok(_) => {
                crate::axum_handler::TCP_SEND_COUNT
                    .fetch_add(len, std::sync::atomic::Ordering::Relaxed);
                continue;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(_e) => {
                break;
            }
        }
    }

    use tokio::io::AsyncWriteExt as _;
    let _ = write.shutdown().await;
}

/// handle spawn a new thread, read the request through the tcpStream, then send response to channel tx
fn handle(stream: tokio::net::TcpStream, tx: crate::Sender) -> tokio::net::tcp::OwnedWriteHalf {
    let (read, write) = stream.into_split();
    tokio::task::spawn(async move {
        let mut req = vec![0;4096];
        // let mut req = [0; 4096];
        loop {
            let readable = read.readable().await; // Wait for the socket to be readable
            if readable.is_ok() {
                // Try to read data, this may still fail with `WouldBlock`
                // if the readiness event is a false positive.
                match read.try_read(&mut req) {
                    Ok(n) => {
                        if n == 0 {
                            let _ = tx.send(TcpEvent::Close);
                            break;
                        }
                        // req.truncate(n);
                        let _ = _handle(&req[0..n], &tx).await;
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    Err(_e) => {
                        let _ = tx.send(TcpEvent::Close);
                        break;
                    }
                }
            };
        }
    });
    write
}

async fn _handle(request: &[u8], tx: &crate::Sender) -> anyhow::Result<()> {
    let str = std::str::from_utf8(request)?;


    // TODO: Decode Into Vec<MessageProtocol>
    // TODO: produce into Kafka()

    match serde_json::from_str(str)? {
        Command::Registe { pin, chats } => {
            crate::axum_handler::REGISTER_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            if let Some(redis_client) = crate::REDIS_CLIENT.get() {
                // FIXME: 
                redis_client.regist(&chats).await?;
            }

            if let Some(event_loop) = crate::EVENT_LOOP.get() {
                event_loop.send(crate::Event::Regist(pin, chats, tx.clone()))?;
            }
        }
        Command::Send { pin, chat, content } => {
            crate::axum_handler::SEND_REQUEST_COUNT
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let message = crate::event_loop::SendRequest {
                pin,
                chat: chat.clone(),
                content,
            };

            if let Some(producer) = crate::KAFKA_CLIENT.get() &&
            let Some(redis_client) = crate::REDIS_CLIENT.get() {
                let routes = redis_client.get_router(chat.as_str()).await?;
                let mut producer = producer.lock().await;
                for route in routes {
                    producer.produce(route, message.clone()).await?;
                }
            }
        }
    }
    Ok(())
}
