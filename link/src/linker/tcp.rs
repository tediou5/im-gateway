use super::MessageCodec;

pub(crate) type Sender = tokio::sync::mpsc::UnboundedSender<Event>;

#[derive(Debug, Clone)]
pub(crate) enum Event {
    Write(std::sync::Arc<crate::linker::Message>),
    WriteBatch(std::sync::Arc<Vec<u8>>),
    Close,
}

impl Event {
    fn to_vec(&self) -> Option<Vec<u8>> {
        match self {
            Event::Close => None,
            Event::WriteBatch(messages) => {
                // FIXME:
                // let len = messages.len() as u64;
                Some(messages.to_vec())
            }
            Event::Write(message) => {
                use tokio_util::codec::Encoder as _;

                let mut codec = crate::linker::MessageCodec {};
                let mut dst = bytes::BytesMut::new();
                let _ = codec.encode(message.as_ref(), &mut dst);
                Some(dst.to_vec())
            }
        }
    }
}

pub(crate) async fn process(stream: tokio::net::TcpStream) {
    // Use an unbounded channel to handle buffering and flushing of messages
    // to the event source...
    // let (tx, rx) = tokio::sync::mpsc::channel::<TcpEvent>(10240);
    // let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
    let mut write = handle(stream, tx);

    use futures::SinkExt as _;
    use tokio_stream::StreamExt as _;

    'tcp: while let Some(event) = rx.next().await {
        if let Err(_e) = match event.to_vec() {
            Some(messages) => write.feed(messages).await,
            None => break 'tcp,
        } {
            // TODO: handle error
            break 'tcp;
        };

        let mut len = 1;
        let sleep = tokio::time::sleep(std::time::Duration::from_millis(10));
        tokio::pin!(sleep);

        loop {
            if let Err(_e) = tokio::select! {
                _ = &mut sleep => {
                    crate::axum_handler::LINK_SEND_COUNT
                        .fetch_add(len, std::sync::atomic::Ordering::Relaxed);
                    write.flush().await
                },
                Some(event) = rx.next() => match event.to_vec() {
                    Some(messages) => {
                        len += 1;
                        write.feed(messages).await
                    }
                    None => break 'tcp,
                },
            } {
                // TODO: handle error
                break 'tcp;
            };
        }
    }

    let _ = write.close().await;
}

fn handle(
    stream: tokio::net::TcpStream,
    tx: Sender,
) -> futures::stream::SplitSink<
    tokio_util::codec::Framed<tokio::net::TcpStream, MessageCodec>,
    Vec<u8>,
> {
    use futures::StreamExt as _;
    use tokio_util::codec::Decoder as _;

    let codec = MessageCodec;
    let (sink, mut input) = codec.framed(stream).split();

    tokio::task::spawn(async move {
        while let Some(message) = input.next().await {
            if let Ok(message) = message {
                tracing::debug!("received message: {message:?}");
                let message = message
                    .handle(|platform| {
                        tracing::debug!("platform connection: {platform:?}");
                        match platform {
                            "app" => Ok(super::Platform::App(tx.clone())),
                            "pc" => Ok(super::Platform::Pc(tx.clone())),
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
                        tracing::error!("no kafka or redis");
                        let _ = tx.send(Event::Close);
                        return;
                    }
                }
            }
        }
    });
    sink
}
