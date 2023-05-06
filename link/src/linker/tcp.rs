use super::MessageCodec;

pub(crate) type Sender = tokio::sync::mpsc::UnboundedSender<Event>;

#[derive(Debug, Clone)]
pub(crate) enum Event {
    Write(std::sync::Arc<crate::linker::Message>),
    WriteBatch(std::sync::Arc<Vec<u8>>),
    Close,
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
    while let Some(event) = rx.next().await {
        tracing::error!("++++++++++++++++++\ntcp wait for send\n++++++++++++++++++");
        let res = match event {
            Event::Close => break,
            Event::WriteBatch(messages) => {
                // FIXME:
                // let len = messages.len() as u64;
                write.send(messages).await.map(|_| 1)
            }
            Event::Write(message) => {
                use tokio_util::codec::Encoder as _;

                let mut codec = crate::linker::MessageCodec {};
                let mut dst = bytes::BytesMut::new();
                let _ = codec.encode(message.as_ref(), &mut dst);
                let message = dst.to_vec();

                write.send(message.into()).await.map(|_| 1)
            }
        };

        match res {
            Ok(len) => {
                tracing::error!("tcp send ok");
                crate::axum_handler::LINK_SEND_COUNT
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
    tx: Sender,
) -> futures::stream::SplitSink<
    tokio_util::codec::Framed<tokio::net::TcpStream, MessageCodec>,
    std::sync::Arc<Vec<u8>>,
> {
    use futures::StreamExt as _;
    use tokio_util::codec::Decoder as _;

    let codec = MessageCodec;
    let (sink, mut input) = codec.framed(stream).split();
    // let (mut sink, mut input) = codec.framed(stream);
    // let (read, write) = stream.split();
    tokio::task::spawn(async move {
        // let sender = super::Platform::Tcp(tx);
        while let Some(message) = input.next().await {
            if let Ok(message) = message {
                tracing::debug!("received message: {message:?}");
                let message = message.handle(|platform| {
                    tracing::error!("platform connection: {platform:?}");
                    match platform {
                        "app" => Ok(super::Platform::App(tx.clone())),
                        "pc" => Ok(super::Platform::Pc(tx.clone())),
                        _ => Err(anyhow::anyhow!("unexpected platform")),
                    }
                }).await;

                tracing::error!("handle message: {message:?}");

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
