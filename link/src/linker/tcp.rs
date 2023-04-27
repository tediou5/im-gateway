use super::{Event, Message, MessageCodec};

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
    while let Some(text) = rx.next().await {
        let messages = match text {
            Event::Close => break,
            Event::WriteBatch(messages) => messages,
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

                let message = message.handle(&tx).await;

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
            }
        }
    });
    sink
}
