use super::MessageCodec;

pub(crate) type Sender = tokio::sync::mpsc::UnboundedSender<Event>;

#[derive(Debug, Clone)]
pub(crate) enum Event {
    WriteBatch(std::sync::Arc<Vec<u8>>),
    Close,
}

impl Event {
    fn to_vec(&self) -> Option<Vec<u8>> {
        match self {
            Event::Close => None,
            Event::WriteBatch(messages) => Some(messages.to_vec()),
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
    // let mut write = framed_handle(stream, tx);

    // use futures::SinkExt as _;
    use tokio_stream::StreamExt as _;

    while let Some(event) = rx.next().await &&
    let Some(messages) = event.to_vec() {
        if (write.writable().await).is_err() {
            break;
        };
        match write.try_write(messages.as_slice()) {
            Ok(_) => {
                crate::axum_handler::LINK_SEND_COUNT
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

    // &&
    // let Ok(_) = write.feed(messages).await &&
    // let Ok(_) = write.flush().await {
    //     tracing::trace!("tcp send ok");
    //     crate::axum_handler::LINK_SEND_COUNT
    //         .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // }

    // let _ = write.close().await;
}

/// handle spawn a new thread, read the request through the tcpStream, then send response to channel tx
fn handle(stream: tokio::net::TcpStream, tx: Sender) -> tokio::net::tcp::OwnedWriteHalf {
    let (read, write) = stream.into_split();
    tokio::task::spawn(async move {
        let mut is_auth = false;
        let mut req = [0; 4096];
        loop {
            let readable = read.readable().await; // Wait for the socket to be readable
            if readable.is_ok() {
                // Try to read data, this may still fail with `WouldBlock`
                // if the readiness event is a false positive.
                match read.try_read(&mut req) {
                    Ok(n) => {
                        if n == 0 {
                            let _ = tx.send(Event::Close);
                            break;
                        }
                        // req.truncate(n);
                        let _ = _handle((req[0..n]).to_vec(), &tx, &mut is_auth).await;
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // req.clear();
                        // req.resize(1024, 0);
                        // req.fill(0);
                        continue;
                    }
                    Err(_e) => {
                        let _ = tx.send(Event::Close);
                        break;
                    }
                }
            };
        }
    });
    write
}

#[allow(dead_code)]
fn framed_handle(
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
        let mut is_auth = false;
        while let Some(message) = input.next().await {
            tracing::trace!("received message: {message:?}");
            if let Ok(message) = message {
                _handle(message, &tx, &mut is_auth).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    });
    sink
}

async fn _handle(message_bytes: Vec<u8>, tx: &Sender, is_auth: &mut bool) -> anyhow::Result<()> {
    tracing::trace!("received message: {message_bytes:?}, is auth: {is_auth}");
    if let false = is_auth &&
    let Ok(super::message::Flow::Next(message)) =
        TryInto::<crate::linker::Message>::try_into(message_bytes.as_slice())?
            .content
            .handle(|platform| {
                tracing::debug!("platform connection: {platform:?}");
                match platform {
                    "app" => Ok(super::Platform::App(tx.clone())),
                    "pc" => Ok(super::Platform::Pc(tx.clone())),
                    _ => Err(anyhow::anyhow!("unexpected platform")),
                }
            })
            .await
    {
        tx.send(Event::WriteBatch((&message).into()))?;
        *is_auth = true
    } else {
        let kafka = crate::KAFKA_CLIENT
            .get()
            .ok_or(anyhow::anyhow!("kafka is not available"))?;
        kafka.produce(crate::kafka::VecValue(message_bytes)).await?;
    }

    Ok(())
}
