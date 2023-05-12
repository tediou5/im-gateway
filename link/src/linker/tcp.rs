pub(crate) type Sender = tokio::sync::mpsc::UnboundedSender<Event>;

#[derive(Debug, Clone)]
pub(crate) enum Event {
    WriteBatch(std::rc::Rc<Vec<u8>>),
    // WriteBatch(std::sync::Arc<Vec<u8>>),
    Close,
}

// auth tcp then handle it.
pub(crate) async fn auth(mut stream: tokio::net::TcpStream) -> anyhow::Result<()> {
    crate::axum_handler::LINK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut req = [0; 4096];

    let auth = match stream.read(&mut req).await {
        Ok(n) => {
            if n == 0 {
                return Err(anyhow::anyhow!("tcp socket is closed"));
            }
            &req[0..n]
        }
        Err(e) => {
            return Err(anyhow::anyhow!("failed to read from socket; err = {e}"));
        }
    };

    tracing::trace!("recv auth request from tcp: {auth:?}");

    TryInto::<crate::linker::Message>::try_into(auth)?
        .content
        .handle_auth(async move |platform, message| {
            tracing::trace!("platform connection: {platform:?} with baseinfo:\n{message:?}");
            let message: Vec<u8> = (&message).into();
            if let Err(e) = stream.write_all(message.as_slice()).await {
                return Err(anyhow::anyhow!("failed to write to socket; err = {e}"));
            }
            match platform.as_str() {
                "app" => Ok(super::Platform::App(stream)),
                "pc" => Ok(super::Platform::Pc(stream)),
                _ => Err(anyhow::anyhow!("unexpected platform")),
            }
        })
        .await
}

pub(crate) fn process(stream: tokio::net::TcpStream) -> Sender {
    tracing::trace!("ready to process tcp message: {:?}", stream.peer_addr());
    // Use an unbounded channel to handle buffering and flushing of messages
    // to the event source...
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

    let mut write = handle(stream, tx.clone());

    tokio::task::spawn_local(async move {
        use tokio_stream::StreamExt as _;

        while let Some(event) = rx.next().await {
            let message = match event {
                Event::WriteBatch(message) => message,
                Event::Close => break,
            };

            tracing::trace!("tcp waiting for write message");

            if (write.writable().await).is_err() {
                break;
            };
            tracing::trace!("tcp try to write");
            match write.try_write(message.as_slice()) {
                Ok(_) => {
                    tracing::trace!("tcp write message");
                    crate::axum_handler::LINK_SEND_COUNT
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(_e) => {
                    break;
                }
            }
        }
        tracing::trace!("tcp closed");
        use tokio::io::AsyncWriteExt as _;
        let _ = write.shutdown().await;

        crate::axum_handler::LINK_COUNT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    });

    tx
}

/// handle spawn a new thread, read the request through the tcpStream, then send response to channel tx
fn handle(stream: tokio::net::TcpStream, tx: Sender) -> tokio::net::tcp::OwnedWriteHalf {
    tracing::trace!("ready to handle tcp request: {:?}", stream.peer_addr());
    let (read, write) = stream.into_split();
    tokio::task::spawn_local(async move {
        let mut req = [0; 4096];
        loop {
            // Wait for the socket to be readable
            tracing::trace!("tcp waiting for read request");
            if (read.readable()).await.is_err() {
                break;
            }
            tracing::trace!("tcp try to read");
            // Try to read data, this may still fail with `WouldBlock`
            // if the readiness event is a false positive.
            match read.try_read(&mut req) {
                Ok(n) => {
                    if n == 0 {
                        let _ = tx.send(Event::Close);
                        break;
                    }
                    tracing::trace!("tcp read message");
                    let kafka = crate::KAFKA_CLIENT
                        .get()
                        .ok_or(anyhow::anyhow!("kafka is not available"))?;
                    kafka
                        .produce(crate::kafka::VecValue(req[0..n].to_vec()))
                        .await?;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(_e) => {
                    let _ = tx.send(Event::Close);
                    break;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    write
}
