pub(crate) type Sender = local_sync::mpsc::unbounded::Tx<Event>;
// pub(crate) type Sender = tokio::sync::mpsc::UnboundedSender<Event>;

#[derive(Debug, Clone)]
pub(crate) enum Event {
    WriteBatch(std::rc::Rc<Vec<u8>>),
    // WriteBatch(std::sync::Arc<Vec<u8>>),
    Close,
}

// auth tcp then handle it.
pub(crate) async fn auth(mut stream: tokio_uring::net::TcpStream) -> anyhow::Result<()> {
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
            tracing::info!("platform connection: {platform:?} with baseinfo:\n{message:?}");
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

pub(crate) fn process(stream: tokio_uring::net::TcpStream, pin: std::rc::Rc<String>) -> Sender {
    tracing::info!(
        "[{pin}] ready to process tcp message: {:?}",
        stream.peer_addr()
    );
    // Use an unbounded channel to handle buffering and flushing of messages
    // to the event source...
    let (tx, rx) = local_sync::mpsc::unbounded::channel::<Event>();
    let mut rx = local_sync::stream_wrappers::unbounded::ReceiverStream::new(rx);
    // let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    // let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

    let write = handle(stream, tx.clone(), pin.clone());

    tokio_uring::spawn(async move {
        // tokio::task::spawn_local(async move {
        use tokio_stream::StreamExt as _;

        while let Some(event) = rx.next().await {
            let message = match event {
                Event::WriteBatch(message) => message,
                Event::Close => break,
            };

            tracing::info!("[{pin}] tcp waiting for write message");

            if stream.write_all(&message).await.is_err() {
                break;
            };

            tracing::info!("[{pin}] tcp write message");
            crate::axum_handler::LINK_SEND_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        tracing::info!("[{pin}] tcp closed");
        if let Some(redis) = crate::REDIS_CLIENT.get() {
            if let Err(e) = redis.del_heartbeat(pin.to_string()).await {
                tracing::error!("del [{pin}] heartbeat error: {}", e)
            };
        }

        let _ = write.shutdown(std::net::Shutdown::Both);

        crate::axum_handler::LINK_COUNT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    });

    tx
}

/// handle spawn a new thread, read the request through the tcpStream, then send response to channel tx
fn handle(
    stream: tokio_uring::net::TcpStream,
    tx: Sender,
    pin: std::rc::Rc<String>,
) -> std::rc::Rc<tokio_uring::net::TcpStream> {
    // tracing::trace!("ready to handle tcp request: {:?}", stream.peer_addr());
    let read = std::rc::Rc::new(stream);
    let write = read.clone();
    // let (read, write) = stream.into_split();
    // let (read, write) = stream.into_split();
    tokio_uring::spawn(async move {
        let mut buf = [0; 2048];
        loop {
            let pin = pin.clone();
            // Wait for the socket to be readable
            tracing::trace!("tcp waiting for read request");

            let (result, nbuf) = read.read(buf).await;
            let num = result?;
            if num == 0 {
                break;
            }

            tokio_uring::spawn(async move {
                if let Some(redis) = crate::REDIS_CLIENT.get() {
                    if let Err(e) = redis.heartbeat(pin.to_string()).await {
                        tracing::error!("update [{pin}] heartbeat error: {}", e)
                    };
                }
            });

            tracing::info!("tcp read message");
            let kafka = match crate::KAFKA_CLIENT
                .get()
                .ok_or(anyhow::anyhow!("kafka is not available"))
            {
                Ok(kafka) => kafka,
                Err(e) => {
                    tracing::error!("Tcp Error: System Error: {e}");
                    break;
                }
            };

            tracing::info!("Tcp produce message");
            if let Err(e) = kafka
                .produce(crate::kafka::VecValue(nbuf[0..num].to_vec()))
                .await
            {
                tracing::error!("Tcp Error: Kafka Error: {e}");
                break;
            };
        }
        tracing::info!("Tcp: [{pin}] read error.");
        let _ = read.shutdown(std::net::Shutdown::Both);
        let _ = tx.send(Event::Close);
        Ok::<(), anyhow::Error>(())
    });

    write
}
