pub(crate) type Sender = local_sync::mpsc::unbounded::Tx<Event>;
// pub(crate) type Sender = tokio::sync::mpsc::UnboundedSender<Event>;

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
    let mut req = [0; 2048];

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

pub(crate) fn process(stream: tokio::net::TcpStream, pin: std::rc::Rc<String>) -> Sender {
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

    let (mut write, ack_windows) = handle(stream, tx.clone(), pin.clone());

    let ack_windows_c = ack_windows.clone();
    tokio::task::spawn_local(async move {
        use tokio_stream::StreamExt as _;

        while let Some(event) = rx.next().await {
            let message = match event {
                Event::WriteBatch(message) => message,
                Event::Close => break,
            };

            // FIXME: error when compressed.
            // Read msg_id marker.
            let mut msg_id = [0u8; 32];
            msg_id.copy_from_slice(&message[4..36]);

            if let Err(_) = ack_windows_c.acquire(msg_id, message.clone()).await {
                break;
            };

            tracing::trace!("[{pin}] tcp waiting for write message");

            if (write.writable().await).is_err() {
                break;
            };
            tracing::trace!("[{pin}] tcp try to write");
            match write.try_write(message.as_slice()) {
                Ok(_) => {
                    tracing::info!("[{pin}] tcp write message");
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

        tracing::info!("[{pin}] tcp closed");
        if let Some(redis) = crate::REDIS_CLIENT.get() {
            if let Err(e) = redis.del_heartbeat(pin.to_string()).await {
                tracing::error!("del [{pin}] heartbeat error: {}", e)
            };
        }
        use tokio::io::AsyncWriteExt as _;
        let _ = write.shutdown().await;

        crate::axum_handler::LINK_COUNT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    });

    tx
}

/// handle spawn a new thread, read the request through the tcpStream, then send response to channel tx
fn handle<T: std::hash::Hash + std::cmp::Ord + std::cmp::PartialOrd>(
    stream: tokio::net::TcpStream,
    tx: Sender,
    pin: std::rc::Rc<String>,
) -> (
    tokio::net::tcp::OwnedWriteHalf,
    super::ack_window::AckWindow<T>,
) {
    let ack_windows = super::ack_window::AckWindow::new(*crate::TCP_WINDOW_SIZE.get().unwrap());

    tracing::trace!("ready to handle tcp request: {:?}", stream.peer_addr());
    let (read, write) = stream.into_split();

    tokio::task::spawn_local(async move {
        let mut req = [0; 2048];
        loop {
            let pin = pin.clone();
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

                    let control: super::control_protocol::Control = match (req.as_slice())
                        .try_into()
                    {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!("tcp error: try into control protocol error: {}", e);
                            break;
                        }
                    };

                    if let Some(_) = control.bad_network {
                        // TODO: handle for bad network quality
                    };

                    if let Some(_) = control.heartbeat {
                        tokio::task::spawn_local(async move {
                            if let Some(redis) = crate::REDIS_CLIENT.get() {
                                if let Err(e) = redis.heartbeat(pin.to_string()).await {
                                    tracing::error!("update [{pin}] heartbeat error: {}", e)
                                };
                            }
                        });
                    };

                    match control.event {
                        crate::linker::control_protocol::Event::Ack(acks) => todo!(),
                        crate::linker::control_protocol::Event::Package(pkg) => {
                            tracing::info!("tcp read message length: {n}");
                            let kafka = crate::KAFKA_CLIENT.get().unwrap();

                            tracing::info!("Tcp produce message");
                            if let Err(e) = kafka
                                .produce(crate::kafka::VecValue(pkg[0..n].to_vec()))
                                .await
                            {
                                tracing::error!("Tcp Error: Kafka Error: {e}");
                                break;
                            };
                        }
                        crate::linker::control_protocol::Event::WeakAck => todo!(),
                        crate::linker::control_protocol::Event::WeakPackage => todo!(),
                    }
                    // is link control protocol

                    // if n <= 100 {
                    //     tokio::task::spawn_local(async move {
                    //         if let Some(redis) = crate::REDIS_CLIENT.get() {
                    //             if let Err(e) = redis.heartbeat(pin.to_string()).await {
                    //                 tracing::error!("update [{pin}] heartbeat error: {}", e)
                    //             };
                    //         }
                    //     });
                    // }
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
        tracing::info!("Tcp: [{pin}] read error.");
        let _ = tx.send(Event::Close);
        Ok::<(), anyhow::Error>(())
    });

    (write, ack_windows)
}
