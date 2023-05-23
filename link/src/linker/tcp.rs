pub(crate) type Sender = local_sync::mpsc::unbounded::Tx<Event>;

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

    let (tcp_collect, tcp_sender) = local_sync::mpsc::unbounded::channel::<Event>();
    let mut tcp_sender = local_sync::stream_wrappers::unbounded::ReceiverStream::new(tcp_sender);

    let (mut write, ack_windows) = handle(stream, tx.clone(), pin.clone());

    let tcp_collect_c = tcp_collect.clone();
    let ack_windows_c = ack_windows.clone();
    tokio::task::spawn_local(async move {
        use tokio_stream::StreamExt as _;

        while let Some(event) = rx.next().await {
            let message = match event {
                Event::WriteBatch(message) => message,
                Event::Close => {
                    let _ = tcp_collect_c.send(event.clone());
                    return;
                }
            };

            // FIXME: error when compressed.
            if let Err(e) = ack_windows_c
                .acquire(message[4..36].to_vec(), message.clone())
                .await
            {
                tracing::error!("acquire ack windows failed: {e}");
                return;
            };
            let _ = tcp_collect_c.send(Event::WriteBatch(message));
        }
    });

    tokio::task::spawn_local(async move {
        let retry_timeout = crate::TCP_CONFIG.get().unwrap().retry_timeout;
        'Loop: loop {
            let retry = ack_windows.try_again().await;
            let timeout = match get_retry_timeout(retry.times.into(), retry_timeout) {
                Ok(timeout) => timeout,
                Err(_) => break,
            };
            tokio::time::sleep(tokio::time::Duration::from_millis(timeout)).await;
            for message in retry.messages.iter() {
                if let Err(e) = tcp_collect.send(Event::WriteBatch(message.clone())) {
                    tracing::error!("tcp error: send retry message error: {e:?}");
                    break 'Loop;
                };
            }
        }
        let _ = tcp_collect.send(Event::Close);
    });

    tokio::task::spawn_local(async move {
        use tokio_stream::StreamExt as _;

        while let Some(event) = tcp_sender.next().await {
            let message = match event {
                Event::WriteBatch(message) => message,
                Event::Close => break,
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
fn handle(
    stream: tokio::net::TcpStream,
    tx: Sender,
    pin: std::rc::Rc<String>,
) -> (
    tokio::net::tcp::OwnedWriteHalf,
    super::ack_window::AckWindow<Vec<u8>>,
) {
    let ack_windows =
        super::ack_window::AckWindow::new(crate::TCP_CONFIG.get().unwrap().window_size);

    tracing::trace!("ready to handle tcp request: {:?}", stream.peer_addr());
    let (read, write) = stream.into_split();
    let ack_windows_c = ack_windows.clone();
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

                    if control.bad_network.is_some() {
                        // TODO: handle for bad network quality
                    };

                    if control.heartbeat.is_some() {
                        tokio::task::spawn_local(async move {
                            if let Some(redis) = crate::REDIS_CLIENT.get() {
                                if let Err(e) = redis.heartbeat(pin.to_string()).await {
                                    tracing::error!("update [{pin}] heartbeat error: {}", e)
                                };
                            }
                        });
                    };

                    match control.event {
                        crate::linker::control_protocol::Event::Ack(acks) => {
                            for trace_id in acks.iter() {
                                let _ = ack_windows_c.ack(trace_id.to_vec());
                            }
                        }
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

fn get_retry_timeout(retry_times: u64, timeout: u64) -> anyhow::Result<u64> {
    let times: u64 = match retry_times {
        less_than_three if less_than_three < 3 => less_than_three + 1,
        other if other < 10 => 4,
        _ => {
            tracing::error!("retry to many times, close connection");
            return Err(anyhow::anyhow!("retry to many times, close connection"));
        }
    };
    Ok(times * timeout)
}

#[cfg(test)]
mod test {
    #[test]
    fn get_retry_timeout() {
        let timeout = super::get_retry_timeout(0, 3).unwrap();
        assert_eq!(timeout, 3);
        let timeout = super::get_retry_timeout(1, 3).unwrap();
        assert_eq!(timeout, 6);
        let timeout = super::get_retry_timeout(4, 3).unwrap();
        assert_eq!(timeout, 12);
        let timeout = super::get_retry_timeout(8, 3).unwrap();
        assert_eq!(timeout, 12);
        let timeout = super::get_retry_timeout(10, 3);
        assert!(timeout.is_err());
        let timeout = super::get_retry_timeout(13, 3);
        assert!(timeout.is_err());
    }
}
