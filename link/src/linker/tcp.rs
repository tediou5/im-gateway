pub(crate) type Sender = local_sync::mpsc::unbounded::Tx<Event>;

#[derive(Debug, Clone)]
pub(crate) enum Event {
    WriteBatch(u64, std::rc::Rc<Vec<u8>>),
    Close,
}

#[derive(Debug, Clone)]
pub(crate) enum SenderEvent {
    WriteBatch(std::rc::Rc<Vec<u8>>),
    Close,
}

// auth tcp then handle it.
pub(crate) async fn auth(mut stream: tokio::net::TcpStream) -> anyhow::Result<()> {
    crate::axum_handler::LINK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    use tokio::io::AsyncReadExt;
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

    let mut auth = TryInto::<crate::linker::protocol::Controls>::try_into(auth)?;
    if let crate::linker::protocol::control::Event::Package(_len, _trace_id, auth) =
        auth.0.pop().unwrap().event
    {
        // let auth = auth.try_into()?;
        TryInto::<crate::linker::Content>::try_into(auth)?
            .handle_auth(async move |platform, message| {
                tracing::info!("platform connection: {platform:?}");
                match platform.as_str() {
                    "app" => Ok(super::Login {
                        platform: super::Platform::App(stream),
                        auth_message: message,
                    }),
                    "pc" => Ok(super::Login {
                        platform: super::Platform::Pc(stream),
                        auth_message: message,
                    }),
                    _ => Err(anyhow::anyhow!("unexpected platform")),
                }
            })
            .await
    } else {
        Err(anyhow::anyhow!("must authenticate first"))
    }
}

pub(crate) fn process(
    stream: tokio::net::TcpStream,
    pin: std::rc::Rc<String>,
    auth_message: super::Content,
) -> Sender {
    tracing::info!(
        "[{pin}] ready to process tcp message: {:?}",
        stream.peer_addr()
    );
    // Use an unbounded channel to handle buffering and flushing of messages
    // to the event source...
    let (tx, rx) = local_sync::mpsc::unbounded::channel::<Event>();
    let mut rx = local_sync::stream_wrappers::unbounded::ReceiverStream::new(rx);

    let (tcp_collect, tcp_sender) = local_sync::mpsc::unbounded::channel::<SenderEvent>();
    let mut tcp_sender = local_sync::stream_wrappers::unbounded::ReceiverStream::new(tcp_sender);

    let (mut write, ack_window) = handle(stream, tx.clone(), pin.clone());

    // send auth message first, auth message alse need ack.
    // FIXME:
    let auth_message: Vec<u8> = auth_message.to_vec().unwrap();
    let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
    let (trace_id, auth_message) =
        crate::linker::Content::pack_message(&auth_message, &mut id_worker).unwrap();
    let auth_message: std::rc::Rc<Vec<u8>> = auth_message.into();
    tracing::info!("auth message trace id: {}", trace_id);
    let _ = tx.send(Event::WriteBatch(trace_id, auth_message));

    let retry_config = &crate::TCP_CONFIG.get().unwrap().retry;

    let tcp_collect_c = tcp_collect.clone();
    let ack_window_c = ack_window.clone();
    tokio::task::spawn_local(async move {
        use tokio_stream::StreamExt as _;

        while let Some(event) = rx.next().await {
            let (trace_id, message) = match event {
                Event::WriteBatch(trace_id, message) => (trace_id, message),
                Event::Close => {
                    let _ = tcp_collect_c.send(SenderEvent::Close);
                    return;
                }
            };

            if let Some(ref ack_window) = ack_window_c {
                // FIXME: error when compressed.
                // FIXME: use trace id
                if let Err(e) = ack_window.acquire(trace_id, message.clone()).await {
                    tracing::error!("acquire ack windows failed: {e}");
                    return;
                };
            }

            let _ = tcp_collect_c.send(SenderEvent::WriteBatch(message));
        }
    });

    if let Some(retry_config) = retry_config &&
    let Some(ack_windows) = ack_window {
        let max_times = retry_config.max_times;
        let retry_timeout = retry_config.timeout;

        let pin_c = pin.to_string();
        tokio::task::spawn_local(async move {
            'retry: loop {
                let retry = ack_windows.try_again().await;
                let timeout = match super::ack_window::AckWindow::<u64>::get_retry_timeout(retry.times, retry_timeout, max_times)
                {
                    Ok(timeout) => timeout,
                    Err(_) => break,
                };
                for message in retry.messages.iter() {
                    if let Err(e) = tcp_collect.send(SenderEvent::WriteBatch(message.clone())) {
                        tracing::error!("tcp error: send retry message error: {e:?}");
                        break 'retry;
                    };
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(timeout)).await;
            }
            tracing::error!("[{pin_c}]tcp retry error, close connection");
            let _ = tcp_collect.send(SenderEvent::Close);
        });
    }

    tokio::task::spawn_local(async move {
        use tokio_stream::StreamExt as _;

        while let Some(event) = tcp_sender.next().await {
            let message = match event {
                SenderEvent::WriteBatch(message) => message,
                SenderEvent::Close => break,
            };

            tracing::trace!("[{pin}] tcp waiting for write message");

            if (write.writable().await).is_err() {
                break;
            };
            tracing::trace!("[{pin}] tcp try to write");
            match write.try_write(message.as_slice()) {
                Ok(_) => {
                    tracing::debug!("[{pin}] tcp write message");
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
    Option<super::ack_window::AckWindow<u64>>,
) {
    let ack_window = if let Some(ref retry) = crate::TCP_CONFIG.get().unwrap().retry {
        tracing::info!("[{pin}]tcp retry: set ack window");
        Some(super::ack_window::AckWindow::new(retry.window_size))
    } else {
        None
    };

    tracing::trace!("ready to handle tcp request: {:?}", stream.peer_addr());
    let (read, write) = stream.into_split();
    let ack_window_c = ack_window.clone();
    tokio::task::spawn_local(async move {
        let mut req = [0; 4096];
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

                    if let Err(e) = crate::linker::protocol::control::Control::process(
                        pin.as_str(),
                        &req[..n],
                        &ack_window_c,
                    )
                    .await
                    {
                        tracing::error!("tcp error: control protocol process error: {e}");
                        break;
                    };
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

    (write, ack_window)
}

#[cfg(test)]
mod test {
    #[test]
    fn get_retry_timeout() {
        let timeout =
            crate::linker::ack_window::AckWindow::<u64>::get_retry_timeout(0, 3, 10).unwrap();
        assert_eq!(timeout, 3);
        let timeout =
            crate::linker::ack_window::AckWindow::<u64>::get_retry_timeout(1, 3, 10).unwrap();
        assert_eq!(timeout, 6);
        let timeout =
            crate::linker::ack_window::AckWindow::<u64>::get_retry_timeout(4, 3, 10).unwrap();
        assert_eq!(timeout, 12);
        let timeout =
            crate::linker::ack_window::AckWindow::<u64>::get_retry_timeout(8, 3, 10).unwrap();
        assert_eq!(timeout, 12);
        let timeout = crate::linker::ack_window::AckWindow::<u64>::get_retry_timeout(10, 3, 10);
        assert!(timeout.is_err());
        let timeout = crate::linker::ack_window::AckWindow::<u64>::get_retry_timeout(13, 3, 10);
        assert!(timeout.is_err());
    }
}
