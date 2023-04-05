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

    use tokio_util::codec::Decoder as _;
    let mut dst = bytes::BytesMut::from(req.as_slice());
    let auth = crate::linker::protocol::ControlCodec
        .decode(&mut dst)
        .unwrap()
        .unwrap();

    if let crate::linker::protocol::control::Event::Package(.., auth) = auth.event {
        TryInto::<crate::linker::Content>::try_into(auth.as_slice())?
            .handle_auth(async move |platform, message| {
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

    let (close, read_close_rx) = tokio::sync::broadcast::channel::<()>(1);
    let mut retry_close_rx = close.subscribe();

    let (mut sink, ack_window) = handle(pin.clone(), stream, tx.clone(), read_close_rx);

    // send auth message first, auth message alse need ack.
    let auth_message: Vec<u8> = auth_message.to_vec().unwrap();
    let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
    let (trace_id, auth_message) =
        crate::linker::Content::pack_message(&auth_message, &mut id_worker).unwrap();
    let auth_message: std::rc::Rc<Vec<u8>> = auth_message.into();
    tracing::debug!("auth message trace id: {}", trace_id);
    let _ = tx.send(Event::WriteBatch(trace_id, auth_message));

    let retry_config = &crate::TCP_CONFIG.get().unwrap().retry;

    let pin_c = pin.to_string();
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

            // FIXME: error when compressed.
            if let Some(ref ack_window) = ack_window_c &&
            let Err(e) = ack_window.acquire(pin_c.as_str(), trace_id, &message).await {
                tracing::error!("acquire ack windows failed: {e}");
                let _ = tcp_collect_c.send(SenderEvent::Close);
                return;
            }

            let _ = tcp_collect_c.send(SenderEvent::WriteBatch(message));
        }
    });

    if let Some(crate::config::Retry { timeout, max_times, .. }) = retry_config &&
    let Some(ack_windows) = ack_window {
        let pin_c = pin.to_string();
        tokio::task::spawn_local(async move {
            'retry: loop {
                let retry = tokio::select! {
                    retry = ack_windows.try_again() => retry,
                    _ = retry_close_rx.recv() => break,
                };
    
                let retry_timeout = match super::ack_window::AckWindow::<u64>::get_retry_timeout(retry.times, *timeout, *max_times)
                {
                    Ok(timeout) => timeout,
                    Err(_) => break 'retry,
                };
                for message in retry.messages.iter() {
                    if let Err(e) = tcp_collect.send(SenderEvent::WriteBatch(message.clone())) {
                        tracing::error!("tcp error: send retry message error: {e:?}");
                        break 'retry;
                    };
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(retry_timeout)).await;
            }
            tracing::error!("[{pin_c}]tcp retry error, close connection");
            let _ = tcp_collect.send(SenderEvent::Close);
        });
    }

    tokio::task::spawn_local(async move {
        use futures::SinkExt as _;
        use tokio_stream::StreamExt as _;

        while let Some(event) = tcp_sender.next().await {
            match event {
                SenderEvent::WriteBatch(message) => {
                    if sink.send(message).await.is_err() {
                        break;
                    }
                }
                SenderEvent::Close => break,
            }
        }

        // close read task & retry task
        let _ = close.send(());
        tracing::error!("[{pin}] tcp closed");
        let _ = sink.close().await;

        crate::axum_handler::LINK_COUNT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    });

    tx
}

/// handle spawn a new task, read the request through the tcpStream, then send response to channel tx
fn handle(
    pin: std::rc::Rc<String>,
    stream: tokio::net::TcpStream,
    tx: Sender,
    mut read_close_rx: tokio::sync::broadcast::Receiver<()>,
) -> (
    futures::stream::SplitSink<
        tokio_util::codec::Framed<tokio::net::TcpStream, crate::linker::protocol::ControlCodec>,
        std::rc::Rc<Vec<u8>>,
    >,
    Option<super::ack_window::AckWindow<u64>>,
) {
    let ack_window = if let Some(ref retry) = crate::TCP_CONFIG.get().unwrap().retry {
        Some(super::ack_window::AckWindow::new(retry.window_size))
    } else {
        None
    };

    tracing::trace!("ready to handle tcp request: {:?}", stream.peer_addr());
    use futures::StreamExt as _;
    use tokio_util::codec::Decoder as _;
    let codec = crate::linker::protocol::ControlCodec {};
    let (sink, mut stream) = codec.framed(stream).split();

    let ack_window_c = ack_window.clone();
    tokio::task::spawn_local(async move {
        loop {
            let control = tokio::select! {
                Some(control) = stream.next() => control,
                _ = read_close_rx.recv() => break,
            };

            if let Ok(control) = control &&
            let Err(e) = control.process(pin.as_str(), &ack_window_c,).await {
                tracing::error!("tcp error: control protocol process error: {e}");
                break;
            }
        };

        tracing::error!("Tcp: [{pin}] read error.");
        let _ = tx.send(Event::Close);
    });

    (sink, ack_window)
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
