pub(crate) type Sender = local_sync::mpsc::unbounded::Tx<Event>;

#[derive(Debug)]
pub(crate) enum Event {
    WriteBatch(u64, std::rc::Rc<Vec<u8>>),
    Close,
}

#[derive(Debug, Clone)]
pub(crate) enum SenderEvent {
    WriteBatch(std::rc::Rc<Vec<u8>>),
    Close,
}

pub(crate) async fn websocket(
    websocket: axum::extract::ws::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    websocket.on_upgrade(async move |mut socket| {
        let auth = match socket.recv().await {
            Some(Ok(axum::extract::ws::Message::Binary(message))) => message,
            _ => return,
        };

        use tokio_util::codec::Decoder as _;
        let mut dst = bytes::BytesMut::from(auth.as_slice());
        let auth = crate::linker::protocol::ControlCodec
            .decode(&mut dst)
            .unwrap()
            .unwrap();

        let _ = if let crate::linker::protocol::control::Event::Package(.., auth) = auth.event {
            TryInto::<crate::linker::Content>::try_into(auth.as_slice())
                .unwrap()
                .handle_auth(async move |platform, message| {
                    tracing::info!("platform connection: {platform:?}");
                    match platform.as_str() {
                        "web" => Ok(super::Login {
                            platform: super::Platform::Web(socket),
                            auth_message: message,
                        }),
                        _ => Err(anyhow::anyhow!("unexpected platform")),
                    }
                })
                .await
        } else {
            tracing::error!("must authenticate first");
            return;
        };
    })
}

pub(crate) fn process(
    socket: axum::extract::ws::WebSocket,
    pin: std::rc::Rc<String>,
    auth_message: super::Content,
) -> Sender {
    let (tx, rx) = local_sync::mpsc::unbounded::channel::<Event>();
    let mut rx = local_sync::stream_wrappers::unbounded::ReceiverStream::new(rx);

    let (ws_collect, ws_sender) = local_sync::mpsc::unbounded::channel::<SenderEvent>();
    let mut ws_sender = local_sync::stream_wrappers::unbounded::ReceiverStream::new(ws_sender);

    let (close, read_close_rx) = tokio::sync::broadcast::channel::<()>(1);
    let mut retry_close_rx = close.subscribe();

    let (mut write, ack_window) = handle(pin.clone(), socket, tx.clone(), read_close_rx);

    // FIXME: maybe panic if auth_message serialize  error.
    let auth_message: Vec<u8> = auth_message.to_vec().unwrap();
    let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
    let (trace_id, auth_message) =
        crate::linker::Content::pack_message(&auth_message, &mut id_worker).unwrap();
    let auth_message: std::rc::Rc<Vec<u8>> = auth_message.into();

    let _ = tx.send(Event::WriteBatch(trace_id, auth_message));

    let retry_config = &crate::TCP_CONFIG.get().unwrap().retry;

    let pin_c = pin.to_string();
    let ws_collect_c = ws_collect.clone();
    let ack_window_c = ack_window.clone();
    tokio::task::spawn_local(async move {
        use tokio_stream::StreamExt as _;

        while let Some(event) = rx.next().await {
            let (trace_id, message) = match event {
                Event::WriteBatch(trace_id, message) => (trace_id, message),
                Event::Close => {
                    let _ = ws_collect_c.send(SenderEvent::Close);
                    return;
                }
            };

            // FIXME: error when compressed.
            if let Some(ref ack_window) = ack_window_c &&
            let Err(e) = ack_window.acquire(pin_c.as_str(), trace_id, &message).await {
                tracing::error!("acquire ack windows failed: {e}");
                let _ = ws_collect_c.send(SenderEvent::Close);
                return;
            }

            let _ = ws_collect_c.send(SenderEvent::WriteBatch(message));
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

                let retry_timeout = match crate::linker::ack_window::AckWindow::<u64>::get_retry_timeout(retry.times, *timeout, *max_times)
                {
                    Ok(timeout) => timeout,
                    Err(_) => break,
                };
                for message in retry.messages.iter() {
                    if let Err(e) = ws_collect.send(SenderEvent::WriteBatch(message.clone())) {
                        tracing::error!("websocket error: send retry message error: {e:?}");
                        break 'retry;
                    };
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(retry_timeout)).await;
            }
            tracing::error!("[{pin_c}]websocket retry error, close connection");
            let _ = ws_collect.send(SenderEvent::Close);
        });
    }

    tokio::task::spawn_local(async move {
        use futures::SinkExt as _;
        use futures::StreamExt as _;

        while let Some(event) = ws_sender.next().await {
            let content = match event {
                SenderEvent::Close => break,
                SenderEvent::WriteBatch(content) => content,
            };

            match write
                .send(axum::extract::ws::Message::Binary(content.to_vec()))
                .await
            {
                Ok(()) => {
                    tracing::debug!("[{pin}] websocket send ok");
                    crate::axum_handler::LINK_SEND_COUNT
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::error!("[{pin}] websocket send error: {e:?}");
                    break;
                }
            };
        }

        // close read task & retry task
        let _ = close.send(());
        tracing::error!("[{pin}] websocket closed");
        let _ = write.close().await;
    });

    tx
}

fn handle(
    pin: std::rc::Rc<String>,
    socket: axum::extract::ws::WebSocket,
    tx: Sender,
    mut read_close_rx: tokio::sync::broadcast::Receiver<()>,
) -> (
    futures::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>,
    Option<super::ack_window::AckWindow<u64>>,
) {
    let ack_window = if let Some(ref retry) = crate::TCP_CONFIG.get().unwrap().retry {
        tracing::info!("[{pin}]websocket retry: set ack window");
        Some(super::ack_window::AckWindow::new(retry.window_size))
    } else {
        None
    };

    use futures::StreamExt as _;

    let (sink, mut stream) = socket.split();

    let ack_window_c = ack_window.clone();
    tokio::task::spawn_local(async move {
        loop {
            let message = tokio::select! {
                Some(Ok(message)) = stream.next() => message,
                _ = read_close_rx.recv() => break,
            };
            match message {
                axum::extract::ws::Message::Close(_close) => {
                    break;
                }
                axum::extract::ws::Message::Binary(message) => {
                    use tokio_util::codec::Decoder as _;

                    let mut dst = bytes::BytesMut::from(message.as_slice());
                    let control = crate::linker::protocol::ControlCodec
                        .decode(&mut dst)
                        .unwrap()
                        .unwrap();
                    if let Err(e) = control.process(pin.as_str(), &ack_window_c).await {
                        tracing::error!("websocket error: control protocol process error: {e}");
                        break;
                    };
                }
                _ => continue,
            }
        }
        tracing::error!("websocket: [{pin}] read error.");
        let _ = tx.send(Event::Close);
        Ok::<(), anyhow::Error>(())
    });
    (sink, ack_window)
}
