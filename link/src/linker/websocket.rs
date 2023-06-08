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
                .handle_auth(async move |platform| match platform.as_str() {
                    "web" => Ok(super::Platform::Web(socket)),
                    _ => Err(anyhow::anyhow!("unexpected platform")),
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
    auth_message: Vec<u8>,
) -> (
    crate::linker::Sender,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = local_sync::mpsc::unbounded::channel::<crate::linker::Event>();
    let rx = local_sync::stream_wrappers::unbounded::ReceiverStream::new(rx);

    let (ws_collect, ws_sender) =
        local_sync::mpsc::unbounded::channel::<crate::linker::SenderEvent>();
    let mut ws_sender = local_sync::stream_wrappers::unbounded::ReceiverStream::new(ws_sender);

    let (close, read_close_rx) = tokio::sync::broadcast::channel::<()>(1);
    let retry_close_rx = close.subscribe();

    let (mut write, ack_window, read_handler) =
        handle(pin.clone(), socket, tx.clone(), read_close_rx);

    let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
    let (trace_id, auth_message) =
        crate::linker::Content::pack_message(&auth_message, &mut id_worker).unwrap();
    let auth_message: std::rc::Rc<Vec<u8>> = auth_message.into();

    let _ = tx.send(crate::linker::Event::WriteBatch(trace_id, auth_message));

    ack_window.run(pin.as_str(), retry_close_rx, ws_collect, rx);

    // write
    let write_handler = tokio::task::spawn_local(async move {
        use futures::SinkExt as _;
        use futures::StreamExt as _;

        while let Some(event) = ws_sender.next().await {
            let content = match event {
                crate::linker::SenderEvent::Close(need_reconnect, reason) => {
                    let _ = write
                        .send(axum::extract::ws::Message::Binary(
                            crate::linker::Control::new_disconnect_bytes(reason, need_reconnect),
                        ))
                        .await;
                    break;
                }
                crate::linker::SenderEvent::WriteBatch(content) => content,
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

    (tx, read_handler, write_handler)
}

fn handle(
    pin: std::rc::Rc<String>,
    socket: axum::extract::ws::WebSocket,
    tx: crate::linker::Sender,
    mut read_close_rx: tokio::sync::broadcast::Receiver<()>,
) -> (
    futures::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>,
    super::ack_window::AckWindow,
    tokio::task::JoinHandle<()>,
) {
    let ack_window =
        super::ack_window::AckWindow::new(crate::RETRY_CONFIG.get().unwrap().window_size);

    use futures::StreamExt as _;

    let (sink, mut stream) = socket.split();

    let ack_window_c = ack_window.clone();
    let read_handler = tokio::task::spawn_local(async move {
        loop {
            let control = tokio::select! {
                _ = read_close_rx.recv() => return,
                control = stream.next() => control,
            };

            let control = match control {
                Some(Ok(control)) => control,
                _ => break,
            };

            match control {
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
        let _ = tx.send(crate::linker::Event::Close(
            true,
            "websocket read error".to_string(),
        ));
    });
    (sink, ack_window, read_handler)
}
