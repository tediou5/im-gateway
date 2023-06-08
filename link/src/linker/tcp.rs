// auth tcp then handle it.
pub(crate) async fn auth(stream: tokio::net::TcpStream) -> anyhow::Result<()> {
    crate::axum_handler::LINK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    use futures::StreamExt as _;
    use tokio_util::codec::Decoder as _;
    let codec = crate::linker::protocol::ControlCodec {};
    let mut framed = codec.framed(stream);

    if let Some(Ok(control)) = framed.next().await &&
    let super::Control { event , .. } = control &&
    let super::protocol::control::Event::Package(.., content) = event &&
    let Ok(auth) = TryInto::<crate::linker::Content>::try_into(content.as_slice()) {
        auth.handle_auth(async move |platform| match platform.as_str() {
            "app" => Ok(super::Platform::App(framed)),
            "pc" => Ok(super::Platform::Pc(framed)),
            _ => Err(anyhow::anyhow!("unexpected platform")),
        }).await
    } else {
        tracing::error!("must authenticate first");
        Err(anyhow::anyhow!("must authenticate first"))
    }
}

pub(crate) fn process(
    framed: crate::linker::TcpFramed,
    pin: std::rc::Rc<String>,
    auth_message: Vec<u8>,
) -> (
    crate::linker::Sender,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    use futures::StreamExt as _;

    let (mut sink, stream) = framed.split();

    // Use an unbounded channel to handle buffering and flushing of messages
    // to the event source...
    let (tx, rx) = local_sync::mpsc::unbounded::channel::<crate::linker::Event>();
    let rx = local_sync::stream_wrappers::unbounded::ReceiverStream::new(rx);

    let (tcp_collect, tcp_sender) =
        local_sync::mpsc::unbounded::channel::<crate::linker::SenderEvent>();
    let mut tcp_sender = local_sync::stream_wrappers::unbounded::ReceiverStream::new(tcp_sender);

    let (close, read_close_rx) = tokio::sync::broadcast::channel::<()>(1);
    let retry_close_rx = close.subscribe();

    let (ack_window, read_handler) = handle(pin.clone(), stream, tx.clone(), read_close_rx);

    // send auth message first, auth message alse need ack.
    let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
    let (trace_id, auth_message) =
        crate::linker::Content::pack_message(&auth_message, &mut id_worker).unwrap();
    let auth_message: std::rc::Rc<Vec<u8>> = auth_message.into();
    tracing::debug!("auth message trace id: {}", trace_id);
    let _ = tx.send(crate::linker::Event::WriteBatch(trace_id, auth_message));

    ack_window.run(pin.as_str(), retry_close_rx, tcp_collect, rx);

    // write
    let write_hander = tokio::task::spawn_local(async move {
        use futures::SinkExt as _;

        while let Some(event) = tcp_sender.next().await {
            match event {
                crate::linker::SenderEvent::WriteBatch(message) => {
                    if sink.send(message).await.is_err() {
                        break;
                    }
                }
                crate::linker::SenderEvent::Close(need_reconnect, reason) => {
                    let _ = sink
                        .send(
                            crate::linker::Control::new_disconnect_bytes(reason, need_reconnect)
                                .into(),
                        )
                        .await;
                    break;
                }
            }
        }

        // close read task & retry task

        let _ = close.send(());
        tracing::error!("[{pin}] tcp closed");
        let _ = sink.close().await;

        crate::axum_handler::LINK_COUNT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    });

    (tx, read_handler, write_hander)
}

/// handle spawn a new task, read the request through the tcpStream, then send response to channel tx
fn handle(
    pin: std::rc::Rc<String>,
    mut stream: futures::stream::SplitStream<crate::linker::TcpFramed>,
    tx: crate::linker::Sender,
    mut read_close_rx: tokio::sync::broadcast::Receiver<()>,
) -> (super::ack_window::AckWindow, tokio::task::JoinHandle<()>) {
    let ack_window =
        super::ack_window::AckWindow::new(crate::RETRY_CONFIG.get().unwrap().window_size);

    use futures::StreamExt as _;

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

            if let Err(e) = control.process(pin.as_str(), &ack_window_c).await {
                tracing::error!("tcp error: control protocol process error: {e}");
                break;
            }
        }

        tracing::error!("Tcp: [{pin}] read error.");
        let _ = tx.send(crate::linker::Event::Close(
            true,
            "tcp read error".to_string(),
        ));
    });

    (ack_window, read_handler)
}
