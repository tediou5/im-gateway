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
    let auth = crate::linker::protocol::ControlCodec.decode(&mut dst)?;

    if let Some(crate::linker::protocol::control::Event::Package(.., auth)) =
        auth.map(|auth| auth.event)
    {
        TryInto::<crate::linker::Content>::try_into(auth.as_slice())?
            .handle_auth(async move |platform, message| match platform.as_str() {
                "app" => Ok(super::Login {
                    platform: super::Platform::App(stream),
                    auth_message: message,
                }),
                "pc" => Ok(super::Login {
                    platform: super::Platform::Pc(stream),
                    auth_message: message,
                }),
                _ => Err(anyhow::anyhow!("unexpected platform")),
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
) -> crate::linker::Sender {
    tracing::info!(
        "[{pin}] ready to process tcp message: {:?}",
        stream.peer_addr()
    );
    // Use an unbounded channel to handle buffering and flushing of messages
    // to the event source...
    let (tx, rx) = local_sync::mpsc::unbounded::channel::<crate::linker::Event>();
    let rx = local_sync::stream_wrappers::unbounded::ReceiverStream::new(rx);

    let (tcp_collect, tcp_sender) =
        local_sync::mpsc::unbounded::channel::<crate::linker::SenderEvent>();
    let mut tcp_sender = local_sync::stream_wrappers::unbounded::ReceiverStream::new(tcp_sender);

    let (close, read_close_rx) = tokio::sync::broadcast::channel::<()>(1);
    let retry_close_rx = close.subscribe();

    let (mut sink, ack_window) = handle(pin.clone(), stream, tx.clone(), read_close_rx);

    // send auth message first, auth message alse need ack.
    let auth_message: Vec<u8> = auth_message.to_vec().unwrap();
    let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
    let (trace_id, auth_message) =
        crate::linker::Content::pack_message(&auth_message, &mut id_worker).unwrap();
    let auth_message: std::rc::Rc<Vec<u8>> = auth_message.into();
    tracing::debug!("auth message trace id: {}", trace_id);
    let _ = tx.send(crate::linker::Event::WriteBatch(trace_id, auth_message));

    ack_window.run(pin.as_str(), retry_close_rx, tcp_collect, rx);

    // write
    tokio::task::spawn_local(async move {
        use futures::SinkExt as _;
        use tokio_stream::StreamExt as _;

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

    tx
}

/// handle spawn a new task, read the request through the tcpStream, then send response to channel tx
fn handle(
    pin: std::rc::Rc<String>,
    stream: tokio::net::TcpStream,
    tx: crate::linker::Sender,
    mut read_close_rx: tokio::sync::broadcast::Receiver<()>,
) -> (
    futures::stream::SplitSink<
        tokio_util::codec::Framed<tokio::net::TcpStream, crate::linker::protocol::ControlCodec>,
        std::rc::Rc<Vec<u8>>,
    >,
    super::ack_window::AckWindow,
) {
    let ack_window =
        super::ack_window::AckWindow::new(crate::RETRY_CONFIG.get().unwrap().window_size);

    tracing::trace!("ready to handle tcp request: {:?}", stream.peer_addr());
    use futures::StreamExt as _;
    use tokio_util::codec::Decoder as _;
    let codec = crate::linker::protocol::ControlCodec {};
    let (sink, mut stream) = codec.framed(stream).split();

    let ack_window_c = ack_window.clone();
    tokio::task::spawn_local(async move {
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

    (sink, ack_window)
}
