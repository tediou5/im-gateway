use super::{auth, Content, Event, Message, MessageCodec};

pub(super) async fn process(
    ws: axum::extract::ws::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(async move |socket| {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
        let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let mut write = handle(socket, tx);

        use futures::StreamExt as _;

        while let Some(event) = rx.next().await {
            match event {
                Event::WriteBatch(_) => todo!(),
                Event::Close => todo!(),
            }
        }

        // TODO:
        // user_ws_rx stream will keep processing as long as the user stays
        // connected. Once they disconnect, then...
        // user_disconnected(&username, &uuid).await;
        tracing::info!("disconnecting");
    })
}

fn handle(
    socket: axum::extract::ws::WebSocket,
    tx: crate::Sender,
) -> futures::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message> {
    use futures::SinkExt as _;
    use futures::StreamExt as _;
    use futures::TryFutureExt as _;

    let (tx, mut rx) = socket.split();

    tokio::task::spawn(async move {
        while let Some(Ok(message)) = rx.next().await {
            match message {
                axum::extract::ws::Message::Text(message) => {
                    todo!()
                }
                axum::extract::ws::Message::Close(close) => {
                    todo!()
                }
                _ => continue,
            }
        }
    });
    tx
}
