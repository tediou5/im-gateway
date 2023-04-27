use crate::Sender;
use ahash::AHashMap;

#[derive(Debug)]
pub(super) enum Event {
    Regist(String /* uid */, Vec<String> /* chats */, Sender),
    Send(String /* recv */, crate::linker::Message),
    SendBatch(
        String,                            /* chat */
        std::collections::HashSet<String>, /* exclusions */
        std::collections::HashSet<String>, /* additional */
        Vec<crate::linker::Message>,
    ),
}

pub(super) async fn run() -> anyhow::Result<()> {
    // let (collect_tx, collect_rx) = tokio::sync::mpsc::channel::<Event>(10240);
    // let mut collect_rx = tokio_stream::wrappers::ReceiverStream::new(collect_rx);
    let (collect_tx, collect_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut collect_rx = tokio_stream::wrappers::UnboundedReceiverStream::new(collect_rx);
    crate::EVENT_LOOP.set(collect_tx).unwrap();

    let mut users: AHashMap<std::sync::Arc<String>, Sender> = AHashMap::new();
    let mut chats: AHashMap<String, std::collections::HashSet<std::sync::Arc<String>>> =
        AHashMap::new();

    use tokio_stream::StreamExt as _;
    while let Some(event) = collect_rx.next().await {
        _handle(&mut users, &mut chats, event).await?;
    }
    Ok(())
}

async fn _handle(
    users: &mut AHashMap<std::sync::Arc<String>, Sender>,
    chats: &mut AHashMap<String, std::collections::HashSet<std::sync::Arc<String>>>,
    event: Event,
) -> anyhow::Result<()> {
    match event {
        Event::Regist(user, chat_list, sender) => {
            let user = std::sync::Arc::new(user);
            if let Some(sender) = users.insert(user.clone(), sender) {
                let _ = sender.send(crate::linker::Event::Close);
            };

            for chat in chat_list {
                // FIXME: maybe have a better way to do this
                let member = chats.entry(chat).or_default();
                member.insert(user.clone());
            }
        }
        Event::Send(recv, content) => {
            if let Some(sender) = users.get(&recv) {
                let content = std::sync::Arc::new(vec![content]);
                let _ = sender.send(crate::linker::Event::WriteBatch(content));
            };
        }
        Event::SendBatch(chat, exclusions, additional, message) => {
            if let Some(online) = chats.get_mut(chat.as_str()) {
                let message = std::sync::Arc::new(message);

                let mut recv_list: std::collections::HashSet<&str> =
                    online.iter().map(|one| one.as_str()).collect();
                let exclusions = &exclusions.iter().map(|exc| exc.as_str()).collect();
                additional
                    .iter()
                    .map(|add| add.as_str())
                    .collect_into(&mut recv_list);

                let recv_list: std::collections::HashSet<&&str> =
                    recv_list.difference(exclusions).collect();
                for &&recv in recv_list.iter() {
                    if let Some(sender) = users.get(&recv.to_string()) {
                        let _ = sender.send(crate::linker::Event::WriteBatch(message.clone()));
                    };
                }
            }
        }
    }
    Ok(())
}
