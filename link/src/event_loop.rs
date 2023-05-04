use crate::Sender;
use ahash::AHashMap;

pub(crate) enum Platform {
    App(Sender),
    Web(Sender),
    PC(Sender),
}

pub(crate) struct UserConnections {
    app: Option<Sender>,
    web: Option<Sender>,
    pc: Option<Sender>,
}

#[derive(Debug)]
pub(super) enum Event {
    Regist(String /* uid */, Vec<String> /* chats */, Sender),
    Send(
        std::collections::HashSet<String>, /* recvs */
        crate::linker::Message,
    ),
    SendBatch(
        String,                            /* chat */
        std::collections::HashSet<String>, /* exclusions */
        std::collections::HashSet<String>, /* additional */
        Vec<crate::linker::Message>,
    ),
    Join(
        String,                            /* chat */
        std::collections::HashSet<String>, /* users */
    ),
    Leave(
        String,                            /* chat */
        std::collections::HashSet<String>, /* users */
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
            }

            for chat in chat_list {
                // FIXME: maybe have a better way to do this
                let member = chats.entry(chat).or_default();
                member.insert(user.clone());
            }
        }
        Event::Send(recvs, content) => {
            let content = std::sync::Arc::new(vec![content]);
            for recv in recvs {
                if let Some(sender) = users.get(&recv) {
                    let _ = sender.send(crate::linker::Event::WriteBatch(content.clone()));
                }
            }
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
        Event::Join(chat, members) => {
            tracing::error!(">>>>>> join [{chat}] <<<<<<");
            let members = _join(users, members);
            if !members.is_empty() {
                let online = chats.entry(chat.clone()).or_default();
                if online.is_empty() {
                    if let Some(redis_client) = crate::REDIS_CLIENT.get() {
                        tracing::error!(
                            "--------->>>>>>>>>>>>>>>>>>>>>>>>>\nadd [{chat}] into router"
                        );
                        redis_client
                            .regist(&vec![chat])
                            .await
                            .inspect_err(|e| tracing::error!("regist error: {e}"))?;
                    }
                }
                members.into_iter().collect_into(online);
            }
        }
        Event::Leave(chat, members) => {
            if let Some(online) = chats.get_mut(chat.as_str()) {
                for member in members {
                    online.remove(&member);
                }
            }
        }
    }
    Ok(())
}

fn _join(
    users: &mut AHashMap<std::sync::Arc<String>, Sender>,
    members: std::collections::HashSet<String>,
) -> std::collections::HashSet<std::sync::Arc<String>> {
    let mut users_keys: std::collections::HashSet<std::sync::Arc<String>> =
        std::collections::HashSet::new();
    for member in members {
        if let Some((user, _)) = users.get_key_value(&member) {
            users_keys.insert(user.clone());
        }
    }

    users_keys
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, sync::Arc};

    use ahash::AHashMap;

    use super::_join;

    #[test]
    fn join() {
        let (tx, _) = tokio::sync::mpsc::unbounded_channel::<crate::linker::Event>();
        let mut users = AHashMap::from([
            (Arc::new("uu1".to_string()), tx.clone()),
            (Arc::new("uu2".to_string()), tx.clone()),
            (Arc::new("uu3".to_string()), tx.clone()),
            (Arc::new("uu4".to_string()), tx.clone()),
            (Arc::new("uu5".to_string()), tx.clone()),
        ]);

        let mut chat = std::collections::HashSet::new();
        chat.insert(Arc::new("uu6".to_string()));

        let member = _join(
            &mut users,
            // &mut chat,
            HashSet::from_iter(["uu0".to_string(), "uu2".to_string(), "uu3".to_string()]),
        );

        member.into_iter().collect_into(&mut chat);

        let new_chat = std::collections::HashSet::from_iter([
            Arc::new("uu6".to_string()),
            Arc::new("uu2".to_string()),
            Arc::new("uu3".to_string()),
        ]);

        assert_eq!(chat, new_chat);
    }
}
