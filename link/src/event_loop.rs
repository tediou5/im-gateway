#[derive(Debug)]
pub(super) enum Event {
    Regist(
        String,      /* uid */
        Vec<String>, /* chats */
        crate::linker::Platform,
    ),
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

    let mut users: ahash::AHashMap<std::sync::Arc<String>, crate::linker::User> =
        ahash::AHashMap::new();
    let mut chats: ahash::AHashMap<String, std::collections::HashSet<std::sync::Arc<String>>> =
        ahash::AHashMap::new();

    use tokio_stream::StreamExt as _;
    while let Some(event) = collect_rx.next().await {
        _handle(&mut users, &mut chats, event)?;
    }
    Ok(())
}

fn _handle(
    users: &mut ahash::AHashMap<std::sync::Arc<String>, crate::linker::User>,
    chats: &mut ahash::AHashMap<String, std::collections::HashSet<std::sync::Arc<String>>>,
    event: Event,
) -> anyhow::Result<()> {
    match event {
        Event::Regist(user, chat_list, platform) => {
            let user = std::sync::Arc::new(user);
            let user_connection = users.entry(user.clone()).or_default();

            match platform {
                crate::linker::Platform::App(sender) => {
                    user_connection.app.replace(sender).map(|old| {
                        tracing::error!("remove old app connection");
                        let _ = old.send(crate::linker::tcp::Event::Close);
                    })
                }
                crate::linker::Platform::Pc(sender) => {
                    user_connection.app.replace(sender).map(|old| {
                        tracing::error!("remove old pc connection");
                        let _ = old.send(crate::linker::tcp::Event::Close);
                    })
                }
                crate::linker::Platform::Web(sender) => {
                    user_connection.web.replace(sender).map(|old| {
                        tracing::error!("remove old web connection");
                        let _ = old.send(crate::linker::websocket::Event::Close);
                    })
                }
            };

            for chat in chat_list {
                // FIXME: maybe have a better way to do this
                let member = chats.entry(chat).or_default();
                member.insert(user.clone());
            }
        }
        Event::Send(recv_list, content) => {
            tracing::error!("send private message: {}", recv_list.len());
            let content = std::sync::Arc::new(content);
            for recv in recv_list {
                if let Some(sender) = users.get_mut(&recv) {
                    tracing::debug!("send user: {recv}");
                    if sender.send(content.clone()).is_err() {
                        tracing::debug!("remove user: {recv}");
                        users.remove(&recv);
                    };
                }
            }
        }
        Event::SendBatch(chat, exclusions, additional, messages) => {
            if let Some(online) = chats.get_mut(chat.as_str()) {
                use tokio_util::codec::Encoder as _;

                let mut codec = crate::linker::MessageCodec {};
                let mut dst = bytes::BytesMut::new();
                let _ = codec.encode(&messages, &mut dst);
                let dst = dst.to_vec();
                let messages_bytes = std::sync::Arc::new(dst);

                let contents: Vec<String> = messages
                    .into_iter()
                    .map(|message| serde_json::to_string(&message.content))
                    .try_collect()?;
                let contents = std::sync::Arc::new(contents);

                // TODO: FIXME:
                // let mut recv_list: std::collections::HashSet<&str> =
                //     online.iter().map(|one| one.as_str()).collect();
                // let exclusions = &exclusions.iter().map(|exc| exc.as_str()).collect();
                // additional
                //     .iter()
                //     .map(|add| add.as_str())
                //     .collect_into(&mut recv_list);

                // let recv_list: std::collections::HashSet<&&str> =
                //     recv_list.difference(exclusions).collect();

                tracing::error!("send group [{}] message", online.len());

                online.retain(|one| {
                    if let Some(sender) = users.get_mut(one.as_ref()) &&
                    let Err(_) = sender.send_batch(contents.clone(), messages_bytes.clone()) {
                        tracing::debug!("remove user: {one}");
                        users.remove(one);
                        false
                    } else {
                        true
                    }
                });

                // for recv in online.iter() {
                //     if let Some(sender) = users.get_mut(recv.as_ref()) &&
                //     let Err(_) = sender.send_batch(contents.clone(), messages_bytes.clone()) {
                //         tracing::debug!("remove user: {recv}");
                //         users.remove(recv);
                //     };
                // }
            } else {
                tracing::error!("no such chat: {chat}");
            }
        }
        Event::Join(chat, members) => {
            tracing::debug!("join [{chat}]");
            let members = _join(users, members);
            if !members.is_empty() {
                let online = chats.entry(chat.clone()).or_default();
                if online.is_empty() {
                    if let Some(redis_client) = crate::REDIS_CLIENT.get() {
                        tokio::task::spawn(async move {
                            tracing::debug!("add [{chat}] into router");
                            redis_client
                                .regist(&vec![chat])
                                .await
                                .inspect_err(|e| tracing::error!("regist error: {e}"))?;
                            Ok::<(), anyhow::Error>(())
                        });
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
    users: &mut ahash::AHashMap<std::sync::Arc<String>, crate::linker::User>,
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

    use super::_join;

    #[test]
    fn join() {
        let (tx, _) = tokio::sync::mpsc::unbounded_channel::<crate::linker::tcp::Event>();
        let tx = crate::linker::User {
            app: None,
            web: None,
            pc: Some(tx),
        };
        let mut users = ahash::AHashMap::from([
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
