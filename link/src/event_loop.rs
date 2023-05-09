#[derive(Debug)]
pub(super) enum Event {
    Login(
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
    let (collect_tx, collect_rx) = tokio::sync::mpsc::channel::<Event>(20480);
    let mut collect_rx = tokio_stream::wrappers::ReceiverStream::new(collect_rx);
    // let (collect_tx, collect_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    // let mut collect_rx = tokio_stream::wrappers::UnboundedReceiverStream::new(collect_rx);
    crate::EVENT_LOOP.set(collect_tx).unwrap();

    let mut users: ahash::AHashMap<std::rc::Rc<String>, crate::linker::User> =
        ahash::AHashMap::new();
    let mut chats: ahash::AHashMap<String, std::collections::HashSet<crate::linker::User>> =
        ahash::AHashMap::new();

    use tokio_stream::StreamExt as _;

    // let mut len = 0;
    // let mut linger = 10;

    // let sleep = tokio::time::sleep(tokio::time::Duration::from_millis(10));
    // tokio::pin!(sleep);

    // loop {
    //     tokio::select! {
    //         _ = &mut sleep => {
    //             linger = if len > 10 { 20 } else { 10 };
    //             len = 0;

    //             users.retain(|_, user| {
    //                 user.flush().is_ok()
    //             });
    //         }
    //         event = collect_rx.next() => {
    //             match event {
    //                 Some(event) => {
    //                     if sleep.is_elapsed() && len == 0 {
    //                         sleep.as_mut().reset(tokio::time::Instant::now() + tokio::time::Duration::from_millis(linger));
    //                     }
    //                     _handle(&mut users, &mut chats, event)?;
    //                 },
    //                 None => break,
    //             }

    //         }
    //     };

    while let Some(event) = collect_rx.next().await {
        _handle(&mut users, &mut chats, event).await?;
    }
    // }
    Ok(())
}

async fn _handle(
    users: &mut ahash::AHashMap<std::rc::Rc<String>, crate::linker::User>,
    chats: &mut ahash::AHashMap<String, std::collections::HashSet<crate::linker::User>>,
    event: Event,
) -> anyhow::Result<()> {
    match event {
        Event::Login(user, chat_list, platform) => {
            tracing::error!("{user} login");
            let user = std::rc::Rc::new(user);
            let user_connection = users
                .entry(user)
                .or_insert_with_key(|pin| crate::linker::User::from_pin(pin.clone()));

            user_connection.update(platform);

            let mut regiest_chats = Vec::new();

            for chat in chat_list {
                // FIXME: maybe have a better way to do this
                let member = chats.entry(chat).or_insert_with_key(|chat| {
                    regiest_chats.push(chat.to_string());
                    Default::default()
                });
                member.insert(user_connection.clone());
            }
            tokio::task::spawn(async move {
                let redis_client = crate::REDIS_CLIENT.get().unwrap();
                redis_client.regist(regiest_chats).await?;
                Ok::<(), anyhow::Error>(())
            });
        }
        Event::Send(recv_list, content) => {
            let content = std::sync::Arc::new(content);
            for recv in recv_list {
                if let Some(sender) = users.get_mut(&recv) {
                    tracing::trace!("send user: {recv}");
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

                tracing::trace!("send group [{}] message", online.len());

                online.retain(|one| {
                    one.send_batch(contents.clone(), messages_bytes.clone())
                        .inspect_err(|_e| {
                            users.remove(one.pin.as_ref());
                            one.close()
                        })
                        .is_ok()
                });
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
                        // tokio::task::spawn(async move {
                        tracing::debug!("add [{chat}] into router");
                        redis_client
                            .regist(vec![chat])
                            .await
                            .inspect_err(|e| tracing::error!("regist error: {e}"))?;
                        //     Ok::<(), anyhow::Error>(())
                        // });
                    }
                }
                members.into_iter().collect_into(online);
            }
        }
        Event::Leave(chat, members) => {
            if let Some(online) = chats.get_mut(chat.as_str()) {
                for member in members {
                    let user = crate::linker::User::from_pin(member.into());
                    online.remove(&user);
                }
            }
        }
    }
    Ok(())
}

fn _join(
    users: &mut ahash::AHashMap<std::rc::Rc<String>, crate::linker::User>,
    members: std::collections::HashSet<String>,
) -> std::collections::HashSet<crate::linker::User> {
    let mut users_keys: std::collections::HashSet<crate::linker::User> =
        std::collections::HashSet::new();
    for member in members {
        if let Some((_, user)) = users.get_key_value(&member) {
            users_keys.insert(user.clone());
        }
    }

    users_keys
}

#[cfg(test)]
mod test {
    use super::_join;
    use crate::linker::User;
    use std::{collections::HashSet, rc::Rc};

    #[test]
    fn join() {
        let uu1 = Rc::new("uu1".to_string());
        let uu2 = Rc::new("uu2".to_string());
        let uu3 = Rc::new("uu3".to_string());
        let uu4 = Rc::new("uu4".to_string());
        let uu5 = Rc::new("uu5".to_string());
        let uu6 = Rc::new("uu6".to_string());

        let mut users = ahash::AHashMap::from([
            (uu1.clone(), User::from_pin(uu1.clone())),
            (uu2.clone(), User::from_pin(uu2.clone())),
            (uu3.clone(), User::from_pin(uu3.clone())),
            (uu4.clone(), User::from_pin(uu4.clone())),
            (uu5.clone(), User::from_pin(uu5.clone())),
        ]);

        let mut chat = std::collections::HashSet::new();
        chat.insert(User::from_pin(uu6.clone()));

        let member = _join(
            &mut users,
            // &mut chat,
            HashSet::from_iter(["uu0".to_string(), "uu2".to_string(), "uu3".to_string()]),
        );

        member.into_iter().collect_into(&mut chat);

        let new_chat = std::collections::HashSet::from_iter([
            User::from_pin(uu6),
            User::from_pin(uu2),
            User::from_pin(uu3),
        ]);

        assert_eq!(chat, new_chat);
    }
}
