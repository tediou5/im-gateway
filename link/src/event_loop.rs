#[derive(Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(untagged)]
pub(super) enum Event {
    #[serde(skip)]
    Login(
        String,      /* uid */
        Vec<String>, /* chats */
        crate::linker::Platform,
    ),
    Private(
        std::collections::HashSet<String>, /* recvs */
        #[serde(with = "hex")] Vec<u8>,
    ),
    Group(
        String,                            /* chat */
        std::collections::HashSet<String>, /* exclusions */
        std::collections::HashSet<String>, /* additional */
        #[serde(with = "hex")] Vec<u8>,
    ),
    Chat(chat::Action),
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
        Event::Private(recv_list, message) => {
            let mut content = None;
            let message = std::sync::Arc::new(message);
            for recv in recv_list {
                if let Some(sender) = users.get_mut(&recv) {
                    tracing::trace!("send user: {recv}");
                    if sender.send(&message, &mut content).is_err() {
                        tracing::debug!("remove user: {recv}");
                        users.remove(&recv);
                    };
                }
            }
        }
        Event::Group(chat, exclusions, additional, message) => {
            if let Some(online) = chats.get_mut(chat.as_str()) {
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

                let mut content = None;
                let message = std::sync::Arc::new(message);

                tracing::trace!("send group [{}] message", online.len());

                online.retain(|one| {
                    one.send(&message, &mut content)
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
        Event::Chat(chat::Action::Join(chat, members)) => {
            tracing::debug!("join [{chat}]");
            let members = _join(users, members);
            if !members.is_empty() {
                let online = chats.entry(chat.clone()).or_default();
                if online.is_empty() {
                    if let Some(redis_client) = crate::REDIS_CLIENT.get() {
                        tracing::debug!("add [{chat}] into router");
                        redis_client
                            .regist(vec![chat])
                            .await
                            .inspect_err(|e| tracing::error!("regist error: {e}"))?;
                    }
                }
                members.into_iter().collect_into(online);
            }
        }
        Event::Chat(chat::Action::Leave(chat, members)) => {
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

pub(crate) mod chat {
    #[derive(Debug, Clone, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Action {
        Join(String, std::collections::HashSet<String>),
        Leave(String, std::collections::HashSet<String>),
    }
}

#[cfg(test)]
mod test {
    use super::{Event, _join};
    use crate::linker::User;
    use std::{collections::HashSet, rc::Rc};

    const HEX_STRING: &str = "7b2264617461223a207b22636861744964223a2022222c20226d7367466f726d6174223a202254455854222c20226d73674964223a20223261626665366266303331333461356238613831623262326531373236643461222c20226e6f7469636554797065223a2022555345525f424153455f494e464f222c2022626f6479223a20227b5c2261707049645c223a5c2253564f4148626c705c222c5c226176617461725c223a5c226176617461725c222c5c226973426f745c223a66616c73652c5c226e69636b6e616d655c223a5c227465737420757365725c222c5c2270696e5c223a5c2232663734393634616132383734663066383032613761323638653061653632375c222c5c227365785c223a66616c73652c5c227569645c223a5c22515245314c4f55425c227d222c2022636861744d736754797065223a20224e6f74696365222c202266726f6d4964223a20223266373439363461613238373466306638303261376132363865306165363237222c20226170704964223a202253564f4148626c70222c20226368617454797065223a202250726976617465222c202274696d657374616d70223a20313638323431373837323337367d2c202270726f746f636f6c223a20224d657373616765227d";

    #[test]
    fn from_kafaka_record_private_to_event() {
        let record_private =
            serde_json::to_vec(&serde_json::json!([["uu_1"], HEX_STRING])).unwrap();

        let record_private: Event = serde_json::from_slice(record_private.as_slice()).unwrap();

        let private = Event::Private(
            HashSet::from_iter(["uu_1".to_string()]),
            hex::decode(HEX_STRING).unwrap(),
        );

        assert_eq!(record_private, private);
    }

    #[test]
    fn from_kafaka_record_group_to_event() {
        let record_group =
            serde_json::to_vec(&serde_json::json!(["cc_1", [], [], HEX_STRING])).unwrap();

        let record_group: Event = serde_json::from_slice(record_group.as_slice()).unwrap();

        let group = Event::Group(
            "cc_1".to_string(),
            HashSet::new(),
            HashSet::new(),
            hex::decode(HEX_STRING).unwrap(),
        );

        assert_eq!(record_group, group);
    }

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
