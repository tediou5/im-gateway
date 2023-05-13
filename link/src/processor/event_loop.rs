pub(super) enum Event {
    Login(
        String,                      /* pin */
        Vec<std::sync::Arc<String>>, /* chats */
        crate::linker::Platform,
    ),
    Private(String /* recv */, std::sync::Arc<Vec<u8>>),
    Group(
        String,                            /* chat */
        std::collections::HashSet<String>, /* exclusions */
        std::sync::Arc<Vec<u8>>,
    ),
    Chat(super::chat::Action),
}

#[derive(Clone)]
pub(super) struct EventLoop {
    name: std::rc::Rc<String>,
    pub(super) mailbox: tokio::sync::mpsc::Sender<Event>,
}

impl crate::conhash::Node for EventLoop {
    fn name(&self) -> String {
        self.name.to_string()
    }
}

pub(super) fn run(core_id: core_affinity::CoreId) -> EventLoop {
    let (collect_tx, collect_rx) = tokio::sync::mpsc::channel::<Event>(2048);
    let mut collect_rx = tokio_stream::wrappers::ReceiverStream::new(collect_rx);

    let id = core_id.id;

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let res = core_affinity::set_for_current(core_id);
        if res {
            tracing::error!(
                "running event loop: >>> processor-event-loop-{id} set for core id: [{id}] <<<"
            );
        } else {
            tracing::error!("error for pin current cpu");
        }

        let local = tokio::task::LocalSet::new();
        let local_set = local.run_until(async {
            tokio::task::spawn_local(async move {
                use tokio_stream::StreamExt as _;

                let mut users: ahash::AHashMap<std::rc::Rc<String>, crate::linker::User> =
                    ahash::AHashMap::new();
                let mut chats: ahash::AHashMap<
                    String,
                    std::collections::HashSet<crate::linker::User>,
                > = ahash::AHashMap::new();

                while let Some(event) = collect_rx.next().await {
                    if let Err(e) = process(&mut users, &mut chats, event).await {
                        // TODO: handle error
                        tracing::error!("preocess event error: {e}");
                    };
                }
            })
            .await
            .unwrap();
        });

        rt.block_on(local_set);
        tracing::error!("processor-event-loop-{id} has been cancelled");
    });

    EventLoop {
        name: format!("processor-event-loop-{id}").into(),
        mailbox: collect_tx,
    }
}

async fn process(
    users: &mut ahash::AHashMap<std::rc::Rc<String>, crate::linker::User>,
    chats: &mut ahash::AHashMap<String, std::collections::HashSet<crate::linker::User>>,
    event: Event,
) -> anyhow::Result<()> {
    match event {
        Event::Login(pin, chat_list, platform) => {
            tracing::error!("{pin} login");
            let pin = std::rc::Rc::new(pin);
            if let Some(redis) = crate::REDIS_CLIENT.get() {
                if let Err(e) = redis.heartbeat(pin.to_string()).await {
                    tracing::error!("update [{pin}] heartbeat error: {}", e)
                };
            }
            let user_connection = users
                .entry(pin)
                .or_insert_with_key(|pin| crate::linker::User::from_pin(pin.clone()));

            user_connection.update(platform);

            let mut regiest_chats = Vec::new();

            for chat in chat_list {
                let member = chats.entry(chat.to_string()).or_insert_with_key(|chat| {
                    regiest_chats.push(chat.to_string());
                    Default::default()
                });
                member.insert(user_connection.clone());
            }
            tokio::task::spawn_local(async move {
                let redis_client = crate::REDIS_CLIENT.get().unwrap();
                redis_client.regist(regiest_chats).await?;
                Ok::<(), anyhow::Error>(())
            });
        }
        Event::Private(pin, message) => {
            let mut content = None;
            let message = std::rc::Rc::new(message.as_ref().clone());

            if let Some(sender) = users.get_mut(&pin) {
                tracing::info!("send user: {pin}");
                if sender.send(&message, &mut content).is_err() {
                    tracing::info!("remove user: {pin}");
                    users.remove(&pin);
                };
            }
        }
        Event::Group(chat, exclusions, message) => {
            if let Some(online) = chats.get_mut(&chat) {
                let exclusions: std::collections::HashSet<_> = exclusions
                    .iter()
                    .filter_map(|exc| users.get(exc).cloned())
                    .collect();

                let recv_list: std::collections::HashSet<crate::linker::User> =
                    online.difference(&exclusions).map(Clone::clone).collect();

                let mut content = None;
                let message = std::rc::Rc::new(message.as_ref().clone());

                tracing::info!("send group [{}] message", online.len());

                for one in recv_list.iter() {
                    if one.send(&message, &mut content).is_err() {
                        users.remove(one.pin.as_ref());
                        online.remove(one);
                        one.close()
                    }
                }
            };
        }
        Event::Chat(crate::processor::chat::Action::Leave(chat, members)) => {
            if let Some(online) = chats.get_mut(&chat.to_string()) {
                for member in members {
                    let user = crate::linker::User::from_pin(member.into());
                    online.remove(&user);
                }
            }
        }
        Event::Chat(crate::processor::chat::Action::Join(chat, members)) => {
            let members = _join(users, members);
            if !members.is_empty() {
                tracing::info!("add {members:?} into [{chat}]");
                let online = chats.entry(chat.to_string()).or_default();
                if online.is_empty() {
                    if let Some(redis_client) = crate::REDIS_CLIENT.get() {
                        redis_client
                            .regist(vec![chat.to_string()])
                            .await
                            .inspect_err(|e| tracing::error!("regist error: {e}"))?;
                    }
                }
                members.into_iter().collect_into(online);
            }
        }
        Event::Chat(super::chat::Action::Notice(chat, message)) => {
            tracing::info!("send notice to [{chat}]");
            if let Some(online) = chats.get_mut(&chat.to_string()) {
                tracing::info!("[{chat}] with online members: {online:?}");
                let mut content = None;
                let message = std::rc::Rc::new(message);

                tracing::info!("send group [{}] message", online.len());

                online.retain(|one| {
                    one.send(&message, &mut content)
                        .inspect_err(|_e| {
                            users.remove(one.pin.as_ref());
                            one.close()
                        })
                        .is_ok()
                });
            };
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
    use crate::linker::User;
    use crate::processor::{event_loop::_join, Event};
    use std::{collections::HashSet, rc::Rc};

    const HEX_STRING: &str = "7b2264617461223a207b22636861744964223a2022222c20226d7367466f726d6174223a202254455854222c20226d73674964223a20223261626665366266303331333461356238613831623262326531373236643461222c20226e6f7469636554797065223a2022555345525f424153455f494e464f222c2022626f6479223a20227b5c2261707049645c223a5c2253564f4148626c705c222c5c226176617461725c223a5c226176617461725c222c5c226973426f745c223a66616c73652c5c226e69636b6e616d655c223a5c227465737420757365725c222c5c2270696e5c223a5c2232663734393634616132383734663066383032613761323638653061653632375c222c5c227365785c223a66616c73652c5c227569645c223a5c22515245314c4f55425c227d222c2022636861744d736754797065223a20224e6f74696365222c202266726f6d4964223a20223266373439363461613238373466306638303261376132363865306165363237222c20226170704964223a202253564f4148626c70222c20226368617454797065223a202250726976617465222c202274696d657374616d70223a20313638323431373837323337367d2c202270726f746f636f6c223a20224d657373616765227d";

    #[test]
    fn from_kafaka_record_private_to_event() {
        let record_private =
            serde_json::to_string(&serde_json::json!([["uu_1"], HEX_STRING])).unwrap();

        let private = Event::Private(
            HashSet::from_iter(["uu_1".to_string()]),
            hex::decode(HEX_STRING).unwrap(),
        );
        let private = serde_json::to_string(&private).unwrap();

        assert_eq!(record_private, private);
    }

    #[test]
    fn from_kafaka_record_group_to_event() {
        let record_group =
            serde_json::to_string(&serde_json::json!(["cc_1", [], HEX_STRING])).unwrap();

        let group = Event::Group(
            "cc_1".to_string(),
            HashSet::new(),
            hex::decode(HEX_STRING).unwrap(),
        );
        let group = serde_json::to_string(&group).unwrap();

        assert_eq!(record_group, group);
    }

    #[test]
    fn join() {
        let uu2 = Rc::new("uu2".to_string());
        let uu3 = Rc::new("uu3".to_string());
        let uu6 = Rc::new("uu6".to_string());

        let mut users = new_user();

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

    fn new_user() -> ahash::AHashMap<Rc<String>, User> {
        let uu1 = Rc::new("uu1".to_string());
        let uu2 = Rc::new("uu2".to_string());
        let uu3 = Rc::new("uu3".to_string());
        let uu4 = Rc::new("uu4".to_string());
        let uu5 = Rc::new("uu5".to_string());

        ahash::AHashMap::from([
            (uu1.clone(), User::from_pin(uu1.clone())),
            (uu2.clone(), User::from_pin(uu2.clone())),
            (uu3.clone(), User::from_pin(uu3.clone())),
            (uu4.clone(), User::from_pin(uu4.clone())),
            (uu5.clone(), User::from_pin(uu5.clone())),
        ])
    }
}
