use crate::conhash;

pub(crate) mod chat;
pub(crate) mod event_loop;

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(untagged)]
pub(crate) enum Event {
    #[serde(skip)]
    Connect(
        u64,    /* trace_id */
        String, /* uid */
        crate::linker::Platform,
    ),
    LoginFailed(u64 /* trace_id */, String /* reason */),
    Login(
        u64,         /* trace_id */
        String,      /* auth_message */
        Vec<String>, /* chats */
    ),
    Private(
        std::collections::HashSet<String>, /* recvs */
        #[serde(with = "hex")] Vec<u8>,
    ),
    Group(
        String,                            /* chat */
        std::collections::HashSet<String>, /* exclusions */
        #[serde(with = "hex")] Vec<u8>,
    ),
    Chat(chat::Action),
}

impl Clone for Event {
    fn clone(&self) -> Self {
        match self {
            Self::Connect(..) => panic!("you cannot clone Event::Connect"),
            Self::LoginFailed(..) => panic!("you cannot clone Event::LoginFailed"),
            Self::Login(arg0, arg1, arg2) => Self::Login(arg0.clone(), arg1.clone(), arg2.clone()),
            Self::Private(arg0, arg1) => Self::Private(arg0.clone(), arg1.clone()),
            Self::Group(arg0, arg1, arg2) => Self::Group(arg0.clone(), arg1.clone(), arg2.clone()),
            Self::Chat(arg0) => Self::Chat(arg0.clone()),
        }
    }
}

pub async fn run(core_ids: Vec<core_affinity::CoreId>) -> anyhow::Result<()> {
    let (collect_tx, collect_rx) = tokio::sync::mpsc::channel::<Event>(20480);
    let mut collect_rx = tokio_stream::wrappers::ReceiverStream::new(collect_rx);
    crate::DISPATCHER.set(collect_tx).unwrap();

    // save every event_loop
    let mut event_loops = vec![];
    let mut conhash = conhash::ConsistentHash::new();
    for id in core_ids.into_iter() {
        let event_loop: event_loop::EventLoop = event_loop::run(id);
        conhash.add(&event_loop, 3);
        event_loops.push(event_loop);
    }

    let mut unverified: ahash::AHashMap<
        u64, /* trace_id */
        (String /* uid */, crate::linker::Platform),
    > = ahash::AHashMap::new();

    use tokio_stream::StreamExt as _;
    while let Some(event) = collect_rx.next().await {
        dispatch(&conhash, &event_loops, &mut unverified, event).await?;
    }
    Ok(())
}

async fn dispatch(
    conhash: &crate::conhash::ConsistentHash<event_loop::EventLoop>,
    event_loops: &Vec<event_loop::EventLoop>,
    unverified: &mut ahash::AHashMap<u64, (String, crate::linker::Platform)>,
    event: Event,
) -> anyhow::Result<()> {
    match event {
        Event::Connect(trace_id, uid, platform) => {
            tracing::debug!(
                "[{uid}] Connected in <{platform:?}> platform with trace id: {trace_id}"
            );
            unverified.insert(trace_id, (uid, platform));
        }
        Event::LoginFailed(trace_id, reason) => {
            if let Some((uid, platform)) = unverified.remove(&trace_id) {
                tracing::error!("[{uid}] Login Failed because of <{reason}>");
                platform.close(reason).await;
            };
        }
        Event::Login(trace_id, auth_message, chats) => {
            tracing::debug!("trace id: {trace_id} login");
            if let Some((uid, platform)) = unverified.remove(&trace_id) &&
            let Some(node) = conhash.get(uid.as_bytes()) {
                use crate::conhash::Node as _;
                tracing::info!("[{uid}] login in {} node.", node.name());
                let auth_message =  auth_message.as_bytes().to_vec();
                let login = crate::linker::Login{ auth_message, platform };
                if let Err(e) = node
                    .mailbox
                    .send(event_loop::Event::Login(uid, chats, login))
                    .await
                {
                    return Err(anyhow::anyhow!("Dispatcher Event::Login Error: {e}"));
                };
            };
        }
        Event::Private(recvs, message) => {
            tracing::debug!("send private message to {recvs:?}");
            let message = std::sync::Arc::new(message);
            for pin in recvs {
                if let Some(node) = conhash.get(pin.as_bytes()) {
                    if let Err(e) = node
                        .mailbox
                        .send(event_loop::Event::Private(pin, message.clone()))
                        .await
                    {
                        return Err(anyhow::anyhow!("Dispatcher Event::Private Error: {e}"));
                    };
                };
            }
        }
        Event::Group(chat, exclusions, message) => {
            tracing::debug!("send group message to {chat:?}, exclusions: {exclusions:?}");
            let message = std::sync::Arc::new(message);
            for event_loop in event_loops {
                if let Err(e) = event_loop
                    .mailbox
                    .send(event_loop::Event::Group(
                        chat.clone(),
                        exclusions.clone(),
                        // additional.clone(),
                        message.clone(),
                    ))
                    .await
                {
                    return Err(anyhow::anyhow!("Dispatcher Event::Group Error: {e}"));
                };
            }
        }
        Event::Chat(chat::Action::Notice(chat, message)) => {
            tracing::debug!("send group notice to {chat:?}");
            for event_loop in event_loops {
                if let Err(e) = event_loop
                    .mailbox
                    .send(event_loop::Event::Chat(chat::Action::Notice(
                        chat.clone(),
                        message.clone(),
                    )))
                    .await
                {
                    return Err(anyhow::anyhow!("Dispatcher Event::Group Error: {e}"));
                };
            }
        }
        Event::Chat(action) => {
            tracing::debug!("send group action {action:?}");
            for event_loop in event_loops {
                if let Err(e) = event_loop
                    .mailbox
                    .send(event_loop::Event::Chat(action.clone()))
                    .await
                {
                    return Err(anyhow::anyhow!("Dispatcher Event::Group Error: {e}"));
                };
            }
        }
    }

    Ok(())
}
