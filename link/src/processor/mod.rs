use crate::conhash;

pub(crate) mod chat;
pub(crate) mod event_loop;

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(untagged)]
pub(crate) enum Event {
    #[serde(skip)]
    Login(
        String,                      /* pin */
        Vec<std::sync::Arc<String>>, /* chats */
        crate::linker::Platform,
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
            Self::Login(..) => panic!("you cannot clone Event::Login"),
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

    use tokio_stream::StreamExt as _;

    while let Some(event) = collect_rx.next().await {
        dispatch(&conhash, &event_loops, event).await?;
    }
    Ok(())
}

async fn dispatch(
    conhash: &crate::conhash::ConsistentHash<event_loop::EventLoop>,
    event_loops: &Vec<event_loop::EventLoop>,
    event: Event,
) -> anyhow::Result<()> {
    match event {
        Event::Login(pin, chats, platform) => {
            if let Some(node) = conhash.get(pin.as_bytes()) {
                use crate::conhash::Node as _;
                tracing::info!("[{pin}] login in {} node.", node.name());
                if let Err(e) = node
                    .mailbox
                    .send(event_loop::Event::Login(pin, chats, platform))
                    .await
                {
                    return Err(anyhow::anyhow!("Dispatcher Event::Login Error: {e}"));
                };
            };
        }
        Event::Private(recvs, message) => {
            tracing::info!("send private message to {recvs:?}");
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
            tracing::info!("send group message to {chat:?}");
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
            tracing::info!("send group notice to {chat:?}");
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
