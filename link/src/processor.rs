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
        std::collections::HashSet<String>, /* additional */
        #[serde(with = "hex")] Vec<u8>,
    ),
    Chat(chat::Action),
}

impl Clone for Event {
    fn clone(&self) -> Self {
        match self {
            Self::Login(..) => panic!("you cannot clone Event::Login"),
            Self::Private(arg0, arg1) => Self::Private(arg0.clone(), arg1.clone()),
            Self::Group(arg0, arg1, arg2, arg3) => {
                Self::Group(arg0.clone(), arg1.clone(), arg2.clone(), arg3.clone())
            }
            Self::Chat(arg0) => Self::Chat(arg0.clone()),
        }
    }
}

// pub(crate) struct Dispatcher {
//     conhash: crate::conhash::ConsistentHash<event_loop::EventLoop>,
//     group_sender: flume::Sender<event_loop::Group>,
// }

// impl Dispatcher {
// pub(crate) async fn new(worker_number: usize) -> Self {
//     let (group_sender, group_recver) = flume::unbounded();

//     let mut conhash = conhash::ConsistentHash::new();
//     for id in 0..(worker_number) {
//         tracing::error!("running event loop");
//         conhash.add(&event_loop::run(id, group_recver.clone()), 3);
//     }

//     Self {
//         conhash,
//         // matedata,
//         group_sender,
//     }
// }

pub async fn run(worker_number: usize) -> anyhow::Result<()> {
    let (collect_tx, collect_rx) = tokio::sync::mpsc::channel::<Event>(20480);
    let mut collect_rx = tokio_stream::wrappers::ReceiverStream::new(collect_rx);
    // let (collect_tx, collect_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    // let mut collect_rx = tokio_stream::wrappers::UnboundedReceiverStream::new(collect_rx);
    crate::DISPATCHER.set(collect_tx).unwrap();

    let (group_sender, group_recver) = flume::unbounded();

    let mut conhash = conhash::ConsistentHash::new();
    for id in 0..(worker_number) {
        tracing::error!("running event loop");
        conhash.add(&event_loop::run(id, group_recver.clone()), 3);
    }

    use tokio_stream::StreamExt as _;

    while let Some(event) = collect_rx.next().await {
        dispatch(&conhash, &group_sender, event).await?;
    }
    Ok(())
}

async fn dispatch(
    conhash: &crate::conhash::ConsistentHash<event_loop::EventLoop>,
    group_sender: &flume::Sender<event_loop::Group>,
    event: Event,
) -> anyhow::Result<()> {
    match event {
        Event::Login(pin, chats, platform) => {
            if let Some(node) = conhash.get(pin.as_bytes()) {
                if let Err(e) = node
                    .mailbox
                    .send(event_loop::Private::Login(pin, chats, platform))
                    .await
                {
                    return Err(anyhow::anyhow!("Dispatcher Event::Login Error: {e}"));
                };
            };
        }
        Event::Private(recvs, message) => {
            let message = std::sync::Arc::new(message);
            for pin in recvs {
                if let Some(node) = conhash.get(pin.as_bytes()) {
                    if let Err(e) = node
                        .mailbox
                        .send(event_loop::Private::Message(pin, message.clone()))
                        .await
                    {
                        return Err(anyhow::anyhow!("Dispatcher Event::Private Error: {e}"));
                    };
                };
            }
        }
        Event::Group(chat, exclusions, additional, message) => {
            let message = std::sync::Arc::new(message);
            if let Err(e) = group_sender
                .send_async(event_loop::Group::Message(
                    chat, exclusions, additional, message,
                ))
                .await
            {
                return Err(anyhow::anyhow!("Dispatcher Event::Group Error: {e}"));
            };
        }
        Event::Chat(action) => {
            if let Err(e) = group_sender
                .send_async(event_loop::Group::Chat(action))
                .await
            {
                return Err(anyhow::anyhow!("Dispatcher Event::Group Error: {e}"));
            };
        }
    }

    Ok(())
}
// }
