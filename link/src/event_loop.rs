use std::sync::Arc;

use crate::Sender;
use ahash::{AHashMap, AHashSet};

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct SendRequest {
    pub(crate) pin: String,
    pub(crate) chat: String,
    pub(crate) content: String,
}

impl From<SendRequest> for rskafka::record::Record {
    fn from(value: SendRequest) -> Self {
        use time::OffsetDateTime;

        Self {
            key: None,
            // Handle error
            value: serde_json::to_vec(&value).ok(),
            headers: std::collections::BTreeMap::new(),
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}

impl TryFrom<rskafka::record::Record> for SendRequest {
    type Error = anyhow::Error;

    fn try_from(value: rskafka::record::Record) -> anyhow::Result<Self> {
        let rskafka::record::Record { value, .. } = value;

        Ok(serde_json::from_str(
            String::from_utf8(value.ok_or(anyhow::anyhow!("value is empty"))?)?.as_str(),
        )?)
    }
}

#[derive(Debug)]
pub(super) enum Event {
    // uid, chats
    Regist(String, Vec<String>, Sender),
    // recv_list, content
    Send(SendRequest),
    SendBatch(ahash::AHashMap<String, Vec<SendRequest>>),
}

pub(super) async fn run() -> anyhow::Result<()> {
    // let (collect_tx, collect_rx) = tokio::sync::mpsc::channel::<Event>(10240);
    // let mut collect_rx = tokio_stream::wrappers::ReceiverStream::new(collect_rx);
    let (collect_tx, collect_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut collect_rx = tokio_stream::wrappers::UnboundedReceiverStream::new(collect_rx);
    crate::EVENT_LOOP.set(collect_tx).unwrap();

    let mut users: AHashMap<Arc<String>, Sender> = AHashMap::new();
    let mut chats: AHashMap<String, AHashSet<String>> = AHashMap::new();

    use tokio_stream::StreamExt as _;
    while let Some(event) = collect_rx.next().await {
        _handle(&mut users, &mut chats, event).await?;
    }
    Ok(())
}

async fn _handle(
    users: &mut AHashMap<Arc<String>, Sender>,
    chats: &mut AHashMap<String, AHashSet<String>>,
    event: Event,
) -> anyhow::Result<()> {
    use crate::processor::TcpEvent;
    match event {
        Event::Regist(user, chat_list, sender) => {
            let user = Arc::new(user);
            if let Some(sender) = users.insert(user.clone(), sender) {
                let _ = sender.send(TcpEvent::Close);
            };

            for chat in chat_list {
                // TODO: maybe have a better way to do this
                let member = chats.entry(chat).or_default();
                member.insert(user.to_string());
            }
        }
        Event::Send(SendRequest {
            pin: _,
            chat,
            content,
        }) => {
            if let Some(online) = chats.get_mut(&chat) {
                let content = std::sync::Arc::new(content);
                for recv in online.iter() {
                    if let Some(sender) = users.get(recv) {
                        let _ = sender.send(TcpEvent::Write(content.clone()));
                    };
                }
            };
        }
        Event::SendBatch(chats_batch) => {
            for (chat, batch) in chats_batch.iter() {
                if let Some(online) = chats.get_mut(chat) {
                    let len = batch.len();
                    let contents: String = batch
                        .iter()
                        .map(|request| format!("^^{}^^", request.content))
                        .collect();
                    let contents = std::sync::Arc::new(contents);
                    for recv in online.iter() {
                        if let Some(sender) = users.get(recv) {
                            let _ = sender.send(TcpEvent::WriteBatch(contents.clone(), len as u64));
                        };
                    }
                };
            }
        }
    }
    Ok(())
}
