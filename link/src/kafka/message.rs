use crate::linker;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(untagged)]
pub(crate) enum Message {
    Private(
        std::collections::HashSet<String>, /* recvs */
        linker::Message,
    ),
    Group(
        String,                            /* chat */
        std::collections::HashSet<String>, /* exclusions */
        std::collections::HashSet<String>, /* additional */
        linker::Message,
    ),
    Chat(chat::Action),
}

impl From<Message> for rskafka::record::Record {
    fn from(value: Message) -> Self {
        use time::OffsetDateTime;

        Self {
            key: None,
            value: serde_json::to_vec(&value).ok(),
            headers: std::collections::BTreeMap::new(),
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}

impl TryFrom<rskafka::record::Record> for Message {
    type Error = anyhow::Error;

    fn try_from(value: rskafka::record::Record) -> anyhow::Result<Self> {
        #[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
        #[serde(untagged)]
        pub(crate) enum TempMessage {
            Private(
                std::collections::HashSet<String>, /* recvs */
                serde_json::Value,
            ),
            Group(
                String,                            /* chat */
                std::collections::HashSet<String>, /* exclusions */
                std::collections::HashSet<String>, /* additional */
                serde_json::Value,
            ),
            Chat(chat::Action),
        }
        let rskafka::record::Record { value, .. } = value;
        let value = String::from_utf8(value.ok_or(anyhow::anyhow!("value is empty"))?)?;
        let value: TempMessage = serde_json::from_str(value.as_str())
            .inspect_err(|e| tracing::error!("serde temp message error: {e}"))?;
        let temp_message = match &value {
            TempMessage::Private(_, temp_message) | TempMessage::Group(.., temp_message) => {
                temp_message.clone()
            }
            TempMessage::Chat(action) => return Ok(Message::Chat(action.clone())),
        };
        let message = temp_message.try_into()?;

        let message = match value {
            TempMessage::Private(recv, _) => Message::Private(recv, message),
            TempMessage::Group(chat, exclusions, additional, _) => {
                Message::Group(chat, exclusions, additional, message)
            }
            TempMessage::Chat(_) => panic!("wtf"),
        };
        Ok(message)
    }
}

pub(crate) mod chat {
    #[derive(Debug, Clone, serde_derive::Deserialize, serde_derive::Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Action {
        Join(String, std::collections::HashSet<String>),
        Leave(String, std::collections::HashSet<String>),
    }
}
