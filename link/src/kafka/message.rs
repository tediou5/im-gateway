use crate::linker;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(untagged)]
pub(crate) enum Message {
    Private(String /* recv */, linker::Message),
    Group(
        String,                            /* chat */
        std::collections::HashSet<String>, /* exclusions */
        std::collections::HashSet<String>, /* additional */
        linker::Message,
    ),
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
                String, /* recv */
                std::collections::HashMap<String, serde_json::Value>,
            ),
            Group(
                String,                            /* chat */
                std::collections::HashSet<String>, /* exclusions */
                std::collections::HashSet<String>, /* additional */
                std::collections::HashMap<String, serde_json::Value>,
            ),
        }
        let rskafka::record::Record { value, .. } = value;
        let value = String::from_utf8(value.ok_or(anyhow::anyhow!("value is empty"))?)?;
        let mut value: TempMessage = serde_json::from_str(value.as_str())
            .inspect_err(|e| tracing::error!("serde temp message error: {e}"))?;
        let temp_message = match &mut value {
            TempMessage::Private(_, temp_message) | TempMessage::Group(.., temp_message) => {
                temp_message
            }
        };

        // parse Message.msgId
        let msg_id = temp_message
            .get("msgId")
            .ok_or(anyhow::anyhow!("content not exist"))?
            .clone();
        let msg_id: String = serde_json::from_value(msg_id)?;
        let msg_id = msg_id.as_bytes();
        let msg_id = serde_json::to_value(msg_id)?;

        // parse Message.content
        let content = temp_message
            .get("content")
            .ok_or(anyhow::anyhow!("content not exist"))?
            .clone();

        let content: String = serde_json::from_value(content)?;
        let content: linker::Content = serde_json::from_str(content.as_str())
            .inspect_err(|e| tracing::error!("serde content error: {e}"))?;
        let content = serde_json::to_value(content)?;

        temp_message.insert("content".to_string(), content);
        temp_message.insert("msgId".to_string(), msg_id);

        let temp = serde_json::to_value(temp_message)?;
        let message: linker::Message = serde_json::from_value(temp)
            .inspect_err(|e| tracing::error!("serde message error: {e}"))?;
        let message = match value {
            TempMessage::Private(recv, _) => Message::Private(recv, message),
            TempMessage::Group(chat, exclusions, additional, _) => {
                Message::Group(chat, exclusions, additional, message)
            }
        };
        Ok(message)
    }
}
