use super::auth;

pub(crate) struct MessageCodec;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "protocol", content = "data")]
pub(crate) enum Content {
    Heart {
        status: i32,
    },
    Connect {
        #[serde(rename(serialize = "appId", deserialize = "appId"))]
        app_id: String,
        token: String,
        platform: String,
    },
    Message {
        #[serde(flatten)]
        _ext: std::collections::HashMap<String, serde_json::Value>,
    },
    Response {
        #[serde(flatten)]
        _ext: std::collections::HashMap<String, serde_json::Value>,
    },
}

impl Content {
    pub(super) fn new_base_info_content(
        app_id: &str,
        id: &str,
        timestamp: i64,
        base_info: &auth::BaseInfo,
    ) -> Content {
        let data = std::collections::HashMap::from([
            ("chatId".to_string(), serde_json::json!("")),
            ("msgFormat".to_string(), serde_json::json!("TEXT")),
            ("msgId".to_string(), serde_json::json!(&id[0..32])),
            (
                "noticeType".to_string(),
                serde_json::json!("USER_BASE_INFO"),
            ),
            (
                "body".to_string(),
                serde_json::json!(serde_json::to_string(&base_info).unwrap_or_default()),
            ),
            ("chatMsgType".to_string(), serde_json::json!("Notice")),
            ("fromId".to_string(), serde_json::json!(&base_info.pin)),
            ("appId".to_string(), serde_json::json!(app_id)),
            ("chatType".to_string(), serde_json::json!("Private")),
            ("timestamp".to_string(), serde_json::json!(timestamp)),
        ]);
        Content::Message { _ext: data }
    }
}

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct Message {
    pub(crate) length: i32,
    #[serde(rename(deserialize = "msgId", serialize = "msgId"))]
    pub(crate) msg_id: [u8; 32],
    pub(crate) timestamp: i64,
    pub(crate) content: Content,
}

impl Message {
    pub(super) async fn handle(self, tx: &crate::Sender) -> anyhow::Result<Option<Self>> {
        let Message { content, .. } = &self;

        match content {
            Content::Heart { .. } => {
                // TODO:
                Ok(None)
            }
            Content::Connect {
                app_id,
                token,
                platform,
            } => {
                auth::auth(app_id.as_str(), token.as_str(), platform.as_str())
                    .await?
                    .check(app_id, tx)
                    .await?;
                Ok(None)
            }
            Content::Message { _ext } => Ok(Some(self)),
            Content::Response { _ext } => Ok(Some(self)),
        }
    }
}

impl From<Content> for Message {
    fn from(content: Content) -> Self {
        let id = uuid::Uuid::new_v4().to_string();

        (id, content).into()
    }
}

impl From<(String, Content)> for Message {
    fn from((id, content): (String, Content)) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        (id, timestamp, content).into()
    }
}

impl From<(String, i64, Content)> for Message {
    fn from((mut id, timestamp, content): (String, i64, Content)) -> Self {
        let content_vec: Vec<u8> = serde_json::to_vec(&content).unwrap();
        let length = content_vec.len() as i32;

        id.remove_matches("-");
        let id = id.as_bytes();
        let mut msg_id = [0u8; 32];
        msg_id.copy_from_slice(&id[0..32]);

        Message {
            length,
            msg_id,
            timestamp,
            content,
        }
    }
}

impl TryFrom<serde_json::Value> for Message {
    type Error = anyhow::Error;

    fn try_from(value: serde_json::Value) -> anyhow::Result<Self> {
        #[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
        struct TempMessage {
            pub(crate) length: i32,
            #[serde(rename(deserialize = "msgId", serialize = "msgId"))]
            pub(crate) msg_id: String,
            pub(crate) timestamp: i64,
            pub(crate) content: String,
        }

        let temp: TempMessage = serde_json::from_value(value)?;
        let content: Content = serde_json::from_str(&temp.content)?;

        let mut msg_id = [0u8; 32];
        msg_id.copy_from_slice(&temp.msg_id.as_bytes()[0..32]);

        Ok(Self {
            length: temp.length,
            msg_id,
            timestamp: temp.timestamp,
            content,
        })
    }
}

// FIXME: if message is too large, the value is empty: Some([])
impl From<Message> for rskafka::record::Record {
    fn from(value: Message) -> Self {
        use time::OffsetDateTime;

        let mut codec = MessageCodec {};
        let mut dst = bytes::BytesMut::new();

        use tokio_util::codec::Encoder as _;
        let _ = codec.encode(&value, &mut dst);

        Self {
            key: None,
            value: Some(dst.to_vec()),
            headers: std::collections::BTreeMap::new(),
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}

const MAX: usize = 8 * 1024 * 1024;

impl tokio_util::codec::Encoder<std::sync::Arc<Vec<u8>>> for MessageCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: std::sync::Arc<Vec<u8>>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        dst.extend(item.iter());
        Ok(())
    }
}

impl tokio_util::codec::Encoder<Vec<Message>> for MessageCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        items: Vec<Message>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        for item in items.iter() {
            let mut codec = MessageCodec {};
            let _ = codec.encode(item, dst);
        }
        Ok(())
    }
}

impl tokio_util::codec::Encoder<&Message> for MessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: &Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let content_vec: Vec<u8> = serde_json::to_vec(&item.content).unwrap();
        let length = content_vec.len() as i32;
        if 44 + length as usize > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of content length {} is too large.", length),
            ));
        }
        let len_bytes = i32::to_be_bytes(length);

        let timestamp = if item.timestamp == 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        } else {
            item.timestamp
        };

        let timestamp_bytes = i64::to_be_bytes(timestamp);

        // Reserve space in the buffer.
        dst.reserve(44 + length as usize);

        // Write Message to the buffer.
        dst.extend_from_slice(&len_bytes);
        dst.extend_from_slice(&item.msg_id);
        dst.extend_from_slice(&timestamp_bytes);
        dst.extend_from_slice(&content_vec);
        Ok(())
    }
}

impl tokio_util::codec::Decoder for MessageCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 44 {
            // Not enough data to read length marker.
            return Ok(None);
        }

        // Read length marker.
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = i32::from_be_bytes(length_bytes);
        let length_usize = length as usize;

        // Check that the length is not too large to avoid a denial of
        // service attack where the server runs out of memory.
        if length_usize > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", length),
            ));
        }

        if src.len() < 44 + length_usize {
            // The full string has not yet arrived.
            //
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(44 + length_usize - src.len());

            // We inform the Framed that we need more bytes to form the next
            // frame.
            return Ok(None);
        }

        // Read msg_id marker.
        let mut msg_id = [0u8; 32];
        msg_id.copy_from_slice(&src[4..36]);

        let mut timestamp_bytes = [0u8; 8];
        timestamp_bytes.copy_from_slice(&src[36..44]);
        let timestamp = i64::from_be_bytes(timestamp_bytes);

        // Use advance to modify src such that it no longer contains
        // this frame.
        let content = &src[44..(44 + length_usize)];

        // Convert the data to a string, or fail if it is serde_json error.
        let content: Content = serde_json::from_slice(content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        use bytes::Buf as _;
        src.advance(44 + length_usize);

        Ok(Some(Message {
            length,
            msg_id,
            timestamp,
            content,
        }))
    }
}
