pub(crate) struct MessageCodec;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "protocol", content = "data")]
pub(crate) enum Content {
    Heart {
        #[serde(skip_serializing_if = "Option::is_none")]
        status: Option<i32>,
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
    pub(crate) fn new_base_info_content(
        app_id: &str,
        id: &str,
        timestamp: i64,
        base_info: &crate::linker::auth::BaseInfo,
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

    pub(crate) async fn handle_auth<F, U>(self, platform_op: F) -> anyhow::Result<()>
    where
        F: FnOnce(String, Message) -> U,
        U: std::future::Future<Output = anyhow::Result<crate::linker::Login>>,
    {
        if let Content::Connect {
            app_id,
            token,
            platform,
        } = self
        {
            crate::linker::auth::auth(app_id.as_str(), token.as_str(), platform.as_str())
                .await?
                .check(app_id, platform.to_lowercase(), platform_op)
                .await?;
        }

        Ok(())
    }
}

impl TryFrom<&[u8]> for Content {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        // Convert the data to a string, or fail if it is serde_json error.
        Ok(serde_json::from_slice(value)?)
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

impl TryFrom<&[u8]> for Message {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 44 {
            // Not enough data to read length marker.
            return Err(anyhow::anyhow!(
                "Invalid Data: length must bigger than 44 bytes"
            ));
        }

        // Read length marker.
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&value[..4]);
        let length = i32::from_be_bytes(length_bytes);
        let length_usize = length as usize;

        // Check that the length is not too large to avoid a denial of
        // service attack where the server runs out of memory.
        // if length_usize > MAX {
        //     return Err(std::io::Error::new(
        //         std::io::ErrorKind::InvalidData,
        //         format!("Frame of length {} is too large.", length),
        //     ));
        // }

        if value.len() < 44 + length_usize {
            // The full string has not yet arrived.
            //
            return Err(anyhow::anyhow!(
                "Invalid Data: length must bigger than 44 + {length_usize} bytes"
            ));
        }

        // Read msg_id marker.
        let mut msg_id = [0u8; 32];
        msg_id.copy_from_slice(&value[4..36]);

        let mut timestamp_bytes = [0u8; 8];
        timestamp_bytes.copy_from_slice(&value[36..44]);
        let timestamp = i64::from_be_bytes(timestamp_bytes);

        // Use advance to modify src such that it no longer contains
        // this frame.
        let content = &value[44..(44 + length_usize)];

        // Convert the data to a string, or fail if it is serde_json error.
        let content: Content = serde_json::from_slice(content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(Message {
            length,
            msg_id,
            timestamp,
            content,
        })
    }
}

impl From<&Message> for Vec<u8> {
    fn from(val: &Message) -> Self {
        use tokio_util::codec::Encoder as _;

        let mut codec = crate::linker::MessageCodec {};
        let mut dst = bytes::BytesMut::new();
        let _ = codec.encode(val, &mut dst);
        dst.to_vec()
    }
}

impl From<&Message> for std::sync::Arc<Vec<u8>> {
    fn from(val: &Message) -> Self {
        use tokio_util::codec::Encoder as _;

        let mut codec = crate::linker::MessageCodec {};
        let mut dst = bytes::BytesMut::new();
        let _ = codec.encode(val, &mut dst);
        std::sync::Arc::new(dst.to_vec())
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

impl From<Message> for rskafka::record::Record {
    fn from(value: Message) -> Self {
        let mut codec = MessageCodec {};
        let mut dst = bytes::BytesMut::new();

        use tokio_util::codec::Encoder as _;
        let _ = codec.encode(&value, &mut dst);

        Self {
            key: None,
            value: Some(dst.to_vec()),
            headers: std::collections::BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        }
    }
}

// const MAX: usize = 8 * 1024 * 1024;

impl tokio_util::codec::Encoder<Vec<u8>> for MessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Vec<u8>, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        dst.extend(item.iter());
        Ok(())
    }
}

impl tokio_util::codec::Encoder<&Vec<Message>> for MessageCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        items: &Vec<Message>,
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
        // if 44 + length as usize > MAX {
        //     return Err(std::io::Error::new(
        //         std::io::ErrorKind::InvalidData,
        //         format!("Frame of content length {} is too large.", length),
        //     ));
        // }
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
    type Item = Vec<u8>;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 44 {
            // Not enough data to read length marker.
            return Ok(None);
        }

        // Read length marker.
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = i32::from_be_bytes(length_bytes) as usize;
        // let length_usize = length as usize;

        let length = length + 44;

        if src.len() < length {
            // The full string has not yet arrived.
            //
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(length - src.len());

            // We inform the Framed that we need more bytes to form the next
            // frame.
            return Ok(None);
        }

        use bytes::Buf as _;

        let mut dst = vec![0; length];
        dst.copy_from_slice(&src[0..length]);

        src.advance(length);
        Ok(Some(dst))
    }
}
