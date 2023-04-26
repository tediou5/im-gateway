pub(crate) struct MessageCodec;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct Message {
    pub(crate) length: i32,
    #[serde(rename(deserialize = "msgId", serialize = "msgId"))]
    pub(crate) msg_id: [u8; 32],
    pub(crate) timestamp: i64,
    pub(crate) content: Content,
}

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
}

// impl TryFrom<std::collections::HashMap<String, serde_json::Value>> for Message {
//     type Error = anyhow::Error;

//     fn try_from(mut value: std::collections::HashMap<String, serde_json::Value>) -> anyhow::Result<Self> {
//         let length = value.remove("length").ok_or(anyhow::anyhow!("miss match field length"))?;
//         let msg_id = value.remove("msgId").ok_or(anyhow::anyhow!("miss match field msgId"))?;
//         let timestamp = value.remove("timestamp").ok_or(anyhow::anyhow!("miss match field timestamp"))?;

//         let content = value.remove("content").ok_or(anyhow::anyhow!("miss match field content"))?;
//         let content: String = serde_json::from_value(content)?;



//         todo!()
//     }
// }

// FIXME: if message is too large, the value is empty: Some([])
impl From<Message> for rskafka::record::Record {
    fn from(value: Message) -> Self {
        use time::OffsetDateTime;

        let mut codec = MessageCodec {};
        let mut dst = bytes::BytesMut::new();

        use tokio_util::codec::Encoder as _;
        let _ = codec.encode(value, &mut dst);

        Self {
            key: None,
            value: Some(dst.to_vec()),
            headers: std::collections::BTreeMap::new(),
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}

const MAX: usize = 8 * 1024 * 1024;

impl tokio_util::codec::Encoder<Message> for MessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
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

        // Convert the length into a byte array.
        // The cast to u32 cannot overflow due to the length check above.
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

impl tokio_util::codec::Encoder<std::sync::Arc<Vec<Message>>> for MessageCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        items: std::sync::Arc<Vec<Message>>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        for item in items.iter() {
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

            // Convert the length into a byte array.
            // The cast to u32 cannot overflow due to the length check above.
            let timestamp_bytes = i64::to_be_bytes(timestamp);

            // Reserve space in the buffer.
            dst.reserve(44 + length as usize);

            // Write Message to the buffer.
            dst.extend_from_slice(&len_bytes);
            dst.extend_from_slice(&item.msg_id);
            dst.extend_from_slice(&timestamp_bytes);
            dst.extend_from_slice(&content_vec);
        }
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
