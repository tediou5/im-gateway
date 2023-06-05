#[derive(Debug, Clone, PartialEq, Eq, serde_derive::Serialize, serde_derive::Deserialize)]
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
    pub(crate) fn pack_message(
        content: &[u8],
        id_worker: &mut crate::snowflake::SnowflakeIdWorkerInner,
    ) -> anyhow::Result<(u64, Vec<u8>)> {
        let flag: u8 = 0b00000001;
        let len = content.len() as u16;
        let trace_id = id_worker.next_id()?;
        let mut dst = bytes::BytesMut::new();

        use bytes::BufMut as _;
        // Reserve space in the buffer.
        dst.reserve((11 + len).into());

        dst.put_bytes(flag, 1);
        dst.put_u16(len);
        dst.put_u64(trace_id);
        dst.extend_from_slice(content);

        Ok((trace_id, dst.to_vec()))
    }

    pub(crate) fn to_vec(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
}

impl TryFrom<&[u8]> for Content {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        // Convert the data to a string, or fail if it is serde_json error.
        Ok(serde_json::from_slice(value)?)
    }
}
