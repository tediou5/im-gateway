#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "protocol", content = "data")]
pub(crate) enum Content {
    Heart {
        #[serde(skip_serializing_if = "Option::is_none")]
        status: Option<i32>,
    },
    Connect {
        #[serde(
            default = "gen_id",
            rename(serialize = "traceId", deserialize = "traceId")
        )]
        trace_id: u64,
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
    pub(crate) async fn handle_auth<F, U>(self, platform_op: F) -> anyhow::Result<()>
    where
        F: FnOnce(String) -> U,
        U: std::future::Future<Output = anyhow::Result<crate::linker::Platform>>,
    {
        if let Content::Connect {
            trace_id,
            token,
            platform,
            ..
        } = &self
        {
            let uid = crate::linker::protocol::token::parse_user_token(token.as_str())?;
            let platform = platform.to_lowercase();
            let dispatcher = crate::DISPATCHER.get().unwrap();
            let kafka = crate::KAFKA_CLIENT.get().unwrap();
            dispatcher
                .send(crate::processor::Event::Connect(
                    *trace_id,
                    uid,
                    platform_op(platform).await?,
                ))
                .await
                .map_err(|e| anyhow::anyhow!("system error: dispatcher send error: {e}"))?;

            let message: crate::kafka::VecValue = self.to_vec()?.into();
            kafka
                .produce(message)
                .await
                .map_err(|e| anyhow::anyhow!("kafka error: produce error: {e}"))?;
        }
        Ok(())
    }

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

fn gen_id() -> u64 {
    crate::snowflake::SnowflakeIdWorkerInner::new(1, 1)
        .unwrap()
        .next_id()
        .unwrap()
}

#[cfg(test)]
mod test {
    #[test]
    fn serialize_connect_with_default_id() {
        let conn_content = serde_json::json!({
            "data": {
                "appId": "appid",
                "token": "token",
                "platform": "app",
            },
            "protocol": "Connect"
        });
        let conn: super::Content = serde_json::from_value(conn_content.clone()).unwrap();
        if let super::Content::Connect { trace_id, .. } = conn {
            assert_ne!(trace_id, 0);
        } else {
            panic!("serde error")
        };
    }
}
