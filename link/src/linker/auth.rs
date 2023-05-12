use super::Content;

pub(super) async fn auth(app_id: &str, token: &str, platform: &str) -> anyhow::Result<Response> {
    let auth_url = crate::AUTH_URL
        .get()
        .ok_or(anyhow::anyhow!("Config Error: auth_url not exist"))?;
    let http = crate::HTTP_CLIENT
        .get()
        .ok_or(anyhow::anyhow!("System Error: http client not exist"))?;

    Ok(http
        .post(auth_url)
        .json(&serde_json::json!({
            "appId": app_id,
            "token": token,
            "platform": platform,
        }))
        .send()
        .await?
        .json::<Response>()
        .await?)
}

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
pub(super) struct BaseInfo {
    pub(super) pin: String,
    #[serde(flatten)]
    pub(super) _ext: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
pub(super) struct Data {
    #[serde(rename(serialize = "baseInfo", deserialize = "baseInfo"))]
    pub(super) base_info: BaseInfo,
    pub(super) chats: Vec<String>,
}

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
pub(super) struct Response {
    pub(super) code: String,
    pub(super) data: Option<Data>,
    pub(super) message: String,
}

impl Response {
    pub(super) async fn check<F, U>(
        self,
        app_id: String,
        platform: String,
        platform_op: F,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(String, super::Message) -> U,
        U: std::future::Future<Output = anyhow::Result<super::Platform>>,
    {
        tracing::trace!("auth body: {:#?}", self);

        if let "0" = self.code.as_str() &&
        let Some(Data { base_info, chats }) = self.data &&
        // let Some(redis_client) = crate::REDIS_CLIENT.get() &&
        let Some(dispatcher) = crate::DISPATCHER.get() {
            let id = uuid::Uuid::new_v4().to_string();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                .as_secs() as i64;

            let content =
                Content::new_base_info_content(app_id.as_str(), id.as_str(), timestamp, &base_info);
            let message = (id, content).into();

            let chats = chats.into_iter().map(|chat| chat.into()).collect();
            // redis_client.regist(&chats).await?;
            match dispatcher.send(crate::processor::Event::Login(
                base_info.pin.clone(),
                chats,
                platform_op(platform, message).await?,
            )).await {
                Ok(_) => Ok(()),
                Err(_) => Err(anyhow::anyhow!("System Error: login failed")),
            }
        } else {
            // Authorization Error, close connection
            // FIXME: send error message to client and close connection
            tracing::error!("Authorization Error");
            Err(anyhow::anyhow!("Authorization Error"))
        }
    }
}
