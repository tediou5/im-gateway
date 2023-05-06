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
    pub(super) async fn check(self, app_id: &str, platform: super::Platform) -> anyhow::Result<()> {
        if let "0" = self.code.as_str() &&
        let Some(Data { base_info, chats }) = self.data &&
        let Some(redis_client) = crate::REDIS_CLIENT.get() &&
        let Some(event_loop) = crate::EVENT_LOOP.get() &&
        let Ok(_) = redis_client.regist(&chats).await &&
        let Ok(_) = event_loop.send(crate::event_loop::Event::Regist(
            base_info.pin.clone(),
            chats,
            platform.clone(),
        )) {
            let id = uuid::Uuid::new_v4().to_string();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            let content =
                Content::new_base_info_content(app_id, id.as_str(), timestamp, &base_info);

            let message: crate::linker::Message = (id, content).into();
            let _ = platform.send_one(std::sync::Arc::new(message));
            Ok(())
        } else {
            // Authorization Error, close connection
                // FIXME: send error message to client and close connection
                tracing::error!("Authorization Error");
                let _ = platform.close();
                Err(anyhow::anyhow!("Authorization Error"))
        }
    }
}
