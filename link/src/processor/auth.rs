#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
pub(super) struct Response {
    pub(super) code: String,
    pub(super) data: Data,
    pub(super) message: String,
}

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
pub(super) struct Data {
    #[serde(rename(serialize = "baseInfo", deserialize = "baseInfo"))]
    pub(super) base_info: BaseInfo,
    pub(super) chats: Vec<String>,
}

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
pub(super) struct BaseInfo {
    pub(super) pin: String,
    #[serde(flatten)]
    pub(super) _ext: std::collections::HashMap<String, serde_json::Value>,
}