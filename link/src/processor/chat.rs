#[derive(Debug, Clone, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    Join(
        std::sync::Arc<String>, /* chat */
        std::collections::HashSet<String>,
    ),
    Leave(
        std::sync::Arc<String>, /* chat */
        std::collections::HashSet<String>,
    ),
    Notice(
        std::sync::Arc<String>, /* chat */
        #[serde(with = "hex")] Vec<u8>,
    ),
}
