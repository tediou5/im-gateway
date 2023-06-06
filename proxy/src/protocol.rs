#[derive(Debug, Clone, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
#[serde(untagged)]
pub(crate) enum LinkProtocol {
    Private(
        std::collections::HashSet<String>, /* recvs */
        #[serde(with = "hex")] Vec<u8>,    /* content */
    ),
    Group(
        String,                            /* chat */
        std::collections::HashSet<String>, /* exclusions */
        #[serde(with = "hex")] Vec<u8>,    /* content */
    ),
    LoginFailed(u64 /* trace_id */, String /* reason */),
    Login(
        u64,         /* trace_id */
        String,      /* auth_message */
        Vec<String>, /* chats */
    ),
    Chat(chat::Action),
}

impl TryFrom<rskafka::record::Record> for LinkProtocol {
    type Error = anyhow::Error;

    fn try_from(value: rskafka::record::Record) -> anyhow::Result<Self> {
        let rskafka::record::Record { value, .. } = value;
        let value = value.ok_or(anyhow::anyhow!("kafka value is empty"))?;
        Ok(serde_json::from_slice(&value)?)
    }
}

impl From<LinkProtocol> for rskafka::record::Record {
    fn from(value: LinkProtocol) -> Self {
        Self {
            key: None,
            value: serde_json::to_vec(&value).ok(),
            headers: std::collections::BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        }
    }
}

pub(crate) mod chat {
    #[derive(Debug, Clone, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Action {
        Join(String /* chat */, std::collections::HashSet<String>),
        Leave(String /* chat */, std::collections::HashSet<String>),
        Notice(String /* chat */, #[serde(with = "hex")] Vec<u8>),
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::{chat::Action, LinkProtocol};

    #[test]
    fn proto_join() {
        let join = LinkProtocol::Chat(Action::Join(
            "cc_1".to_string(),
            HashSet::from_iter(["uu_1".to_string(), "uu_2".to_string()]),
        ));

        let join_from_json: LinkProtocol =
            serde_json::from_str(r#"{"join": ["cc_1", ["uu_1", "uu_2"]]}"#).unwrap();

        assert_eq!(join, join_from_json);
    }

    #[test]
    fn proto_leave() {
        let leave = LinkProtocol::Chat(Action::Leave(
            "cc_1".to_string(),
            HashSet::from_iter(["uu_1".to_string(), "uu_2".to_string()]),
        ));

        let leave_from_json: LinkProtocol =
            serde_json::from_str(r#"{"leave": ["cc_1", ["uu_1", "uu_2"]]}"#).unwrap();

        assert_eq!(leave, leave_from_json)
    }

    #[test]
    fn proto_login() {
        let login = LinkProtocol::Login(
            1,
            "baseinfo".to_string(),
            vec!["cc1".to_string(), "cc2".to_string()],
        );

        let login_from_json: LinkProtocol =
            serde_json::from_str(r#"[1, "baseinfo", ["cc1", "cc2"]]"#).unwrap();

        assert_eq!(login, login_from_json)
    }

    #[test]
    fn proto_login_failed() {
        let failed = LinkProtocol::LoginFailed(1, "loginfailed".to_string());

        let failed_from_json: LinkProtocol = serde_json::from_str(r#"[1, "loginfailed"]"#).unwrap();

        assert_eq!(failed, failed_from_json)
    }
}
