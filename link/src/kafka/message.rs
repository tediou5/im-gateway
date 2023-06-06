#[derive(Debug)]
pub(crate) struct VecValue(pub(crate) Vec<u8>);
impl From<Vec<u8>> for VecValue {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<VecValue> for rskafka::record::Record {
    fn from(value: VecValue) -> Self {
        Self {
            key: None,
            value: Some(value.0),
            headers: std::collections::BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        }
    }
}
