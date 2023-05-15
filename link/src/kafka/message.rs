#[derive(Debug)]
pub(crate) struct VecValue(pub(crate) Vec<u8>);

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
