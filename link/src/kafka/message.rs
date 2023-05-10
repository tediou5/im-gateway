#[derive(Debug)]
pub(crate) struct VecValue(pub(crate) Vec<u8>);

impl From<VecValue> for rskafka::record::Record {
    fn from(value: VecValue) -> Self {
        use time::OffsetDateTime;

        Self {
            key: None,
            value: Some(value.0),
            headers: std::collections::BTreeMap::new(),
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}
