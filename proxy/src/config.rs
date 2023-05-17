#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Config {
    pub(crate) http: Http,
    pub(crate) kafka: Kafka,
    pub(crate) redis: Redis,
    pub(crate) compress: Option<Compress>,
}

impl Config {
    pub(crate) fn init<P: AsRef<std::path::Path>>(path: P) -> Config {
        let config_text = std::fs::read_to_string(path).unwrap();
        toml::from_str(&config_text).unwrap()
    }
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Http {
    pub(crate) port: u16,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Redis {
    pub(crate) addrs: String,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Kafka {
    pub(crate) addrs: Vec<String>,
    pub(crate) producer: Producer,
    pub(crate) consumer: Consumer,
}

#[allow(dead_code)]
#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Producer {
    pub(crate) linger: Option<u64>,
    pub(crate) max_batch_size: usize,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Consumer {
    pub(crate) min_batch_size: i32,
    pub(crate) max_batch_size: i32,
    pub(crate) max_wait_ms: i32,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Compress {
    pub(crate) dict: String,
}
