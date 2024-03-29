#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Config {
    pub(crate) tcp: Tcp,
    pub(crate) http: Http,
    pub(crate) kafka: Kafka,
    pub(crate) redis: Redis,
    pub(crate) retry: Retry,
}

impl Config {
    pub(crate) fn init<P: AsRef<std::path::Path>>(path: P) -> Config {
        let config_text = std::fs::read_to_string(path).unwrap();
        toml::from_str(&config_text).unwrap()
    }

    pub(crate) fn get_tcp_addr_str(&self) -> String {
        format!("0.0.0.0:{}", self.tcp.port)
    }
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Tcp {
    pub(crate) port: u16,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Retry {
    // millis
    pub(crate) timeout: usize,
    pub(crate) max_times: usize,
    pub(crate) window_size: usize,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Http {
    pub(crate) port: u16,
    pub(crate) websocket_router: String,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Redis {
    pub(crate) addrs: String,
    pub(crate) heartbeat_interval: i64,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Kafka {
    pub(crate) addrs: Vec<String>,
    pub(crate) producer: Producer,
    pub(crate) consumer: Consumer,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Producer {
    pub(crate) linger: Option<u64>,
    pub(crate) max_batch_size: usize,
    pub(crate) business_topic: String,
    pub(crate) business_partition: i32,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Consumer {
    pub(crate) min_batch_size: i32,
    pub(crate) max_batch_size: i32,
    pub(crate) max_wait_ms: i32,
}
