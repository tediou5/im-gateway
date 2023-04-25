#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Config {
    pub(crate) tcp: Tcp,
    pub(crate) kafka: Kafka,
    pub(crate) redis: Redis,
}

impl Config {
    pub(crate) fn init() -> Config {
        let config_text = std::fs::read_to_string("./config.toml").unwrap();
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
pub(crate) struct Redis {
    pub(crate) addrs: String,
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
    pub(crate) business_partition: Option<u64>,
}

#[derive(serde_derive::Deserialize, Debug, Clone)]
pub(crate) struct Consumer {
    pub(crate) min_batch_size: i32,
    pub(crate) max_batch_size: i32,
    pub(crate) max_wait_ms: i32,
}
