use fred::{pool::RedisPool, prelude::*};

pub(crate) struct Client {
    local_address: String,
    inner: fred::pool::RedisPool,
    heartbeat_interval: usize,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("local_address", &self.local_address)
            .finish()
    }
}

impl Client {
    const LINKERS_KEY: &str = "linkers";

    pub(crate) async fn new(local_address: String, config: crate::config::Redis) -> anyhow::Result<Self> {
        let perf = PerformanceConfig::default();
        let policy = ReconnectPolicy::default();
        let pool = RedisPool::new(RedisConfig::from_url(config.addrs.as_str())?, Some(perf), Some(policy), 100)?;
        // let client = RedisClient::new(config, Some(perf), Some(policy));

        let _ = pool.connect();
        pool.wait_for_connect().await?;

        //add into links list
        pool.sadd(Self::LINKERS_KEY, local_address.as_str()).await?;

        Ok(Self {
            local_address,
            inner: pool,
            heartbeat_interval: config.heartbeat_interval,
        })
    }

    fn get_router_key(chat: &str) -> String {
        format!("router:{chat}")
    }

    pub(crate) async fn regist(&self, chats: Vec<String>) -> anyhow::Result<()> {
        let pipe = self.inner.pipeline();
        for chat in chats {
            pipe.sadd(
                Self::get_router_key(chat.as_str()),
                self.local_address.as_str(),
            )
            .await?;
        }
        pipe.all().await?;
        Ok(())
    }

    pub(crate) async fn heartbeat(&self, pin: String) -> anyhow::Result<()> {
        // let now = std::time::SystemTime::now()
        // .duration_since(std::time::SystemTime::UNIX_EPOCH)
        // .unwrap()
        // .as_secs();
        self.inner.set(pin, (), Some(Expiration::EX(self.heartbeat_interval as i64)), None, false).await?;

        Ok(())
    }
}
