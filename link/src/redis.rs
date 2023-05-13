use fred::{pool::RedisPool, prelude::*};

pub(crate) struct Client {
    local_address: String,
    inner: fred::pool::RedisPool,
    heartbeat_interval: i64,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("local_address", &self.local_address)
            .finish()
    }
}

impl Client {
    const ROUTER_KEY: &str = "router";
    const LINKERS_KEY: &str = "linkers";
    const HEARTBEAT_KEY: &str = "heartbeat";

    pub(crate) async fn new(
        local_address: String,
        config: crate::config::Redis,
    ) -> anyhow::Result<Self> {
        let perf = PerformanceConfig::default();
        let policy = ReconnectPolicy::default();
        let pool = RedisPool::new(
            RedisConfig::from_url(config.addrs.as_str())?,
            Some(perf),
            Some(policy),
            100,
        )?;
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
        format!("{}:{chat}", Self::ROUTER_KEY)
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
        tracing::info!("[{pin}] heartbeat");
        self.inner
            .set(
                format!("{}:{pin}", Self::HEARTBEAT_KEY),
                "",
                Some(Expiration::EX(self.heartbeat_interval + 1)),
                None,
                false,
            )
            .await?;

        Ok(())
    }

    pub(crate) async fn del_heartbeat(&self, pin: String) -> anyhow::Result<()> {
        tracing::info!("[{pin}] disconnect");
        self.inner.del(pin).await?;

        Ok(())
    }
}
