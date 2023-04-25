use fred::{pool::RedisPool, prelude::*};

pub(crate) struct Client {
    inner: fred::pool::RedisPool,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish()
    }
}

impl Client {
    const LINKERS_KEY: &str = "linkers";

    pub(crate) async fn new(addrs: String) -> anyhow::Result<Self> {
        let config = RedisConfig::from_url(addrs.as_str())?;
        let perf = PerformanceConfig::default();
        let policy = ReconnectPolicy::default();
        let pool = RedisPool::new(config, Some(perf), Some(policy), 100)?;
        // let client = RedisClient::new(config, Some(perf), Some(policy));

        let _ = pool.connect();
        pool.wait_for_connect().await?;
        Ok(Self { inner: pool })
    }

    fn get_router_key(chat: &str) -> String {
        format!("router:{chat}")
    }

    pub(crate) async fn get_router(
        &self,
        chat: &str,
    ) -> anyhow::Result<std::collections::HashSet<String>> {
        Ok(self.inner.smembers(Self::get_router_key(chat)).await?)
    }

    pub(crate) async fn get_linkers(&self) -> anyhow::Result<std::collections::HashSet<String>> {
        Ok(self.inner.smembers(Self::LINKERS_KEY).await?)
    }
}
