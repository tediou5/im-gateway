use fred::{pool::RedisPool, prelude::*};

pub(crate) struct Client {
    local_address: String,
    inner: fred::pool::RedisPool,
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

    pub(crate) async fn new(local_address: String, addrs: String) -> anyhow::Result<Self> {
        let config = RedisConfig::from_url(addrs.as_str())?;
        let perf = PerformanceConfig::default();
        let policy = ReconnectPolicy::default();
        let pool = RedisPool::new(config, Some(perf), Some(policy), 100)?;
        // let client = RedisClient::new(config, Some(perf), Some(policy));

        let _ = pool.connect();
        pool.wait_for_connect().await?;

        //add into links list
        pool.sadd(Self::LINKERS_KEY, local_address.as_str()).await?;

        Ok(Self {
            local_address,
            inner: pool,
        })
    }

    fn get_router_key(chat: &str) -> String {
        format!("router:{chat}")
    }

    pub(crate) async fn regist(&self, chats: &Vec<String>) -> anyhow::Result<()> {
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

    // pub(crate) async fn get_router(
    //     &self,
    //     chat: &str,
    // ) -> anyhow::Result<std::collections::HashSet<String>> {
    //     Ok(self.inner.smembers(Self::get_router_key(chat)).await?)
    // }
}
