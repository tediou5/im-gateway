#[derive(Debug)]
pub(crate) struct Client {
    topics: std::sync::Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, crate::TokioSender<rskafka::record::Record>>,
        >,
    >,
    inner: std::sync::Arc<rskafka::client::Client>,
    local_addr: String,
    config: crate::config::Kafka,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            topics: self.topics.clone(),
            inner: self.inner.clone(),
            config: self.config.clone(),
            local_addr: self.local_addr.clone(),
        }
    }
}

impl Client {
    pub(crate) async fn new(
        local_addr: String,
        config: crate::config::Kafka,
    ) -> anyhow::Result<Self> {
        use rskafka::client::ClientBuilder;

        // get partition client
        let client = std::sync::Arc::new(
            ClientBuilder::new(config.addrs.clone())
                .build()
                .await
                .unwrap(),
        );

        Ok(Self {
            topics: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            inner: client,
            config,
            local_addr,
        })
    }

    fn get_partition_client(
        &self,
        topic: &str,
    ) -> anyhow::Result<rskafka::client::partition::PartitionClient> {
        Ok(self.inner.partition_client(topic, 0)?)
    }

    fn handle(
        producer: rskafka::client::partition::PartitionClient,
        config: &crate::config::Kafka,
    ) -> crate::TokioSender<rskafka::record::Record> {
        use rskafka::client::producer::{aggregator::RecordAggregator, BatchProducerBuilder};
        use std::time::Duration;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<rskafka::record::Record>();
        let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        // let (tx, rx) = tokio::sync::mpsc::channel::<rskafka::record::Record>(10240);
        // let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);

        // construct batch producer
        let mut producer = BatchProducerBuilder::new(producer.into());

        if let Some(linger) = config.producer.linger {
            producer = producer.with_linger(Duration::from_millis(linger));
        }

        let producer = producer.build(RecordAggregator::new(
            config.producer.max_batch_size, // maximum bytes
        ));
        let producer = std::sync::Arc::new(producer);
        tokio::task::spawn(async move {
            use tokio_stream::StreamExt as _;

            while let Some(message) = rx.next().await {
                let p = producer.clone();
                tokio::task::spawn(async move {
                    Self::_handle(p, message).await;
                });
            }
        });

        tx
    }

    async fn _handle<M: Into<rskafka::record::Record>>(
        producer: std::sync::Arc<
            rskafka::client::producer::BatchProducer<
                rskafka::client::producer::aggregator::RecordAggregator,
            >,
        >,
        message: M,
    ) {
        producer.produce(message.into()).await.unwrap();
    }

    pub(crate) async fn produce<M: Into<rskafka::record::Record>>(
        &self,
        topic: String,
        message: M,
    ) -> anyhow::Result<()> {
        let client = self.clone();
        let mut topics = self.topics.lock().await;
        let producer = topics.entry(topic).or_insert_with_key(|topic| {
            Self::handle(
                client.get_partition_client(topic.as_str()).unwrap(),
                &self.config,
            )
        });
        producer.send(message.into())?;
        Ok(())
    }

    pub(crate) async fn consume<F, U>(self, redis_config: crate::config::Redis, op: F)
    where
        F: Fn(rskafka::record::RecordAndOffset, crate::redis::Client, Client) -> U,
        U: std::future::Future<Output = ()>,
    {
        use futures::StreamExt;
        use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};

        let partition_client = self
            .inner
            .partition_client(format!("proxy:{}", self.local_addr), 0)
            .unwrap();

        // construct stream consumer
        let mut stream = StreamConsumerBuilder::new(partition_client.into(), StartOffset::Latest)
            .with_min_batch_size(self.config.consumer.min_batch_size)
            .with_max_batch_size(self.config.consumer.max_batch_size)
            .with_max_wait_ms(self.config.consumer.max_wait_ms)
            .build();

        // consume data
        let redis = crate::redis::Client::new(redis_config.addrs).await;
        if let Ok(redis) = redis {
            while let Some(Ok((record, _high_water_mark))) = stream.next().await {
                op(record, redis.clone(), self.clone() /* KafkaClient */).await
            }
        }
    }
}
