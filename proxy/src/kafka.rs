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

    fn handle(&self, topic: String) -> crate::TokioSender<rskafka::record::Record> {
        use rskafka::client::producer::{aggregator::RecordAggregator, BatchProducerBuilder};
        use std::time::Duration;

        let client = self.inner.clone();
        let config = self.config.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<rskafka::record::Record>();
        let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        tokio::task::spawn(async move {
            use tokio_stream::StreamExt as _;

            let producer = client
                .partition_client(
                    topic,
                    0,
                    rskafka::client::partition::UnknownTopicHandling::Retry,
                )
                .await
                .unwrap();
            // construct batch producer
            let mut producer = BatchProducerBuilder::new(producer.into());

            if let Some(linger) = config.producer.linger {
                producer = producer.with_linger(Duration::from_millis(linger));
            }

            let producer = producer.build(RecordAggregator::new(
                config.producer.max_batch_size, // maximum bytes
            ));
            let producer = std::sync::Arc::new(producer);

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
        let mut topics = self.topics.lock().await;
        let producer = topics
            .entry(topic)
            .or_insert_with_key(|topic| self.handle(topic.to_string()));
        producer.send(message.into())?;
        crate::axum_handler::PRODUCE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub(crate) async fn consume<F, U>(self, redis_config: crate::config::Redis, op: F)
    where
        F: Fn(rskafka::record::RecordAndOffset, crate::redis::Client, Client) -> U,
        U: std::future::Future<Output = ()>,
    {
        use futures::StreamExt;
        use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};

        let topic = format!("proxy-{}", self.local_addr);

        let controller_client = self.inner.controller_client().unwrap();
        let _ = controller_client
            .create_topic(topic.as_str(), 1, 1, 500)
            .await;

        let partition_client = self
            .inner
            .partition_client(
                topic,
                0,
                rskafka::client::partition::UnknownTopicHandling::Retry,
            )
            .await
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
                crate::axum_handler::CONSUME_COUNT
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                op(record, redis.clone(), self.clone() /* KafkaClient */).await
            }
        }
    }
}
