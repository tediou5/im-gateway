pub(crate) use message::VecValue;

mod message;
#[derive(Debug)]
pub(crate) struct Client {
    bussiness_client: crate::TokioSender<rskafka::record::Record>,
    inner: std::sync::Arc<rskafka::client::Client>,
    local_addr: String,
    config: crate::config::Kafka,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            bussiness_client: self.bussiness_client.clone(),
            inner: self.inner.clone(),
            local_addr: self.local_addr.clone(),
            config: self.config.clone(),
        }
    }
}

impl Client {
    pub(crate) async fn new(
        local_addr: &str,
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
        let controller_client = client.controller_client()?;
        let _ = controller_client.create_topic(local_addr, 1, 1, 500).await;
        let bussiness_client = client
            .partition_client(
                config.producer.business_topic.as_str(),
                config.producer.business_partition,
                rskafka::client::partition::UnknownTopicHandling::Retry,
            )
            .await?;
        let tx = Self::handle(bussiness_client, &config);
        Ok(Self {
            bussiness_client: tx,
            inner: client,
            local_addr: local_addr.to_string(),
            config,
        })
    }

    fn handle(
        producer: rskafka::client::partition::PartitionClient,
        config: &crate::config::Kafka,
    ) -> crate::TokioSender<rskafka::record::Record> {
        use rskafka::client::producer::{aggregator::RecordAggregator, BatchProducerBuilder};
        use std::time::Duration;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<rskafka::record::Record>();
        let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

        // construct batch producer
        let mut producer = BatchProducerBuilder::new(producer.into());

        if let Some(linger) = config.producer.linger {
            producer = producer.with_linger(Duration::from_millis(linger));
        }

        let producer = producer.build(RecordAggregator::new(
            config.producer.max_batch_size, // maximum bytes
        ));

        tokio::task::spawn_local(async move {
            use tokio_stream::StreamExt as _;

            let producer = std::sync::Arc::new(producer);
            while let Some(message) = rx.next().await {
                let p = producer.clone();
                tokio::task::spawn_local(async move {
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
        if let Err(e) = producer.produce(message.into()).await {
            tracing::error!("Kafka Produce error: {e:?}")
        };
    }

    pub(crate) async fn consume<F, U>(self, op: F) -> anyhow::Result<()>
    where
        F: Fn(rskafka::record::RecordAndOffset) -> U,
        U: std::future::Future<Output = anyhow::Result<()>>,
    {
        use futures::StreamExt;
        use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};

        let partition_client = self
            .inner
            .partition_client(
                self.local_addr.as_str(),
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
        while let Some(Ok((record, _high_water_mark))) = stream.next().await {
            op(record).await?
        }
        Ok(())
    }

    pub(crate) async fn produce<M>(&self, message: M) -> anyhow::Result<()>
    where
        M: Into<rskafka::record::Record> + std::fmt::Debug,
    {
        tracing::trace!("kafka produce: {message:?}");
        crate::axum_handler::KAFKA_PRODUCE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bussiness_client.send(message.into())?;
        Ok(())
    }
}
