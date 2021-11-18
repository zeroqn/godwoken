use anyhow::{anyhow, bail, Result};
use futures::future::{self, FutureExt};
use futures::{Stream, StreamExt, TryStreamExt};
use gw_types::packed::L2Transaction;
use gw_types::prelude::Entity;
use rdkafka::consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::AsyncRuntime;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use std::future::Future;
use std::time::{Duration, Instant};

const KAFKA_MESSAGE_TIMEOUT_MS: &str = "5000";
const KAFKA_SESSION_TIMEOUT_MS: &str = "6000";
const KAFKA_MEM_POOL_GROUP_ID: &str = "godwoken-mem-pool-txs";
const KAFKA_MEM_POOL_TXS_TOPIC: &str = "godwoken-mem-pool-txs";

pub struct Kafka {
    producer: FutureProducer,
    consumer: StreamConsumer<DefaultConsumerContext, SmolRuntime>,
}

impl Kafka {
    pub fn build(brokers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", KAFKA_MESSAGE_TIMEOUT_MS)
            .create()?;

        let consumer: StreamConsumer<_, SmolRuntime> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("session.timeout.ms", KAFKA_SESSION_TIMEOUT_MS)
            .set("enable.auto.commit", "false") // Dont auto commit
            .set("auto.offset.reset", "earliest")
            .set("group.id", KAFKA_MEM_POOL_GROUP_ID)
            .create()?;
        consumer.subscribe(&[KAFKA_MEM_POOL_TXS_TOPIC])?;

        Ok(Kafka { producer, consumer })
    }

    pub fn send_tx(&self, tx: &L2Transaction) -> Result<()> {
        // NOTE: txs size are capped by mem pool batch queue and MAX_MEM_BLOCK_TXS, their size
        // should be much smaller than producer queue size, so we dont try here, simply throw
        // error.
        let delivery_status = self.producer.send_result::<Vec<u8>, _>(
            FutureRecord::to(KAFKA_MEM_POOL_TXS_TOPIC).payload(tx.as_slice()),
        );
        if let Err((e, _)) = delivery_status {
            bail!(e);
        }

        Ok(())
    }

    pub async fn get_all_txs_list(&self) -> Result<TopicPartitionList> {
        let msgs: Vec<_> = self.tx_msg_stream().try_collect().await?;

        let mut list = TopicPartitionList::new();
        for msg in msgs {
            list.add_partition_offset(msg.topic(), msg.partition(), Offset::Offset(msg.offset()))?;
        }

        Ok(list)
    }

    pub fn commit_txs_list(&self, list: TopicPartitionList) -> Result<()> {
        self.consumer.commit(&list, CommitMode::Async)?;

        Ok(())
    }

    pub async fn get_all_txs(&mut self) -> Result<Vec<L2Transaction>> {
        let tx_stream = self.tx_msg_stream().map(|maybe_msg| match maybe_msg {
            Ok(msg) => match msg.payload() {
                Some(payload) => L2Transaction::from_slice(payload).map_err(anyhow::Error::from),
                None => Err(anyhow!("unexpected kafka tx msg without payload")),
            },
            Err(err) => Err(anyhow!(err.to_string())),
        });

        tx_stream.try_collect().await
    }

    fn tx_msg_stream(&self) -> impl Stream<Item = Result<BorrowedMessage<'_>>> {
        TryStreamExt::map_err(self.consumer.stream(), |err| anyhow!(err.to_string()))
    }
}

struct SmolRuntime;

impl AsyncRuntime for SmolRuntime {
    type Delay = future::Map<smol::Timer, fn(Instant)>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        smol::spawn(task).detach()
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        FutureExt::map(smol::Timer::after(duration), |_| ())
    }
}
