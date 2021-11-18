use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use gw_types::packed::L2Transaction;
use gw_types::prelude::Entity;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

const KAFKA_MESSAGE_TIMEOUT_MS: &str = "5000";
const KAFKA_SESSION_TIMEOUT_MS: &str = "6000";
const KAFKA_MEM_POOL_GROUP_ID: &str = "godwoken-mem-pool-txs";
const KAFKA_MEM_POOL_TXS_TOPIC: &str = "godwoken-mem-pool-txs";

pub struct Kafka {
    producer: FutureProducer,
    consumer: BaseConsumer<DefaultConsumerContext>,
}

impl Kafka {
    pub fn build(brokers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", KAFKA_MESSAGE_TIMEOUT_MS)
            .create()?;

        let consumer: BaseConsumer<_> = ClientConfig::new()
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

    pub fn get_all_txs_list(&self) -> Result<Option<TopicPartitionList>> {
        let mut list = TopicPartitionList::new();

        while let Some(maybe_msg) = self.consumer.poll(Duration::from_millis(100)) {
            match maybe_msg {
                Ok(msg) => list.add_partition_offset(
                    msg.topic(),
                    msg.partition(),
                    Offset::Offset(msg.offset()),
                )?,
                Err(KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition)) => {
                    return Ok(None)
                }
                Err(err) => return Err(anyhow!(err.to_string())),
            }
        }

        Ok(Some(list))
    }

    pub fn commit_txs_list(&self, list: TopicPartitionList) -> Result<()> {
        self.consumer.commit(&list, CommitMode::Sync)?;

        Ok(())
    }

    pub fn get_all_txs(&self) -> Result<Vec<L2Transaction>> {
        let mut txs = Vec::new();

        while let Some(maybe_msg) = self.consumer.poll(Duration::from_millis(100)) {
            match maybe_msg {
                Ok(msg) => match msg.payload() {
                    Some(payload) => {
                        txs.push(L2Transaction::from_slice(payload).map_err(anyhow::Error::from)?)
                    }
                    None => return Err(anyhow!("unexpected kafka tx msg without payload")),
                },
                Err(KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition)) => {
                    return Ok(vec![])
                }
                Err(err) => return Err(anyhow!(err.to_string())),
            }
        }

        Ok(txs)
    }
}
