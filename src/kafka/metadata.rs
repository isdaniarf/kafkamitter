use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
}

impl TopicInfo {
    pub fn is_internal(&self) -> bool {
        self.name.starts_with("__")
    }

    pub fn partition_ids(&self) -> Vec<i32> {
        self.partitions.iter().map(|p| p.id).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterInfo {
    pub brokers: Vec<BrokerInfo>,
    pub topics: Vec<TopicInfo>,
}

pub fn fetch_cluster(consumer: &BaseConsumer, timeout: Duration) -> KafkaResult<ClusterInfo> {
    let metadata = consumer.fetch_metadata(None, timeout)?;
    let brokers = metadata
        .brokers()
        .iter()
        .map(|b| BrokerInfo {
            id: b.id(),
            host: b.host().to_string(),
            port: b.port(),
        })
        .collect();
    let mut topics: Vec<TopicInfo> = metadata
        .topics()
        .iter()
        .map(|t| TopicInfo {
            name: t.name().to_string(),
            partitions: t
                .partitions()
                .iter()
                .map(|p| PartitionInfo {
                    id: p.id(),
                    leader: p.leader(),
                    replicas: p.replicas().to_vec(),
                    isr: p.isr().to_vec(),
                })
                .collect(),
        })
        .collect();
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    for topic in &mut topics {
        topic.partitions.sort_by_key(|p| p.id);
    }
    Ok(ClusterInfo { brokers, topics })
}

pub fn fetch_topic(consumer: &BaseConsumer, topic: &str, timeout: Duration) -> KafkaResult<Option<TopicInfo>> {
    let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
    let found = metadata.topics().iter().find(|t| t.name() == topic).map(|t| TopicInfo {
        name: t.name().to_string(),
        partitions: t
            .partitions()
            .iter()
            .map(|p| PartitionInfo {
                id: p.id(),
                leader: p.leader(),
                replicas: p.replicas().to_vec(),
                isr: p.isr().to_vec(),
            })
            .collect(),
    });
    Ok(found.filter(|t| !t.partitions.is_empty()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermarks {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
}

pub fn fetch_watermarks(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
    timeout: Duration,
) -> KafkaResult<Vec<Watermarks>> {
    partitions
        .iter()
        .map(|&partition| {
            let (low, high) = consumer.fetch_watermarks(topic, partition, timeout)?;
            Ok(Watermarks {
                partition,
                low,
                high,
            })
        })
        .collect()
}
