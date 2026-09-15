use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use smol::channel::Sender;

use crate::kafka::config::viewer_consumer_config;
use crate::kafka::metadata::fetch_topic;
use crate::model::message::MessageRecord;

const BATCH_MAX_MESSAGES: usize = 256;
const BATCH_MAX_AGE: Duration = Duration::from_millis(16);
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    Newest(i64),
    Latest,
    Beginning,
    Offset(i64),
    Timestamp(i64),
}

pub enum ConsumeEvent {
    Assigned(Vec<i32>),
    Batch(Vec<MessageRecord>),
    Eof(i32),
    Error(String),
}

pub struct ConsumeSession {
    stop: Arc<AtomicBool>,
}

impl ConsumeSession {
    pub fn start(
        base: &ClientConfig,
        topic: String,
        partitions: Vec<i32>,
        start: StartFrom,
        tx: Sender<ConsumeEvent>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let config = viewer_consumer_config(base);
        let thread_stop = stop.clone();
        let thread_name = format!("kafka-consume-{topic}");
        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || run(config, topic, partitions, start, tx, thread_stop))
            .expect("spawn consume thread");
        Self { stop }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl Drop for ConsumeSession {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run(
    config: ClientConfig,
    topic: String,
    partitions: Vec<i32>,
    start: StartFrom,
    tx: Sender<ConsumeEvent>,
    stop: Arc<AtomicBool>,
) {
    let consumer: BaseConsumer = match config.create() {
        Ok(consumer) => consumer,
        Err(err) => {
            let _ = tx.send_blocking(ConsumeEvent::Error(format!("consumer: {err}")));
            return;
        }
    };
    let partitions = if partitions.is_empty() {
        match fetch_topic(&consumer, &topic, METADATA_TIMEOUT) {
            Ok(Some(info)) => info.partition_ids(),
            Ok(None) => {
                let _ = tx.send_blocking(ConsumeEvent::Error(format!("topic {topic} not found")));
                return;
            }
            Err(err) => {
                let _ = tx.send_blocking(ConsumeEvent::Error(format!("metadata: {err}")));
                return;
            }
        }
    } else {
        partitions
    };
    let assignment = match build_assignment(&consumer, &topic, &partitions, start) {
        Ok(tpl) => tpl,
        Err(err) => {
            let _ = tx.send_blocking(ConsumeEvent::Error(format!("offsets: {err}")));
            return;
        }
    };
    if let Err(err) = consumer.assign(&assignment) {
        let _ = tx.send_blocking(ConsumeEvent::Error(format!("assign: {err}")));
        return;
    }
    if tx.send_blocking(ConsumeEvent::Assigned(partitions)).is_err() {
        return;
    }

    let topic_name: Arc<str> = Arc::from(topic.as_str());
    let mut batch: Vec<MessageRecord> = Vec::with_capacity(BATCH_MAX_MESSAGES);
    let mut batch_started = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        let poll_timeout = if batch.is_empty() {
            Duration::from_millis(100)
        } else {
            Duration::from_millis(5)
        };
        match consumer.poll(poll_timeout) {
            Some(Ok(message)) => {
                if batch.is_empty() {
                    batch_started = Instant::now();
                }
                batch.push(record_from(&message, &topic_name));
                let batch_ready = batch.len() >= BATCH_MAX_MESSAGES || batch_started.elapsed() >= BATCH_MAX_AGE;
                if batch_ready && !flush(&tx, &mut batch) {
                    break;
                }
            }
            Some(Err(KafkaError::PartitionEOF(partition))) => {
                if !flush(&tx, &mut batch) {
                    break;
                }
                if tx.send_blocking(ConsumeEvent::Eof(partition)).is_err() {
                    break;
                }
            }
            Some(Err(err)) => {
                if tx.send_blocking(ConsumeEvent::Error(err.to_string())).is_err() {
                    break;
                }
            }
            None => {
                if !flush(&tx, &mut batch) {
                    break;
                }
            }
        }
    }
    let _ = consumer.unassign();
}

fn flush(tx: &Sender<ConsumeEvent>, batch: &mut Vec<MessageRecord>) -> bool {
    if batch.is_empty() {
        return true;
    }
    let ready = std::mem::replace(batch, Vec::with_capacity(BATCH_MAX_MESSAGES));
    tx.send_blocking(ConsumeEvent::Batch(ready)).is_ok()
}

fn build_assignment(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
    start: StartFrom,
) -> anyhow::Result<TopicPartitionList> {
    let mut tpl = TopicPartitionList::new();
    match start {
        StartFrom::Newest(count) => {
            for &p in partitions {
                let (low, high) = consumer.fetch_watermarks(topic, p, METADATA_TIMEOUT)?;
                let start = (high - count.max(0)).max(low);
                tpl.add_partition_offset(topic, p, Offset::Offset(start))?;
            }
        }
        StartFrom::Latest => {
            for &p in partitions {
                tpl.add_partition_offset(topic, p, Offset::End)?;
            }
        }
        StartFrom::Beginning => {
            for &p in partitions {
                tpl.add_partition_offset(topic, p, Offset::Beginning)?;
            }
        }
        StartFrom::Offset(offset) => {
            for &p in partitions {
                tpl.add_partition_offset(topic, p, Offset::Offset(offset.max(0)))?;
            }
        }
        StartFrom::Timestamp(timestamp_ms) => {
            let mut query = TopicPartitionList::new();
            for &p in partitions {
                query.add_partition_offset(topic, p, Offset::Offset(timestamp_ms.max(0)))?;
            }
            let resolved = consumer.offsets_for_times(query, METADATA_TIMEOUT)?;
            for element in resolved.elements() {
                let offset = match element.offset() {
                    Offset::Offset(n) => Offset::Offset(n),
                    _ => Offset::End,
                };
                tpl.add_partition_offset(topic, element.partition(), offset)?;
            }
        }
    }
    Ok(tpl)
}

fn record_from(message: &BorrowedMessage<'_>, topic: &Arc<str>) -> MessageRecord {
    let headers: Vec<(&str, Option<&[u8]>)> = message
        .headers()
        .map(|headers| headers.iter().map(|header| (header.key, header.value)).collect())
        .unwrap_or_default();
    MessageRecord::new(
        topic.clone(),
        message.partition(),
        message.offset(),
        message.timestamp().to_millis(),
        message.key(),
        message.payload(),
        &headers,
    )
}
