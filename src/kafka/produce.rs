use std::time::Duration;

use futures::channel::oneshot;
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::error::KafkaResult;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
use rdkafka::Message;

use crate::kafka::config::producer_config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivered {
    pub partition: i32,
    pub offset: i64,
}

pub type DeliveryReply = oneshot::Sender<anyhow::Result<Delivered>>;

pub struct DeliveryContext;

impl ClientContext for DeliveryContext {}

impl ProducerContext for DeliveryContext {
    type DeliveryOpaque = Box<DeliveryReply>;

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, delivery_opaque: Self::DeliveryOpaque) {
        let outcome = match delivery_result {
            Ok(message) => Ok(Delivered {
                partition: message.partition(),
                offset: message.offset(),
            }),
            Err((err, _)) => Err(anyhow::anyhow!("delivery failed: {err}")),
        };
        let _ = delivery_opaque.send(outcome);
    }
}

pub type KafkaProducer = BaseProducer<DeliveryContext>;

pub fn create_producer(base: &ClientConfig) -> KafkaResult<KafkaProducer> {
    producer_config(base).create_with_context(DeliveryContext)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

pub fn produce(producer: &KafkaProducer, request: &ProduceRequest, reply: DeliveryReply, timeout: Duration) {
    let mut headers = OwnedHeaders::new_with_capacity(request.headers.len());
    for (key, value) in &request.headers {
        headers = headers.insert(Header {
            key,
            value: value.as_deref(),
        });
    }
    let mut record: BaseRecord<'_, [u8], [u8], Box<DeliveryReply>> =
        BaseRecord::with_opaque_to(&request.topic, Box::new(reply)).headers(headers);
    if let Some(key) = request.key.as_deref() {
        record = record.key(key);
    }
    if let Some(value) = request.value.as_deref() {
        record = record.payload(value);
    }
    if let Some(partition) = request.partition {
        record = record.partition(partition);
    }
    if let Err((err, failed)) = producer.send(record) {
        let _ = failed
            .delivery_opaque
            .send(Err(anyhow::anyhow!("enqueue failed: {err}")));
        return;
    }
    if let Err(err) = producer.flush(timeout) {
        log::warn!("producer flush: {err}");
    }
}
