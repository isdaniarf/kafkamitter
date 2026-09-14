use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaResult;

pub type Admin = AdminClient<DefaultClientContext>;

pub fn create_admin(base: &ClientConfig) -> KafkaResult<Admin> {
    base.create()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopicSpec {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub configs: Vec<(String, String)>,
}

fn options(timeout: Duration) -> AdminOptions {
    AdminOptions::new()
        .operation_timeout(Some(timeout))
        .request_timeout(Some(timeout))
}

pub fn create_topic(admin: &Admin, spec: &NewTopicSpec, timeout: Duration) -> anyhow::Result<()> {
    let mut topic = NewTopic::new(
        &spec.name,
        spec.partitions,
        TopicReplication::Fixed(spec.replication_factor),
    );
    for (key, value) in &spec.configs {
        topic = topic.set(key, value);
    }
    let results = futures::executor::block_on(admin.create_topics([&topic], &options(timeout)))?;
    for result in results {
        if let Err((name, code)) = result {
            anyhow::bail!("create topic {name}: {code}");
        }
    }
    Ok(())
}

pub fn delete_topic(admin: &Admin, name: &str, timeout: Duration) -> anyhow::Result<()> {
    let results = futures::executor::block_on(admin.delete_topics(&[name], &options(timeout)))?;
    for result in results {
        if let Err((name, code)) = result {
            anyhow::bail!("delete topic {name}: {code}");
        }
    }
    Ok(())
}
