use std::time::Duration;

use futures::channel::oneshot;
use rdkafka::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use smol::channel::{Receiver, Sender, bounded};

use crate::kafka::admin::{self, Admin, NewTopicSpec};
use crate::kafka::config::viewer_consumer_config;
use crate::kafka::group_offsets;
use crate::kafka::groups::{self, CommittedOffset, GroupSummary};
use crate::kafka::metadata::{self, ClusterInfo, TopicInfo, Watermarks};
use crate::kafka::produce::{self, Delivered, KafkaProducer, ProduceRequest};

pub type Reply<T> = oneshot::Sender<anyhow::Result<T>>;

const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const ADMIN_TIMEOUT: Duration = Duration::from_secs(30);

pub enum Cmd {
    FetchCluster {
        reply: Reply<ClusterInfo>,
    },
    FetchTopic {
        topic: String,
        reply: Reply<Option<TopicInfo>>,
    },
    FetchWatermarks {
        topic: String,
        partitions: Vec<i32>,
        reply: Reply<Vec<Watermarks>>,
    },
    CreateTopic {
        spec: NewTopicSpec,
        reply: Reply<()>,
    },
    DeleteTopic {
        name: String,
        reply: Reply<()>,
    },
    Produce {
        request: ProduceRequest,
        reply: Reply<Delivered>,
    },
    ListGroups {
        reply: Reply<Vec<GroupSummary>>,
    },
    CommittedOffsets {
        group: String,
        topic: String,
        partitions: Vec<i32>,
        reply: Reply<Vec<CommittedOffset>>,
    },
    ScanGroupOffsets {
        groups: Vec<String>,
        topic: String,
        partitions: Vec<i32>,
        results: Sender<(String, anyhow::Result<Vec<CommittedOffset>>)>,
        reply: Reply<()>,
    },
}

pub struct WorkerHandle {
    tx: Sender<Cmd>,
    base: ClientConfig,
}

impl WorkerHandle {
    pub fn spawn(name: &str, base: ClientConfig) -> Self {
        let (tx, rx) = bounded::<Cmd>(32);
        let worker = Worker {
            base: base.clone(),
            meta: None,
            admin: None,
            producer: None,
        };
        std::thread::Builder::new()
            .name(format!("kafka-{name}"))
            .spawn(move || worker.run(rx))
            .expect("spawn kafka worker thread");
        Self { tx, base }
    }

    pub fn base_config(&self) -> &ClientConfig {
        &self.base
    }

    pub async fn call<T>(&self, make: impl FnOnce(Reply<T>) -> Cmd) -> anyhow::Result<T> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(make(tx))
            .await
            .map_err(|_| anyhow::anyhow!("the kafka worker has stopped"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("the kafka worker dropped the request"))?
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.tx.close();
    }
}

struct Worker {
    base: ClientConfig,
    meta: Option<BaseConsumer>,
    admin: Option<Admin>,
    producer: Option<KafkaProducer>,
}

impl Worker {
    fn meta(&mut self) -> anyhow::Result<&BaseConsumer> {
        if self.meta.is_none() {
            let consumer: BaseConsumer = viewer_consumer_config(&self.base).create()?;
            self.meta = Some(consumer);
        }
        Ok(self.meta.as_ref().expect("metadata consumer"))
    }

    fn admin(&mut self) -> anyhow::Result<&Admin> {
        if self.admin.is_none() {
            self.admin = Some(admin::create_admin(&self.base)?);
        }
        Ok(self.admin.as_ref().expect("admin client"))
    }

    fn producer(&mut self) -> anyhow::Result<&KafkaProducer> {
        if self.producer.is_none() {
            self.producer = Some(produce::create_producer(&self.base)?);
        }
        Ok(self.producer.as_ref().expect("producer"))
    }

    fn run(mut self, rx: Receiver<Cmd>) {
        while let Ok(cmd) = rx.recv_blocking() {
            match cmd {
                Cmd::FetchCluster { reply } => {
                    let result = self
                        .meta()
                        .and_then(|c| Ok(metadata::fetch_cluster(c, CALL_TIMEOUT)?));
                    let _ = reply.send(result);
                }
                Cmd::FetchTopic { topic, reply } => {
                    let result = self
                        .meta()
                        .and_then(|c| Ok(metadata::fetch_topic(c, &topic, CALL_TIMEOUT)?));
                    let _ = reply.send(result);
                }
                Cmd::FetchWatermarks {
                    topic,
                    partitions,
                    reply,
                } => {
                    let result = self.meta().and_then(|c| {
                        Ok(metadata::fetch_watermarks(c, &topic, &partitions, CALL_TIMEOUT)?)
                    });
                    let _ = reply.send(result);
                }
                Cmd::CreateTopic { spec, reply } => {
                    let result = self
                        .admin()
                        .and_then(|a| admin::create_topic(a, &spec, ADMIN_TIMEOUT));
                    let _ = reply.send(result);
                }
                Cmd::DeleteTopic { name, reply } => {
                    let result = self
                        .admin()
                        .and_then(|a| admin::delete_topic(a, &name, ADMIN_TIMEOUT));
                    let _ = reply.send(result);
                }
                Cmd::Produce { request, reply } => match self.producer() {
                    Ok(producer) => produce::produce(producer, &request, reply, CALL_TIMEOUT),
                    Err(err) => {
                        let _ = reply.send(Err(err));
                    }
                },
                Cmd::ListGroups { reply } => {
                    let result = self
                        .meta()
                        .and_then(|c| Ok(groups::list_groups(c, CALL_TIMEOUT)?));
                    let _ = reply.send(result);
                }
                Cmd::ScanGroupOffsets {
                    groups,
                    topic,
                    partitions,
                    results,
                    reply,
                } => {
                    let result = self.admin().and_then(|admin| {
                        group_offsets::scan_group_offsets(admin, &groups, &topic, &partitions, CALL_TIMEOUT, |name, result| {
                            results.send_blocking((name, result)).is_ok()
                        })
                    });
                    let _ = reply.send(result);
                }
                Cmd::CommittedOffsets {
                    group,
                    topic,
                    partitions,
                    reply,
                } => {
                    let result = self.admin().and_then(|admin| {
                        group_offsets::list_group_offsets(admin, &group, &topic, &partitions, CALL_TIMEOUT)
                    });
                    let _ = reply.send(result);
                }
            }
        }
    }
}
