use std::time::{Duration, Instant};

use futures::channel::oneshot;
use kafkamitter::kafka::admin::{self, NewTopicSpec};
use kafkamitter::kafka::config;
use kafkamitter::kafka::consume::{ConsumeEvent, ConsumeSession, StartFrom};
use kafkamitter::kafka::groups;
use kafkamitter::kafka::metadata;
use kafkamitter::kafka::produce::{self, ProduceRequest};
use kafkamitter::model::lag::compute_lag;
use kafkamitter::model::profile::{ConnectionProfile, Security};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::Message;

const TIMEOUT: Duration = Duration::from_secs(15);

fn bootstrap() -> Option<String> {
    std::env::var("KAFKAMITTER_TEST_BOOTSTRAP").ok().filter(|s| !s.is_empty())
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos:x}")
}

struct TopicGuard {
    admin: admin::Admin,
    name: String,
}

impl Drop for TopicGuard {
    fn drop(&mut self) {
        let _ = admin::delete_topic(&self.admin, &self.name, TIMEOUT);
    }
}

#[test]
fn end_to_end_against_a_real_broker() {
    let Some(bootstrap) = bootstrap() else {
        eprintln!("KAFKAMITTER_TEST_BOOTSTRAP is not set, skipping");
        return;
    };
    let profile = ConnectionProfile {
        id: "it".into(),
        name: "integration".into(),
        bootstrap_servers: bootstrap,
        security: Security::Plaintext,
        ca_location: None,
        password: None,
    };
    let base = config::base_config(&profile, None, None);
    let topic = unique("kafkamitter-it");

    let admin = admin::create_admin(&base).expect("admin client");
    admin::create_topic(
        &admin,
        &NewTopicSpec {
            name: topic.clone(),
            partitions: 2,
            replication_factor: 1,
            configs: vec![("retention.ms".into(), "3600000".into())],
        },
        TIMEOUT,
    )
    .expect("create topic");
    let _guard = TopicGuard {
        admin: admin::create_admin(&base).expect("admin client"),
        name: topic.clone(),
    };

    let meta: BaseConsumer = config::viewer_consumer_config(&base).create().expect("metadata consumer");
    let deadline = Instant::now() + TIMEOUT;
    let info = loop {
        if let Ok(Some(info)) = metadata::fetch_topic(&meta, &topic, TIMEOUT) {
            if info.partitions.len() == 2 {
                break info;
            }
        }
        assert!(Instant::now() < deadline, "topic did not appear in metadata");
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(info.partition_ids(), vec![0, 1]);
    let cluster = metadata::fetch_cluster(&meta, TIMEOUT).expect("cluster metadata");
    assert!(cluster.topics.iter().any(|t| t.name == topic));
    assert!(!cluster.brokers.is_empty());

    let producer = produce::create_producer(&base).expect("producer");
    for i in 0..3i64 {
        let (tx, rx) = oneshot::channel();
        produce::produce(
            &producer,
            &ProduceRequest {
                topic: topic.clone(),
                partition: Some(0),
                key: Some(format!("k{i}").into_bytes()),
                value: Some(format!("{{\"i\":{i}}}").into_bytes()),
                headers: vec![("source".into(), Some(b"it".to_vec()))],
            },
            tx,
            TIMEOUT,
        );
        let delivered = futures::executor::block_on(rx).expect("reply").expect("delivery");
        assert_eq!(delivered.partition, 0);
        assert_eq!(delivered.offset, i);
    }

    let watermarks = metadata::fetch_watermarks(&meta, &topic, &[0, 1], TIMEOUT).expect("watermarks");
    assert_eq!((watermarks[0].low, watermarks[0].high), (0, 3));
    assert_eq!((watermarks[1].low, watermarks[1].high), (0, 0));

    let (tx, rx) = smol::channel::bounded(64);
    let session = ConsumeSession::start(&base, topic.clone(), Vec::new(), StartFrom::Beginning, tx);
    let mut records = Vec::new();
    let mut eof = std::collections::BTreeSet::new();
    let mut position = None;
    let deadline = Instant::now() + TIMEOUT;
    while (records.len() < 3 || eof.len() < 2) && Instant::now() < deadline {
        match rx.recv_blocking() {
            Ok(ConsumeEvent::Batch(batch)) => {
                if let Some(found) = batch.positions.iter().find(|p| p.partition == 0) {
                    position = Some((found.first, found.last, found.high));
                }
                records.extend(batch.records);
            }
            Ok(ConsumeEvent::Eof(p)) => {
                eof.insert(p);
            }
            Ok(ConsumeEvent::Error(err)) => panic!("consume error: {err}"),
            Ok(ConsumeEvent::Assigned(p)) => assert_eq!(p, vec![0, 1]),
            Err(_) => break,
        }
    }
    session.stop();
    assert_eq!(records.len(), 3, "expected 3 records");
    assert_eq!(position, Some((0, 2, 3)), "the batch must carry the offsets and the cached high watermark");

    let (tx, rx) = smol::channel::bounded(64);
    let newest = ConsumeSession::start(&base, topic.clone(), vec![0], StartFrom::Newest(2), tx);
    let mut newest_records = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    let mut newest_eof = false;
    while !newest_eof && Instant::now() < deadline {
        match rx.recv_blocking() {
            Ok(ConsumeEvent::Batch(batch)) => newest_records.extend(batch.records),
            Ok(ConsumeEvent::Eof(_)) => newest_eof = true,
            Ok(ConsumeEvent::Error(err)) => panic!("newest consume error: {err}"),
            Ok(ConsumeEvent::Assigned(_)) => {}
            Err(_) => break,
        }
    }
    newest.stop();
    let mut newest_offsets: Vec<i64> = newest_records.iter().map(|r| r.offset).collect();
    newest_offsets.sort_unstable();
    assert_eq!(newest_offsets, vec![1, 2], "newest 2 must return the last two offsets");
    assert_eq!(eof.len(), 2, "expected EOF on both partitions");
    let first = records.iter().find(|r| r.offset == 0).expect("offset 0");
    assert_eq!(first.key(), Some(b"k0".as_slice()));
    assert_eq!(first.value(), Some(br#"{"i":0}"#.as_slice()));
    let headers: Vec<(&str, Option<&[u8]>)> = first.headers().collect();
    assert_eq!(headers, vec![("source", Some(b"it".as_slice()))]);
    assert_eq!(first.value_preview().as_ref(), r#"{"i":0}"#);

    let group = unique("kafkamitter-it-group");
    let mut member_config = base.clone();
    member_config
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    let member: BaseConsumer = member_config.create().expect("member consumer");
    member.subscribe(&[topic.as_str()]).expect("subscribe");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut seen = 0;
    while seen < 3 && Instant::now() < deadline {
        if let Some(Ok(message)) = member.poll(Duration::from_millis(500)) {
            assert_eq!(message.partition(), 0);
            seen += 1;
        }
    }
    assert_eq!(seen, 3, "member consumer did not receive all messages");
    member.commit_consumer_state(CommitMode::Sync).expect("commit");

    let listed = groups::list_groups(&meta, TIMEOUT).expect("list groups");
    let summary = listed.iter().find(|g| g.name == group).expect("group is listed");
    assert!(summary.consumes(&topic), "group assignment covers the topic");
    assert_eq!(summary.partitions_of(&topic).len(), 2);

    let committed = groups::committed_offsets(&base, &group, &topic, &[0, 1], TIMEOUT).expect("committed offsets");
    let p0 = committed.iter().find(|c| c.partition == 0).expect("partition 0");
    assert_eq!(p0.committed, Some(3));
    let via_admin = kafkamitter::kafka::group_offsets::list_group_offsets(&admin, &group, &topic, &[0, 1], TIMEOUT)
        .expect("admin committed offsets");
    assert_eq!(via_admin, committed);
    let unknown = kafkamitter::kafka::group_offsets::list_group_offsets(&admin, "kafkamitter-it-no-such-group", &topic, &[0, 1], TIMEOUT)
        .expect("unknown group yields no commits");
    assert!(unknown.iter().all(|c| c.committed.is_none()));
    let lag = compute_lag(0, 3, p0.committed);
    assert_eq!(lag.value, Some(0));
    drop(member);
}
