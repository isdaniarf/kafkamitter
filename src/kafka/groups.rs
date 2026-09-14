use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use crate::kafka::config::group_reader_config;
use crate::model::assignment::{TopicAssignment, decode_assignment};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignment: Vec<TopicAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSummary {
    pub name: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<GroupMember>,
}

impl GroupSummary {
    pub fn partitions_of(&self, topic: &str) -> Vec<(i32, &GroupMember)> {
        let mut out = Vec::new();
        for member in &self.members {
            for assignment in &member.assignment {
                if assignment.topic == topic {
                    out.extend(assignment.partitions.iter().map(|&p| (p, member)));
                }
            }
        }
        out.sort_by_key(|(p, _)| *p);
        out
    }

    pub fn consumes(&self, topic: &str) -> bool {
        self.members
            .iter()
            .any(|m| m.assignment.iter().any(|a| a.topic == topic && !a.partitions.is_empty()))
    }
}

pub fn list_groups(consumer: &BaseConsumer, timeout: Duration) -> KafkaResult<Vec<GroupSummary>> {
    let list = consumer.fetch_group_list(None, timeout)?;
    let mut groups: Vec<GroupSummary> = list
        .groups()
        .iter()
        .map(|g| GroupSummary {
            name: g.name().to_string(),
            state: g.state().to_string(),
            protocol_type: g.protocol_type().to_string(),
            protocol: g.protocol().to_string(),
            members: g
                .members()
                .iter()
                .map(|m| GroupMember {
                    member_id: m.id().to_string(),
                    client_id: m.client_id().to_string(),
                    client_host: m.client_host().to_string(),
                    assignment: m
                        .assignment()
                        .and_then(|bytes| decode_assignment(bytes).ok())
                        .unwrap_or_default(),
                })
                .collect(),
        })
        .collect();
    groups.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(groups)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedOffset {
    pub partition: i32,
    pub committed: Option<i64>,
}

pub fn committed_offsets(
    base: &ClientConfig,
    group: &str,
    topic: &str,
    partitions: &[i32],
    timeout: Duration,
) -> KafkaResult<Vec<CommittedOffset>> {
    let consumer: BaseConsumer = group_reader_config(base, group).create()?;
    let mut tpl = TopicPartitionList::new();
    for &partition in partitions {
        tpl.add_partition(topic, partition);
    }
    let committed = consumer.committed_offsets(tpl, timeout)?;
    let mut out: Vec<CommittedOffset> = committed
        .elements()
        .iter()
        .map(|e| CommittedOffset {
            partition: e.partition(),
            committed: match e.offset() {
                Offset::Offset(n) => Some(n),
                _ => None,
            },
        })
        .collect();
    out.sort_by_key(|c| c.partition);
    Ok(out)
}
