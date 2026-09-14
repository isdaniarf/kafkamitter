use std::ffi::{CStr, CString, c_char, c_int};
use std::time::Duration;

use rdkafka::TopicPartitionList;
use rdkafka_sys::bindings::*;

use crate::kafka::admin::Admin;
use crate::kafka::groups::CommittedOffset;

struct Request(*mut rd_kafka_ListConsumerGroupOffsets_t);

impl Drop for Request {
    fn drop(&mut self) {
        unsafe { rd_kafka_ListConsumerGroupOffsets_destroy(self.0) };
    }
}

struct Options(*mut rd_kafka_AdminOptions_t);

impl Drop for Options {
    fn drop(&mut self) {
        unsafe { rd_kafka_AdminOptions_destroy(self.0) };
    }
}

struct Queue(*mut rd_kafka_queue_t);

impl Drop for Queue {
    fn drop(&mut self) {
        unsafe { rd_kafka_queue_destroy(self.0) };
    }
}

struct Event(*mut rd_kafka_event_t);

impl Drop for Event {
    fn drop(&mut self) {
        unsafe { rd_kafka_event_destroy(self.0) };
    }
}

fn admin_options(rk: *mut rd_kafka_t, timeout_ms: c_int) -> anyhow::Result<Options> {
    let options = Options(unsafe {
        rd_kafka_AdminOptions_new(rk, rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS)
    });
    anyhow::ensure!(!options.0.is_null(), "cannot build admin options");
    let mut errstr = [0 as c_char; 512];
    let err = unsafe {
        rd_kafka_AdminOptions_set_request_timeout(options.0, timeout_ms, errstr.as_mut_ptr(), errstr.len())
    };
    if err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        let message = unsafe { CStr::from_ptr(errstr.as_ptr()) }.to_string_lossy().into_owned();
        anyhow::bail!("admin options: {message}");
    }
    Ok(options)
}

fn enqueue(
    rk: *mut rd_kafka_t,
    queue: &Queue,
    options: &Options,
    group: &str,
    tpl: &TopicPartitionList,
) -> anyhow::Result<()> {
    let group_c = CString::new(group)?;
    let request = Request(unsafe { rd_kafka_ListConsumerGroupOffsets_new(group_c.as_ptr(), tpl.ptr()) });
    anyhow::ensure!(!request.0.is_null(), "cannot build ListConsumerGroupOffsets request");
    let mut requests = [request.0];
    unsafe { rd_kafka_ListConsumerGroupOffsets(rk, requests.as_mut_ptr(), 1, options.0, queue.0) };
    Ok(())
}

fn parse_event(event: &Event) -> anyhow::Result<(String, anyhow::Result<Vec<CommittedOffset>>)> {
    let event_type = unsafe { rd_kafka_event_type(event.0) };
    anyhow::ensure!(
        event_type == RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT,
        "unexpected admin event type {event_type}"
    );
    let err = unsafe { rd_kafka_event_error(event.0) };
    if err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        let message = unsafe { CStr::from_ptr(rd_kafka_event_error_string(event.0)) }
            .to_string_lossy()
            .into_owned();
        anyhow::bail!("ListConsumerGroupOffsets: {message}");
    }
    unsafe {
        let result = rd_kafka_event_ListConsumerGroupOffsets_result(event.0);
        let mut count: usize = 0;
        let groups = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut count);
        anyhow::ensure!(count >= 1 && !groups.is_null(), "ListConsumerGroupOffsets result has no group");
        let group_result = *groups;
        let name = CStr::from_ptr(rd_kafka_group_result_name(group_result)).to_string_lossy().into_owned();
        let group_error = rd_kafka_group_result_error(group_result);
        if !group_error.is_null() {
            let message = CStr::from_ptr(rd_kafka_error_string(group_error)).to_string_lossy().into_owned();
            return Ok((name, Err(anyhow::anyhow!("{message}"))));
        }
        let mut out = Vec::new();
        let list = rd_kafka_group_result_partitions(group_result);
        if !list.is_null() {
            let count = usize::try_from((*list).cnt).unwrap_or(0);
            for ix in 0..count {
                let elem = &*(*list).elems.add(ix);
                let committed = if elem.err == rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR && elem.offset >= 0 {
                    Some(elem.offset)
                } else {
                    None
                };
                out.push(CommittedOffset {
                    partition: elem.partition,
                    committed,
                });
            }
        }
        out.sort_by_key(|c| c.partition);
        Ok((name, Ok(out)))
    }
}

fn partition_list(topic: &str, partitions: &[i32]) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    for &partition in partitions {
        tpl.add_partition(topic, partition);
    }
    tpl
}

pub fn list_group_offsets(
    admin: &Admin,
    group: &str,
    topic: &str,
    partitions: &[i32],
    timeout: Duration,
) -> anyhow::Result<Vec<CommittedOffset>> {
    let rk = admin.inner().native_ptr();
    let timeout_ms = c_int::try_from(timeout.as_millis()).unwrap_or(c_int::MAX);
    let tpl = partition_list(topic, partitions);
    let options = admin_options(rk, timeout_ms)?;
    let queue = Queue(unsafe { rd_kafka_queue_new(rk) });
    anyhow::ensure!(!queue.0.is_null(), "cannot create admin queue");
    enqueue(rk, &queue, &options, group, &tpl)?;
    let event = Event(unsafe { rd_kafka_queue_poll(queue.0, timeout_ms.saturating_add(1000)) });
    anyhow::ensure!(!event.0.is_null(), "ListConsumerGroupOffsets timed out for group {group}");
    let (_, result) = parse_event(&event)?;
    result
}

pub const SCAN_WINDOW: usize = 32;

pub fn scan_group_offsets(
    admin: &Admin,
    groups: &[String],
    topic: &str,
    partitions: &[i32],
    timeout: Duration,
    mut on_result: impl FnMut(String, anyhow::Result<Vec<CommittedOffset>>) -> bool,
) -> anyhow::Result<()> {
    let rk = admin.inner().native_ptr();
    let timeout_ms = c_int::try_from(timeout.as_millis()).unwrap_or(c_int::MAX);
    let tpl = partition_list(topic, partitions);
    let options = admin_options(rk, timeout_ms)?;
    let queue = Queue(unsafe { rd_kafka_queue_new(rk) });
    anyhow::ensure!(!queue.0.is_null(), "cannot create admin queue");
    for window in groups.chunks(SCAN_WINDOW) {
        let mut pending = 0usize;
        for group in window {
            match enqueue(rk, &queue, &options, group, &tpl) {
                Ok(()) => pending += 1,
                Err(err) => {
                    if !on_result(group.clone(), Err(err)) {
                        return Ok(());
                    }
                }
            }
        }
        let deadline = std::time::Instant::now() + timeout + Duration::from_secs(1);
        while pending > 0 {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                anyhow::bail!("group offset scan timed out with {pending} requests pending");
            }
            let wait_ms = c_int::try_from(remaining.as_millis()).unwrap_or(c_int::MAX);
            let event = Event(unsafe { rd_kafka_queue_poll(queue.0, wait_ms) });
            if event.0.is_null() {
                continue;
            }
            pending -= 1;
            match parse_event(&event) {
                Ok((name, result)) => {
                    if !on_result(name, result) {
                        return Ok(());
                    }
                }
                Err(err) => log::warn!("group offset scan: {err}"),
            }
        }
    }
    Ok(())
}
