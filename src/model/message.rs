use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use crate::model::search;

pub const KEY_PREVIEW_CHARS: usize = 120;
pub const VALUE_PREVIEW_CHARS: usize = 200;

static NULL_PREVIEW: OnceLock<Arc<str>> = OnceLock::new();
static EMPTY_PREVIEW: OnceLock<Arc<str>> = OnceLock::new();

#[derive(Debug, Clone)]
struct HeaderSpan {
    name: Range<u32>,
    value: Option<Range<u32>>,
}

#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    bytes: Box<[u8]>,
    key: Option<Range<u32>>,
    value: Option<Range<u32>>,
    headers: Box<[HeaderSpan]>,
    key_preview: Arc<str>,
    value_preview: Arc<str>,
    timestamp_text: OnceLock<Arc<str>>,
}

impl MessageRecord {
    pub fn new(
        topic: Arc<str>,
        partition: i32,
        offset: i64,
        timestamp_ms: Option<i64>,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> Self {
        let total = key.map_or(0, <[u8]>::len)
            + value.map_or(0, <[u8]>::len)
            + headers
                .iter()
                .map(|(name, value)| name.len() + value.map_or(0, <[u8]>::len))
                .sum::<usize>();
        let mut bytes = Vec::with_capacity(total);
        let key_span = key.map(|key| append(&mut bytes, key));
        let value_span = value.map(|value| append(&mut bytes, value));
        let header_spans = headers
            .iter()
            .map(|(name, value)| HeaderSpan {
                name: append(&mut bytes, name.as_bytes()),
                value: value.map(|value| append(&mut bytes, value)),
            })
            .collect();
        Self {
            topic,
            partition,
            offset,
            timestamp_ms,
            bytes: bytes.into_boxed_slice(),
            key: key_span,
            value: value_span,
            headers: header_spans,
            key_preview: preview(key, KEY_PREVIEW_CHARS),
            value_preview: preview(value, VALUE_PREVIEW_CHARS),
            timestamp_text: OnceLock::new(),
        }
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_ref().map(|span| self.slice(span))
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_ref().map(|span| self.slice(span))
    }

    pub fn headers(&self) -> impl Iterator<Item = (&str, Option<&[u8]>)> {
        self.headers.iter().map(|header| {
            let name = std::str::from_utf8(self.slice(&header.name)).unwrap_or_default();
            (name, header.value.as_ref().map(|span| self.slice(span)))
        })
    }

    pub fn has_headers(&self) -> bool {
        !self.headers.is_empty()
    }

    pub fn key_preview(&self) -> &Arc<str> {
        &self.key_preview
    }

    pub fn value_preview(&self) -> &Arc<str> {
        &self.value_preview
    }

    pub fn timestamp_text(&self) -> &Arc<str> {
        self.timestamp_text
            .get_or_init(|| Arc::from(format_timestamp(self.timestamp_ms)))
    }

    /// The heap bytes of one stored record, including its own struct and the
    /// slots that the store keeps for it.
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
            + self.headers.len() * std::mem::size_of::<HeaderSpan>()
            + self.key_preview.len()
            + self.value_preview.len()
            + std::mem::size_of::<Self>()
            + 32
    }

    fn slice(&self, span: &Range<u32>) -> &[u8] {
        &self.bytes[span.start as usize..span.end as usize]
    }
}

fn append(bytes: &mut Vec<u8>, chunk: &[u8]) -> Range<u32> {
    let start = bytes.len() as u32;
    bytes.extend_from_slice(chunk);
    start..bytes.len() as u32
}

pub fn format_timestamp(timestamp_ms: Option<i64>) -> String {
    match timestamp_ms.and_then(chrono::DateTime::from_timestamp_millis) {
        Some(utc) => utc
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
        None => String::from("-"),
    }
}

pub fn preview(bytes: Option<&[u8]>, max_chars: usize) -> Arc<str> {
    let Some(bytes) = bytes else {
        return NULL_PREVIEW.get_or_init(|| Arc::from("<null>")).clone();
    };
    if bytes.is_empty() {
        return EMPTY_PREVIEW.get_or_init(|| Arc::from("<empty>")).clone();
    }
    let head = &bytes[..bytes.len().min(max_chars * 4)];
    let text = String::from_utf8_lossy(head);
    let mut out = String::with_capacity(head.len() + 3);
    for (count, ch) in text.chars().enumerate() {
        if count == max_chars {
            out.push('…');
            return Arc::from(out);
        }
        out.push(if ch == '\n' || ch == '\r' || ch == '\t' { ' ' } else { ch });
    }
    if bytes.len() > head.len() {
        out.push('…');
    }
    Arc::from(out)
}

pub struct MessageStore {
    items: VecDeque<Arc<MessageRecord>>,
    /// One flag for each item, in the same order, for the current search term.
    hits: VecDeque<bool>,
    query: Option<Vec<u8>>,
    matched: usize,
    max_messages: usize,
    max_bytes: usize,
    total_bytes: usize,
    total_received: u64,
}

impl MessageStore {
    pub fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            items: VecDeque::new(),
            hits: VecDeque::new(),
            query: None,
            matched: 0,
            max_messages: max_messages.max(1),
            max_bytes: max_bytes.max(1),
            total_bytes: 0,
            total_received: 0,
        }
    }

    pub fn push_batch(&mut self, batch: impl IntoIterator<Item = MessageRecord>) {
        for record in batch {
            self.total_bytes += record.byte_len();
            self.total_received += 1;
            let hit = self.query.as_ref().is_none_or(|n| search::matches(&record, n));
            self.matched += usize::from(hit);
            self.hits.push_back(hit);
            self.items.push_back(Arc::new(record));
        }
        self.evict();
    }

    /// Sets the search term. `None` shows every message.
    pub fn set_query(&mut self, query: Option<Vec<u8>>) {
        self.query = query;
        self.matched = 0;
        self.hits.clear();
        for record in &self.items {
            let hit = self.query.as_ref().is_none_or(|n| search::matches(record, n));
            self.matched += usize::from(hit);
            self.hits.push_back(hit);
        }
    }

    pub fn has_query(&self) -> bool {
        self.query.is_some()
    }

    /// The number of messages that the search term keeps.
    pub fn matched_len(&self) -> usize {
        self.matched
    }

    /// The rows that the search term keeps, newest first.
    pub fn matched_rows(&self) -> Vec<usize> {
        if self.query.is_none() {
            return (0..self.items.len()).collect();
        }
        self.hits
            .iter()
            .rev()
            .enumerate()
            .filter(|(_, hit)| **hit)
            .map(|(row, _)| row)
            .collect()
    }

    fn evict(&mut self) {
        while self.items.len() > 1
            && (self.items.len() > self.max_messages || self.total_bytes > self.max_bytes)
        {
            if let Some(old) = self.items.pop_front() {
                self.total_bytes -= old.byte_len();
            }
            if let Some(hit) = self.hits.pop_front() {
                self.matched -= usize::from(hit);
            }
        }
    }

    pub fn set_limits(&mut self, max_messages: usize, max_bytes: usize) {
        self.max_messages = max_messages.max(1);
        self.max_bytes = max_bytes.max(1);
        self.evict();
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<MessageRecord>> {
        self.items.iter().rev()
    }

    pub fn get(&self, row: usize) -> Option<&Arc<MessageRecord>> {
        let len = self.items.len();
        if row >= len {
            return None;
        }
        self.items.get(len - 1 - row)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn total_received(&self) -> u64 {
        self.total_received
    }

    pub fn clear(&mut self) {
        self.items.clear();
        self.hits.clear();
        self.matched = 0;
        self.total_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(offset: i64, value_len: usize) -> MessageRecord {
        record_with_value(offset, &vec![b'x'; value_len])
    }

    fn record_with_value(offset: i64, value: &[u8]) -> MessageRecord {
        MessageRecord::new(
            Arc::from("t"),
            0,
            offset,
            Some(1_700_000_000_000),
            Some(b"k"),
            Some(value),
            &[],
        )
    }

    #[test]
    fn newest_message_is_row_zero() {
        let mut store = MessageStore::new(10, 1 << 20);
        store.push_batch([record(1, 1), record(2, 1), record(3, 1)]);
        assert_eq!(store.get(0).unwrap().offset, 3);
        assert_eq!(store.get(2).unwrap().offset, 1);
        assert!(store.get(3).is_none());
    }

    #[test]
    fn evicts_by_count() {
        let mut store = MessageStore::new(3, 1 << 20);
        store.push_batch((1..=5).map(|o| record(o, 1)));
        assert_eq!(store.len(), 3);
        assert_eq!(store.get(0).unwrap().offset, 5);
        assert_eq!(store.get(2).unwrap().offset, 3);
        assert_eq!(store.total_received(), 5);
    }

    #[test]
    fn evicts_by_bytes_and_keeps_the_newest() {
        let one = record(1, 1000).byte_len();
        let mut store = MessageStore::new(100, one * 2 + one / 2);
        store.push_batch([record(1, 1000), record(2, 1000), record(3, 1000)]);
        assert_eq!(store.len(), 2);
        assert_eq!(store.get(1).unwrap().offset, 2);
        assert!(store.total_bytes() <= one * 2 + one / 2);

        let mut small = MessageStore::new(100, 10);
        small.push_batch([record(9, 5000)]);
        assert_eq!(small.len(), 1);
    }

    #[test]
    fn clear_resets_rows_and_bytes_but_not_received_count() {
        let mut store = MessageStore::new(10, 1 << 20);
        store.push_batch([record(1, 10), record(2, 10)]);
        store.clear();
        assert!(store.is_empty());
        assert_eq!(store.total_bytes(), 0);
        assert_eq!(store.total_received(), 2);
    }

    #[test]
    fn a_search_term_filters_the_rows_and_survives_eviction() {
        let mut store = MessageStore::new(10, 1 << 20);
        store.push_batch([record(1, 10), record(2, 10)]);
        assert!(!store.has_query());
        assert_eq!(store.matched_len(), 2);
        assert_eq!(store.matched_rows(), vec![0, 1]);

        store.set_query(Some(b"xxxx".to_vec()));
        assert!(store.has_query());
        assert_eq!(store.matched_len(), 2, "every value holds the term");

        store.set_query(Some(b"nothing".to_vec()));
        assert_eq!(store.matched_len(), 0);
        assert_eq!(store.matched_rows(), Vec::<usize>::new());

        store.set_query(None);
        assert_eq!(store.matched_len(), 2);
    }

    #[test]
    fn the_match_count_follows_new_and_evicted_messages() {
        let mut store = MessageStore::new(2, 1 << 20);
        store.set_query(Some(b"k".to_vec()));
        store.push_batch([record(1, 5), record(2, 5), record(3, 5)]);
        assert_eq!(store.len(), 2);
        assert_eq!(store.matched_len(), 2, "the key of every message holds the term");

        store.set_query(Some(b"zzz".to_vec()));
        store.push_batch([record(4, 5)]);
        assert_eq!(store.matched_len(), 0);
        store.clear();
        assert_eq!(store.matched_len(), 0);
    }

    #[test]
    fn matched_rows_are_newest_first() {
        let mut store = MessageStore::new(10, 1 << 20);
        store.push_batch([record(1, 1), record_with_value(7, b"needle"), record(9, 1)]);
        store.set_query(Some(b"needle".to_vec()));
        let rows = store.matched_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(store.get(rows[0]).unwrap().offset, 7);
    }

    #[test]
    fn a_record_keeps_its_parts_in_one_buffer() {
        let record = MessageRecord::new(
            Arc::from("t"),
            2,
            5,
            None,
            Some(b"key"),
            Some(b"value"),
            &[("trace", Some(b"abc")), ("empty", None)],
        );
        assert_eq!(record.key(), Some(b"key".as_slice()));
        assert_eq!(record.value(), Some(b"value".as_slice()));
        assert!(record.has_headers());
        let headers: Vec<(&str, Option<&[u8]>)> = record.headers().collect();
        assert_eq!(headers, vec![("trace", Some(b"abc".as_slice())), ("empty", None)]);
        assert_eq!(record.key_preview().as_ref(), "key");
        assert_eq!(record.value_preview().as_ref(), "value");

        let tombstone = MessageRecord::new(Arc::from("t"), 0, 0, None, None, None, &[]);
        assert_eq!(tombstone.key(), None);
        assert_eq!(tombstone.value(), None);
        assert!(!tombstone.has_headers());
        assert_eq!(tombstone.value_preview().as_ref(), "<null>");
    }

    #[test]
    fn the_timestamp_text_is_formatted_once() {
        let record = record(1, 1);
        let first = record.timestamp_text().clone();
        assert_eq!(first.as_ref(), format_timestamp(record.timestamp_ms));
        assert!(Arc::ptr_eq(&first, record.timestamp_text()));
        let none = MessageRecord::new(Arc::from("t"), 0, 0, None, None, None, &[]);
        assert_eq!(none.timestamp_text().as_ref(), "-");
    }

    #[test]
    fn previews_flatten_whitespace_and_truncate() {
        assert_eq!(preview(None, 10).as_ref(), "<null>");
        assert_eq!(preview(Some(b""), 10).as_ref(), "<empty>");
        assert_eq!(preview(Some(b"a\nb\tc"), 10).as_ref(), "a b c");
        assert_eq!(preview(Some(b"0123456789abc"), 10).as_ref(), "0123456789…");
        assert_eq!(preview(Some("héllo".as_bytes()), 3).as_ref(), "hél…");
        assert_eq!(preview(Some(b"\xff\xfeok"), 10).as_ref(), "\u{fffd}\u{fffd}ok");
    }
}
