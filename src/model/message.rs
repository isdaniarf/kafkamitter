use std::collections::VecDeque;
use std::sync::Arc;

use crate::model::search;

pub const KEY_PREVIEW_CHARS: usize = 120;
pub const VALUE_PREVIEW_CHARS: usize = 200;

#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub key_preview: String,
    pub value_preview: String,
}

impl MessageRecord {
    pub fn new(
        topic: Arc<str>,
        partition: i32,
        offset: i64,
        timestamp_ms: Option<i64>,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<(String, Option<Vec<u8>>)>,
    ) -> Self {
        let key_preview = preview(key.as_deref(), KEY_PREVIEW_CHARS);
        let value_preview = preview(value.as_deref(), VALUE_PREVIEW_CHARS);
        Self {
            topic,
            partition,
            offset,
            timestamp_ms,
            key,
            value,
            headers,
            key_preview,
            value_preview,
        }
    }

    pub fn byte_len(&self) -> usize {
        let key = self.key.as_ref().map_or(0, Vec::len);
        let value = self.value.as_ref().map_or(0, Vec::len);
        let headers: usize = self
            .headers
            .iter()
            .map(|(k, v)| k.len() + v.as_ref().map_or(0, Vec::len))
            .sum();
        key + value + headers + self.key_preview.len() + self.value_preview.len() + 64
    }
}

pub fn preview(bytes: Option<&[u8]>, max_chars: usize) -> String {
    let Some(bytes) = bytes else {
        return String::from("<null>");
    };
    if bytes.is_empty() {
        return String::from("<empty>");
    }
    let head = &bytes[..bytes.len().min(max_chars * 4)];
    let text = String::from_utf8_lossy(head);
    let mut out = String::with_capacity(max_chars + 1);
    for (count, ch) in text.chars().enumerate() {
        if count == max_chars {
            out.push('…');
            return out;
        }
        out.push(if ch == '\n' || ch == '\r' || ch == '\t' { ' ' } else { ch });
    }
    if bytes.len() > head.len() {
        out.push('…');
    }
    out
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
        MessageRecord::new(
            Arc::from("t"),
            0,
            offset,
            Some(1_700_000_000_000),
            Some(b"k".to_vec()),
            Some(vec![b'x'; value_len]),
            vec![],
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
        let mut odd = record(7, 1);
        odd.value = Some(b"needle".to_vec());
        store.push_batch([record(1, 1), odd, record(9, 1)]);
        store.set_query(Some(b"needle".to_vec()));
        let rows = store.matched_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(store.get(rows[0]).unwrap().offset, 7);
    }

    #[test]
    fn previews_flatten_whitespace_and_truncate() {
        assert_eq!(preview(None, 10), "<null>");
        assert_eq!(preview(Some(b""), 10), "<empty>");
        assert_eq!(preview(Some(b"a\nb\tc"), 10), "a b c");
        assert_eq!(preview(Some(b"0123456789abc"), 10), "0123456789…");
        assert_eq!(preview(Some("héllo".as_bytes()), 3), "hél…");
        assert_eq!(preview(Some(b"\xff\xfeok"), 10), "\u{fffd}\u{fffd}ok");
    }
}
