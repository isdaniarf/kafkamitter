use crate::model::message::MessageRecord;

/// Builds the search term for an ASCII case-insensitive match.
/// An empty query gives `None`, which means "show every message".
pub fn needle(query: &str) -> Option<Vec<u8>> {
    if query.is_empty() {
        None
    } else {
        Some(query.to_ascii_lowercase().into_bytes())
    }
}

/// Reports whether `haystack` holds `needle`. The needle must be lowercase.
/// Only ASCII letters fold, so the search never allocates.
pub fn contains_ignore_ascii_case(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    let lower = needle[0];
    let upper = lower.to_ascii_uppercase();
    let last_start = haystack.len() - needle.len();
    let mut start = 0;
    while start <= last_start {
        let Some(offset) = memchr::memchr2(lower, upper, &haystack[start..=last_start]) else {
            return false;
        };
        let at = start + offset;
        if haystack[at..at + needle.len()].eq_ignore_ascii_case(needle) {
            return true;
        }
        start = at + 1;
    }
    false
}

/// Reports whether the value, the key, or any header of `record` holds the term.
pub fn matches(record: &MessageRecord, needle: &[u8]) -> bool {
    let holds = |bytes: Option<&[u8]>| bytes.is_some_and(|b| contains_ignore_ascii_case(b, needle));
    holds(record.value.as_deref())
        || holds(record.key.as_deref())
        || record.headers.iter().any(|(name, value)| {
            contains_ignore_ascii_case(name.as_bytes(), needle) || holds(value.as_deref())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn record(key: Option<&str>, value: Option<&str>, headers: &[(&str, Option<&str>)]) -> MessageRecord {
        MessageRecord::new(
            Arc::from("t"),
            0,
            1,
            Some(0),
            key.map(|k| k.as_bytes().to_vec()),
            value.map(|v| v.as_bytes().to_vec()),
            headers
                .iter()
                .map(|(n, v)| ((*n).to_string(), v.map(|v| v.as_bytes().to_vec())))
                .collect(),
        )
    }

    #[test]
    fn builds_a_lowercase_needle_and_ignores_an_empty_query() {
        assert_eq!(needle(""), None);
        assert_eq!(needle("AbC").as_deref(), Some(b"abc".as_slice()));
    }

    #[test]
    fn finds_a_term_in_any_case() {
        assert!(contains_ignore_ascii_case(b"Hello World", b"hello"));
        assert!(contains_ignore_ascii_case(b"HELLO", b"ell"));
        assert!(contains_ignore_ascii_case(b"xxhellox", b"hello"));
        assert!(!contains_ignore_ascii_case(b"Hello", b"world"));
    }

    #[test]
    fn handles_the_edges_of_the_haystack() {
        assert!(contains_ignore_ascii_case(b"abc", b"abc"));
        assert!(contains_ignore_ascii_case(b"abc", b"c"));
        assert!(contains_ignore_ascii_case(b"abc", b"a"));
        assert!(!contains_ignore_ascii_case(b"ab", b"abc"));
        assert!(!contains_ignore_ascii_case(b"", b"a"));
        assert!(contains_ignore_ascii_case(b"", b""));
    }

    #[test]
    fn retries_after_a_partial_match() {
        assert!(contains_ignore_ascii_case(b"aab", b"ab"));
        assert!(contains_ignore_ascii_case(b"AAAAB", b"aab"));
        assert!(!contains_ignore_ascii_case(b"aaaa", b"aab"));
    }

    #[test]
    fn searches_bytes_that_are_not_utf8() {
        assert!(contains_ignore_ascii_case(&[0xff, b'O', b'K', 0xfe], b"ok"));
    }

    #[test]
    fn matches_the_value_the_key_and_the_headers() {
        let r = record(Some("order-77"), Some(r#"{"status":"NEW"}"#), &[("source", Some("ms-account"))]);
        assert!(matches(&r, b"new"));
        assert!(matches(&r, b"order-77"));
        assert!(matches(&r, b"source"));
        assert!(matches(&r, b"ms-account"));
        assert!(!matches(&r, b"missing"));
    }

    #[test]
    fn handles_a_tombstone_and_a_header_without_a_value() {
        let r = record(Some("k1"), None, &[("trace", None)]);
        assert!(matches(&r, b"k1"));
        assert!(matches(&r, b"trace"));
        assert!(!matches(&r, b"anything"));
    }
}
