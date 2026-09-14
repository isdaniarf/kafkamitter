use std::ops::Range;

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonToken {
    Key,
    Str,
    Number,
    Boolean,
    Null,
    Punctuation,
}

pub fn tokenize(text: &str) -> Vec<(Range<usize>, JsonToken)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
                let end = i.min(bytes.len());
                let mut next = end;
                while next < bytes.len() && bytes[next].is_ascii_whitespace() {
                    next += 1;
                }
                let kind = if next < bytes.len() && bytes[next] == b':' {
                    JsonToken::Key
                } else {
                    JsonToken::Str
                };
                out.push((start..end, kind));
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                i += 1;
                while i < bytes.len() && matches!(bytes[i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-') {
                    i += 1;
                }
                out.push((start..i, JsonToken::Number));
            }
            b't' | b'f' | b'n' => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
                    i += 1;
                }
                let kind = match &text[start..i] {
                    "true" | "false" => Some(JsonToken::Boolean),
                    "null" => Some(JsonToken::Null),
                    _ => None,
                };
                if let Some(kind) = kind {
                    out.push((start..i, kind));
                }
            }
            b'{' | b'}' | b'[' | b']' | b':' | b',' => {
                out.push((i..i + 1, JsonToken::Punctuation));
                i += 1;
            }
            _ => i += 1,
        }
    }
    out
}

pub fn looks_like_json(bytes: &[u8]) -> bool {
    let first = bytes.iter().find(|b| !b.is_ascii_whitespace());
    matches!(first, Some(b'{') | Some(b'['))
}

pub fn try_pretty(bytes: &[u8]) -> Option<String> {
    if !looks_like_json(bytes) {
        return None;
    }
    let value: Value = serde_json::from_slice(bytes).ok()?;
    serde_json::to_string_pretty(&value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pretty_prints_an_object_and_keeps_key_order() {
        let out = try_pretty(br#"{"zeta":1,"alpha":{"b":[1,2],"a":null}}"#).unwrap();
        assert_eq!(
            out,
            "{\n  \"zeta\": 1,\n  \"alpha\": {\n    \"b\": [\n      1,\n      2\n    ],\n    \"a\": null\n  }\n}"
        );
    }

    #[test]
    fn pretty_prints_an_array_with_leading_whitespace() {
        let out = try_pretty(b"  \n\t[1, \"x\"]").unwrap();
        assert_eq!(out, "[\n  1,\n  \"x\"\n]");
    }

    #[test]
    fn rejects_a_bare_scalar() {
        assert_eq!(try_pretty(b"42"), None);
        assert_eq!(try_pretty(b"\"text\""), None);
        assert_eq!(try_pretty(b"true"), None);
    }

    #[test]
    fn rejects_invalid_json_and_non_utf8() {
        assert_eq!(try_pretty(b"{\"a\": }"), None);
        assert_eq!(try_pretty(b"{\"a\": \"\xff\xfe\"}"), None);
        assert_eq!(try_pretty(b""), None);
    }

    #[test]
    fn tokenizes_keys_values_and_literals() {
        let text = r#"{"a": 1, "b": [true, null, "x\"y"], "c": -2.5e3}"#;
        let tokens = tokenize(text);
        let kinds: Vec<(&str, JsonToken)> = tokens.iter().map(|(r, k)| (&text[r.clone()], *k)).collect();
        assert_eq!(
            kinds,
            vec![
                ("{", JsonToken::Punctuation),
                ("\"a\"", JsonToken::Key),
                (":", JsonToken::Punctuation),
                ("1", JsonToken::Number),
                (",", JsonToken::Punctuation),
                ("\"b\"", JsonToken::Key),
                (":", JsonToken::Punctuation),
                ("[", JsonToken::Punctuation),
                ("true", JsonToken::Boolean),
                (",", JsonToken::Punctuation),
                ("null", JsonToken::Null),
                (",", JsonToken::Punctuation),
                ("\"x\\\"y\"", JsonToken::Str),
                ("]", JsonToken::Punctuation),
                (",", JsonToken::Punctuation),
                ("\"c\"", JsonToken::Key),
                (":", JsonToken::Punctuation),
                ("-2.5e3", JsonToken::Number),
                ("}", JsonToken::Punctuation),
            ]
        );
    }

    #[test]
    fn tokenizer_survives_unterminated_input() {
        assert_eq!(tokenize("\"abc").len(), 1);
        assert_eq!(tokenize("tru"), vec![]);
        assert_eq!(tokenize("").len(), 0);
    }

    #[test]
    fn handles_a_one_megabyte_document() {
        let mut doc = String::from("[");
        for i in 0..60_000 {
            if i > 0 {
                doc.push(',');
            }
            doc.push_str(&format!("{{\"i\":{i},\"s\":\"abcdef\"}}"));
        }
        doc.push(']');
        assert!(doc.len() > 1_000_000);
        let out = try_pretty(doc.as_bytes()).unwrap();
        assert!(out.starts_with("[\n  {\n    \"i\": 0,"));
    }
}
