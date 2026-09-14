#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    Truncated,
    NegativeLength,
    InvalidUtf8,
    UnsupportedVersion(i16),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Truncated => write!(f, "assignment bytes are truncated"),
            DecodeError::NegativeLength => write!(f, "assignment contains a negative length"),
            DecodeError::InvalidUtf8 => write!(f, "assignment topic name is not UTF-8"),
            DecodeError::UnsupportedVersion(v) => write!(f, "assignment version {v} is not supported"),
        }
    }
}

impl std::error::Error for DecodeError {}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::Truncated)?;
        if end > self.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn i16(&mut self) -> Result<i16, DecodeError> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn len(&mut self) -> Result<usize, DecodeError> {
        let n = self.i32()?;
        usize::try_from(n).map_err(|_| DecodeError::NegativeLength)
    }

    fn string(&mut self) -> Result<String, DecodeError> {
        let n = self.i16()?;
        let n = usize::try_from(n).map_err(|_| DecodeError::NegativeLength)?;
        let raw = self.take(n)?;
        String::from_utf8(raw.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }
}

pub fn decode_assignment(bytes: &[u8]) -> Result<Vec<TopicAssignment>, DecodeError> {
    let mut r = Reader { bytes, pos: 0 };
    let version = r.i16()?;
    if version < 0 {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    let topic_count = r.len()?;
    let mut out = Vec::with_capacity(topic_count.min(1024));
    for _ in 0..topic_count {
        let topic = r.string()?;
        let partition_count = r.len()?;
        let mut partitions = Vec::with_capacity(partition_count.min(4096));
        for _ in 0..partition_count {
            partitions.push(r.i32()?);
        }
        out.push(TopicAssignment { topic, partitions });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(version: i16, topics: &[(&str, &[i32])], user_data: Option<&[u8]>) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&version.to_be_bytes());
        b.extend_from_slice(&(topics.len() as i32).to_be_bytes());
        for (topic, partitions) in topics {
            b.extend_from_slice(&(topic.len() as i16).to_be_bytes());
            b.extend_from_slice(topic.as_bytes());
            b.extend_from_slice(&(partitions.len() as i32).to_be_bytes());
            for p in *partitions {
                b.extend_from_slice(&p.to_be_bytes());
            }
        }
        match user_data {
            Some(d) => {
                b.extend_from_slice(&(d.len() as i32).to_be_bytes());
                b.extend_from_slice(d);
            }
            None => b.extend_from_slice(&(-1i32).to_be_bytes()),
        }
        b
    }

    #[test]
    fn decodes_one_topic() {
        let bytes = encode(0, &[("orders", &[0, 2])], None);
        let out = decode_assignment(&bytes).unwrap();
        assert_eq!(
            out,
            vec![TopicAssignment {
                topic: "orders".into(),
                partitions: vec![0, 2]
            }]
        );
    }

    #[test]
    fn decodes_several_topics_and_ignores_user_data() {
        let bytes = encode(1, &[("a", &[1]), ("b", &[]), ("c", &[3, 4, 5])], Some(b"opaque"));
        let out = decode_assignment(&bytes).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[1].topic, "b");
        assert!(out[1].partitions.is_empty());
        assert_eq!(out[2].partitions, vec![3, 4, 5]);
    }

    #[test]
    fn decodes_an_empty_assignment() {
        let bytes = encode(0, &[], None);
        assert_eq!(decode_assignment(&bytes).unwrap(), vec![]);
    }

    #[test]
    fn rejects_truncated_input() {
        let bytes = encode(0, &[("orders", &[0, 1, 2])], None);
        for cut in 0..bytes.len() - 4 {
            assert_eq!(decode_assignment(&bytes[..cut]), Err(DecodeError::Truncated), "cut at {cut}");
        }
    }

    #[test]
    fn rejects_negative_lengths_and_versions() {
        let mut bytes = encode(0, &[("orders", &[0])], None);
        bytes[2..6].copy_from_slice(&(-5i32).to_be_bytes());
        assert_eq!(decode_assignment(&bytes), Err(DecodeError::NegativeLength));

        let mut bytes = encode(0, &[("orders", &[0])], None);
        bytes[0..2].copy_from_slice(&(-1i16).to_be_bytes());
        assert_eq!(decode_assignment(&bytes), Err(DecodeError::UnsupportedVersion(-1)));
    }

    #[test]
    fn rejects_invalid_utf8_topic_names() {
        let mut bytes = encode(0, &[("ab", &[0])], None);
        bytes[8] = 0xff;
        assert_eq!(decode_assignment(&bytes), Err(DecodeError::InvalidUtf8));
    }
}
