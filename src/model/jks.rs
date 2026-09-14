const JKS_MAGIC: u32 = 0xFEED_FEED;
const TAG_PRIVATE_KEY: u32 = 1;
const TAG_TRUSTED_CERT: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JksCertificate {
    pub alias: String,
    pub der: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JksError {
    NotJks,
    UnsupportedVersion(u32),
    Truncated,
    BadAlias,
    UnknownTag(u32),
}

impl std::fmt::Display for JksError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JksError::NotJks => write!(f, "file is not a JKS keystore"),
            JksError::UnsupportedVersion(v) => write!(f, "JKS version {v} is not supported"),
            JksError::Truncated => write!(f, "JKS keystore is truncated"),
            JksError::BadAlias => write!(f, "JKS alias is not valid UTF-8"),
            JksError::UnknownTag(t) => write!(f, "JKS entry tag {t} is unknown"),
        }
    }
}

impl std::error::Error for JksError {}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], JksError> {
        let end = self.pos.checked_add(n).ok_or(JksError::Truncated)?;
        if end > self.bytes.len() {
            return Err(JksError::Truncated);
        }
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u16(&mut self) -> Result<u16, JksError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, JksError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn utf(&mut self) -> Result<String, JksError> {
        let len = self.u16()? as usize;
        let raw = self.take(len)?;
        String::from_utf8(raw.to_vec()).map_err(|_| JksError::BadAlias)
    }

    fn bytes_u32(&mut self) -> Result<&'a [u8], JksError> {
        let len = self.u32()? as usize;
        self.take(len)
    }
}

pub fn trusted_certificates(bytes: &[u8]) -> Result<Vec<JksCertificate>, JksError> {
    let mut r = Reader { bytes, pos: 0 };
    if r.u32()? != JKS_MAGIC {
        return Err(JksError::NotJks);
    }
    let version = r.u32()?;
    if version != 1 && version != 2 {
        return Err(JksError::UnsupportedVersion(version));
    }
    let count = r.u32()? as usize;
    let mut certs = Vec::new();
    for _ in 0..count {
        let tag = r.u32()?;
        let alias = r.utf()?;
        let _timestamp = r.take(8)?;
        match tag {
            TAG_PRIVATE_KEY => {
                let _encrypted_key = r.bytes_u32()?;
                let chain_len = r.u32()? as usize;
                for _ in 0..chain_len {
                    if version == 2 {
                        let _cert_type = r.utf()?;
                    }
                    let _cert = r.bytes_u32()?;
                }
            }
            TAG_TRUSTED_CERT => {
                if version == 2 {
                    let _cert_type = r.utf()?;
                }
                let der = r.bytes_u32()?.to_vec();
                certs.push(JksCertificate { alias, der });
            }
            other => return Err(JksError::UnknownTag(other)),
        }
    }
    Ok(certs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf(s: &str) -> Vec<u8> {
        let mut out = (s.len() as u16).to_be_bytes().to_vec();
        out.extend_from_slice(s.as_bytes());
        out
    }

    fn build(version: u32, entries: &[(u32, &str, &[u8])]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&JKS_MAGIC.to_be_bytes());
        b.extend_from_slice(&version.to_be_bytes());
        b.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for (tag, alias, data) in entries {
            b.extend_from_slice(&tag.to_be_bytes());
            b.extend_from_slice(&utf(alias));
            b.extend_from_slice(&[0u8; 8]);
            match *tag {
                TAG_TRUSTED_CERT => {
                    if version == 2 {
                        b.extend_from_slice(&utf("X.509"));
                    }
                    b.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    b.extend_from_slice(data);
                }
                _ => {
                    b.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    b.extend_from_slice(data);
                    b.extend_from_slice(&1u32.to_be_bytes());
                    if version == 2 {
                        b.extend_from_slice(&utf("X.509"));
                    }
                    b.extend_from_slice(&3u32.to_be_bytes());
                    b.extend_from_slice(&[9, 9, 9]);
                }
            }
        }
        b.extend_from_slice(&[0u8; 20]);
        b
    }

    #[test]
    fn reads_trusted_certificates_and_skips_private_keys() {
        let bytes = build(2, &[(TAG_PRIVATE_KEY, "client", &[1, 2, 3, 4]), (TAG_TRUSTED_CERT, "ca", &[0x30, 0x03, 0x02, 0x01, 0x01])]);
        let certs = trusted_certificates(&bytes).unwrap();
        assert_eq!(
            certs,
            vec![JksCertificate {
                alias: "ca".into(),
                der: vec![0x30, 0x03, 0x02, 0x01, 0x01]
            }]
        );
    }

    #[test]
    fn reads_version_one_stores() {
        let bytes = build(1, &[(TAG_TRUSTED_CERT, "root", &[7, 7])]);
        assert_eq!(trusted_certificates(&bytes).unwrap()[0].der, vec![7, 7]);
    }

    #[test]
    fn rejects_other_formats_and_truncation() {
        assert_eq!(trusted_certificates(&[0, 1, 2, 3, 0, 0, 0, 2]), Err(JksError::NotJks));
        let bytes = build(2, &[(TAG_TRUSTED_CERT, "ca", &[1, 2, 3])]);
        assert_eq!(trusted_certificates(&bytes[..bytes.len() - 25]), Err(JksError::Truncated));
        let mut bad = build(2, &[(TAG_TRUSTED_CERT, "ca", &[1])]);
        bad[4..8].copy_from_slice(&9u32.to_be_bytes());
        assert_eq!(trusted_certificates(&bad), Err(JksError::UnsupportedVersion(9)));
    }
}
