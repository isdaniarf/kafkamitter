use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanism {
    pub fn kafka_name(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    pub const ALL: [SaslMechanism; 3] = [
        SaslMechanism::Plain,
        SaslMechanism::ScramSha256,
        SaslMechanism::ScramSha512,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Security {
    Plaintext,
    SaslSsl {
        mechanism: SaslMechanism,
        username: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub bootstrap_servers: String,
    pub security: Security,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_location: Option<String>,
    /// The SASL password, stored in this file. The file is readable only by the owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

impl ConnectionProfile {
    pub fn needs_password(&self) -> bool {
        matches!(self.security, Security::SaslSsl { .. })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ProfilesFile {
    version: u32,
    profiles: Vec<ConnectionProfile>,
}

const FILE_VERSION: u32 = 1;

pub fn app_support_dir() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    home.join("Library").join("Application Support").join("kafkamitter")
}

pub fn profiles_path() -> PathBuf {
    app_support_dir().join("connections.json")
}

pub fn ca_dir() -> PathBuf {
    app_support_dir().join("ca")
}

pub fn load_profiles(path: &Path) -> anyhow::Result<Vec<ConnectionProfile>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let file: ProfilesFile = serde_json::from_slice(&bytes)?;
    Ok(file.profiles)
}

pub fn save_profiles(path: &Path, profiles: &[ConnectionProfile]) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let file = ProfilesFile {
        version: FILE_VERSION,
        profiles: profiles.to_vec(),
    };
    let json = serde_json::to_vec_pretty(&file)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json)?;
    restrict_to_owner(&tmp)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// The connections file holds SASL passwords, so only the owner may read it.
fn restrict_to_owner(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

pub fn new_profile_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    /// Two profiles can be created inside one clock tick, so a counter
    /// keeps their ids apart. Importing several files at once does this.
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{:x}-{:x}-{:x}", nanos, std::process::id(), sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kafkamitter-test-{}-{}", std::process::id(), name));
        dir.join("nested").join("connections.json")
    }

    #[test]
    fn round_trips_profiles_through_json() {
        let path = temp_path("roundtrip");
        let profiles = vec![
            ConnectionProfile {
                id: "a".into(),
                name: "local".into(),
                bootstrap_servers: "localhost:9092".into(),
                security: Security::Plaintext,
                ca_location: None,
                password: None,
            },
            ConnectionProfile {
                id: "b".into(),
                name: "cloud".into(),
                bootstrap_servers: "broker:9094".into(),
                security: Security::SaslSsl {
                    mechanism: SaslMechanism::ScramSha512,
                    username: "user".into(),
                },
                ca_location: Some("/tmp/ca.pem".into()),
                password: Some("s3cret".into()),
            },
        ];
        save_profiles(&path, &profiles).unwrap();
        let loaded = load_profiles(&path).unwrap();
        assert_eq!(loaded, profiles);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("\"type\": \"sasl_ssl\""));
        assert!(!text.contains("ca_location\": null"));
        assert!(text.contains("\"password\": \"s3cret\""));
        assert!(!text.contains("password\": null"));
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "the connections file must be owner-only");
        std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap()).unwrap();
    }

    #[test]
    fn missing_file_yields_no_profiles() {
        let path = temp_path("missing");
        assert_eq!(load_profiles(&path).unwrap(), vec![]);
    }

    #[test]
    fn profile_ids_are_unique_even_inside_one_clock_tick() {
        let ids: std::collections::HashSet<String> = (0..1000).map(|_| new_profile_id()).collect();
        assert_eq!(ids.len(), 1000, "every generated id must differ");
    }
}
