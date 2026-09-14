use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::model::profile::{ConnectionProfile, SaslMechanism, Security, new_profile_id};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustStore {
    Pem(PathBuf),
    Jks {
        path: PathBuf,
    },
    Pkcs12 {
        path: PathBuf,
        password: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedConnection {
    pub profile: ConnectionProfile,
    pub password: Option<String>,
    pub trust_store: Option<TrustStore>,
}

pub fn parse_properties(text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let mut pending = String::new();
    for raw in text.lines() {
        let line = raw.trim_start();
        if pending.is_empty() && (line.is_empty() || line.starts_with('#') || line.starts_with('!')) {
            continue;
        }
        let continued = line.ends_with('\\') && !line.ends_with("\\\\");
        let piece = if continued { &line[..line.len() - 1] } else { line };
        pending.push_str(if pending.is_empty() { piece } else { piece.trim_start() });
        if continued {
            continue;
        }
        let entry = std::mem::take(&mut pending);
        let split = entry.find(['=', ':']).map(|i| (entry[..i].trim(), entry[i + 1..].trim()));
        if let Some((key, value)) = split {
            if !key.is_empty() {
                map.insert(key.to_string(), unescape(value));
            }
        }
    }
    map
}

fn unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

fn jaas_value(config: &str, key: &str) -> Option<String> {
    let start = config.find(&format!("{key}=")).map(|i| i + key.len() + 1)?;
    let rest = &config[start..];
    if let Some(quoted) = rest.strip_prefix('"') {
        let end = quoted.find('"')?;
        return Some(quoted[..end].to_string());
    }
    let end = rest.find([' ', ';']).unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

pub fn import_properties(text: &str, name: &str) -> Result<ImportedConnection, String> {
    let props = parse_properties(text);
    let bootstrap = props
        .get("bootstrap.servers")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or("bootstrap.servers is missing")?;
    let protocol = props
        .get("security.protocol")
        .map(|s| s.trim().to_ascii_uppercase())
        .unwrap_or_else(|| "PLAINTEXT".to_string());
    let (security, password) = match protocol.as_str() {
        "PLAINTEXT" => (Security::Plaintext, None),
        "SASL_SSL" => {
            let mechanism = match props
                .get("sasl.mechanism")
                .or_else(|| props.get("sasl.mechanisms"))
                .map(|s| s.trim().to_ascii_uppercase())
                .as_deref()
            {
                Some("PLAIN") => SaslMechanism::Plain,
                Some("SCRAM-SHA-256") => SaslMechanism::ScramSha256,
                Some("SCRAM-SHA-512") => SaslMechanism::ScramSha512,
                Some(other) => return Err(format!("sasl.mechanism {other} is not supported")),
                None => return Err("sasl.mechanism is missing".to_string()),
            };
            let jaas = props.get("sasl.jaas.config").map(String::as_str).unwrap_or("");
            let username = props
                .get("sasl.username")
                .cloned()
                .or_else(|| jaas_value(jaas, "username"))
                .ok_or("SASL username is missing")?;
            let password = props
                .get("sasl.password")
                .cloned()
                .or_else(|| jaas_value(jaas, "password"));
            (
                Security::SaslSsl {
                    mechanism,
                    username,
                },
                password,
            )
        }
        other => return Err(format!("security.protocol {other} is not supported")),
    };
    let trust_store = props
        .get("ssl.ca.location")
        .map(|p| TrustStore::Pem(PathBuf::from(p)))
        .or_else(|| {
            let path = props.get("ssl.truststore.location")?;
            let kind = props
                .get("ssl.truststore.type")
                .map(|t| t.trim().to_ascii_uppercase())
                .unwrap_or_else(|| guess_store_type(Path::new(path)));
            Some(match kind.as_str() {
                "PEM" => TrustStore::Pem(PathBuf::from(path)),
                "PKCS12" => TrustStore::Pkcs12 {
                    path: PathBuf::from(path),
                    password: props.get("ssl.truststore.password").cloned(),
                },
                _ => TrustStore::Jks {
                    path: PathBuf::from(path),
                },
            })
        });
    let ca_location = match &trust_store {
        Some(TrustStore::Pem(path)) => Some(path.to_string_lossy().into_owned()),
        _ => None,
    };
    Ok(ImportedConnection {
        profile: ConnectionProfile {
            id: new_profile_id(),
            name: name.to_string(),
            bootstrap_servers: bootstrap,
            security,
            ca_location,
            password: password.clone(),
        },
        password,
        trust_store,
    })
}

fn guess_store_type(path: &Path) -> String {
    match path.extension().and_then(|e| e.to_str()).map(|e| e.to_ascii_lowercase()).as_deref() {
        Some("pem") | Some("crt") | Some("cer") => "PEM".into(),
        Some("p12") | Some("pfx") => "PKCS12".into(),
        _ => "JKS".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_java_properties_with_comments_and_continuations() {
        let text = "# comment\n! bang\nkey.one=value one\nkey.two: two\nlong=a,\\\n    b\nescaped=x\\=y\n\n";
        let map = parse_properties(text);
        assert_eq!(map["key.one"], "value one");
        assert_eq!(map["key.two"], "two");
        assert_eq!(map["long"], "a,b");
        assert_eq!(map["escaped"], "x=y");
        assert_eq!(map.len(), 4);
    }

    #[test]
    fn imports_a_scram_sasl_ssl_file_with_a_jks_truststore() {
        let text = "bootstrap.servers=broker:26352\nsecurity.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" password=\"s3cr;et\";\nssl.truststore.type=JKS\nssl.truststore.location=/tmp/ts.jks\nssl.truststore.password=tspw\n";
        let imported = import_properties(text, "dev").unwrap();
        assert_eq!(imported.profile.name, "dev");
        assert_eq!(imported.profile.bootstrap_servers, "broker:26352");
        assert_eq!(
            imported.profile.security,
            Security::SaslSsl {
                mechanism: SaslMechanism::ScramSha512,
                username: "alice".into()
            }
        );
        assert_eq!(imported.password.as_deref(), Some("s3cr;et"));
        assert_eq!(
            imported.trust_store,
            Some(TrustStore::Jks {
                path: PathBuf::from("/tmp/ts.jks")
            })
        );
        assert_eq!(imported.profile.ca_location, None);
    }

    #[test]
    fn imports_plaintext_and_pem_ca() {
        let imported = import_properties("bootstrap.servers=localhost:9092\n", "local").unwrap();
        assert_eq!(imported.profile.security, Security::Plaintext);
        assert_eq!(imported.password, None);
        assert_eq!(imported.trust_store, None);

        let text = "bootstrap.servers=b:1\nsecurity.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.username=u\nsasl.password=p\nssl.ca.location=/etc/ca.pem\n";
        let imported = import_properties(text, "cloud").unwrap();
        assert_eq!(imported.profile.ca_location.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(imported.trust_store, Some(TrustStore::Pem(PathBuf::from("/etc/ca.pem"))));
        assert_eq!(imported.password.as_deref(), Some("p"));
    }

    #[test]
    fn the_example_files_in_the_repository_stay_valid() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

        let text = std::fs::read_to_string(root.join("examples/plaintext.properties")).unwrap();
        let local = import_properties(&text, "plaintext").unwrap();
        assert_eq!(local.profile.bootstrap_servers, "localhost:9092");
        assert_eq!(local.profile.security, Security::Plaintext);
        assert_eq!(local.password, None);

        let text = std::fs::read_to_string(root.join("examples/sasl-ssl.properties")).unwrap();
        let cloud = import_properties(&text, "sasl-ssl").unwrap();
        assert_eq!(
            cloud.profile.security,
            Security::SaslSsl {
                mechanism: SaslMechanism::ScramSha512,
                username: "my-user".into()
            }
        );
        assert_eq!(cloud.password.as_deref(), Some("my-password"));
        assert!(matches!(cloud.trust_store, Some(TrustStore::Jks { .. })));
    }

    #[test]
    fn rejects_unsupported_protocols_and_mechanisms() {
        let err = import_properties("bootstrap.servers=b:1\nsecurity.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\n", "x").unwrap_err();
        assert!(err.contains("SASL_PLAINTEXT"));
        let err = import_properties("bootstrap.servers=b:1\nsecurity.protocol=SASL_SSL\nsasl.mechanism=GSSAPI\n", "x").unwrap_err();
        assert!(err.contains("GSSAPI"));
        let err = import_properties("security.protocol=PLAINTEXT\n", "x").unwrap_err();
        assert!(err.contains("bootstrap.servers"));
    }
}
