use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::kafka::certs::der_to_pem;
use crate::kafka::config::base_config;
use crate::kafka::worker::{Cmd, WorkerHandle};
use crate::model::jks;
use crate::model::profile::ConnectionProfile;
use crate::model::properties::{ImportedConnection, TrustStore, import_properties};

pub fn prepare_import(path: &Path, ca_dir: &Path) -> anyhow::Result<ImportedConnection> {
    let text = std::fs::read_to_string(path)
        .map_err(|err| anyhow::anyhow!("cannot read {}: {err}", path.display()))?;
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("imported")
        .to_string();
    let mut imported = import_properties(&text, &name).map_err(|err| anyhow::anyhow!("{err}"))?;
    match &imported.trust_store {
        Some(TrustStore::Jks { path: store }) => {
            let pem_path = convert_jks(store, &imported.profile.name, ca_dir)?;
            imported.profile.ca_location = Some(pem_path.to_string_lossy().into_owned());
        }
        Some(TrustStore::Pkcs12 { path: store, .. }) => {
            anyhow::bail!(
                "truststore {} is PKCS12; convert it to PEM and set ssl.ca.location",
                store.display()
            );
        }
        Some(TrustStore::Pem(store)) if !store.exists() => {
            anyhow::bail!("CA file {} does not exist", store.display());
        }
        Some(TrustStore::Pem(_)) | None => {}
    }
    Ok(imported)
}

fn file_stem_for(name: &str) -> String {
    let stem: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    if stem.is_empty() { "connection".to_string() } else { stem }
}

fn convert_jks(store: &Path, profile_name: &str, ca_dir: &Path) -> anyhow::Result<PathBuf> {
    let bytes = std::fs::read(store)
        .map_err(|err| anyhow::anyhow!("cannot read truststore {}: {err}", store.display()))?;
    let certs = jks::trusted_certificates(&bytes)?;
    if certs.is_empty() {
        anyhow::bail!("truststore {} has no trusted certificates", store.display());
    }
    let pem: String = certs.iter().map(|c| der_to_pem(&c.der)).collect();
    std::fs::create_dir_all(ca_dir)?;
    let pem_path = ca_dir.join(format!("{}.pem", file_stem_for(profile_name)));
    std::fs::write(&pem_path, pem)?;
    Ok(pem_path)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReport {
    pub brokers: usize,
    pub topics: usize,
    pub groups: usize,
    pub elapsed: Duration,
}

pub fn check_connection(profile: &ConnectionProfile, password: Option<&str>) -> anyhow::Result<CheckReport> {
    let started = Instant::now();
    let system_roots = if profile.needs_password() && profile.ca_location.is_none() {
        crate::kafka::certs::system_ca_pem()
    } else {
        None
    };
    let worker = WorkerHandle::spawn(&profile.name, base_config(profile, password, system_roots));
    let cluster = futures::executor::block_on(worker.call(|reply| Cmd::FetchCluster { reply }))?;
    let groups = futures::executor::block_on(worker.call(|reply| Cmd::ListGroups { reply }))?;
    Ok(CheckReport {
        brokers: cluster.brokers.len(),
        topics: cluster.topics.len(),
        groups: groups.len(),
        elapsed: started.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jks_with_one_cert(der: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&0xFEED_FEEDu32.to_be_bytes());
        b.extend_from_slice(&2u32.to_be_bytes());
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&2u32.to_be_bytes());
        b.extend_from_slice(&2u16.to_be_bytes());
        b.extend_from_slice(b"ca");
        b.extend_from_slice(&[0u8; 8]);
        b.extend_from_slice(&5u16.to_be_bytes());
        b.extend_from_slice(b"X.509");
        b.extend_from_slice(&(der.len() as u32).to_be_bytes());
        b.extend_from_slice(der);
        b.extend_from_slice(&[0u8; 20]);
        b
    }

    #[test]
    fn converts_a_jks_truststore_to_pem_and_sets_ca_location() {
        let dir = std::env::temp_dir().join(format!("kafkamitter-import-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("trust.jks");
        std::fs::write(&store, jks_with_one_cert(&[0x30, 0x03, 0x02, 0x01, 0x05])).unwrap();
        let props = dir.join("staging.properties");
        std::fs::write(
            &props,
            format!(
                "bootstrap.servers=b:1\nsecurity.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=x required username=\"u\" password=\"p\";\nssl.truststore.type=JKS\nssl.truststore.location={}\n",
                store.display()
            ),
        )
        .unwrap();
        let ca_dir = dir.join("ca");
        let imported = prepare_import(&props, &ca_dir).unwrap();
        assert_eq!(imported.profile.name, "staging");
        let pem_path = PathBuf::from(imported.profile.ca_location.clone().unwrap());
        assert_eq!(pem_path, ca_dir.join("staging.pem"));
        let pem = std::fs::read_to_string(&pem_path).unwrap();
        assert!(pem.starts_with("-----BEGIN CERTIFICATE-----\nMAMCAQU=\n-----END CERTIFICATE-----"));
        assert_eq!(imported.password.as_deref(), Some("p"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reports_missing_files() {
        let err = prepare_import(Path::new("/nonexistent/x.properties"), Path::new("/tmp")).unwrap_err();
        assert!(err.to_string().contains("cannot read"));
    }
}
