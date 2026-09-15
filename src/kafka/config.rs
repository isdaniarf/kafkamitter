use rdkafka::ClientConfig;

use crate::model::profile::{ConnectionProfile, Security};

pub fn base_config(
    profile: &ConnectionProfile,
    password: Option<&str>,
    system_ca_pem: Option<&str>,
) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &profile.bootstrap_servers)
        .set("client.id", "kafkamitter")
        .set("socket.keepalive.enable", "true")
        .set("log.connection.close", "false");
    match &profile.security {
        Security::Plaintext => {
            cfg.set("security.protocol", "plaintext");
        }
        Security::SaslSsl {
            mechanism,
            username,
        } => {
            cfg.set("security.protocol", "sasl_ssl")
                .set("sasl.mechanism", mechanism.kafka_name())
                .set("sasl.username", username)
                .set("sasl.password", password.unwrap_or_default());
            match (&profile.ca_location, system_ca_pem) {
                (Some(path), _) => {
                    cfg.set("ssl.ca.location", path);
                }
                (None, Some(pem)) => {
                    cfg.set("ssl.ca.pem", pem);
                }
                (None, None) => {}
            }
        }
    }
    cfg
}

pub fn viewer_group_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("kafkamitter-{}-{:x}", std::process::id(), nanos)
}

pub fn viewer_consumer_config(base: &ClientConfig) -> ClientConfig {
    let mut cfg = base.clone();
    cfg.set("group.id", viewer_group_id())
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest")
        .set("queued.max.messages.kbytes", "4096")
        .set("queued.min.messages", "5000")
        .set("fetch.wait.max.ms", "100");
    cfg
}

pub fn group_reader_config(base: &ClientConfig, group_id: &str) -> ClientConfig {
    let mut cfg = base.clone();
    cfg.set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    cfg
}

pub fn producer_config(base: &ClientConfig) -> ClientConfig {
    let mut cfg = base.clone();
    cfg.set("message.timeout.ms", "10000").set("acks", "all");
    cfg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::profile::SaslMechanism;

    fn plaintext_profile() -> ConnectionProfile {
        ConnectionProfile {
            id: "p".into(),
            name: "local".into(),
            bootstrap_servers: "localhost:9092".into(),
            security: Security::Plaintext,
            ca_location: None,
            password: None,
        }
    }

    fn sasl_profile(mechanism: SaslMechanism, ca_location: Option<&str>) -> ConnectionProfile {
        ConnectionProfile {
            id: "s".into(),
            name: "cloud".into(),
            bootstrap_servers: "b1:9094,b2:9094".into(),
            security: Security::SaslSsl {
                mechanism,
                username: "alice".into(),
            },
            ca_location: ca_location.map(String::from),
            password: None,
        }
    }

    #[test]
    fn plaintext_sets_only_the_basics() {
        let cfg = base_config(&plaintext_profile(), None, Some("PEM"));
        assert_eq!(cfg.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(cfg.get("security.protocol"), Some("plaintext"));
        assert_eq!(cfg.get("client.id"), Some("kafkamitter"));
        assert_eq!(cfg.get("sasl.mechanism"), None);
        assert_eq!(cfg.get("ssl.ca.pem"), None);
    }

    #[test]
    fn sasl_ssl_sets_mechanism_credentials_and_system_roots() {
        let cfg = base_config(&sasl_profile(SaslMechanism::Plain, None), Some("secret"), Some("PEM"));
        assert_eq!(cfg.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(cfg.get("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(cfg.get("sasl.username"), Some("alice"));
        assert_eq!(cfg.get("sasl.password"), Some("secret"));
        assert_eq!(cfg.get("ssl.ca.pem"), Some("PEM"));
        assert_eq!(cfg.get("ssl.ca.location"), None);
    }

    #[test]
    fn scram_mechanisms_map_to_kafka_names() {
        let cfg = base_config(&sasl_profile(SaslMechanism::ScramSha256, None), None, None);
        assert_eq!(cfg.get("sasl.mechanism"), Some("SCRAM-SHA-256"));
        assert_eq!(cfg.get("sasl.password"), Some(""));
        let cfg = base_config(&sasl_profile(SaslMechanism::ScramSha512, None), None, None);
        assert_eq!(cfg.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
    }

    #[test]
    fn explicit_ca_location_wins_over_system_roots() {
        let cfg = base_config(&sasl_profile(SaslMechanism::Plain, Some("/etc/ca.pem")), None, Some("PEM"));
        assert_eq!(cfg.get("ssl.ca.location"), Some("/etc/ca.pem"));
        assert_eq!(cfg.get("ssl.ca.pem"), None);
    }

    #[test]
    fn viewer_consumer_never_commits() {
        let cfg = viewer_consumer_config(&base_config(&plaintext_profile(), None, None));
        assert!(cfg.get("group.id").unwrap().starts_with("kafkamitter-"));
        assert_eq!(cfg.get("enable.auto.commit"), Some("false"));
        assert_eq!(cfg.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(cfg.get("enable.partition.eof"), Some("true"));
        assert_eq!(cfg.get("queued.max.messages.kbytes"), Some("4096"));
        assert_eq!(cfg.get("queued.min.messages"), Some("5000"));
        assert_eq!(cfg.get("security.protocol"), Some("plaintext"));
    }

    #[test]
    fn group_reader_uses_the_target_group() {
        let cfg = group_reader_config(&base_config(&plaintext_profile(), None, None), "billing");
        assert_eq!(cfg.get("group.id"), Some("billing"));
        assert_eq!(cfg.get("enable.auto.commit"), Some("false"));
    }
}
