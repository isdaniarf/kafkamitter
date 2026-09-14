use std::sync::OnceLock;

use base64::Engine as _;

static SYSTEM_CA_PEM: OnceLock<Option<String>> = OnceLock::new();

pub fn system_ca_pem() -> Option<&'static str> {
    SYSTEM_CA_PEM.get_or_init(load_system_roots).as_deref()
}

fn load_system_roots() -> Option<String> {
    let result = rustls_native_certs::load_native_certs();
    for err in &result.errors {
        log::warn!("system root certificate load error: {err}");
    }
    if result.certs.is_empty() {
        return None;
    }
    let mut pem = String::new();
    for cert in &result.certs {
        pem.push_str(&der_to_pem(cert.as_ref()));
    }
    Some(pem)
}

pub fn der_to_pem(der: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::with_capacity(encoded.len() + encoded.len() / 64 + 64);
    out.push_str("-----BEGIN CERTIFICATE-----\n");
    for chunk in encoded.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_base64_at_64_columns() {
        let der: Vec<u8> = (0..100u8).collect();
        let pem = der_to_pem(&der);
        let lines: Vec<&str> = pem.lines().collect();
        assert_eq!(lines[0], "-----BEGIN CERTIFICATE-----");
        assert_eq!(lines[lines.len() - 1], "-----END CERTIFICATE-----");
        let body = &lines[1..lines.len() - 1];
        assert_eq!(body.len(), 3);
        assert!(body[..2].iter().all(|l| l.len() == 64));
        assert_eq!(body[2].len(), 136 - 128);
        let joined: String = body.concat();
        let decoded = base64::engine::general_purpose::STANDARD.decode(joined).unwrap();
        assert_eq!(decoded, der);
    }

    #[test]
    fn system_roots_are_available_on_macos() {
        let pem = system_ca_pem().expect("macOS keychain roots");
        assert!(pem.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(pem.matches("-----END CERTIFICATE-----").count() > 10);
    }
}
