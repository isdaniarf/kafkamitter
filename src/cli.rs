use std::path::Path;

use kafkamitter::kafka::import::{check_connection, prepare_import};
use kafkamitter::model::profile::{ca_dir, load_profiles, profiles_path, save_profiles};

pub fn run(args: &[String]) -> Option<i32> {
    match args {
        [flag, path] if flag == "--check" => Some(check(Path::new(path))),
        [flag, path] if flag == "--import" => Some(import(Path::new(path))),
        [flag] if flag == "--help" || flag == "-h" => {
            println!("usage: kafkamitter [--check <file.properties> | --import <file.properties>]");
            Some(0)
        }
        _ => None,
    }
}

fn check(path: &Path) -> i32 {
    let temp_ca_dir = std::env::temp_dir().join(format!("kafkamitter-check-{}", std::process::id()));
    let imported = match prepare_import(path, &temp_ca_dir) {
        Ok(imported) => imported,
        Err(err) => {
            eprintln!("import failed: {err}");
            return 1;
        }
    };
    let profile = &imported.profile;
    println!(
        "checking {} ({}, {})",
        profile.name,
        profile.bootstrap_servers,
        match &profile.security {
            kafkamitter::model::profile::Security::Plaintext => "PLAINTEXT".to_string(),
            kafkamitter::model::profile::Security::SaslSsl { mechanism, username } =>
                format!("SASL_SSL {} as {username}", mechanism.kafka_name()),
        }
    );
    let result = check_connection(profile, imported.password.as_deref());
    let _ = std::fs::remove_dir_all(&temp_ca_dir);
    match result {
        Ok(report) => {
            println!(
                "ok: {} brokers, {} topics, {} consumer groups in {:.0} ms",
                report.brokers,
                report.topics,
                report.groups,
                report.elapsed.as_secs_f64() * 1000.0
            );
            0
        }
        Err(err) => {
            eprintln!("failed: {err}");
            1
        }
    }
}

fn import(path: &Path) -> i32 {
    let imported = match prepare_import(path, &ca_dir()) {
        Ok(imported) => imported,
        Err(err) => {
            eprintln!("import failed: {err}");
            return 1;
        }
    };
    let mut profile = imported.profile;
    let profiles_file = profiles_path();
    let mut profiles = match load_profiles(&profiles_file) {
        Ok(profiles) => profiles,
        Err(err) => {
            eprintln!("cannot read {}: {err}", profiles_file.display());
            return 1;
        }
    };
    if let Some(existing) = profiles.iter().find(|p| p.name == profile.name) {
        profile.id = existing.id.clone();
    }
    let name = profile.name.clone();
    match profiles.iter_mut().find(|p| p.id == profile.id) {
        Some(slot) => *slot = profile,
        None => profiles.push(profile),
    }
    if let Err(err) = save_profiles(&profiles_file, &profiles) {
        eprintln!("cannot save {}: {err}", profiles_file.display());
        return 1;
    }
    println!("imported {name} into {}", profiles_file.display());
    0
}
