use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::model::profile::app_support_dir;

/// Which appearance the window uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeChoice {
    /// Follow the light or dark setting of macOS.
    #[default]
    System,
    Light,
    Dark,
}

impl ThemeChoice {
    pub const ALL: [ThemeChoice; 3] = [ThemeChoice::System, ThemeChoice::Light, ThemeChoice::Dark];

    pub fn label(self) -> &'static str {
        match self {
            ThemeChoice::System => "Follow the system",
            ThemeChoice::Light => "Light",
            ThemeChoice::Dark => "Dark",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub theme: ThemeChoice,
    pub auto_consume_on_select: bool,
    pub newest_per_partition: i64,
    pub open_newest_message: bool,
    pub pretty_json_default: bool,
    pub max_messages: usize,
    pub max_megabytes: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            theme: ThemeChoice::System,
            auto_consume_on_select: true,
            newest_per_partition: 200,
            open_newest_message: true,
            pretty_json_default: true,
            max_messages: 10_000,
            max_megabytes: 256,
        }
    }
}

impl Settings {
    pub fn max_bytes(&self) -> usize {
        self.max_megabytes.max(1) * 1024 * 1024
    }
}

pub fn settings_path() -> PathBuf {
    app_support_dir().join("settings.json")
}

pub fn load_settings(path: &Path) -> Settings {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => Settings::default(),
    }
}

pub fn save_settings(path: &Path, settings: &Settings) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let json = serde_json::to_vec_pretty(settings)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_defaults_missing_fields() {
        let dir = std::env::temp_dir().join(format!("kafkamitter-settings-{}", std::process::id()));
        let path = dir.join("settings.json");
        assert_eq!(load_settings(&path), Settings::default());
        let custom = Settings {
            theme: ThemeChoice::Dark,
            newest_per_partition: 50,
            max_megabytes: 64,
            ..Settings::default()
        };
        save_settings(&path, &custom).unwrap();
        assert_eq!(load_settings(&path), custom);
        std::fs::write(&path, br#"{"auto_consume_on_select": false, "unknown": 1}"#).unwrap();
        let partial = load_settings(&path);
        assert!(!partial.auto_consume_on_select);
        assert_eq!(partial.newest_per_partition, 200);
        assert_eq!(partial.theme, ThemeChoice::System, "a missing theme follows the system");
        assert_eq!(Settings::default().max_bytes(), 256 * 1024 * 1024);
        std::fs::write(&path, br#"{"theme":"dark"}"#).unwrap();
        assert_eq!(load_settings(&path).theme, ThemeChoice::Dark);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
