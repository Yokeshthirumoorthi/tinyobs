use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

/// TinyObs configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub application: ApplicationSettings,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub compaction: CompactionConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
}

/// Application server settings
#[derive(Debug, Clone, Deserialize)]
pub struct ApplicationSettings {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

/// Runtime environment
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Local,
    Production,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Local => "local",
            Environment::Production => "production",
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "local" | "dev" | "development" => Ok(Environment::Local),
            "production" | "prod" => Ok(Environment::Production),
            other => anyhow::bail!("Unknown environment: {}", other),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    4318
}

/// Storage configuration
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_retention_hours")]
    pub retention_hours: u64,
}

/// Compaction configuration
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionConfig {
    #[serde(default = "default_compaction_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_hot_window")]
    pub hot_window_secs: u64,
    #[serde(default = "default_merge_interval")]
    pub merge_interval_secs: u64,
    #[serde(default = "default_merge_min_size")]
    pub merge_min_size_bytes: u64,
}

/// Ingest configuration
#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    #[serde(default = "default_session_attribute")]
    pub session_attribute: String,
}

fn default_data_dir() -> String {
    "./data".to_string()
}

fn default_retention_hours() -> u64 {
    168
}

fn default_compaction_interval() -> u64 {
    5
}

fn default_hot_window() -> u64 {
    10
}

fn default_merge_interval() -> u64 {
    3600
}

fn default_merge_min_size() -> u64 {
    1024 * 1024
}

fn default_session_attribute() -> String {
    "session.id".to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            retention_hours: default_retention_hours(),
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_compaction_interval(),
            hot_window_secs: default_hot_window(),
            merge_interval_secs: default_merge_interval(),
            merge_min_size_bytes: default_merge_min_size(),
        }
    }
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            session_attribute: default_session_attribute(),
        }
    }
}

impl Default for ApplicationSettings {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            application: ApplicationSettings::default(),
            storage: StorageConfig::default(),
            compaction: CompactionConfig::default(),
            ingest: IngestConfig::default(),
        }
    }
}

impl Config {
    pub fn get_configuration() -> Result<Self> {
        let base_path = std::env::current_dir().context("Failed to get current directory")?;
        let config_dir = base_path.join("configuration");

        let environment: Environment = std::env::var("TINYOBS_ENV")
            .unwrap_or_else(|_| "local".into())
            .try_into()?;

        let environment_filename = format!("{}.toml", environment.as_str());

        let settings = config::Config::builder()
            .add_source(config::File::from(config_dir.join("base.toml")))
            .add_source(config::File::from(config_dir.join(&environment_filename)).required(false))
            .add_source(
                config::Environment::with_prefix("TINYOBS")
                    .prefix_separator("_")
                    .separator("__"),
            )
            .build()
            .context("Failed to build configuration")?;

        settings
            .try_deserialize()
            .context("Failed to deserialize configuration")
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn from_str(content: &str) -> Result<Self> {
        let config: Config = toml::from_str(content)?;
        Ok(config)
    }

    pub fn sqlite_path(&self) -> String {
        format!("{}/spans.db", self.storage.data_dir)
    }

    pub fn parquet_dir(&self) -> String {
        format!("{}/traces", self.storage.data_dir)
    }

    pub fn retention_secs(&self) -> u64 {
        self.storage.retention_hours * 3600
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.application.host, "0.0.0.0");
        assert_eq!(config.application.port, 4318);
        assert_eq!(config.storage.data_dir, "./data");
        assert_eq!(config.storage.retention_hours, 168);
        assert_eq!(config.compaction.interval_secs, 5);
        assert_eq!(config.compaction.hot_window_secs, 10);
        assert_eq!(config.ingest.session_attribute, "session.id");
    }

    #[test]
    fn test_parse_minimal_toml() {
        let toml = r#"
[storage]
data_dir = "/var/tinyobs"
"#;
        let config = Config::from_str(toml).unwrap();
        assert_eq!(config.storage.data_dir, "/var/tinyobs");
        assert_eq!(config.application.port, 4318);
        assert_eq!(config.compaction.interval_secs, 5);
    }

    #[test]
    fn test_environment_parsing() {
        assert_eq!(
            Environment::try_from("local".to_string()).unwrap(),
            Environment::Local
        );
        assert_eq!(
            Environment::try_from("production".to_string()).unwrap(),
            Environment::Production
        );
        assert!(Environment::try_from("unknown".to_string()).is_err());
    }
}
