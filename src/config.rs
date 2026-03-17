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
    /// Host to bind to
    #[serde(default = "default_host")]
    pub host: String,
    /// Port to listen on
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
    4319
}

/// Storage configuration
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Directory for all tinyobs data (SQLite + Parquet)
    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    /// Retention period in hours (default: 168 = 7 days)
    #[serde(default = "default_retention_hours")]
    pub retention_hours: u64,
}

/// Compaction configuration
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionConfig {
    /// How often to run compaction (seconds)
    #[serde(default = "default_compaction_interval")]
    pub interval_secs: u64,

    /// How long spans stay in SQLite before moving to Parquet (seconds)
    #[serde(default = "default_hot_window")]
    pub hot_window_secs: u64,

    /// How often to merge small Parquet files (seconds)
    #[serde(default = "default_merge_interval")]
    pub merge_interval_secs: u64,

    /// Minimum file size to trigger merge (bytes)
    #[serde(default = "default_merge_min_size")]
    pub merge_min_size_bytes: u64,
}

/// Ingest configuration
#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    /// Attribute name to extract session ID from
    #[serde(default = "default_session_attribute")]
    pub session_attribute: String,
}

// Default values
fn default_data_dir() -> String {
    "./data".to_string()
}

fn default_retention_hours() -> u64 {
    168 // 7 days
}

fn default_compaction_interval() -> u64 {
    5
}

fn default_hot_window() -> u64 {
    10
}

fn default_merge_interval() -> u64 {
    3600 // 1 hour
}

fn default_merge_min_size() -> u64 {
    1024 * 1024 // 1MB
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
    /// Load configuration from environment-based config files
    ///
    /// Loads configuration in the following order (later sources override earlier):
    /// 1. `configuration/base.toml` - Base settings
    /// 2. `configuration/{environment}.toml` - Environment-specific overrides
    /// 3. Environment variables with `TINYOBS__` prefix
    ///
    /// The environment is determined by `TINYOBS_ENV` (defaults to "local").
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

    /// Load configuration from a TOML file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    /// Load from string (useful for tests)
    pub fn from_str(content: &str) -> Result<Self> {
        let config: Config = toml::from_str(content)?;
        Ok(config)
    }

    /// Path to the SQLite database
    pub fn sqlite_path(&self) -> String {
        format!("{}/spans.db", self.storage.data_dir)
    }

    /// Path to the Parquet directory
    pub fn parquet_dir(&self) -> String {
        format!("{}/traces", self.storage.data_dir)
    }

    /// Retention period in seconds
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
        assert_eq!(config.application.port, 4319);
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
        // Defaults should apply
        assert_eq!(config.application.port, 4319);
        assert_eq!(config.compaction.interval_secs, 5);
    }

    #[test]
    fn test_parse_full_toml() {
        let toml = r#"
[application]
host = "127.0.0.1"
port = 8080

[storage]
data_dir = "/var/tinyobs"
retention_hours = 24

[compaction]
interval_secs = 10
hot_window_secs = 30
merge_interval_secs = 1800

[ingest]
session_attribute = "conversation.id"
"#;
        let config = Config::from_str(toml).unwrap();
        assert_eq!(config.application.host, "127.0.0.1");
        assert_eq!(config.application.port, 8080);
        assert_eq!(config.storage.data_dir, "/var/tinyobs");
        assert_eq!(config.storage.retention_hours, 24);
        assert_eq!(config.compaction.interval_secs, 10);
        assert_eq!(config.compaction.hot_window_secs, 30);
        assert_eq!(config.ingest.session_attribute, "conversation.id");
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
        assert_eq!(
            Environment::try_from("dev".to_string()).unwrap(),
            Environment::Local
        );
        assert_eq!(
            Environment::try_from("prod".to_string()).unwrap(),
            Environment::Production
        );
        assert!(Environment::try_from("unknown".to_string()).is_err());
    }
}
