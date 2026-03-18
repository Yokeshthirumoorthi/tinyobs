use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

/// TinyObs Pro configuration
#[derive(Debug, Clone, Deserialize)]
pub struct ProConfig {
    #[serde(default)]
    pub application: ApplicationSettings,
    #[serde(default)]
    pub clickhouse: ClickHouseConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
}

/// Application server settings
#[derive(Debug, Clone, Deserialize)]
pub struct ApplicationSettings {
    #[serde(default = "default_host")]
    pub host: String,
    /// Server port (ingest + query API)
    #[serde(default = "default_port")]
    pub port: u16,
}

/// ClickHouse connection settings
#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseConfig {
    /// ClickHouse HTTP URL
    #[serde(default = "default_clickhouse_url")]
    pub url: String,
    /// Database name
    #[serde(default = "default_database")]
    pub database: String,
    /// Batch insert flush interval in milliseconds
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    /// Batch insert max rows before flush
    #[serde(default = "default_max_rows")]
    pub max_rows: u64,
}

/// Ingest configuration
#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    #[serde(default = "default_session_attribute")]
    pub session_attribute: String,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    4318
}
fn default_clickhouse_url() -> String {
    "http://127.0.0.1:8123".to_string()
}
fn default_database() -> String {
    "default".to_string()
}
fn default_flush_interval_ms() -> u64 {
    1000
}
fn default_max_rows() -> u64 {
    10000
}
fn default_session_attribute() -> String {
    "session.id".to_string()
}

impl Default for ApplicationSettings {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_clickhouse_url(),
            database: default_database(),
            flush_interval_ms: default_flush_interval_ms(),
            max_rows: default_max_rows(),
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

impl Default for ProConfig {
    fn default() -> Self {
        Self {
            application: ApplicationSettings::default(),
            clickhouse: ClickHouseConfig::default(),
            ingest: IngestConfig::default(),
        }
    }
}

impl ProConfig {
    pub fn get_configuration() -> Result<Self> {
        let base_path = std::env::current_dir().context("Failed to get current directory")?;
        let config_dir = base_path.join("configuration");

        let environment = std::env::var("TINYOBS_ENV").unwrap_or_else(|_| "local".into());
        let environment_filename = format!("{}.toml", environment);

        let settings = config::Config::builder()
            .add_source(config::File::from(config_dir.join("base.toml")).required(false))
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
        let config: ProConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
