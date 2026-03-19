//! Shared configuration types for tinyobs

use anyhow::{Context, Result};
use serde::Deserialize;

/// Application server settings (shared between lite and pro)
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

/// Storage configuration (for lite / embedded)
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_retention_hours")]
    pub retention_hours: u64,
}

/// Ingest configuration (shared)
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

/// Load configuration from TOML files + environment variables
pub fn get_configuration<T: serde::de::DeserializeOwned>() -> Result<T> {
    let base_path = std::env::current_dir().context("Failed to get current directory")?;
    let config_dir = base_path.join("configuration");

    let environment: Environment = std::env::var("TINYOBS_ENV")
        .unwrap_or_else(|_| "local".into())
        .try_into()?;

    let environment_filename = format!("{}.toml", environment.as_str());

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
