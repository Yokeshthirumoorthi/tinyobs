use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// TinyObs configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub compaction: CompactionConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
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

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            compaction: CompactionConfig::default(),
            ingest: IngestConfig::default(),
        }
    }
}

impl Config {
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
        assert_eq!(config.compaction.interval_secs, 5);
    }

    #[test]
    fn test_parse_full_toml() {
        let toml = r#"
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
        assert_eq!(config.storage.data_dir, "/var/tinyobs");
        assert_eq!(config.storage.retention_hours, 24);
        assert_eq!(config.compaction.interval_secs, 10);
        assert_eq!(config.compaction.hot_window_secs, 30);
        assert_eq!(config.ingest.session_attribute, "conversation.id");
    }
}
