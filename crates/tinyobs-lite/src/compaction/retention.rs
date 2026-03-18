use anyhow::Result;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

/// Manages retention by deleting old Parquet files
pub struct RetentionManager {
    parquet_dir: PathBuf,
    retention_secs: u64,
}

impl RetentionManager {
    pub fn new(parquet_dir: PathBuf, retention_secs: u64) -> Self {
        Self {
            parquet_dir,
            retention_secs,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let cutoff = SystemTime::now() - Duration::from_secs(self.retention_secs);
        let mut deleted = 0;

        let entries = match std::fs::read_dir(&self.parquet_dir) {
            Ok(e) => e,
            Err(_) => return Ok(()),
        };

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "parquet") {
                if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        if modified < cutoff {
                            if let Err(e) = std::fs::remove_file(&path) {
                                tracing::warn!(
                                    path = %path.display(),
                                    error = %e,
                                    "Failed to delete expired parquet file"
                                );
                            } else {
                                deleted += 1;
                            }
                        }
                    }
                }
            }
        }

        if deleted > 0 {
            tracing::info!(count = deleted, "Deleted expired parquet files");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_retention_manager_no_files() {
        let dir = tempdir().unwrap();
        let manager = RetentionManager::new(dir.path().to_path_buf(), 3600);
        manager.run().await.unwrap();
    }
}
