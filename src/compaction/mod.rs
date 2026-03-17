mod basic;
mod merge;
mod retention;

pub use basic::BasicCompactor;
pub use merge::MergeCompactor;
pub use retention::RetentionManager;

use crate::config::Config;
use crate::db::WriteDb;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::interval;

/// Manages all compaction tasks
pub struct CompactionManager {
    config: Arc<Config>,
    write_db: WriteDb,
    parquet_dir: PathBuf,
    shutdown_rx: watch::Receiver<bool>,
}

impl CompactionManager {
    pub fn new(
        config: Arc<Config>,
        write_db: WriteDb,
        parquet_dir: PathBuf,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            write_db,
            parquet_dir,
            shutdown_rx,
        }
    }

    /// Run compaction loop (basic compaction + merge + retention)
    pub async fn run(&self) {
        let mut basic_ticker = interval(Duration::from_secs(self.config.compaction.interval_secs));
        let mut merge_ticker =
            interval(Duration::from_secs(self.config.compaction.merge_interval_secs));
        let mut retention_ticker = interval(Duration::from_secs(3600)); // Check retention hourly

        let basic_compactor = BasicCompactor::new(
            self.write_db.clone(),
            self.parquet_dir.clone(),
            self.config.compaction.hot_window_secs,
        );

        let merge_compactor = MergeCompactor::new(
            self.parquet_dir.clone(),
            self.config.compaction.merge_min_size_bytes,
        );

        let retention_manager =
            RetentionManager::new(self.parquet_dir.clone(), self.config.retention_secs());

        tracing::info!(
            interval_secs = self.config.compaction.interval_secs,
            hot_window_secs = self.config.compaction.hot_window_secs,
            merge_interval_secs = self.config.compaction.merge_interval_secs,
            retention_hours = self.config.storage.retention_hours,
            parquet_dir = %self.parquet_dir.display(),
            "Starting compaction manager"
        );

        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = basic_ticker.tick() => {
                    if let Err(e) = basic_compactor.run().await {
                        tracing::error!(error = %e, "Basic compaction failed");
                    }
                }
                _ = merge_ticker.tick() => {
                    if let Err(e) = merge_compactor.run().await {
                        tracing::error!(error = %e, "Merge compaction failed");
                    }
                }
                _ = retention_ticker.tick() => {
                    if let Err(e) = retention_manager.run().await {
                        tracing::error!(error = %e, "Retention cleanup failed");
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Compaction manager shutting down");
                        break;
                    }
                }
            }
        }
    }
}
