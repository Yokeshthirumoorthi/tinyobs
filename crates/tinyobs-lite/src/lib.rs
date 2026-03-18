//! TinyObs - Minimal Observability Storage
//!
//! A pure ingest + storage layer for OTLP telemetry (traces, logs, metrics).
//! No REST API, no dashboard. Consumers query via DuckDB SQL and build their own endpoints.
//!
//! # Architecture
//!
//! ```text
//! OTLP Data -> POST /v1/{traces,logs,metrics} -> SQLite (hot, <10s)
//!                                                     |
//!                                                     v (compaction, 5s)
//!                                                Parquet files (cold)
//!                                                     |
//!                                                     v (merge, hourly)
//!                                                Merged Parquet
//!                                                     |
//! Consumer SQL -> tinyobs.query() -> DuckDB -> UNION ALL (hot + cold) -> Results
//! ```

pub mod compaction;
pub mod config;
pub mod db;
pub mod ingest;
pub mod startup;
pub mod telemetry;

// Re-export core types for backwards compatibility
pub use tinyobs_core::schema;
pub use tinyobs_core::backend;
pub use tinyobs_core::{
    LogRecord, LogRow, Metric, MetricKind, MetricRow, SeverityLevel, Span, SpanRow, SpanStatus,
};

pub use config::{ApplicationSettings, Config, Environment};
pub use duckdb::ToSql;

use anyhow::Result;
use axum::{routing::get, routing::post, Router};
use serde::de::DeserializeOwned;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::compaction::CompactionManager;
use crate::db::{ReadDb, WriteDb};
use crate::ingest::{health_check, receive_logs, receive_metrics, receive_traces, IngestState};

/// TinyObs - Main entry point
#[derive(Clone)]
pub struct TinyObs {
    config: Arc<Config>,
    write_db: WriteDb,
    read_db: ReadDb,
    ingest_state: IngestState,
    shutdown_tx: Arc<watch::Sender<bool>>,
}

/// Handle for background tasks
pub struct TinyObsHandle {
    tinyobs: TinyObs,
    compaction_handle: JoinHandle<()>,
}

impl TinyObs {
    /// Start TinyObs with the given configuration
    pub async fn start(config: Config) -> Result<TinyObsHandle> {
        let config = Arc::new(config);

        // Create data directories
        std::fs::create_dir_all(&config.storage.data_dir)?;
        let parquet_dir = PathBuf::from(config.parquet_dir());
        std::fs::create_dir_all(&parquet_dir)?;

        // Initialize databases
        let write_db = WriteDb::new(&config.sqlite_path())?;
        let read_db = ReadDb::new(&config.sqlite_path(), &config.parquet_dir())?;

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Create ingest state
        let ingest_state = IngestState {
            write_db: write_db.clone(),
            config: Arc::new(config.ingest.clone()),
        };

        // Spawn compaction manager
        let compaction_manager = CompactionManager::new(
            config.clone(),
            write_db.clone(),
            parquet_dir,
            shutdown_rx,
        );

        let compaction_handle = tokio::spawn(async move {
            compaction_manager.run().await;
        });

        let tinyobs = TinyObs {
            config,
            write_db,
            read_db,
            ingest_state,
            shutdown_tx: Arc::new(shutdown_tx),
        };

        tracing::info!(
            sqlite_path = tinyobs.config.sqlite_path(),
            parquet_dir = tinyobs.config.parquet_dir(),
            "TinyObs started"
        );

        Ok(TinyObsHandle {
            tinyobs,
            compaction_handle,
        })
    }

    /// Get an Axum router for OTLP ingest endpoints
    pub fn ingest_router(&self) -> Router {
        Router::new()
            .route("/v1/traces", post(receive_traces))
            .route("/v1/logs", post(receive_logs))
            .route("/v1/metrics", post(receive_metrics))
            .route("/health", get(health_check))
            .with_state(self.ingest_state.clone())
    }

    /// Ingest a single span directly (bypassing HTTP)
    pub fn ingest(&self, span: Span) -> Result<()> {
        let row = span.to_row();
        self.write_db.insert_spans(&[row])
    }

    /// Ingest multiple spans directly (bypassing HTTP)
    pub fn ingest_batch(&self, spans: Vec<Span>) -> Result<()> {
        let rows: Vec<SpanRow> = spans.iter().map(|s| s.to_row()).collect();
        self.write_db.insert_spans(&rows)
    }

    /// Query spans using SQL
    pub fn query<T: DeserializeOwned>(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<T>> {
        self.read_db.query(sql, params)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn write_db(&self) -> &WriteDb {
        &self.write_db
    }

    pub fn read_db(&self) -> &ReadDb {
        &self.read_db
    }
}

impl TinyObsHandle {
    pub fn tinyobs(&self) -> &TinyObs {
        &self.tinyobs
    }

    pub fn clone_tinyobs(&self) -> TinyObs {
        self.tinyobs.clone()
    }

    pub async fn shutdown(self) -> Result<()> {
        tracing::info!("Shutting down TinyObs");
        self.tinyobs.shutdown_tx.send(true)?;
        self.compaction_handle.await?;
        tracing::info!("TinyObs shutdown complete");
        Ok(())
    }
}

impl std::ops::Deref for TinyObsHandle {
    type Target = TinyObs;

    fn deref(&self) -> &Self::Target {
        &self.tinyobs
    }
}
