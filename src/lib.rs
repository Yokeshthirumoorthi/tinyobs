//! TinyObs — minimal OTEL observability engine
//!
//! Use `TinyObs` as the primary public API:
//!
//! ```no_run
//! use tinyobs::TinyObs;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let tinyobs = TinyObs::lite("./data")?;
//! tinyobs.init_schema().await?;
//! tinyobs.execute("CREATE TABLE IF NOT EXISTS my_events (id UInt64) ENGINE = MergeTree() ORDER BY id").await?;
//! let rows = tinyobs.query("SELECT * FROM my_events", 100).await?;
//! # Ok(())
//! # }
//! ```

pub mod api_types;
pub mod backend;
pub(crate) mod ch;
#[cfg(feature = "client")]
pub mod client;
pub mod config;
pub mod ingest;
pub mod schema;
pub mod server;
#[doc(hidden)]
pub mod transport;

#[cfg(feature = "cli")]
pub mod cli;
#[cfg(feature = "mcp")]
pub mod mcp;

pub use schema::{LogRecord, Metric, MetricKind, SeverityLevel, Span, SpanStatus};

// ============================================================================
// TinyObs — clean public API
// ============================================================================

use anyhow::Result;
use backend::{ChBackend, IngestBackend, ManagedBackend};
use server::IngestState;
use std::sync::Arc;

/// TinyObs observability engine.
///
/// This is the primary public API. Downstream crates use this instead of
/// interacting with transports or backends directly.
#[derive(Clone)]
pub struct TinyObs<B: IngestBackend + ManagedBackend + Clone + 'static> {
    backend: B,
    ingest_config: Arc<config::IngestConfig>,
}

#[cfg(feature = "lite")]
impl TinyObs<ChBackend<transport::chdb::ChdbTransport>> {
    /// Create a TinyObs instance with embedded storage.
    ///
    /// Call `init_schema()` after creation to set up OTEL tables.
    pub fn lite(data_path: &str) -> Result<Self> {
        let full_path = format!("{}/chdb", data_path);
        std::fs::create_dir_all(&full_path)?;
        let transport = transport::chdb::ChdbTransport::new(&full_path)?;
        let backend = ChBackend::new(transport);
        Ok(Self {
            backend,
            ingest_config: Arc::new(config::IngestConfig::default()),
        })
    }
}

impl<B: IngestBackend + ManagedBackend + Clone + 'static> TinyObs<B> {
    /// Create a TinyObs instance from an existing backend.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            ingest_config: Arc::new(config::IngestConfig::default()),
        }
    }

    /// Set the ingest configuration.
    pub fn with_ingest_config(mut self, config: config::IngestConfig) -> Self {
        self.ingest_config = Arc::new(config);
        self
    }

    /// Initialize the standard OTEL schema tables (traces, logs, metrics).
    /// Safe to call multiple times.
    pub async fn init_schema(&self) -> Result<()> {
        self.backend.init_schema().await
    }

    /// Execute a raw SQL statement (DDL or DML). Use for custom table creation.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.backend.execute_raw(sql).await
    }

    /// Query any table with raw SQL. Returns JSON rows.
    pub async fn query(&self, sql: &str, limit: usize) -> Result<Vec<serde_json::Value>> {
        self.backend.raw_query(sql, limit).await
    }

    /// Get an Axum router for OTLP ingest endpoints.
    ///
    /// Provides: `POST /v1/traces`, `POST /v1/logs`, `POST /v1/metrics`, `GET /health`
    pub fn ingest_router(&self) -> axum::Router {
        let state = IngestState {
            backend: self.backend.clone(),
            config: self.ingest_config.clone(),
        };
        server::ingest_router(state)
    }

    /// Check if the backend is healthy.
    pub async fn health_check(&self) -> Result<bool> {
        self.backend.health_check().await
    }

    /// Shut down background tasks and flush pending data.
    pub async fn shutdown(&self) -> Result<()> {
        self.backend.stop_background_tasks().await
    }

    /// Access the underlying backend for advanced operations.
    pub fn backend(&self) -> &B {
        &self.backend
    }
}
