//! Backend abstraction for telemetry storage and query
//!
//! This module provides the `TelemetryBackend` trait which abstracts over
//! different telemetry storage backends:
//!
//! - **DuckDB** (Lite edition): Self-contained with SQLite + Parquet + DuckDB
//! - **ClickHouse** (Pro edition): Connect to existing ClickHouse cluster
//! - **Splunk** (Pro edition): Connect to existing Splunk deployment
//!
//! # Example
//!
//! ```rust,ignore
//! use tinyobs::{TelemetryBackend, DuckDbBackend, SpanFilter, TimeRange};
//!
//! async fn example(backend: impl TelemetryBackend) -> anyhow::Result<()> {
//!     let filter = SpanFilter {
//!         service: Some("auth".into()),
//!         time_range: TimeRange::last_hour(),
//!         limit: 100,
//!         ..Default::default()
//!     };
//!     let spans = backend.query_spans(filter).await?;
//!     Ok(())
//! }
//! ```

pub mod types;

#[cfg(feature = "lite")]
pub mod duckdb;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;

#[cfg(feature = "splunk")]
pub mod splunk;

pub use types::*;

use crate::schema::{LogRecord, Metric, Span};
use anyhow::Result;
use async_trait::async_trait;

/// Unified telemetry backend trait
///
/// This trait provides a common interface for querying telemetry data
/// across different storage backends. Implementations handle the specifics
/// of query translation and data retrieval.
///
/// All methods are async to support both local and remote backends.
#[async_trait]
pub trait TelemetryBackend: Send + Sync {
    /// Query spans with the given filter
    ///
    /// Returns spans matching the filter criteria, ordered by start_time DESC
    /// unless otherwise specified.
    async fn query_spans(&self, filter: SpanFilter) -> Result<Vec<Span>>;

    /// Query logs with the given filter
    ///
    /// Returns log records matching the filter criteria.
    async fn query_logs(&self, filter: LogFilter) -> Result<Vec<LogRecord>>;

    /// Query metrics with the given filter
    ///
    /// Returns metric data points matching the filter criteria.
    async fn query_metrics(&self, filter: MetricFilter) -> Result<Vec<Metric>>;

    /// Get all spans for a specific trace
    ///
    /// Returns all spans belonging to the given trace ID, useful for
    /// trace visualization.
    async fn get_trace(&self, trace_id: &str) -> Result<Vec<Span>>;

    /// Get all spans for a specific session
    ///
    /// Returns all spans associated with the given session ID.
    async fn get_session(&self, session_id: &str) -> Result<Vec<Span>>;

    /// List all services seen in the given time range
    ///
    /// Returns summary information for each service including span counts,
    /// error rates, and latency statistics.
    async fn list_services(&self, time_range: TimeRange) -> Result<Vec<ServiceSummary>>;

    /// Get available attribute keys for a signal type
    ///
    /// Useful for UI autocomplete and filter builders.
    async fn attribute_keys(&self, signal: Signal, service: Option<&str>) -> Result<Vec<String>>;

    /// Get health metrics for services
    ///
    /// Returns RED metrics (Rate, Errors, Duration) for each service.
    async fn service_health(&self, time_range: TimeRange) -> Result<Vec<ServiceHealth>>;

    /// Compare metrics between two time periods
    ///
    /// Useful for detecting regressions or improvements.
    async fn compare_periods(
        &self,
        a: TimeRange,
        b: TimeRange,
        service: Option<&str>,
    ) -> Result<Comparison>;

    /// Execute a raw query (backend-specific syntax)
    ///
    /// For DuckDB: SQL query
    /// For ClickHouse: SQL query
    /// For Splunk: SPL query
    ///
    /// Returns results as JSON values.
    async fn raw_query(&self, query: &str, limit: usize) -> Result<Vec<serde_json::Value>>;

    /// Discover the schema of the backend
    ///
    /// Returns information about available tables, columns, and attribute keys.
    async fn discover_schema(&self) -> Result<Schema>;
}

/// Backend type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    /// DuckDB backend (Lite edition) - local SQLite + Parquet
    DuckDb,
    /// ClickHouse backend (Pro edition) - remote cluster
    ClickHouse,
    /// Splunk backend (Pro edition) - remote deployment
    Splunk,
}

impl std::fmt::Display for BackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendType::DuckDb => write!(f, "duckdb"),
            BackendType::ClickHouse => write!(f, "clickhouse"),
            BackendType::Splunk => write!(f, "splunk"),
        }
    }
}

/// Extension trait for backends that support ingestion
///
/// Only the DuckDB (Lite) backend supports ingestion.
/// ClickHouse and Splunk backends are query-only.
#[async_trait]
pub trait IngestBackend: TelemetryBackend {
    /// Ingest a single span
    async fn ingest_span(&self, span: Span) -> Result<()>;

    /// Ingest a batch of spans
    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()>;

    /// Ingest a single log record
    async fn ingest_log(&self, log: LogRecord) -> Result<()>;

    /// Ingest a batch of log records
    async fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<()>;

    /// Ingest a single metric data point
    async fn ingest_metric(&self, metric: Metric) -> Result<()>;

    /// Ingest a batch of metric data points
    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()>;
}

/// Extension trait for backends with lifecycle management
///
/// Backends may need initialization, compaction, or cleanup routines.
#[async_trait]
pub trait ManagedBackend: TelemetryBackend {
    /// Start background tasks (compaction, retention, etc.)
    async fn start_background_tasks(&self) -> Result<()>;

    /// Stop background tasks gracefully
    async fn stop_background_tasks(&self) -> Result<()>;

    /// Check if the backend is healthy
    async fn health_check(&self) -> Result<bool>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_type_display() {
        assert_eq!(BackendType::DuckDb.to_string(), "duckdb");
        assert_eq!(BackendType::ClickHouse.to_string(), "clickhouse");
        assert_eq!(BackendType::Splunk.to_string(), "splunk");
    }
}
