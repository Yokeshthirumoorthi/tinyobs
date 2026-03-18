//! Backend abstraction for telemetry storage and query

pub mod types;

pub use types::*;

use crate::schema::{LogRecord, Metric, Span};
use anyhow::Result;
use async_trait::async_trait;

/// Unified telemetry backend trait
#[async_trait]
pub trait TelemetryBackend: Send + Sync {
    async fn query_spans(&self, filter: SpanFilter) -> Result<Vec<Span>>;
    async fn query_logs(&self, filter: LogFilter) -> Result<Vec<LogRecord>>;
    async fn query_metrics(&self, filter: MetricFilter) -> Result<Vec<Metric>>;
    async fn get_trace(&self, trace_id: &str) -> Result<Vec<Span>>;
    async fn get_session(&self, session_id: &str) -> Result<Vec<Span>>;
    async fn list_services(&self, time_range: TimeRange) -> Result<Vec<ServiceSummary>>;
    async fn attribute_keys(&self, signal: Signal, service: Option<&str>) -> Result<Vec<String>>;
    async fn service_health(&self, time_range: TimeRange) -> Result<Vec<ServiceHealth>>;
    async fn compare_periods(
        &self,
        a: TimeRange,
        b: TimeRange,
        service: Option<&str>,
    ) -> Result<Comparison>;
    async fn raw_query(&self, query: &str, limit: usize) -> Result<Vec<serde_json::Value>>;
    async fn discover_schema(&self) -> Result<Schema>;
}

/// Backend type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    DuckDb,
    ClickHouse,
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
#[async_trait]
pub trait IngestBackend: TelemetryBackend {
    async fn ingest_span(&self, span: Span) -> Result<()>;
    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()>;
    async fn ingest_log(&self, log: LogRecord) -> Result<()>;
    async fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<()>;
    async fn ingest_metric(&self, metric: Metric) -> Result<()>;
    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()>;
}

/// Extension trait for backends with lifecycle management
#[async_trait]
pub trait ManagedBackend: TelemetryBackend {
    async fn start_background_tasks(&self) -> Result<()>;
    async fn stop_background_tasks(&self) -> Result<()>;
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
