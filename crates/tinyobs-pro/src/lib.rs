//! TinyObs Pro - ClickHouse-backed observability storage
//!
//! Single binary handling both OTLP ingest (:4318) and query API (:8080),
//! backed by ClickHouse for high-throughput storage and querying.

pub(crate) mod clickhouse;
pub(crate) mod config;
pub(crate) mod ingest;

// Re-export backend and config types
pub use clickhouse::TinyObsDB;
pub use config::{BackendConfig, IngestConfig, ProConfig};
pub use ingest::{health_check, receive_logs, receive_metrics, receive_traces, IngestState};

// Re-export core types
pub use tinyobs_core::schema;
pub use tinyobs_core::{
    LogRecord, LogRow, Metric, MetricKind, MetricRow, SeverityLevel, Span, SpanRow, SpanStatus,
};

// Re-export backend traits
pub use tinyobs_core::backend::{
    BackendType, IngestBackend, ManagedBackend, TelemetryBackend,
};

// Re-export backend filter/response types
pub use tinyobs_core::backend::{
    ColumnMapping, ColumnSchema, Comparison, FieldMapping, LogFilter, MetricAggregation,
    MetricFilter, MetricType, OrderBy, PeriodMetrics, Schema, ServiceHealth, ServiceSummary,
    Signal, SpanFilter, SpanStatusFilter, TableSchema, TimeRange,
};
