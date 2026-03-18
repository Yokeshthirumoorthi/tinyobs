//! TinyObs Pro - ClickHouse-backed observability storage
//!
//! Single binary handling both OTLP ingest (:4318) and query API (:8080),
//! backed by ClickHouse for high-throughput storage and querying.

pub mod clickhouse;
pub mod config;
pub mod ingest;

// Re-export core types
pub use tinyobs_core::backend;
pub use tinyobs_core::schema;
pub use tinyobs_core::{
    LogRecord, LogRow, Metric, MetricKind, MetricRow, SeverityLevel, Span, SpanRow, SpanStatus,
};
