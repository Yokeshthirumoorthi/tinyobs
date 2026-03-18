//! TinyObs Core - Shared types, traits, and OTLP parsing
//!
//! This crate provides the common foundation used by both `tinyobs-lite` (free tier)
//! and `tinyobs-pro` (paid tier / ClickHouse backend).

pub mod backend;
pub mod ingest;
pub mod schema;

pub use schema::{
    LogRecord, LogRow, Metric, MetricKind, MetricRow, SeverityLevel, Span, SpanRow, SpanStatus,
};
