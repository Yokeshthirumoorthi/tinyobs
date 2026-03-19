//! TinyObs - Shared types, traits, transports, and OTLP parsing
//!
//! This crate provides the common foundation used by both the lite (free tier)
//! and pro (paid tier / remote ClickHouse backend) binaries.

pub mod backend;
pub(crate) mod ch;
pub mod config;
pub mod ingest;
pub mod schema;
pub mod server;
#[doc(hidden)]
pub mod transport;

pub use schema::{LogRecord, Metric, MetricKind, SeverityLevel, Span, SpanStatus};
