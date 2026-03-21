//! TinyObs - Shared types, traits, transports, and OTLP parsing
//!
//! This crate provides the common foundation used by both the lite (free tier)
//! and pro (paid tier / remote ClickHouse backend) binaries.

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
