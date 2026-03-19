//! Transport abstraction for ClickHouse query execution
//!
//! The only real difference between lite (embedded chdb) and pro (remote ClickHouse)
//! is how SQL gets executed. This trait captures that.

#[cfg(feature = "lite")]
pub mod chdb;
#[cfg(feature = "pro")]
pub mod remote;

use anyhow::Result;
use async_trait::async_trait;

/// Abstraction over SQL execution — embedded chdb vs remote ClickHouse HTTP
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Execute a query and return JSON rows
    async fn query_json(&self, sql: &str) -> Result<Vec<serde_json::Value>>;

    /// Execute a SQL statement (INSERT, DDL) without returning results
    async fn execute(&self, sql: &str) -> Result<()>;

    /// Check if the backend is responsive
    async fn ping(&self) -> Result<bool>;
}
