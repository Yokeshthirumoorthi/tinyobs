//! Embedded ClickHouse transport using chdb-rust

use anyhow::{Context, Result};
use async_trait::async_trait;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::{Session, SessionBuilder};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::ch;
use super::Transport;

/// Embedded ClickHouse transport powered by chdb
#[derive(Clone)]
pub struct ChdbTransport {
    session: Arc<Mutex<Session>>,
}

impl ChdbTransport {
    /// Create a new chdb transport. Call `init_schema()` on the backend to create tables.
    pub fn new(data_path: &str) -> Result<Self> {
        std::fs::create_dir_all(data_path)?;

        let session = SessionBuilder::new()
            .with_data_path(data_path)
            .build()
            .context("Failed to create chdb session")?;

        Ok(Self {
            session: Arc::new(Mutex::new(session)),
        })
    }
}

#[async_trait]
impl Transport for ChdbTransport {
    async fn query_json(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let sql = sql.to_string();
        let session = self.session.clone();
        let body = tokio::task::spawn_blocking(move || -> Result<String> {
            let sess = session.blocking_lock();
            let result = sess
                .execute(
                    &sql,
                    Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
                )
                .context("chdb query failed")?;
            Ok(result.data_utf8_lossy().into_owned())
        })
        .await
        .context("chdb query task panicked")??;

        Ok(ch::parse_json_rows(&body))
    }

    async fn execute(&self, sql: &str) -> Result<()> {
        let sql = sql.to_string();
        let session = self.session.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let sess = session.blocking_lock();
            sess.execute(&sql, None)
                .context("chdb execute failed")?;
            Ok(())
        })
        .await
        .context("chdb execute task panicked")??;
        Ok(())
    }

    async fn ping(&self) -> Result<bool> {
        let rows = self.query_json("SELECT 1").await?;
        Ok(!rows.is_empty())
    }
}
