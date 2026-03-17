use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;

use crate::db::WriteDb;
use crate::schema::SpanRow;

/// Compacts old spans from SQLite to Parquet
pub struct BasicCompactor {
    write_db: WriteDb,
    parquet_dir: PathBuf,
    hot_window_secs: u64,
}

impl BasicCompactor {
    pub fn new(write_db: WriteDb, parquet_dir: PathBuf, hot_window_secs: u64) -> Self {
        Self {
            write_db,
            parquet_dir,
            hot_window_secs,
        }
    }

    /// Run one compaction cycle
    pub async fn run(&self) -> Result<()> {
        // Get old spans
        let spans = self.write_db.get_old_spans(self.hot_window_secs)?;

        if spans.is_empty() {
            return Ok(());
        }

        tracing::info!(count = spans.len(), "Found old spans to compact");

        // Generate parquet filename with timestamp
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3f");
        let parquet_path = self.parquet_dir.join(format!("{}.parquet", timestamp));

        // Write to parquet
        write_spans_to_parquet(&spans, &parquet_path)?;

        tracing::info!(
            count = spans.len(),
            path = %parquet_path.display(),
            "Wrote spans to Parquet"
        );

        // Delete from SQLite only after successful parquet write
        self.write_db.delete_old_spans(self.hot_window_secs)?;

        Ok(())
    }
}

/// Write spans to a Parquet file using DuckDB
fn write_spans_to_parquet(spans: &[SpanRow], path: &std::path::Path) -> Result<()> {
    let conn = Connection::open_in_memory()?;

    // Create table with span schema
    conn.execute_batch(
        r#"
        CREATE TABLE spans (
            trace_id VARCHAR,
            span_id VARCHAR,
            parent_span_id VARCHAR,
            session_id VARCHAR,
            service_name VARCHAR,
            operation VARCHAR,
            start_time BIGINT,
            duration_ns BIGINT,
            status INTEGER,
            attributes VARCHAR,
            resource_attrs VARCHAR,
            created_at BIGINT
        )
        "#,
    )?;

    // Insert spans
    {
        let mut stmt = conn.prepare(
            r#"
            INSERT INTO spans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )?;

        for span in spans {
            stmt.execute(duckdb::params![
                span.trace_id,
                span.span_id,
                span.parent_span_id,
                span.session_id,
                span.service_name,
                span.operation,
                span.start_time,
                span.duration_ns,
                span.status,
                span.attributes,
                span.resource_attrs,
                span.created_at,
            ])?;
        }
    }

    // Export to parquet
    let export_query = format!(
        "COPY spans TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        path.display()
    );
    conn.execute_batch(&export_query)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_parquet() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let spans = vec![SpanRow {
            trace_id: "trace1".to_string(),
            span_id: "span1".to_string(),
            parent_span_id: None,
            session_id: Some("session1".to_string()),
            service_name: "test-service".to_string(),
            operation: "test-op".to_string(),
            start_time: 1234567890,
            duration_ns: 1000000,
            status: 0,
            attributes: "{}".to_string(),
            resource_attrs: "{}".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        }];

        write_spans_to_parquet(&spans, &path).unwrap();
        assert!(path.exists());
    }
}
