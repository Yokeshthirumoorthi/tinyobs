use anyhow::Result;
use rusqlite::Connection;
use rusqlite_migration::{Migrations, M};
use std::path::Path;
use std::sync::{Arc, Mutex};

use tinyobs_core::{LogRow, MetricRow, SpanRow};

fn migrations() -> Migrations<'static> {
    Migrations::new(vec![
        M::up(include_str!("../../migrations/01_spans.sql")),
        M::up(include_str!("../../migrations/02_logs_metrics.sql")),
    ])
}

/// Write-optimized SQLite connection with WAL mode
#[derive(Clone)]
pub struct WriteDb {
    conn: Arc<Mutex<Connection>>,
}

impl WriteDb {
    pub fn new(sqlite_path: &str) -> Result<Self> {
        if let Some(parent) = Path::new(sqlite_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut conn = Connection::open(sqlite_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "cache_size", "-64000")?;
        migrations().to_latest(&mut conn)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn insert_spans(&self, spans: &[SpanRow]) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        conn.execute("BEGIN TRANSACTION", [])?;

        {
            let mut stmt = conn.prepare_cached(
                r#"
                INSERT OR REPLACE INTO spans
                    (trace_id, span_id, parent_span_id, session_id, service_name,
                     operation, start_time, duration_ns, status, attributes,
                     resource_attrs, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                "#,
            )?;

            for span in spans {
                stmt.execute(rusqlite::params![
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

        conn.execute("COMMIT", [])?;
        tracing::debug!(count = spans.len(), "Inserted spans to SQLite");
        Ok(())
    }

    pub fn insert_logs(&self, logs: &[LogRow]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        conn.execute("BEGIN TRANSACTION", [])?;

        {
            let mut stmt = conn.prepare_cached(
                r#"
                INSERT INTO logs
                    (timestamp, observed_timestamp, trace_id, span_id, severity_number,
                     severity_text, body, service_name, attributes, resource_attrs, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                "#,
            )?;

            for log in logs {
                stmt.execute(rusqlite::params![
                    log.timestamp,
                    log.observed_timestamp,
                    log.trace_id,
                    log.span_id,
                    log.severity_number,
                    log.severity_text,
                    log.body,
                    log.service_name,
                    log.attributes,
                    log.resource_attrs,
                    log.created_at,
                ])?;
            }
        }

        conn.execute("COMMIT", [])?;
        tracing::debug!(count = logs.len(), "Inserted logs to SQLite");
        Ok(())
    }

    pub fn insert_metrics(&self, metrics: &[MetricRow]) -> Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        conn.execute("BEGIN TRANSACTION", [])?;

        {
            let mut stmt = conn.prepare_cached(
                r#"
                INSERT INTO metrics
                    (name, description, unit, kind, timestamp, service_name,
                     value, sum, count, min, max, quantiles, buckets,
                     attributes, resource_attrs, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
                "#,
            )?;

            for metric in metrics {
                stmt.execute(rusqlite::params![
                    metric.name,
                    metric.description,
                    metric.unit,
                    metric.kind,
                    metric.timestamp,
                    metric.service_name,
                    metric.value,
                    metric.sum,
                    metric.count,
                    metric.min,
                    metric.max,
                    metric.quantiles,
                    metric.buckets,
                    metric.attributes,
                    metric.resource_attrs,
                    metric.created_at,
                ])?;
            }
        }

        conn.execute("COMMIT", [])?;
        tracing::debug!(count = metrics.len(), "Inserted metrics to SQLite");
        Ok(())
    }

    pub fn lookup_session_ids(
        &self,
        trace_ids: &[String],
    ) -> Result<std::collections::HashMap<String, String>> {
        if trace_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        let placeholders: String = trace_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "SELECT DISTINCT trace_id, session_id FROM spans WHERE trace_id IN ({}) AND session_id IS NOT NULL",
            placeholders
        );

        let mut stmt = conn.prepare(&query)?;
        let params: Vec<&dyn rusqlite::ToSql> =
            trace_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

        let rows = stmt.query_map(params.as_slice(), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut result = std::collections::HashMap::new();
        for row in rows {
            let (trace_id, session_id) = row?;
            result.insert(trace_id, session_id);
        }

        Ok(result)
    }

    pub fn get_old_spans(&self, hot_window_secs: u64) -> Result<Vec<SpanRow>> {
        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        let cutoff = chrono::Utc::now().timestamp() - hot_window_secs as i64;

        let mut stmt = conn.prepare(
            r#"
            SELECT trace_id, span_id, parent_span_id, session_id, service_name,
                   operation, start_time, duration_ns, status, attributes,
                   resource_attrs, created_at
            FROM spans
            WHERE created_at < ?
            "#,
        )?;

        let rows = stmt.query_map([cutoff], |row| {
            Ok(SpanRow {
                trace_id: row.get(0)?,
                span_id: row.get(1)?,
                parent_span_id: row.get(2)?,
                session_id: row.get(3)?,
                service_name: row.get(4)?,
                operation: row.get(5)?,
                start_time: row.get(6)?,
                duration_ns: row.get(7)?,
                status: row.get(8)?,
                attributes: row.get(9)?,
                resource_attrs: row.get(10)?,
                created_at: row.get(11)?,
            })
        })?;

        let mut spans = Vec::new();
        for row in rows {
            spans.push(row?);
        }

        Ok(spans)
    }

    pub fn delete_old_spans(&self, hot_window_secs: u64) -> Result<usize> {
        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        let cutoff = chrono::Utc::now().timestamp() - hot_window_secs as i64;

        let deleted = conn.execute("DELETE FROM spans WHERE created_at < ?", [cutoff])?;
        tracing::debug!(count = deleted, "Deleted old spans from SQLite");
        Ok(deleted)
    }

    #[cfg(test)]
    pub fn connection(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_insert_and_query() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = WriteDb::new(path.to_str().unwrap()).unwrap();

        let span = SpanRow {
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
        };

        db.insert_spans(&[span]).unwrap();

        let sessions = db.lookup_session_ids(&["trace1".to_string()]).unwrap();
        assert_eq!(sessions.get("trace1"), Some(&"session1".to_string()));
    }
}
