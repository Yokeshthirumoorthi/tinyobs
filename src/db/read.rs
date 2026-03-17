use anyhow::Result;
use duckdb::{Connection, ToSql};
use serde::de::DeserializeOwned;
use std::sync::{Arc, Mutex};

/// DuckDB-based read database that queries both hot (SQLite) and cold (Parquet) data
#[derive(Clone)]
pub struct ReadDb {
    conn: Arc<Mutex<Connection>>,
    sqlite_path: String,
    parquet_dir: String,
}

impl ReadDb {
    pub fn new(sqlite_path: &str, parquet_dir: &str) -> Result<Self> {
        // Create parquet directory if needed
        std::fs::create_dir_all(parquet_dir)?;

        let conn = Connection::open_in_memory()?;

        // Install and load SQLite extension
        conn.execute_batch(
            r#"
            INSTALL sqlite;
            LOAD sqlite;
            "#,
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            sqlite_path: sqlite_path.to_string(),
            parquet_dir: parquet_dir.to_string(),
        })
    }

    /// Check if parquet files exist
    fn has_parquet_files(&self) -> bool {
        std::fs::read_dir(&self.parquet_dir)
            .map(|mut dir| {
                dir.any(|e| {
                    e.map(|f| f.path().extension().is_some_and(|ext| ext == "parquet"))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false)
    }

    /// Build the unified spans CTE that queries both hot and cold data
    fn build_spans_cte(&self) -> String {
        let parquet_glob = format!("{}/*.parquet", self.parquet_dir);

        if self.has_parquet_files() {
            format!(
                r#"spans AS (
                    SELECT trace_id, span_id, parent_span_id, session_id, service_name,
                           operation, start_time, duration_ns, status, attributes,
                           resource_attrs, created_at
                    FROM sqlite_scan('{}', 'spans')
                    UNION ALL
                    SELECT trace_id, span_id, parent_span_id, session_id, service_name,
                           operation, start_time, duration_ns, status, attributes,
                           resource_attrs, created_at
                    FROM read_parquet('{}')
                )"#,
                self.sqlite_path, parquet_glob
            )
        } else {
            format!(
                r#"spans AS (
                    SELECT trace_id, span_id, parent_span_id, session_id, service_name,
                           operation, start_time, duration_ns, status, attributes,
                           resource_attrs, created_at
                    FROM sqlite_scan('{}', 'spans')
                )"#,
                self.sqlite_path
            )
        }
    }

    /// Execute a SQL query and deserialize results to type T
    ///
    /// The query has access to a `spans` table/view with columns:
    /// - trace_id, span_id, parent_span_id, session_id, service_name
    /// - operation, start_time, duration_ns, status
    /// - attributes (JSON), resource_attrs (JSON), created_at
    ///
    /// Example:
    /// ```ignore
    /// let results: Vec<MyRow> = db.query(
    ///     "SELECT trace_id, COUNT(*) as count FROM spans GROUP BY trace_id",
    ///     &[]
    /// )?;
    /// ```
    pub fn query<T: DeserializeOwned>(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<T>> {
        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;

        // Wrap user query with spans CTE
        let spans_cte = self.build_spans_cte();
        let full_query = format!("WITH {} {}", spans_cte, sql);

        let mut stmt = conn.prepare(&full_query)?;

        // Execute the query first, then get column info
        let mut rows = stmt.query(params)?;

        // Get column names from statement after execution
        let stmt_ref = rows.as_ref().expect("rows should have statement reference");
        let column_count = stmt_ref.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt_ref.column_name(i).map(|s| s.to_string()).unwrap_or_default())
            .collect();

        let mut results = Vec::new();

        while let Some(row) = rows.next()? {
            // Build a JSON object from the row
            let mut map = serde_json::Map::new();
            for (i, name) in column_names.iter().enumerate() {
                let value = row_value_to_json(row, i);
                map.insert(name.clone(), value);
            }
            let json_value = serde_json::Value::Object(map);
            let typed: T = serde_json::from_value(json_value)?;
            results.push(typed);
        }

        Ok(results)
    }

    /// Execute a raw SQL query (no CTE wrapper) - useful for DDL or non-spans queries
    pub fn execute_raw(&self, sql: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        conn.execute_batch(sql)?;
        Ok(())
    }
}

/// Convert a DuckDB row value to a JSON value
fn row_value_to_json(row: &duckdb::Row, idx: usize) -> serde_json::Value {
    // Try different types in order of likelihood
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.get::<_, f64>(idx) {
        return serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.get::<_, String>(idx) {
        return serde_json::Value::String(v);
    }
    if let Ok(v) = row.get::<_, bool>(idx) {
        return serde_json::Value::Bool(v);
    }
    if let Ok(v) = row.get::<_, Option<String>>(idx) {
        return v
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null);
    }
    serde_json::Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tempfile::tempdir;

    #[derive(Debug, Deserialize, PartialEq)]
    struct CountRow {
        count: i64,
    }

    #[test]
    fn test_read_db_creation() {
        let dir = tempdir().unwrap();
        let sqlite_path = dir.path().join("test.db");
        let parquet_dir = dir.path().join("parquet");

        // Create empty SQLite file with schema
        let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        conn.execute_batch(include_str!("../../migrations/01_spans.sql"))
            .unwrap();
        drop(conn);

        let db = ReadDb::new(sqlite_path.to_str().unwrap(), parquet_dir.to_str().unwrap()).unwrap();

        // Should be able to query empty table
        let results: Vec<CountRow> = db
            .query("SELECT COUNT(*) as count FROM spans", &[])
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].count, 0);
    }
}
