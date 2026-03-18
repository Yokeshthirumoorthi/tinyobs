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
        std::fs::create_dir_all(parquet_dir)?;

        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            r#"
            INSTALL sqlite;
            LOAD sqlite;
            "#,
        )?;

        conn.execute_batch(
            r#"
            CREATE MACRO attr(json_col, key_name) AS (
                list_extract(
                    list_filter(
                        json_col::JSON[]::VARCHAR[][],
                        x -> x[1] = key_name
                    ),
                    1
                )[2]
            );
            "#,
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            sqlite_path: sqlite_path.to_string(),
            parquet_dir: parquet_dir.to_string(),
        })
    }

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

    pub fn query<T: DeserializeOwned>(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<T>> {
        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        let spans_cte = self.build_spans_cte();
        let full_query = format!("WITH {} {}", spans_cte, sql);

        let mut stmt = conn.prepare(&full_query)?;
        let mut rows = stmt.query(params)?;

        let stmt_ref = rows.as_ref().expect("rows should have statement reference");
        let column_count = stmt_ref.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt_ref.column_name(i).map(|s| s.to_string()).unwrap_or_default())
            .collect();

        let mut results = Vec::new();

        while let Some(row) = rows.next()? {
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

    pub fn execute_raw(&self, sql: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        conn.execute_batch(sql)?;
        Ok(())
    }
}

fn row_value_to_json(row: &duckdb::Row, idx: usize) -> serde_json::Value {
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

        let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        conn.execute_batch(include_str!("../../migrations/01_spans.sql"))
            .unwrap();
        drop(conn);

        let db = ReadDb::new(sqlite_path.to_str().unwrap(), parquet_dir.to_str().unwrap()).unwrap();

        let results: Vec<CountRow> = db
            .query("SELECT COUNT(*) as count FROM spans", &[])
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].count, 0);
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct AttrRow {
        val: Option<String>,
    }

    #[test]
    fn test_attr_macro() {
        let dir = tempdir().unwrap();
        let sqlite_path = dir.path().join("test.db");
        let parquet_dir = dir.path().join("parquet");

        let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        conn.execute_batch(include_str!("../../migrations/01_spans.sql"))
            .unwrap();
        conn.execute(
            "INSERT INTO spans (trace_id, span_id, service_name, operation, start_time, duration_ns, status, attributes)
             VALUES ('t1', 's1', 'svc', 'op', 1000, 500, 0, ?)",
            [r#"[["source","web"],["env","prod"]]"#],
        ).unwrap();
        drop(conn);

        let db = ReadDb::new(sqlite_path.to_str().unwrap(), parquet_dir.to_str().unwrap()).unwrap();

        let results: Vec<AttrRow> = db
            .query("SELECT attr(attributes, 'source') as val FROM spans", &[])
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].val, Some("web".to_string()));

        let results: Vec<AttrRow> = db
            .query("SELECT attr(attributes, 'env') as val FROM spans", &[])
            .unwrap();
        assert_eq!(results[0].val, Some("prod".to_string()));

        let results: Vec<AttrRow> = db
            .query("SELECT attr(attributes, 'missing') as val FROM spans", &[])
            .unwrap();
        assert_eq!(results[0].val, None);
    }
}
