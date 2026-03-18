//! Storage backend implementing TelemetryBackend + IngestBackend
//!
//! Uses the `clickhouse` crate for typed inserts (with LZ4 compression)
//! and `reqwest` for JSON-format queries.

use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client as HttpClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use tinyobs_core::backend::*;
use tinyobs_core::schema::{LogRecord, Metric, MetricKind, Span, SpanStatus, SeverityLevel};

use crate::config::BackendConfig;

// ============================================================================
// ClickHouse Row types for typed inserts
// ============================================================================

#[derive(Debug, clickhouse::Row, serde::Serialize)]
struct ChSpanRow {
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "ParentSpanId")]
    parent_span_id: String,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "SpanName")]
    span_name: String,
    #[serde(rename = "Timestamp")]
    timestamp: i64,
    #[serde(rename = "Duration")]
    duration: i64,
    #[serde(rename = "StatusCode")]
    status_code: i32,
    #[serde(rename = "SpanAttributes")]
    span_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: HashMap<String, String>,
}

impl From<&Span> for ChSpanRow {
    fn from(s: &Span) -> Self {
        Self {
            trace_id: s.trace_id.clone(),
            span_id: s.span_id.clone(),
            parent_span_id: s.parent_span_id.clone().unwrap_or_default(),
            service_name: s.service_name.clone(),
            span_name: s.operation.clone(),
            timestamp: s.start_time,
            duration: s.duration_ns,
            status_code: i32::from(s.status),
            span_attributes: s.attributes.iter().cloned().collect(),
            resource_attributes: s.resource_attrs.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, clickhouse::Row, serde::Serialize)]
struct ChLogRow {
    #[serde(rename = "Timestamp")]
    timestamp: i64,
    #[serde(rename = "ObservedTimestamp")]
    observed_timestamp: i64,
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "SeverityNumber")]
    severity_number: i32,
    #[serde(rename = "SeverityText")]
    severity_text: String,
    #[serde(rename = "Body")]
    body: String,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "LogAttributes")]
    log_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: HashMap<String, String>,
}

impl From<&LogRecord> for ChLogRow {
    fn from(l: &LogRecord) -> Self {
        Self {
            timestamp: l.timestamp,
            observed_timestamp: l.observed_timestamp.unwrap_or(0),
            trace_id: l.trace_id.clone().unwrap_or_default(),
            span_id: l.span_id.clone().unwrap_or_default(),
            severity_number: l.severity_number as i32,
            severity_text: l.severity_text.clone().unwrap_or_default(),
            body: l.body.clone(),
            service_name: l.service_name.clone(),
            log_attributes: l.attributes.iter().cloned().collect(),
            resource_attributes: l.resource_attrs.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, clickhouse::Row, serde::Serialize)]
struct ChMetricRow {
    #[serde(rename = "MetricName")]
    metric_name: String,
    #[serde(rename = "Description")]
    description: String,
    #[serde(rename = "Unit")]
    unit: String,
    #[serde(rename = "Timestamp")]
    timestamp: i64,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "Value")]
    value: f64,
    #[serde(rename = "Sum")]
    sum: f64,
    #[serde(rename = "Count")]
    count: u64,
    #[serde(rename = "Min")]
    min: f64,
    #[serde(rename = "Max")]
    max: f64,
    #[serde(rename = "Attributes")]
    attributes: HashMap<String, String>,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: HashMap<String, String>,
}

impl From<&Metric> for ChMetricRow {
    fn from(m: &Metric) -> Self {
        Self {
            metric_name: m.name.clone(),
            description: m.description.clone().unwrap_or_default(),
            unit: m.unit.clone().unwrap_or_default(),
            timestamp: m.timestamp,
            service_name: m.service_name.clone(),
            value: m.value.unwrap_or(0.0),
            sum: m.sum.unwrap_or(0.0),
            count: m.count.unwrap_or(0),
            min: m.min.unwrap_or(0.0),
            max: m.max.unwrap_or(0.0),
            attributes: m.attributes.iter().cloned().collect(),
            resource_attributes: m.resource_attrs.iter().cloned().collect(),
        }
    }
}

// ============================================================================
// TinyObsDB
// ============================================================================

/// Product-branded telemetry storage backend
#[derive(Clone)]
pub struct TinyObsDB {
    client: clickhouse::Client,
    http: HttpClient,
    config: Arc<BackendConfig>,
    spans: Arc<Mutex<clickhouse::inserter::Inserter<ChSpanRow>>>,
    logs: Arc<Mutex<clickhouse::inserter::Inserter<ChLogRow>>>,
    metrics: Arc<Mutex<clickhouse::inserter::Inserter<ChMetricRow>>>,
}

fn make_inserter<T: clickhouse::Row>(
    client: &clickhouse::Client,
    table: &str,
    config: &BackendConfig,
) -> clickhouse::inserter::Inserter<T> {
    client
        .inserter::<T>(table)
        .with_period(Some(Duration::from_millis(config.flush_interval_ms)))
        .with_max_rows(config.max_rows)
}

impl TinyObsDB {
    pub fn new(config: BackendConfig) -> Self {
        let client = clickhouse::Client::default()
            .with_url(&config.url)
            .with_database(&config.database);
        let http = HttpClient::new();
        let spans = Arc::new(Mutex::new(make_inserter::<ChSpanRow>(&client, "otel_traces", &config)));
        let logs = Arc::new(Mutex::new(make_inserter::<ChLogRow>(&client, "otel_logs", &config)));
        let metrics = Arc::new(Mutex::new(make_inserter::<ChMetricRow>(&client, "otel_metrics", &config)));
        Self {
            client,
            http,
            config: Arc::new(config),
            spans,
            logs,
            metrics,
        }
    }

    /// Check if ClickHouse is reachable
    pub async fn ping(&self) -> Result<bool> {
        let resp = self
            .http
            .get(&self.config.url)
            .query(&[("query", "SELECT 1")])
            .send()
            .await;
        Ok(resp.is_ok())
    }

    /// Execute a query and deserialize each row into `T`.
    ///
    /// The underlying ClickHouse HTTP call uses `JSONEachRow` format,
    /// so `T` must match the column names/types returned by the SQL.
    pub async fn query<T: serde::de::DeserializeOwned>(&self, sql: &str) -> Result<Vec<T>> {
        let rows = self.query_json(sql).await?;
        rows.into_iter()
            .map(|v| serde_json::from_value(v).context("Failed to deserialize row"))
            .collect()
    }

    /// Execute a query and return JSON rows
    async fn query_json(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let full_sql = format!("{} FORMAT JSONEachRow", sql);
        let resp = self
            .http
            .post(&self.config.url)
            .query(&[("database", &self.config.database)])
            .body(full_sql)
            .send()
            .await
            .context("ClickHouse HTTP request failed")?;

        let status = resp.status();
        let body = resp.text().await.context("Failed to read response body")?;

        if !status.is_success() {
            anyhow::bail!("ClickHouse error ({}): {}", status, body);
        }

        let rows: Vec<serde_json::Value> = body
            .lines()
            .filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();

        Ok(rows)
    }

    // ========================================================================
    // Query helpers
    // ========================================================================

    fn build_span_where_clause(filter: &SpanFilter) -> String {
        let mut conditions = Vec::new();

        let start_micros = filter.time_range.start.timestamp_micros();
        let end_micros = filter.time_range.end.timestamp_micros();
        conditions.push(format!("Timestamp >= {}", start_micros));
        conditions.push(format!("Timestamp <= {}", end_micros));

        if let Some(ref service) = filter.service {
            conditions.push(format!("ServiceName = '{}'", escape_ch(service)));
        }
        if let Some(ref operation) = filter.operation {
            conditions.push(format!("SpanName = '{}'", escape_ch(operation)));
        }
        if let Some(ref trace_id) = filter.trace_id {
            conditions.push(format!("TraceId = '{}'", escape_ch(trace_id)));
        }
        if let Some(ref session_id) = filter.session_id {
            conditions.push(format!(
                "SpanAttributes['session.id'] = '{}'",
                escape_ch(session_id)
            ));
        }
        if let Some(ref status_filter) = filter.status {
            let code = match status_filter {
                SpanStatusFilter::Ok => 1,
                SpanStatusFilter::Error => 2,
                SpanStatusFilter::Unset => 0,
            };
            conditions.push(format!("StatusCode = {}", code));
        }
        if let Some(min_dur) = filter.min_duration_ns {
            conditions.push(format!("Duration >= {}", min_dur));
        }
        if let Some(max_dur) = filter.max_duration_ns {
            conditions.push(format!("Duration <= {}", max_dur));
        }
        for (key, val) in &filter.attributes {
            conditions.push(format!(
                "SpanAttributes['{}'] = '{}'",
                escape_ch(key),
                escape_ch(val)
            ));
        }

        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }

    fn build_log_where_clause(filter: &LogFilter) -> String {
        let mut conditions = Vec::new();

        let start_micros = filter.time_range.start.timestamp_micros();
        let end_micros = filter.time_range.end.timestamp_micros();
        conditions.push(format!("Timestamp >= {}", start_micros));
        conditions.push(format!("Timestamp <= {}", end_micros));

        if let Some(ref service) = filter.service {
            conditions.push(format!("ServiceName = '{}'", escape_ch(service)));
        }
        if let Some(ref severity) = filter.severity {
            conditions.push(format!("SeverityText = '{}'", escape_ch(severity)));
        }
        if let Some(min_sev) = filter.min_severity_number {
            conditions.push(format!("SeverityNumber >= {}", min_sev));
        }
        if let Some(ref trace_id) = filter.trace_id {
            conditions.push(format!("TraceId = '{}'", escape_ch(trace_id)));
        }
        if let Some(ref span_id) = filter.span_id {
            conditions.push(format!("SpanId = '{}'", escape_ch(span_id)));
        }
        if let Some(ref body) = filter.body_contains {
            conditions.push(format!(
                "positionCaseInsensitive(Body, '{}') > 0",
                escape_ch(body)
            ));
        }
        for (key, val) in &filter.attributes {
            conditions.push(format!(
                "LogAttributes['{}'] = '{}'",
                escape_ch(key),
                escape_ch(val)
            ));
        }

        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }

    fn build_metric_where_clause(filter: &MetricFilter) -> String {
        let mut conditions = Vec::new();

        let start_micros = filter.time_range.start.timestamp_micros();
        let end_micros = filter.time_range.end.timestamp_micros();
        conditions.push(format!("Timestamp >= {}", start_micros));
        conditions.push(format!("Timestamp <= {}", end_micros));

        if let Some(ref service) = filter.service {
            conditions.push(format!("ServiceName = '{}'", escape_ch(service)));
        }
        if let Some(ref name) = filter.name {
            conditions.push(format!("MetricName = '{}'", escape_ch(name)));
        }

        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }

    fn row_to_span(row: &serde_json::Value) -> Option<Span> {
        let obj = row.as_object()?;

        let attrs: Vec<(String, String)> = obj
            .get("SpanAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let resource_attrs: Vec<(String, String)> = obj
            .get("ResourceAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Some(Span {
            trace_id: obj.get("TraceId")?.as_str()?.to_string(),
            span_id: obj.get("SpanId")?.as_str()?.to_string(),
            parent_span_id: obj
                .get("ParentSpanId")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            session_id: attrs
                .iter()
                .find(|(k, _)| k == "session.id")
                .map(|(_, v)| v.clone()),
            service_name: obj
                .get("ServiceName")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            operation: obj
                .get("SpanName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            start_time: obj
                .get("Timestamp")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0),
            duration_ns: obj
                .get("Duration")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0),
            status: SpanStatus::from(
                obj.get("StatusCode")
                    .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                    .unwrap_or(0) as i32,
            ),
            attributes: attrs,
            resource_attrs,
        })
    }

    fn row_to_log(row: &serde_json::Value) -> Option<LogRecord> {
        let obj = row.as_object()?;

        let attrs: Vec<(String, String)> = obj
            .get("LogAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let resource_attrs: Vec<(String, String)> = obj
            .get("ResourceAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let severity_number = obj
            .get("SeverityNumber")
            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0) as i32;

        Some(LogRecord {
            timestamp: obj
                .get("Timestamp")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0),
            observed_timestamp: obj
                .get("ObservedTimestamp")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))),
            trace_id: obj
                .get("TraceId")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            span_id: obj
                .get("SpanId")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            severity_number: SeverityLevel::from(severity_number),
            severity_text: obj
                .get("SeverityText")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            body: obj
                .get("Body")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            service_name: obj
                .get("ServiceName")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            attributes: attrs,
            resource_attrs,
        })
    }
}

/// Escape single quotes for ClickHouse SQL (used in query WHERE clauses)
fn escape_ch(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

#[async_trait]
impl TelemetryBackend for TinyObsDB {
    async fn query_spans(&self, filter: SpanFilter) -> Result<Vec<Span>> {
        let where_clause = Self::build_span_where_clause(&filter);
        let order = filter
            .order_by
            .as_ref()
            .map(|o| {
                let dir = if o.descending { "DESC" } else { "ASC" };
                format!("ORDER BY {} {}", o.field, dir)
            })
            .unwrap_or_else(|| "ORDER BY Timestamp DESC".to_string());

        let sql = format!(
            "SELECT * FROM otel_traces {} {} LIMIT {} OFFSET {}",
            where_clause, order, filter.limit, filter.offset
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows.iter().filter_map(Self::row_to_span).collect())
    }

    async fn query_logs(&self, filter: LogFilter) -> Result<Vec<LogRecord>> {
        let where_clause = Self::build_log_where_clause(&filter);

        let sql = format!(
            "SELECT * FROM otel_logs {} ORDER BY Timestamp DESC LIMIT {} OFFSET {}",
            where_clause, filter.limit, filter.offset
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows.iter().filter_map(Self::row_to_log).collect())
    }

    async fn query_metrics(&self, filter: MetricFilter) -> Result<Vec<Metric>> {
        let where_clause = Self::build_metric_where_clause(&filter);

        let sql = if let (Some(agg), Some(bucket)) = (&filter.aggregation, &filter.bucket_secs) {
            let agg_fn = match agg {
                MetricAggregation::Sum => "sum(Value)",
                MetricAggregation::Avg => "avg(Value)",
                MetricAggregation::Min => "min(Value)",
                MetricAggregation::Max => "max(Value)",
                MetricAggregation::Count => "count()",
                MetricAggregation::P50 => "quantile(0.5)(Value)",
                MetricAggregation::P90 => "quantile(0.9)(Value)",
                MetricAggregation::P95 => "quantile(0.95)(Value)",
                MetricAggregation::P99 => "quantile(0.99)(Value)",
            };

            format!(
                "SELECT MetricName as name, ServiceName as service_name, \
                 toStartOfInterval(fromUnixTimestamp64Micro(Timestamp), INTERVAL {} SECOND) as bucket, \
                 {} as value \
                 FROM otel_metrics {} \
                 GROUP BY name, service_name, bucket \
                 ORDER BY bucket DESC LIMIT {}",
                bucket, agg_fn, where_clause, filter.limit
            )
        } else {
            format!(
                "SELECT * FROM otel_metrics {} ORDER BY Timestamp DESC LIMIT {}",
                where_clause, filter.limit
            )
        };

        let rows = self.query_json(&sql).await?;

        if filter.aggregation.is_some() {
            Ok(rows
                .iter()
                .filter_map(|r| {
                    let obj = r.as_object()?;
                    Some(Metric {
                        name: obj.get("name")?.as_str()?.to_string(),
                        description: None,
                        unit: None,
                        kind: MetricKind::Gauge,
                        timestamp: obj
                            .get("bucket")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0),
                        service_name: obj
                            .get("service_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                        value: obj.get("value").and_then(|v| v.as_f64()),
                        sum: None,
                        count: None,
                        min: None,
                        max: None,
                        quantiles: None,
                        buckets: None,
                        attributes: vec![],
                        resource_attrs: vec![],
                    })
                })
                .collect())
        } else {
            Ok(rows
                .iter()
                .filter_map(|r| {
                    let obj = r.as_object()?;
                    Some(Metric {
                        name: obj.get("MetricName")?.as_str()?.to_string(),
                        description: obj
                            .get("Description")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string()),
                        unit: obj
                            .get("Unit")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string()),
                        kind: MetricKind::Gauge,
                        timestamp: obj
                            .get("Timestamp")
                            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                            .unwrap_or(0),
                        service_name: obj
                            .get("ServiceName")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                        value: obj.get("Value").and_then(|v| v.as_f64()),
                        sum: obj.get("Sum").and_then(|v| v.as_f64()),
                        count: obj.get("Count").and_then(|v| v.as_u64()),
                        min: obj.get("Min").and_then(|v| v.as_f64()),
                        max: obj.get("Max").and_then(|v| v.as_f64()),
                        quantiles: None,
                        buckets: None,
                        attributes: vec![],
                        resource_attrs: vec![],
                    })
                })
                .collect())
        }
    }

    async fn get_trace(&self, trace_id: &str) -> Result<Vec<Span>> {
        let sql = format!(
            "SELECT * FROM otel_traces WHERE TraceId = '{}' ORDER BY Timestamp ASC",
            escape_ch(trace_id)
        );
        let rows = self.query_json(&sql).await?;
        Ok(rows.iter().filter_map(Self::row_to_span).collect())
    }

    async fn get_session(&self, session_id: &str) -> Result<Vec<Span>> {
        let sql = format!(
            "SELECT * FROM otel_traces WHERE SpanAttributes['session.id'] = '{}' ORDER BY Timestamp ASC",
            escape_ch(session_id)
        );
        let rows = self.query_json(&sql).await?;
        Ok(rows.iter().filter_map(Self::row_to_span).collect())
    }

    async fn list_services(&self, time_range: TimeRange) -> Result<Vec<ServiceSummary>> {
        let start = time_range.start.timestamp_micros();
        let end = time_range.end.timestamp_micros();

        let sql = format!(
            r#"SELECT
                ServiceName as name,
                count() as span_count,
                uniqExact(SpanName) as operation_count,
                countIf(StatusCode = 2) / count() as error_rate,
                avg(Duration) as avg_duration_ns,
                quantile(0.99)(Duration) as p99_duration_ns,
                min(Timestamp) as first_seen,
                max(Timestamp) as last_seen
            FROM otel_traces
            WHERE Timestamp >= {} AND Timestamp <= {}
            GROUP BY ServiceName
            ORDER BY span_count DESC"#,
            start, end
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let obj = r.as_object()?;
                Some(ServiceSummary {
                    name: obj.get("name")?.as_str()?.to_string(),
                    span_count: obj.get("span_count")?.as_u64()?,
                    operation_count: obj.get("operation_count")?.as_u64()?,
                    error_rate: obj.get("error_rate")?.as_f64()?,
                    avg_duration_ns: obj.get("avg_duration_ns")?.as_i64()?,
                    p99_duration_ns: obj.get("p99_duration_ns").and_then(|v| v.as_i64()),
                    first_seen: chrono::DateTime::from_timestamp_micros(
                        obj.get("first_seen").and_then(|v| v.as_i64())?,
                    )
                    .unwrap_or_default(),
                    last_seen: chrono::DateTime::from_timestamp_micros(
                        obj.get("last_seen").and_then(|v| v.as_i64())?,
                    )
                    .unwrap_or_default(),
                })
            })
            .collect())
    }

    async fn attribute_keys(&self, signal: Signal, service: Option<&str>) -> Result<Vec<String>> {
        let (table, col) = match signal {
            Signal::Traces => ("otel_traces", "SpanAttributes"),
            Signal::Logs => ("otel_logs", "LogAttributes"),
            Signal::Metrics => ("otel_metrics", "Attributes"),
        };

        let service_clause = service
            .map(|s| format!("WHERE ServiceName = '{}'", escape_ch(s)))
            .unwrap_or_default();

        let sql = format!(
            "SELECT DISTINCT arrayJoin(mapKeys({})) as key FROM {} {} ORDER BY key LIMIT 1000",
            col, table, service_clause
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows
            .iter()
            .filter_map(|r| r.get("key").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect())
    }

    async fn service_health(&self, time_range: TimeRange) -> Result<Vec<ServiceHealth>> {
        let start = time_range.start.timestamp_micros();
        let end = time_range.end.timestamp_micros();
        let duration_secs = time_range.duration_secs().max(1) as f64;

        let sql = format!(
            r#"SELECT
                ServiceName as service,
                count() / {dur} as request_rate,
                countIf(StatusCode = 2) / count() as error_rate,
                avg(Duration) / 1000000.0 as avg_latency_ms,
                quantile(0.5)(Duration) / 1000000.0 as p50_latency_ms,
                quantile(0.95)(Duration) / 1000000.0 as p95_latency_ms,
                quantile(0.99)(Duration) / 1000000.0 as p99_latency_ms,
                countIf(StatusCode != 2) / {dur} as throughput
            FROM otel_traces
            WHERE Timestamp >= {start} AND Timestamp <= {end}
            GROUP BY ServiceName"#,
            dur = duration_secs,
            start = start,
            end = end,
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let obj = r.as_object()?;
                Some(ServiceHealth {
                    service: obj.get("service")?.as_str()?.to_string(),
                    request_rate: obj.get("request_rate")?.as_f64()?,
                    error_rate: obj.get("error_rate")?.as_f64()?,
                    avg_latency_ms: obj.get("avg_latency_ms")?.as_f64()?,
                    p50_latency_ms: obj.get("p50_latency_ms")?.as_f64()?,
                    p95_latency_ms: obj.get("p95_latency_ms")?.as_f64()?,
                    p99_latency_ms: obj.get("p99_latency_ms")?.as_f64()?,
                    throughput: obj.get("throughput")?.as_f64()?,
                    apdex: None,
                })
            })
            .collect())
    }

    async fn compare_periods(
        &self,
        a: TimeRange,
        b: TimeRange,
        service: Option<&str>,
    ) -> Result<Comparison> {
        let service_clause = service
            .map(|s| format!("AND ServiceName = '{}'", escape_ch(s)))
            .unwrap_or_default();

        let build_period_query = |tr: &TimeRange| {
            let start = tr.start.timestamp_micros();
            let end = tr.end.timestamp_micros();
            let dur = tr.duration_secs().max(1) as f64;
            format!(
                r#"SELECT
                    count() as request_count,
                    countIf(StatusCode = 2) as error_count,
                    countIf(StatusCode = 2) / greatest(count(), 1) as error_rate,
                    avg(Duration) / 1000000.0 as avg_latency_ms,
                    count() / {} as throughput
                FROM otel_traces
                WHERE Timestamp >= {} AND Timestamp <= {} {}"#,
                dur, start, end, service_clause
            )
        };

        let rows_a = self.query_json(&build_period_query(&a)).await?;
        let rows_b = self.query_json(&build_period_query(&b)).await?;

        let parse_period = |rows: &[serde_json::Value], tr: TimeRange| -> PeriodMetrics {
            rows.first()
                .and_then(|r| r.as_object())
                .map(|obj| PeriodMetrics {
                    time_range: tr.clone(),
                    request_count: obj
                        .get("request_count")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                    error_count: obj
                        .get("error_count")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                    error_rate: obj.get("error_rate").and_then(|v| v.as_f64()).unwrap_or(0.0),
                    avg_latency_ms: obj
                        .get("avg_latency_ms")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    throughput: obj.get("throughput").and_then(|v| v.as_f64()).unwrap_or(0.0),
                })
                .unwrap_or(PeriodMetrics {
                    time_range: tr,
                    request_count: 0,
                    error_count: 0,
                    error_rate: 0.0,
                    avg_latency_ms: 0.0,
                    throughput: 0.0,
                })
        };

        let pa = parse_period(&rows_a, a);
        let pb = parse_period(&rows_b, b);

        Ok(Comparison {
            error_rate_delta: pb.error_rate - pa.error_rate,
            latency_delta_ms: pb.avg_latency_ms - pa.avg_latency_ms,
            throughput_delta: pb.throughput - pa.throughput,
            request_count_delta: pb.request_count as i64 - pa.request_count as i64,
            period_a: pa,
            period_b: pb,
        })
    }

    async fn raw_query(&self, query: &str, limit: usize) -> Result<Vec<serde_json::Value>> {
        let sql = if query.to_uppercase().contains("LIMIT") {
            query.to_string()
        } else {
            format!("{} LIMIT {}", query, limit)
        };

        self.query_json(&sql).await
    }

    async fn discover_schema(&self) -> Result<Schema> {
        let span_keys = self
            .attribute_keys(Signal::Traces, None)
            .await
            .unwrap_or_default();
        let log_keys = self
            .attribute_keys(Signal::Logs, None)
            .await
            .unwrap_or_default();

        let metric_rows = self
            .query_json("SELECT DISTINCT MetricName FROM otel_metrics ORDER BY MetricName LIMIT 1000")
            .await
            .unwrap_or_default();
        let metric_names: Vec<String> = metric_rows
            .iter()
            .filter_map(|r| r.get("MetricName").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        let resource_rows = self
            .query_json(
                "SELECT DISTINCT arrayJoin(mapKeys(ResourceAttributes)) as key FROM otel_traces ORDER BY key LIMIT 1000",
            )
            .await
            .unwrap_or_default();
        let resource_keys: Vec<String> = resource_rows
            .iter()
            .filter_map(|r| r.get("key").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        Ok(Schema {
            tables: vec![
                TableSchema {
                    name: "otel_traces".to_string(),
                    signal: Signal::Traces,
                    columns: vec![],
                },
                TableSchema {
                    name: "otel_logs".to_string(),
                    signal: Signal::Logs,
                    columns: vec![],
                },
                TableSchema {
                    name: "otel_metrics".to_string(),
                    signal: Signal::Metrics,
                    columns: vec![],
                },
            ],
            span_attribute_keys: span_keys,
            resource_attribute_keys: resource_keys,
            log_attribute_keys: log_keys,
            metric_names,
        })
    }
}

#[async_trait]
impl IngestBackend for TinyObsDB {
    async fn ingest_span(&self, span: Span) -> Result<()> {
        self.ingest_spans(vec![span]).await
    }

    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let mut inserter = self.spans.lock().await;
        for span in &spans {
            inserter.write(&ChSpanRow::from(span)).await?;
        }
        let quant = inserter.commit().await?;
        if quant.transactions > 0 {
            tracing::debug!(rows = quant.rows, "Flushed spans");
        }
        Ok(())
    }

    async fn ingest_log(&self, log: LogRecord) -> Result<()> {
        self.ingest_logs(vec![log]).await
    }

    async fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        let mut inserter = self.logs.lock().await;
        for log in &logs {
            inserter.write(&ChLogRow::from(log)).await?;
        }
        let quant = inserter.commit().await?;
        if quant.transactions > 0 {
            tracing::debug!(rows = quant.rows, "Flushed logs");
        }
        Ok(())
    }

    async fn ingest_metric(&self, metric: Metric) -> Result<()> {
        self.ingest_metrics(vec![metric]).await
    }

    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        let mut inserter = self.metrics.lock().await;
        for metric in &metrics {
            inserter.write(&ChMetricRow::from(metric)).await?;
        }
        let quant = inserter.commit().await?;
        if quant.transactions > 0 {
            tracing::debug!(rows = quant.rows, "Flushed metrics");
        }
        Ok(())
    }
}

#[async_trait]
impl ManagedBackend for TinyObsDB {
    async fn start_background_tasks(&self) -> Result<()> {
        Ok(())
    }

    async fn stop_background_tasks(&self) -> Result<()> {
        self.spans.lock().await.force_commit().await?;
        self.logs.lock().await.force_commit().await?;
        self.metrics.lock().await.force_commit().await?;
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        self.ping().await
    }
}
