//! Remote ClickHouse transport via HTTP (reqwest)
//!
//! Also provides typed row structs and `Inserter` support for high-throughput
//! batch inserts via the `clickhouse` crate.

use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client as HttpClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::ch;
use crate::schema::{LogRecord, Metric, Span};
use super::Transport;

// ============================================================================
// ClickHouse Row types for typed inserts
// ============================================================================

#[derive(Debug, clickhouse::Row, serde::Serialize)]
pub struct ChSpanRow {
    #[serde(rename = "TraceId")]
    pub trace_id: String,
    #[serde(rename = "SpanId")]
    pub span_id: String,
    #[serde(rename = "ParentSpanId")]
    pub parent_span_id: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "SpanName")]
    pub span_name: String,
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "Duration")]
    pub duration: i64,
    #[serde(rename = "StatusCode")]
    pub status_code: i32,
    #[serde(rename = "SpanAttributes")]
    pub span_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
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
pub struct ChLogRow {
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "ObservedTimestamp")]
    pub observed_timestamp: i64,
    #[serde(rename = "TraceId")]
    pub trace_id: String,
    #[serde(rename = "SpanId")]
    pub span_id: String,
    #[serde(rename = "SeverityNumber")]
    pub severity_number: i32,
    #[serde(rename = "SeverityText")]
    pub severity_text: String,
    #[serde(rename = "Body")]
    pub body: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "LogAttributes")]
    pub log_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
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
pub struct ChMetricRow {
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "Unit")]
    pub unit: String,
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "Value")]
    pub value: f64,
    #[serde(rename = "Sum")]
    pub sum: f64,
    #[serde(rename = "Count")]
    pub count: u64,
    #[serde(rename = "Min")]
    pub min: f64,
    #[serde(rename = "Max")]
    pub max: f64,
    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
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
// Backend config for remote ClickHouse
// ============================================================================

/// Backend connection settings for remote ClickHouse
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BackendConfig {
    #[serde(default = "default_clickhouse_url")]
    pub url: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_max_rows")]
    pub max_rows: u64,
}

fn default_clickhouse_url() -> String {
    "http://127.0.0.1:8123".to_string()
}
fn default_database() -> String {
    "default".to_string()
}
fn default_flush_interval_ms() -> u64 {
    1000
}
fn default_max_rows() -> u64 {
    10000
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            url: default_clickhouse_url(),
            database: default_database(),
            flush_interval_ms: default_flush_interval_ms(),
            max_rows: default_max_rows(),
        }
    }
}

// ============================================================================
// RemoteTransport
// ============================================================================

/// Remote ClickHouse transport via HTTP
#[derive(Clone)]
pub struct RemoteTransport {
    http: HttpClient,
    config: Arc<BackendConfig>,
    // Typed inserters for high-throughput batch inserts
    pub client: clickhouse::Client,
    pub spans: Arc<Mutex<clickhouse::inserter::Inserter<ChSpanRow>>>,
    pub logs: Arc<Mutex<clickhouse::inserter::Inserter<ChLogRow>>>,
    pub metrics: Arc<Mutex<clickhouse::inserter::Inserter<ChMetricRow>>>,
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

impl RemoteTransport {
    pub fn new(config: BackendConfig) -> Self {
        let client = clickhouse::Client::default()
            .with_url(&config.url)
            .with_database(&config.database);
        let http = HttpClient::new();
        let spans = Arc::new(Mutex::new(make_inserter::<ChSpanRow>(&client, "otel_traces", &config)));
        let logs = Arc::new(Mutex::new(make_inserter::<ChLogRow>(&client, "otel_logs", &config)));
        let metrics = Arc::new(Mutex::new(make_inserter::<ChMetricRow>(&client, "otel_metrics", &config)));
        Self {
            http,
            config: Arc::new(config),
            client,
            spans,
            logs,
            metrics,
        }
    }
}

#[async_trait]
impl Transport for RemoteTransport {
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

        Ok(ch::parse_json_rows(&body))
    }

    async fn execute(&self, sql: &str) -> Result<()> {
        let resp = self
            .http
            .post(&self.config.url)
            .query(&[("database", &self.config.database)])
            .body(sql.to_string())
            .send()
            .await
            .context("ClickHouse HTTP request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.context("Failed to read response body")?;
            anyhow::bail!("ClickHouse error ({}): {}", status, body);
        }

        Ok(())
    }

    async fn ping(&self) -> Result<bool> {
        let resp = self
            .http
            .get(&self.config.url)
            .query(&[("query", "SELECT 1")])
            .send()
            .await;
        Ok(resp.is_ok())
    }
}
