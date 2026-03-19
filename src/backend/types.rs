//! Filter and response types for TelemetryBackend operations

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Signal type for telemetry data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Signal {
    Traces,
    Logs,
    Metrics,
}

/// Time range for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl TimeRange {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self { start, end }
    }

    pub fn last_hours(hours: i64) -> Self {
        let end = Utc::now();
        let start = end - Duration::hours(hours);
        Self { start, end }
    }

    pub fn last_hour() -> Self {
        Self::last_hours(1)
    }

    pub fn last_day() -> Self {
        Self::last_hours(24)
    }

    pub fn last_week() -> Self {
        Self::last_hours(24 * 7)
    }

    pub fn duration_secs(&self) -> i64 {
        (self.end - self.start).num_seconds()
    }
}

impl Default for TimeRange {
    fn default() -> Self {
        Self::last_hour()
    }
}

/// Filter for span queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanFilter {
    #[serde(default)]
    pub time_range: TimeRange,
    pub service: Option<String>,
    pub operation: Option<String>,
    pub trace_id: Option<String>,
    pub session_id: Option<String>,
    pub parent_span_id: Option<Option<String>>,
    pub status: Option<SpanStatusFilter>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    pub min_duration_ns: Option<i64>,
    pub max_duration_ns: Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    pub order_by: Option<OrderBy>,
}

fn default_limit() -> usize {
    100
}

impl Default for SpanFilter {
    fn default() -> Self {
        Self {
            time_range: TimeRange::default(),
            service: None,
            operation: None,
            trace_id: None,
            session_id: None,
            parent_span_id: None,
            status: None,
            attributes: HashMap::new(),
            min_duration_ns: None,
            max_duration_ns: None,
            limit: default_limit(),
            offset: 0,
            order_by: None,
        }
    }
}

/// Status filter for spans
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpanStatusFilter {
    Ok,
    Error,
    Unset,
}

/// Order by specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBy {
    pub field: String,
    pub descending: bool,
}

impl Default for OrderBy {
    fn default() -> Self {
        Self {
            field: "start_time".to_string(),
            descending: true,
        }
    }
}

/// Filter for log queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFilter {
    #[serde(default)]
    pub time_range: TimeRange,
    pub service: Option<String>,
    pub severity: Option<String>,
    pub min_severity_number: Option<i32>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub body_contains: Option<String>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

impl Default for LogFilter {
    fn default() -> Self {
        Self {
            time_range: TimeRange::default(),
            service: None,
            severity: None,
            min_severity_number: None,
            trace_id: None,
            span_id: None,
            body_contains: None,
            attributes: HashMap::new(),
            limit: default_limit(),
            offset: 0,
        }
    }
}

/// Filter for metric queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricFilter {
    #[serde(default)]
    pub time_range: TimeRange,
    pub service: Option<String>,
    pub name: Option<String>,
    pub metric_type: Option<MetricType>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    pub aggregation: Option<MetricAggregation>,
    pub bucket_secs: Option<u64>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

impl Default for MetricFilter {
    fn default() -> Self {
        Self {
            time_range: TimeRange::default(),
            service: None,
            name: None,
            metric_type: None,
            attributes: HashMap::new(),
            aggregation: None,
            bucket_secs: None,
            limit: default_limit(),
        }
    }
}

/// Metric types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    Gauge,
    Counter,
    Histogram,
    Summary,
}

/// Metric aggregation functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricAggregation {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    P50,
    P90,
    P95,
    P99,
}

/// Summary of a service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSummary {
    pub name: String,
    pub span_count: u64,
    pub operation_count: u64,
    pub error_rate: f64,
    pub avg_duration_ns: i64,
    pub p99_duration_ns: Option<i64>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

/// Health metrics for a service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceHealth {
    pub service: String,
    pub request_rate: f64,
    pub error_rate: f64,
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub throughput: f64,
    pub apdex: Option<f64>,
}

/// Comparison between two time periods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Comparison {
    pub period_a: PeriodMetrics,
    pub period_b: PeriodMetrics,
    pub error_rate_delta: f64,
    pub latency_delta_ms: f64,
    pub throughput_delta: f64,
    pub request_count_delta: i64,
}

/// Metrics for a single period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodMetrics {
    pub time_range: TimeRange,
    pub request_count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub avg_latency_ms: f64,
    pub throughput: f64,
}

/// Schema information for discovery
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Schema {
    pub tables: Vec<TableSchema>,
    pub span_attribute_keys: Vec<String>,
    pub resource_attribute_keys: Vec<String>,
    pub log_attribute_keys: Vec<String>,
    pub metric_names: Vec<String>,
}

/// Schema for a single table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub signal: Signal,
    pub columns: Vec<ColumnSchema>,
}

/// Schema for a single column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub description: Option<String>,
}

/// Column mapping for external backends (ClickHouse, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    #[serde(default = "default_spans_table")]
    pub spans_table: String,
    #[serde(default = "default_trace_id")]
    pub trace_id: String,
    #[serde(default = "default_span_id")]
    pub span_id: String,
    #[serde(default = "default_parent_span_id")]
    pub parent_span_id: String,
    #[serde(default = "default_service_name")]
    pub service_name: String,
    #[serde(default = "default_operation")]
    pub operation: String,
    #[serde(default = "default_start_time")]
    pub start_time: String,
    #[serde(default = "default_duration")]
    pub duration: String,
    #[serde(default = "default_status")]
    pub status: String,
    #[serde(default = "default_attributes")]
    pub attributes: String,
    #[serde(default = "default_resource_attrs")]
    pub resource_attrs: String,
}

fn default_spans_table() -> String {
    "otel_traces".to_string()
}
fn default_trace_id() -> String {
    "TraceId".to_string()
}
fn default_span_id() -> String {
    "SpanId".to_string()
}
fn default_parent_span_id() -> String {
    "ParentSpanId".to_string()
}
fn default_service_name() -> String {
    "ServiceName".to_string()
}
fn default_operation() -> String {
    "SpanName".to_string()
}
fn default_start_time() -> String {
    "Timestamp".to_string()
}
fn default_duration() -> String {
    "Duration".to_string()
}
fn default_status() -> String {
    "StatusCode".to_string()
}
fn default_attributes() -> String {
    "SpanAttributes".to_string()
}
fn default_resource_attrs() -> String {
    "ResourceAttributes".to_string()
}

impl Default for ColumnMapping {
    fn default() -> Self {
        Self {
            spans_table: default_spans_table(),
            trace_id: default_trace_id(),
            span_id: default_span_id(),
            parent_span_id: default_parent_span_id(),
            service_name: default_service_name(),
            operation: default_operation(),
            start_time: default_start_time(),
            duration: default_duration(),
            status: default_status(),
            attributes: default_attributes(),
            resource_attrs: default_resource_attrs(),
        }
    }
}

/// Field mapping for Splunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    #[serde(default = "default_splunk_index")]
    pub index: String,
    #[serde(default = "default_splunk_sourcetype")]
    pub sourcetype: String,
    #[serde(default = "default_trace_id_field")]
    pub trace_id: String,
    #[serde(default = "default_span_id_field")]
    pub span_id: String,
    #[serde(default = "default_parent_span_id_field")]
    pub parent_span_id: String,
    #[serde(default = "default_service_field")]
    pub service: String,
    #[serde(default = "default_operation_field")]
    pub operation: String,
    #[serde(default = "default_duration_field")]
    pub duration: String,
    #[serde(default = "default_status_field")]
    pub status: String,
}

fn default_splunk_index() -> String {
    "otel".to_string()
}
fn default_splunk_sourcetype() -> String {
    "otel:traces".to_string()
}
fn default_trace_id_field() -> String {
    "trace_id".to_string()
}
fn default_span_id_field() -> String {
    "span_id".to_string()
}
fn default_parent_span_id_field() -> String {
    "parent_span_id".to_string()
}
fn default_service_field() -> String {
    "service.name".to_string()
}
fn default_operation_field() -> String {
    "name".to_string()
}
fn default_duration_field() -> String {
    "duration".to_string()
}
fn default_status_field() -> String {
    "status.code".to_string()
}

impl Default for FieldMapping {
    fn default() -> Self {
        Self {
            index: default_splunk_index(),
            sourcetype: default_splunk_sourcetype(),
            trace_id: default_trace_id_field(),
            span_id: default_span_id_field(),
            parent_span_id: default_parent_span_id_field(),
            service: default_service_field(),
            operation: default_operation_field(),
            duration: default_duration_field(),
            status: default_status_field(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_range_last_hour() {
        let range = TimeRange::last_hour();
        assert!(range.duration_secs() >= 3599 && range.duration_secs() <= 3601);
    }

    #[test]
    fn test_span_filter_default() {
        let filter = SpanFilter::default();
        assert_eq!(filter.limit, 100);
        assert_eq!(filter.offset, 0);
        assert!(filter.service.is_none());
    }

    #[test]
    fn test_column_mapping_defaults() {
        let mapping = ColumnMapping::default();
        assert_eq!(mapping.spans_table, "otel_traces");
        assert_eq!(mapping.trace_id, "TraceId");
    }
}
