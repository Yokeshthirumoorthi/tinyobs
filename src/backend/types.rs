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
    /// Create a new time range
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self { start, end }
    }

    /// Last N hours from now
    pub fn last_hours(hours: i64) -> Self {
        let end = Utc::now();
        let start = end - Duration::hours(hours);
        Self { start, end }
    }

    /// Last hour from now
    pub fn last_hour() -> Self {
        Self::last_hours(1)
    }

    /// Last 24 hours from now
    pub fn last_day() -> Self {
        Self::last_hours(24)
    }

    /// Last 7 days from now
    pub fn last_week() -> Self {
        Self::last_hours(24 * 7)
    }

    /// Duration in seconds
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SpanFilter {
    /// Filter by time range
    #[serde(default)]
    pub time_range: TimeRange,

    /// Filter by service name (exact match)
    pub service: Option<String>,

    /// Filter by operation name (exact match or prefix)
    pub operation: Option<String>,

    /// Filter by trace ID
    pub trace_id: Option<String>,

    /// Filter by session ID
    pub session_id: Option<String>,

    /// Filter by parent span ID (None = root spans only)
    pub parent_span_id: Option<Option<String>>,

    /// Filter by status
    pub status: Option<SpanStatusFilter>,

    /// Filter by attributes (key-value exact match)
    #[serde(default)]
    pub attributes: HashMap<String, String>,

    /// Minimum duration in nanoseconds
    pub min_duration_ns: Option<i64>,

    /// Maximum duration in nanoseconds
    pub max_duration_ns: Option<i64>,

    /// Maximum number of results (default: 100)
    #[serde(default = "default_limit")]
    pub limit: usize,

    /// Offset for pagination
    #[serde(default)]
    pub offset: usize,

    /// Order by field (default: start_time DESC)
    pub order_by: Option<OrderBy>,
}

fn default_limit() -> usize {
    100
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogFilter {
    /// Filter by time range
    #[serde(default)]
    pub time_range: TimeRange,

    /// Filter by service name
    pub service: Option<String>,

    /// Filter by severity level (e.g., "ERROR", "WARN", "INFO", "DEBUG")
    pub severity: Option<String>,

    /// Filter by minimum severity number (1-24, OpenTelemetry spec)
    pub min_severity_number: Option<i32>,

    /// Filter by trace ID (correlate logs with traces)
    pub trace_id: Option<String>,

    /// Filter by span ID (correlate logs with specific spans)
    pub span_id: Option<String>,

    /// Full-text search in log body
    pub body_contains: Option<String>,

    /// Filter by attributes
    #[serde(default)]
    pub attributes: HashMap<String, String>,

    /// Maximum number of results
    #[serde(default = "default_limit")]
    pub limit: usize,

    /// Offset for pagination
    #[serde(default)]
    pub offset: usize,
}

/// Filter for metric queries
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricFilter {
    /// Filter by time range
    #[serde(default)]
    pub time_range: TimeRange,

    /// Filter by service name
    pub service: Option<String>,

    /// Filter by metric name (exact match or prefix)
    pub name: Option<String>,

    /// Filter by metric type (gauge, counter, histogram, summary)
    pub metric_type: Option<MetricType>,

    /// Filter by attributes/labels
    #[serde(default)]
    pub attributes: HashMap<String, String>,

    /// Aggregation function for time series
    pub aggregation: Option<MetricAggregation>,

    /// Time bucket size for aggregation (in seconds)
    pub bucket_secs: Option<u64>,

    /// Maximum number of results
    #[serde(default = "default_limit")]
    pub limit: usize,
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
    /// Service name
    pub name: String,

    /// Total span count in the time range
    pub span_count: u64,

    /// Number of unique operations
    pub operation_count: u64,

    /// Error rate (0.0 - 1.0)
    pub error_rate: f64,

    /// Average duration in nanoseconds
    pub avg_duration_ns: i64,

    /// P99 duration in nanoseconds
    pub p99_duration_ns: Option<i64>,

    /// First span time in the range
    pub first_seen: DateTime<Utc>,

    /// Last span time in the range
    pub last_seen: DateTime<Utc>,
}

/// Health metrics for a service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceHealth {
    /// Service name
    pub service: String,

    /// Request rate (spans per second)
    pub request_rate: f64,

    /// Error rate (0.0 - 1.0)
    pub error_rate: f64,

    /// Average latency in milliseconds
    pub avg_latency_ms: f64,

    /// P50 latency in milliseconds
    pub p50_latency_ms: f64,

    /// P95 latency in milliseconds
    pub p95_latency_ms: f64,

    /// P99 latency in milliseconds
    pub p99_latency_ms: f64,

    /// Throughput (successful requests per second)
    pub throughput: f64,

    /// Apdex score (0.0 - 1.0) with default threshold
    pub apdex: Option<f64>,
}

/// Comparison between two time periods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Comparison {
    /// Period A metrics
    pub period_a: PeriodMetrics,

    /// Period B metrics
    pub period_b: PeriodMetrics,

    /// Change in error rate (positive = increased)
    pub error_rate_delta: f64,

    /// Change in average latency (positive = slower)
    pub latency_delta_ms: f64,

    /// Change in throughput (positive = increased)
    pub throughput_delta: f64,

    /// Change in request count (positive = increased)
    pub request_count_delta: i64,
}

/// Metrics for a single period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodMetrics {
    /// Time range for this period
    pub time_range: TimeRange,

    /// Total request count
    pub request_count: u64,

    /// Error count
    pub error_count: u64,

    /// Error rate (0.0 - 1.0)
    pub error_rate: f64,

    /// Average latency in milliseconds
    pub avg_latency_ms: f64,

    /// Throughput (requests per second)
    pub throughput: f64,
}

/// Schema information for discovery
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Schema {
    /// Available tables/collections
    pub tables: Vec<TableSchema>,

    /// Available attribute keys for spans
    pub span_attribute_keys: Vec<String>,

    /// Available resource attribute keys
    pub resource_attribute_keys: Vec<String>,

    /// Available log attribute keys
    pub log_attribute_keys: Vec<String>,

    /// Available metric names
    pub metric_names: Vec<String>,
}

/// Schema for a single table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name
    pub name: String,

    /// Signal type this table contains
    pub signal: Signal,

    /// Column definitions
    pub columns: Vec<ColumnSchema>,
}

/// Schema for a single column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,

    /// Column data type
    pub data_type: String,

    /// Whether column is nullable
    pub nullable: bool,

    /// Description of the column
    pub description: Option<String>,
}

/// Column mapping for external backends (ClickHouse, etc.)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnMapping {
    /// Table name for spans
    #[serde(default = "default_spans_table")]
    pub spans_table: String,

    /// Column name for trace_id
    #[serde(default = "default_trace_id")]
    pub trace_id: String,

    /// Column name for span_id
    #[serde(default = "default_span_id")]
    pub span_id: String,

    /// Column name for parent_span_id
    #[serde(default = "default_parent_span_id")]
    pub parent_span_id: String,

    /// Column name for service_name
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// Column name for operation/span_name
    #[serde(default = "default_operation")]
    pub operation: String,

    /// Column name for start_time
    #[serde(default = "default_start_time")]
    pub start_time: String,

    /// Column name for duration
    #[serde(default = "default_duration")]
    pub duration: String,

    /// Column name for status
    #[serde(default = "default_status")]
    pub status: String,

    /// Column name for attributes (Map type)
    #[serde(default = "default_attributes")]
    pub attributes: String,

    /// Column name for resource attributes
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

/// Field mapping for Splunk
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FieldMapping {
    /// Index to search
    #[serde(default = "default_splunk_index")]
    pub index: String,

    /// Source type for traces
    #[serde(default = "default_splunk_sourcetype")]
    pub sourcetype: String,

    /// Field name for trace_id
    #[serde(default = "default_trace_id_field")]
    pub trace_id: String,

    /// Field name for span_id
    #[serde(default = "default_span_id_field")]
    pub span_id: String,

    /// Field name for parent_span_id
    #[serde(default = "default_parent_span_id_field")]
    pub parent_span_id: String,

    /// Field name for service
    #[serde(default = "default_service_field")]
    pub service: String,

    /// Field name for operation
    #[serde(default = "default_operation_field")]
    pub operation: String,

    /// Field name for duration
    #[serde(default = "default_duration_field")]
    pub duration: String,

    /// Field name for status
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
