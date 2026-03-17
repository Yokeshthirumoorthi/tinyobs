use serde::{Deserialize, Serialize};

/// Span status codes (following OpenTelemetry conventions)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[repr(i32)]
pub enum SpanStatus {
    #[default]
    Unset = 0,
    Ok = 1,
    Error = 2,
}

impl From<i32> for SpanStatus {
    fn from(value: i32) -> Self {
        match value {
            1 => SpanStatus::Ok,
            2 => SpanStatus::Error,
            _ => SpanStatus::Unset,
        }
    }
}

impl From<SpanStatus> for i32 {
    fn from(status: SpanStatus) -> Self {
        status as i32
    }
}

/// A span representing a unit of work
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    /// Trace ID (hex-encoded)
    pub trace_id: String,

    /// Span ID (hex-encoded)
    pub span_id: String,

    /// Parent span ID (hex-encoded, None for root spans)
    pub parent_span_id: Option<String>,

    /// Session ID (extracted from attributes)
    pub session_id: Option<String>,

    /// Service name (from resource attributes)
    pub service_name: String,

    /// Operation name
    pub operation: String,

    /// Start time in microseconds since Unix epoch
    pub start_time: i64,

    /// Duration in nanoseconds
    pub duration_ns: i64,

    /// Status code
    pub status: SpanStatus,

    /// Span attributes as key-value pairs
    pub attributes: Vec<(String, String)>,

    /// Resource attributes as key-value pairs
    pub resource_attrs: Vec<(String, String)>,
}

/// A span row as stored in SQLite/Parquet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanRow {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub session_id: Option<String>,
    pub service_name: String,
    pub operation: String,
    pub start_time: i64,
    pub duration_ns: i64,
    pub status: i32,
    pub attributes: String,      // JSON
    pub resource_attrs: String,  // JSON
    pub created_at: i64,
}

impl Span {
    /// Convert to a storage row
    pub fn to_row(&self) -> SpanRow {
        SpanRow {
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            parent_span_id: self.parent_span_id.clone(),
            session_id: self.session_id.clone(),
            service_name: self.service_name.clone(),
            operation: self.operation.clone(),
            start_time: self.start_time,
            duration_ns: self.duration_ns,
            status: self.status.into(),
            attributes: serde_json::to_string(&self.attributes).unwrap_or_default(),
            resource_attrs: serde_json::to_string(&self.resource_attrs).unwrap_or_default(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    /// Get an attribute value by key
    pub fn get_attr(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Get a resource attribute value by key
    pub fn get_resource_attr(&self, key: &str) -> Option<&str> {
        self.resource_attrs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

impl SpanRow {
    /// Parse attributes from JSON
    pub fn parse_attributes(&self) -> Vec<(String, String)> {
        serde_json::from_str(&self.attributes).unwrap_or_default()
    }

    /// Parse resource attributes from JSON
    pub fn parse_resource_attrs(&self) -> Vec<(String, String)> {
        serde_json::from_str(&self.resource_attrs).unwrap_or_default()
    }

    /// Convert to a Span
    pub fn to_span(&self) -> Span {
        Span {
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            parent_span_id: self.parent_span_id.clone(),
            session_id: self.session_id.clone(),
            service_name: self.service_name.clone(),
            operation: self.operation.clone(),
            start_time: self.start_time,
            duration_ns: self.duration_ns,
            status: SpanStatus::from(self.status),
            attributes: self.parse_attributes(),
            resource_attrs: self.parse_resource_attrs(),
        }
    }
}

// ============================================================================
// Log Records
// ============================================================================

/// Severity level for log records (OpenTelemetry spec)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[repr(i32)]
pub enum SeverityLevel {
    #[default]
    Unspecified = 0,
    Trace = 1,
    Trace2 = 2,
    Trace3 = 3,
    Trace4 = 4,
    Debug = 5,
    Debug2 = 6,
    Debug3 = 7,
    Debug4 = 8,
    Info = 9,
    Info2 = 10,
    Info3 = 11,
    Info4 = 12,
    Warn = 13,
    Warn2 = 14,
    Warn3 = 15,
    Warn4 = 16,
    Error = 17,
    Error2 = 18,
    Error3 = 19,
    Error4 = 20,
    Fatal = 21,
    Fatal2 = 22,
    Fatal3 = 23,
    Fatal4 = 24,
}

impl From<i32> for SeverityLevel {
    fn from(value: i32) -> Self {
        match value {
            1 => SeverityLevel::Trace,
            2 => SeverityLevel::Trace2,
            3 => SeverityLevel::Trace3,
            4 => SeverityLevel::Trace4,
            5 => SeverityLevel::Debug,
            6 => SeverityLevel::Debug2,
            7 => SeverityLevel::Debug3,
            8 => SeverityLevel::Debug4,
            9 => SeverityLevel::Info,
            10 => SeverityLevel::Info2,
            11 => SeverityLevel::Info3,
            12 => SeverityLevel::Info4,
            13 => SeverityLevel::Warn,
            14 => SeverityLevel::Warn2,
            15 => SeverityLevel::Warn3,
            16 => SeverityLevel::Warn4,
            17 => SeverityLevel::Error,
            18 => SeverityLevel::Error2,
            19 => SeverityLevel::Error3,
            20 => SeverityLevel::Error4,
            21 => SeverityLevel::Fatal,
            22 => SeverityLevel::Fatal2,
            23 => SeverityLevel::Fatal3,
            24 => SeverityLevel::Fatal4,
            _ => SeverityLevel::Unspecified,
        }
    }
}

impl SeverityLevel {
    /// Get the severity text (e.g., "ERROR", "INFO")
    pub fn as_str(&self) -> &'static str {
        match self {
            SeverityLevel::Unspecified => "UNSPECIFIED",
            SeverityLevel::Trace | SeverityLevel::Trace2 | SeverityLevel::Trace3 | SeverityLevel::Trace4 => "TRACE",
            SeverityLevel::Debug | SeverityLevel::Debug2 | SeverityLevel::Debug3 | SeverityLevel::Debug4 => "DEBUG",
            SeverityLevel::Info | SeverityLevel::Info2 | SeverityLevel::Info3 | SeverityLevel::Info4 => "INFO",
            SeverityLevel::Warn | SeverityLevel::Warn2 | SeverityLevel::Warn3 | SeverityLevel::Warn4 => "WARN",
            SeverityLevel::Error | SeverityLevel::Error2 | SeverityLevel::Error3 | SeverityLevel::Error4 => "ERROR",
            SeverityLevel::Fatal | SeverityLevel::Fatal2 | SeverityLevel::Fatal3 | SeverityLevel::Fatal4 => "FATAL",
        }
    }
}

/// A log record representing a single log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Timestamp when the log was emitted (microseconds since epoch)
    pub timestamp: i64,

    /// Observed timestamp (when the log was collected)
    pub observed_timestamp: Option<i64>,

    /// Trace ID for correlation (hex-encoded)
    pub trace_id: Option<String>,

    /// Span ID for correlation (hex-encoded)
    pub span_id: Option<String>,

    /// Severity number (1-24)
    pub severity_number: SeverityLevel,

    /// Severity text (e.g., "ERROR", "INFO")
    pub severity_text: Option<String>,

    /// Log body/message
    pub body: String,

    /// Service name (from resource attributes)
    pub service_name: String,

    /// Log attributes as key-value pairs
    pub attributes: Vec<(String, String)>,

    /// Resource attributes as key-value pairs
    pub resource_attrs: Vec<(String, String)>,
}

/// A log row as stored in SQLite/Parquet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRow {
    pub timestamp: i64,
    pub observed_timestamp: Option<i64>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub severity_number: i32,
    pub severity_text: Option<String>,
    pub body: String,
    pub service_name: String,
    pub attributes: String,      // JSON
    pub resource_attrs: String,  // JSON
    pub created_at: i64,
}

impl LogRecord {
    /// Convert to a storage row
    pub fn to_row(&self) -> LogRow {
        LogRow {
            timestamp: self.timestamp,
            observed_timestamp: self.observed_timestamp,
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            severity_number: self.severity_number as i32,
            severity_text: self.severity_text.clone(),
            body: self.body.clone(),
            service_name: self.service_name.clone(),
            attributes: serde_json::to_string(&self.attributes).unwrap_or_default(),
            resource_attrs: serde_json::to_string(&self.resource_attrs).unwrap_or_default(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    /// Get an attribute value by key
    pub fn get_attr(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

impl LogRow {
    /// Convert to a LogRecord
    pub fn to_log(&self) -> LogRecord {
        LogRecord {
            timestamp: self.timestamp,
            observed_timestamp: self.observed_timestamp,
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            severity_number: SeverityLevel::from(self.severity_number),
            severity_text: self.severity_text.clone(),
            body: self.body.clone(),
            service_name: self.service_name.clone(),
            attributes: serde_json::from_str(&self.attributes).unwrap_or_default(),
            resource_attrs: serde_json::from_str(&self.resource_attrs).unwrap_or_default(),
        }
    }
}

// ============================================================================
// Metrics
// ============================================================================

/// Metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    #[default]
    Gauge,
    Counter,
    Histogram,
    Summary,
}

/// A metric data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    /// Metric name
    pub name: String,

    /// Metric description
    pub description: Option<String>,

    /// Metric unit
    pub unit: Option<String>,

    /// Metric type
    pub kind: MetricKind,

    /// Timestamp (microseconds since epoch)
    pub timestamp: i64,

    /// Service name (from resource attributes)
    pub service_name: String,

    /// Metric value (for gauge/counter)
    pub value: Option<f64>,

    /// Sum value (for histogram/summary)
    pub sum: Option<f64>,

    /// Count value (for histogram/summary)
    pub count: Option<u64>,

    /// Min value (for histogram)
    pub min: Option<f64>,

    /// Max value (for histogram)
    pub max: Option<f64>,

    /// Quantile values (for summary) as (quantile, value) pairs
    pub quantiles: Option<Vec<(f64, f64)>>,

    /// Bucket counts (for histogram) as (upper_bound, count) pairs
    pub buckets: Option<Vec<(f64, u64)>>,

    /// Metric attributes/labels as key-value pairs
    pub attributes: Vec<(String, String)>,

    /// Resource attributes as key-value pairs
    pub resource_attrs: Vec<(String, String)>,
}

/// A metric row as stored in SQLite/Parquet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricRow {
    pub name: String,
    pub description: Option<String>,
    pub unit: Option<String>,
    pub kind: String,
    pub timestamp: i64,
    pub service_name: String,
    pub value: Option<f64>,
    pub sum: Option<f64>,
    pub count: Option<i64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub quantiles: Option<String>,  // JSON
    pub buckets: Option<String>,    // JSON
    pub attributes: String,         // JSON
    pub resource_attrs: String,     // JSON
    pub created_at: i64,
}

impl Metric {
    /// Convert to a storage row
    pub fn to_row(&self) -> MetricRow {
        MetricRow {
            name: self.name.clone(),
            description: self.description.clone(),
            unit: self.unit.clone(),
            kind: format!("{:?}", self.kind).to_lowercase(),
            timestamp: self.timestamp,
            service_name: self.service_name.clone(),
            value: self.value,
            sum: self.sum,
            count: self.count.map(|c| c as i64),
            min: self.min,
            max: self.max,
            quantiles: self.quantiles.as_ref().map(|q| serde_json::to_string(q).unwrap_or_default()),
            buckets: self.buckets.as_ref().map(|b| serde_json::to_string(b).unwrap_or_default()),
            attributes: serde_json::to_string(&self.attributes).unwrap_or_default(),
            resource_attrs: serde_json::to_string(&self.resource_attrs).unwrap_or_default(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    /// Get an attribute value by key
    pub fn get_attr(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

impl MetricRow {
    /// Convert to a Metric
    pub fn to_metric(&self) -> Metric {
        let kind = match self.kind.as_str() {
            "counter" => MetricKind::Counter,
            "histogram" => MetricKind::Histogram,
            "summary" => MetricKind::Summary,
            _ => MetricKind::Gauge,
        };

        Metric {
            name: self.name.clone(),
            description: self.description.clone(),
            unit: self.unit.clone(),
            kind,
            timestamp: self.timestamp,
            service_name: self.service_name.clone(),
            value: self.value,
            sum: self.sum,
            count: self.count.map(|c| c as u64),
            min: self.min,
            max: self.max,
            quantiles: self.quantiles.as_ref().and_then(|q| serde_json::from_str(q).ok()),
            buckets: self.buckets.as_ref().and_then(|b| serde_json::from_str(b).ok()),
            attributes: serde_json::from_str(&self.attributes).unwrap_or_default(),
            resource_attrs: serde_json::from_str(&self.resource_attrs).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_to_row() {
        let span = Span {
            trace_id: "abc123".to_string(),
            span_id: "def456".to_string(),
            parent_span_id: None,
            session_id: Some("session1".to_string()),
            service_name: "test-service".to_string(),
            operation: "test-op".to_string(),
            start_time: 1234567890,
            duration_ns: 1000000,
            status: SpanStatus::Ok,
            attributes: vec![("key".to_string(), "value".to_string())],
            resource_attrs: vec![("service.name".to_string(), "test".to_string())],
        };

        let row = span.to_row();
        assert_eq!(row.trace_id, "abc123");
        assert_eq!(row.service_name, "test-service");
        assert_eq!(row.status, 1);
    }

    #[test]
    fn test_span_status_conversion() {
        assert_eq!(SpanStatus::from(0), SpanStatus::Unset);
        assert_eq!(SpanStatus::from(1), SpanStatus::Ok);
        assert_eq!(SpanStatus::from(2), SpanStatus::Error);
        assert_eq!(SpanStatus::from(99), SpanStatus::Unset);
    }
}
