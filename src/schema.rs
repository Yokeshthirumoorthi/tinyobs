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
