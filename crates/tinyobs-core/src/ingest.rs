//! OTLP protocol parsing shared between tinyobs-lite and tinyobs-pro
//!
//! Provides decoding, format detection, response encoding, and proto-to-schema
//! conversion functions. Axum handler wrappers are in each crate's own ingest module.

use axum::{
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use prost::Message;
use std::collections::HashMap;

use crate::schema::{LogRecord, Metric, MetricKind, Span, SpanStatus, SeverityLevel};

// ============================================================================
// Format detection
// ============================================================================

/// OTLP wire format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpFormat {
    Protobuf,
    Json,
}

/// Detect OTLP format from Content-Type header
pub fn detect_format(headers: &HeaderMap) -> OtlpFormat {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| {
            if ct.starts_with("application/json") {
                OtlpFormat::Json
            } else {
                OtlpFormat::Protobuf
            }
        })
        .unwrap_or(OtlpFormat::Protobuf)
}

// ============================================================================
// Decode functions
// ============================================================================

pub fn decode_trace_request(
    format: OtlpFormat,
    body: &[u8],
) -> Result<ExportTraceServiceRequest, (StatusCode, String)> {
    match format {
        OtlpFormat::Protobuf => ExportTraceServiceRequest::decode(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP protobuf: {}", e))),
        OtlpFormat::Json => serde_json::from_slice(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP JSON: {}", e))),
    }
}

pub fn decode_logs_request(
    format: OtlpFormat,
    body: &[u8],
) -> Result<ExportLogsServiceRequest, (StatusCode, String)> {
    match format {
        OtlpFormat::Protobuf => ExportLogsServiceRequest::decode(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP logs protobuf: {}", e))),
        OtlpFormat::Json => serde_json::from_slice(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP logs JSON: {}", e))),
    }
}

pub fn decode_metrics_request(
    format: OtlpFormat,
    body: &[u8],
) -> Result<ExportMetricsServiceRequest, (StatusCode, String)> {
    match format {
        OtlpFormat::Protobuf => ExportMetricsServiceRequest::decode(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP metrics protobuf: {}", e))),
        OtlpFormat::Json => serde_json::from_slice(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP metrics JSON: {}", e))),
    }
}

// ============================================================================
// Response encoding
// ============================================================================

pub fn encode_trace_response(format: OtlpFormat) -> Response {
    let response = ExportTraceServiceResponse {
        partial_success: None,
    };
    encode_proto_response(format, &response)
}

pub fn encode_logs_response(format: OtlpFormat) -> Response {
    let response = ExportLogsServiceResponse {
        partial_success: None,
    };
    encode_proto_response(format, &response)
}

pub fn encode_metrics_response(format: OtlpFormat) -> Response {
    let response = ExportMetricsServiceResponse {
        partial_success: None,
    };
    encode_proto_response(format, &response)
}

fn encode_proto_response<M: Message + serde::Serialize>(format: OtlpFormat, msg: &M) -> Response {
    match format {
        OtlpFormat::Protobuf => {
            let mut buf = Vec::new();
            msg.encode(&mut buf).ok();
            (
                StatusCode::OK,
                [(CONTENT_TYPE, "application/x-protobuf")],
                buf,
            )
                .into_response()
        }
        OtlpFormat::Json => {
            let json = serde_json::to_vec(msg).unwrap_or_else(|_| b"{}".to_vec());
            (StatusCode::OK, [(CONTENT_TYPE, "application/json")], json).into_response()
        }
    }
}

// ============================================================================
// Span parsing
// ============================================================================

/// Extract session_id mapping from a trace request (first pass).
/// Returns (trace_id -> session_id map, list of trace_ids with unknown sessions).
pub fn extract_session_map(
    request: &ExportTraceServiceRequest,
    session_attribute: &str,
) -> (HashMap<String, String>, Vec<String>) {
    let mut trace_to_session: HashMap<String, String> = HashMap::new();
    let mut unknown_trace_ids: Vec<String> = Vec::new();

    for rs in &request.resource_spans {
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let trace_id = hex::encode(&span.trace_id);
                if let Some(session_id) = span
                    .attributes
                    .iter()
                    .find(|a| a.key == session_attribute)
                    .and_then(|a| a.value.as_ref())
                    .map(val_to_str)
                {
                    trace_to_session.insert(trace_id, session_id);
                } else if !trace_to_session.contains_key(&trace_id) {
                    unknown_trace_ids.push(trace_id);
                }
            }
        }
    }

    unknown_trace_ids.dedup();
    (trace_to_session, unknown_trace_ids)
}

/// Parse OTLP trace request into Span domain objects.
/// `session_map` should include any externally-resolved session mappings.
pub fn parse_spans(
    request: &ExportTraceServiceRequest,
    session_attribute: &str,
    session_map: &HashMap<String, String>,
) -> Vec<Span> {
    let mut spans = Vec::new();

    for rs in &request.resource_spans {
        let service_name = rs
            .resource
            .as_ref()
            .and_then(|r| {
                r.attributes
                    .iter()
                    .find(|a| a.key == "service.name")
                    .and_then(|a| a.value.as_ref())
                    .map(val_to_str)
            })
            .unwrap_or_else(|| "unknown".to_string());

        let resource_attrs = rs
            .resource
            .as_ref()
            .map(|r| attrs_to_pairs(&r.attributes))
            .unwrap_or_default();

        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let trace_id = hex::encode(&span.trace_id);

                let session_id = span
                    .attributes
                    .iter()
                    .find(|a| a.key == session_attribute)
                    .and_then(|a| a.value.as_ref())
                    .map(val_to_str)
                    .or_else(|| session_map.get(&trace_id).cloned());

                let status = span
                    .status
                    .as_ref()
                    .map(|s| SpanStatus::from(s.code as i32))
                    .unwrap_or(SpanStatus::Unset);

                spans.push(Span {
                    trace_id,
                    span_id: hex::encode(&span.span_id),
                    parent_span_id: (!span.parent_span_id.is_empty())
                        .then(|| hex::encode(&span.parent_span_id)),
                    session_id,
                    service_name: service_name.clone(),
                    operation: span.name.clone(),
                    start_time: (span.start_time_unix_nano / 1000) as i64,
                    duration_ns: (span.end_time_unix_nano - span.start_time_unix_nano) as i64,
                    status,
                    attributes: attrs_to_pairs(&span.attributes),
                    resource_attrs: resource_attrs.clone(),
                });
            }
        }
    }

    spans
}

// ============================================================================
// Log parsing
// ============================================================================

/// Parse OTLP logs request into LogRecord domain objects.
pub fn parse_logs(request: &ExportLogsServiceRequest) -> Vec<LogRecord> {
    let mut logs = Vec::new();

    for rl in &request.resource_logs {
        let service_name = rl
            .resource
            .as_ref()
            .and_then(|r| {
                r.attributes
                    .iter()
                    .find(|a| a.key == "service.name")
                    .and_then(|a| a.value.as_ref())
                    .map(val_to_str)
            })
            .unwrap_or_else(|| "unknown".to_string());

        let resource_attrs = rl
            .resource
            .as_ref()
            .map(|r| attrs_to_pairs(&r.attributes))
            .unwrap_or_default();

        for sl in &rl.scope_logs {
            for log_record in &sl.log_records {
                let body = log_record
                    .body
                    .as_ref()
                    .map(val_to_str)
                    .unwrap_or_default();

                let trace_id = if !log_record.trace_id.is_empty() {
                    Some(hex::encode(&log_record.trace_id))
                } else {
                    None
                };

                let span_id = if !log_record.span_id.is_empty() {
                    Some(hex::encode(&log_record.span_id))
                } else {
                    None
                };

                let severity_text = if !log_record.severity_text.is_empty() {
                    Some(log_record.severity_text.clone())
                } else if log_record.severity_number > 0 {
                    Some(severity_number_to_text(log_record.severity_number))
                } else {
                    None
                };

                logs.push(LogRecord {
                    timestamp: (log_record.time_unix_nano / 1000) as i64,
                    observed_timestamp: if log_record.observed_time_unix_nano > 0 {
                        Some((log_record.observed_time_unix_nano / 1000) as i64)
                    } else {
                        None
                    },
                    trace_id,
                    span_id,
                    severity_number: SeverityLevel::from(log_record.severity_number),
                    severity_text,
                    body,
                    service_name: service_name.clone(),
                    attributes: attrs_to_pairs(&log_record.attributes),
                    resource_attrs: resource_attrs.clone(),
                });
            }
        }
    }

    logs
}

// ============================================================================
// Metric parsing
// ============================================================================

/// Parse OTLP metrics request into Metric domain objects.
pub fn parse_metrics(request: &ExportMetricsServiceRequest) -> Vec<Metric> {
    let mut metrics = Vec::new();

    for rm in &request.resource_metrics {
        let service_name = rm
            .resource
            .as_ref()
            .and_then(|r| {
                r.attributes
                    .iter()
                    .find(|a| a.key == "service.name")
                    .and_then(|a| a.value.as_ref())
                    .map(val_to_str)
            })
            .unwrap_or_else(|| "unknown".to_string());

        let resource_attrs = rm
            .resource
            .as_ref()
            .map(|r| attrs_to_pairs(&r.attributes))
            .unwrap_or_default();

        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                let name = metric.name.clone();
                let description = if metric.description.is_empty() {
                    None
                } else {
                    Some(metric.description.clone())
                };
                let unit = if metric.unit.is_empty() {
                    None
                } else {
                    Some(metric.unit.clone())
                };

                if let Some(data) = &metric.data {
                    match data {
                        Data::Gauge(gauge) => {
                            for dp in &gauge.data_points {
                                metrics.push(Metric {
                                    name: name.clone(),
                                    description: description.clone(),
                                    unit: unit.clone(),
                                    kind: MetricKind::Gauge,
                                    timestamp: (dp.time_unix_nano / 1000) as i64,
                                    service_name: service_name.clone(),
                                    value: extract_number_value(dp),
                                    sum: None,
                                    count: None,
                                    min: None,
                                    max: None,
                                    quantiles: None,
                                    buckets: None,
                                    attributes: attrs_to_pairs(&dp.attributes),
                                    resource_attrs: resource_attrs.clone(),
                                });
                            }
                        }
                        Data::Sum(sum) => {
                            for dp in &sum.data_points {
                                metrics.push(Metric {
                                    name: name.clone(),
                                    description: description.clone(),
                                    unit: unit.clone(),
                                    kind: MetricKind::Counter,
                                    timestamp: (dp.time_unix_nano / 1000) as i64,
                                    service_name: service_name.clone(),
                                    value: extract_number_value(dp),
                                    sum: None,
                                    count: None,
                                    min: None,
                                    max: None,
                                    quantiles: None,
                                    buckets: None,
                                    attributes: attrs_to_pairs(&dp.attributes),
                                    resource_attrs: resource_attrs.clone(),
                                });
                            }
                        }
                        Data::Histogram(histogram) => {
                            for dp in &histogram.data_points {
                                let buckets: Vec<(f64, u64)> = dp
                                    .explicit_bounds
                                    .iter()
                                    .zip(dp.bucket_counts.iter())
                                    .map(|(&bound, &count)| (bound, count))
                                    .collect();

                                metrics.push(Metric {
                                    name: name.clone(),
                                    description: description.clone(),
                                    unit: unit.clone(),
                                    kind: MetricKind::Histogram,
                                    timestamp: (dp.time_unix_nano / 1000) as i64,
                                    service_name: service_name.clone(),
                                    value: None,
                                    sum: dp.sum,
                                    count: Some(dp.count),
                                    min: dp.min,
                                    max: dp.max,
                                    quantiles: None,
                                    buckets: Some(buckets),
                                    attributes: attrs_to_pairs(&dp.attributes),
                                    resource_attrs: resource_attrs.clone(),
                                });
                            }
                        }
                        Data::Summary(summary) => {
                            for dp in &summary.data_points {
                                let quantiles: Vec<(f64, f64)> = dp
                                    .quantile_values
                                    .iter()
                                    .map(|qv| (qv.quantile, qv.value))
                                    .collect();

                                metrics.push(Metric {
                                    name: name.clone(),
                                    description: description.clone(),
                                    unit: unit.clone(),
                                    kind: MetricKind::Summary,
                                    timestamp: (dp.time_unix_nano / 1000) as i64,
                                    service_name: service_name.clone(),
                                    value: None,
                                    sum: Some(dp.sum),
                                    count: Some(dp.count),
                                    min: None,
                                    max: None,
                                    quantiles: Some(quantiles),
                                    buckets: None,
                                    attributes: attrs_to_pairs(&dp.attributes),
                                    resource_attrs: resource_attrs.clone(),
                                });
                            }
                        }
                        Data::ExponentialHistogram(_) => {
                            // Skip for now
                        }
                    }
                }
            }
        }
    }

    metrics
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert an OTLP AnyValue to a string
pub fn val_to_str(v: &AnyValue) -> String {
    match v.value.as_ref() {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(Value::BoolValue(b)) => b.to_string(),
        _ => String::new(),
    }
}

/// Convert OTLP KeyValue attributes to a JSON string
pub fn attrs_to_json(attrs: &[KeyValue]) -> String {
    let pairs: Vec<(String, String)> = attrs
        .iter()
        .filter_map(|a| a.value.as_ref().map(|v| (a.key.clone(), val_to_str(v))))
        .collect();
    serde_json::to_string(&pairs).unwrap_or_else(|_| "[]".to_string())
}

/// Convert OTLP KeyValue attributes to Vec of pairs
pub fn attrs_to_pairs(attrs: &[KeyValue]) -> Vec<(String, String)> {
    attrs
        .iter()
        .filter_map(|a| a.value.as_ref().map(|v| (a.key.clone(), val_to_str(v))))
        .collect()
}

/// Convert severity number to text
pub fn severity_number_to_text(severity: i32) -> String {
    match severity {
        1..=4 => "TRACE".to_string(),
        5..=8 => "DEBUG".to_string(),
        9..=12 => "INFO".to_string(),
        13..=16 => "WARN".to_string(),
        17..=20 => "ERROR".to_string(),
        21..=24 => "FATAL".to_string(),
        _ => "UNSPECIFIED".to_string(),
    }
}

fn extract_number_value(
    dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
) -> Option<f64> {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    match &dp.value {
        Some(Value::AsDouble(d)) => Some(*d),
        Some(Value::AsInt(i)) => Some(*i as f64),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_val_to_str() {
        let string_val = AnyValue {
            value: Some(Value::StringValue("test".to_string())),
        };
        assert_eq!(val_to_str(&string_val), "test");

        let int_val = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(val_to_str(&int_val), "42");

        let bool_val = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(val_to_str(&bool_val), "true");
    }
}
