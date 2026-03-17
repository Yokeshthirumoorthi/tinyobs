use axum::{
    body::Bytes,
    extract::State,
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
use std::sync::Arc;

use crate::config::IngestConfig;
use crate::db::WriteDb;
use crate::schema::{LogRow, MetricKind, MetricRow, SpanRow, SpanStatus};

/// OTLP wire format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpFormat {
    Protobuf,
    Json,
}

/// Shared state for the ingest handler
#[derive(Clone)]
pub struct IngestState {
    pub write_db: WriteDb,
    pub config: Arc<IngestConfig>,
}

/// Health check endpoint
pub async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}

/// OTLP trace ingestion endpoint (POST /v1/traces)
pub async fn receive_traces(
    State(state): State<IngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = detect_format(&headers);

    let request = match format {
        OtlpFormat::Protobuf => decode_protobuf(&body)?,
        OtlpFormat::Json => decode_json(&body)?,
    };

    let rows = parse_otlp_request(&request, &state.config, &state.write_db)?;

    let count = rows.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting spans");

        state
            .write_db
            .insert_spans(&rows)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(encode_response(format))
}

/// OTLP logs ingestion endpoint (POST /v1/logs)
pub async fn receive_logs(
    State(state): State<IngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = detect_format(&headers);

    let request = match format {
        OtlpFormat::Protobuf => decode_logs_protobuf(&body)?,
        OtlpFormat::Json => decode_logs_json(&body)?,
    };

    let rows = parse_otlp_logs_request(&request)?;

    let count = rows.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting logs");

        state
            .write_db
            .insert_logs(&rows)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(encode_logs_response(format))
}

/// OTLP metrics ingestion endpoint (POST /v1/metrics)
pub async fn receive_metrics(
    State(state): State<IngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = detect_format(&headers);

    let request = match format {
        OtlpFormat::Protobuf => decode_metrics_protobuf(&body)?,
        OtlpFormat::Json => decode_metrics_json(&body)?,
    };

    let rows = parse_otlp_metrics_request(&request)?;

    let count = rows.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting metrics");

        state
            .write_db
            .insert_metrics(&rows)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(encode_metrics_response(format))
}

/// Detect OTLP format from Content-Type header
fn detect_format(headers: &HeaderMap) -> OtlpFormat {
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

/// Decode protobuf-encoded OTLP request
fn decode_protobuf(body: &Bytes) -> Result<ExportTraceServiceRequest, (StatusCode, String)> {
    ExportTraceServiceRequest::decode(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP protobuf: {}", e)))
}

/// Decode JSON-encoded OTLP request
fn decode_json(body: &Bytes) -> Result<ExportTraceServiceRequest, (StatusCode, String)> {
    serde_json::from_slice(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP JSON: {}", e)))
}

/// Decode protobuf-encoded OTLP logs request
fn decode_logs_protobuf(body: &Bytes) -> Result<ExportLogsServiceRequest, (StatusCode, String)> {
    ExportLogsServiceRequest::decode(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP logs protobuf: {}", e)))
}

/// Decode JSON-encoded OTLP logs request
fn decode_logs_json(body: &Bytes) -> Result<ExportLogsServiceRequest, (StatusCode, String)> {
    serde_json::from_slice(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP logs JSON: {}", e)))
}

/// Decode protobuf-encoded OTLP metrics request
fn decode_metrics_protobuf(
    body: &Bytes,
) -> Result<ExportMetricsServiceRequest, (StatusCode, String)> {
    ExportMetricsServiceRequest::decode(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP metrics protobuf: {}", e)))
}

/// Decode JSON-encoded OTLP metrics request
fn decode_metrics_json(body: &Bytes) -> Result<ExportMetricsServiceRequest, (StatusCode, String)> {
    serde_json::from_slice(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP metrics JSON: {}", e)))
}

/// Encode response in the same format as the request
fn encode_response(format: OtlpFormat) -> Response {
    let response = ExportTraceServiceResponse {
        partial_success: None,
    };

    match format {
        OtlpFormat::Protobuf => {
            let mut buf = Vec::new();
            response.encode(&mut buf).ok();
            (
                StatusCode::OK,
                [(CONTENT_TYPE, "application/x-protobuf")],
                buf,
            )
                .into_response()
        }
        OtlpFormat::Json => {
            let json = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
            (StatusCode::OK, [(CONTENT_TYPE, "application/json")], json).into_response()
        }
    }
}

/// Encode logs response in the same format as the request
fn encode_logs_response(format: OtlpFormat) -> Response {
    let response = ExportLogsServiceResponse {
        partial_success: None,
    };

    match format {
        OtlpFormat::Protobuf => {
            let mut buf = Vec::new();
            response.encode(&mut buf).ok();
            (
                StatusCode::OK,
                [(CONTENT_TYPE, "application/x-protobuf")],
                buf,
            )
                .into_response()
        }
        OtlpFormat::Json => {
            let json = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
            (StatusCode::OK, [(CONTENT_TYPE, "application/json")], json).into_response()
        }
    }
}

/// Encode metrics response in the same format as the request
fn encode_metrics_response(format: OtlpFormat) -> Response {
    let response = ExportMetricsServiceResponse {
        partial_success: None,
    };

    match format {
        OtlpFormat::Protobuf => {
            let mut buf = Vec::new();
            response.encode(&mut buf).ok();
            (
                StatusCode::OK,
                [(CONTENT_TYPE, "application/x-protobuf")],
                buf,
            )
                .into_response()
        }
        OtlpFormat::Json => {
            let json = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
            (StatusCode::OK, [(CONTENT_TYPE, "application/json")], json).into_response()
        }
    }
}

/// Parse OTLP request into SpanRows
fn parse_otlp_request(
    request: &ExportTraceServiceRequest,
    config: &IngestConfig,
    write_db: &WriteDb,
) -> Result<Vec<SpanRow>, (StatusCode, String)> {
    // First pass: build trace_id -> session_id map from spans that have session attribute
    let mut trace_to_session: HashMap<String, String> = HashMap::new();
    let mut unknown_trace_ids: Vec<String> = Vec::new();

    for rs in &request.resource_spans {
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let trace_id = hex::encode(&span.trace_id);
                if let Some(session_id) = span
                    .attributes
                    .iter()
                    .find(|a| a.key == config.session_attribute)
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

    // Look up session_ids for unknown trace_ids from SQLite
    if !unknown_trace_ids.is_empty() {
        unknown_trace_ids.dedup();
        if let Ok(mappings) = write_db.lookup_session_ids(&unknown_trace_ids) {
            trace_to_session.extend(mappings);
        }
    }

    // Second pass: create rows
    let mut rows = Vec::new();
    let created_at = chrono::Utc::now().timestamp();

    for rs in &request.resource_spans {
        // Extract service.name from resource attributes
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
            .map(|r| attrs_to_json(&r.attributes))
            .unwrap_or_else(|| "{}".to_string());

        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let trace_id = hex::encode(&span.trace_id);

                // Get session_id from span attribute or from trace mapping
                let session_id = span
                    .attributes
                    .iter()
                    .find(|a| a.key == config.session_attribute)
                    .and_then(|a| a.value.as_ref())
                    .map(val_to_str)
                    .or_else(|| trace_to_session.get(&trace_id).cloned());

                // Convert status
                let status: i32 = span
                    .status
                    .as_ref()
                    .map(|s| s.code as i32)
                    .unwrap_or(SpanStatus::Unset as i32);

                rows.push(SpanRow {
                    trace_id,
                    span_id: hex::encode(&span.span_id),
                    parent_span_id: (!span.parent_span_id.is_empty())
                        .then(|| hex::encode(&span.parent_span_id)),
                    session_id,
                    service_name: service_name.clone(),
                    operation: span.name.clone(),
                    start_time: (span.start_time_unix_nano / 1000) as i64, // nano to micro
                    duration_ns: (span.end_time_unix_nano - span.start_time_unix_nano) as i64,
                    status,
                    attributes: attrs_to_json(&span.attributes),
                    resource_attrs: resource_attrs.clone(),
                    created_at,
                });
            }
        }
    }

    Ok(rows)
}

fn val_to_str(v: &AnyValue) -> String {
    match v.value.as_ref() {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(Value::BoolValue(b)) => b.to_string(),
        _ => String::new(),
    }
}

fn attrs_to_json(attrs: &[KeyValue]) -> String {
    let pairs: Vec<(String, String)> = attrs
        .iter()
        .filter_map(|a| a.value.as_ref().map(|v| (a.key.clone(), val_to_str(v))))
        .collect();
    serde_json::to_string(&pairs).unwrap_or_else(|_| "[]".to_string())
}

/// Parse OTLP logs request into LogRows
fn parse_otlp_logs_request(
    request: &ExportLogsServiceRequest,
) -> Result<Vec<LogRow>, (StatusCode, String)> {
    let mut rows = Vec::new();
    let created_at = chrono::Utc::now().timestamp();

    for rl in &request.resource_logs {
        // Extract service.name from resource attributes
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
            .map(|r| attrs_to_json(&r.attributes))
            .unwrap_or_else(|| "{}".to_string());

        for sl in &rl.scope_logs {
            for log_record in &sl.log_records {
                // Extract body text
                let body = log_record
                    .body
                    .as_ref()
                    .map(val_to_str)
                    .unwrap_or_default();

                // Extract trace/span IDs for correlation
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

                // Get severity text - use provided or derive from number
                let severity_text = if !log_record.severity_text.is_empty() {
                    Some(log_record.severity_text.clone())
                } else if log_record.severity_number > 0 {
                    Some(severity_number_to_text(log_record.severity_number))
                } else {
                    None
                };

                rows.push(LogRow {
                    timestamp: (log_record.time_unix_nano / 1000) as i64, // nano to micro
                    observed_timestamp: if log_record.observed_time_unix_nano > 0 {
                        Some((log_record.observed_time_unix_nano / 1000) as i64)
                    } else {
                        None
                    },
                    trace_id,
                    span_id,
                    severity_number: log_record.severity_number,
                    severity_text,
                    body,
                    service_name: service_name.clone(),
                    attributes: attrs_to_json(&log_record.attributes),
                    resource_attrs: resource_attrs.clone(),
                    created_at,
                });
            }
        }
    }

    Ok(rows)
}

/// Convert severity number to text
fn severity_number_to_text(severity: i32) -> String {
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

/// Parse OTLP metrics request into MetricRows
fn parse_otlp_metrics_request(
    request: &ExportMetricsServiceRequest,
) -> Result<Vec<MetricRow>, (StatusCode, String)> {
    let mut rows = Vec::new();
    let created_at = chrono::Utc::now().timestamp();

    for rm in &request.resource_metrics {
        // Extract service.name from resource attributes
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
            .map(|r| attrs_to_json(&r.attributes))
            .unwrap_or_else(|| "{}".to_string());

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

                // Process based on metric data type
                if let Some(data) = &metric.data {
                    match data {
                        Data::Gauge(gauge) => {
                            for dp in &gauge.data_points {
                                rows.push(create_metric_row(
                                    &name,
                                    &description,
                                    &unit,
                                    MetricKind::Gauge,
                                    dp.time_unix_nano,
                                    &service_name,
                                    extract_number_value(dp),
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    &dp.attributes,
                                    &resource_attrs,
                                    created_at,
                                ));
                            }
                        }
                        Data::Sum(sum) => {
                            for dp in &sum.data_points {
                                rows.push(create_metric_row(
                                    &name,
                                    &description,
                                    &unit,
                                    MetricKind::Counter,
                                    dp.time_unix_nano,
                                    &service_name,
                                    extract_number_value(dp),
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    &dp.attributes,
                                    &resource_attrs,
                                    created_at,
                                ));
                            }
                        }
                        Data::Histogram(histogram) => {
                            for dp in &histogram.data_points {
                                // Build bucket pairs: (upper_bound, cumulative_count)
                                let buckets: Vec<(f64, u64)> = dp
                                    .explicit_bounds
                                    .iter()
                                    .zip(dp.bucket_counts.iter())
                                    .map(|(&bound, &count)| (bound, count))
                                    .collect();

                                rows.push(create_metric_row(
                                    &name,
                                    &description,
                                    &unit,
                                    MetricKind::Histogram,
                                    dp.time_unix_nano,
                                    &service_name,
                                    None,
                                    dp.sum,
                                    Some(dp.count),
                                    dp.min,
                                    dp.max,
                                    None,
                                    Some(buckets),
                                    &dp.attributes,
                                    &resource_attrs,
                                    created_at,
                                ));
                            }
                        }
                        Data::Summary(summary) => {
                            for dp in &summary.data_points {
                                // Build quantile pairs: (quantile, value)
                                let quantiles: Vec<(f64, f64)> = dp
                                    .quantile_values
                                    .iter()
                                    .map(|qv| (qv.quantile, qv.value))
                                    .collect();

                                rows.push(create_metric_row(
                                    &name,
                                    &description,
                                    &unit,
                                    MetricKind::Summary,
                                    dp.time_unix_nano,
                                    &service_name,
                                    None,
                                    Some(dp.sum),
                                    Some(dp.count),
                                    None,
                                    None,
                                    Some(quantiles),
                                    None,
                                    &dp.attributes,
                                    &resource_attrs,
                                    created_at,
                                ));
                            }
                        }
                        Data::ExponentialHistogram(_) => {
                            // Exponential histograms are more complex; convert to regular histogram format
                            // For now, skip these (can be added later if needed)
                        }
                    }
                }
            }
        }
    }

    Ok(rows)
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

#[allow(clippy::too_many_arguments)]
fn create_metric_row(
    name: &str,
    description: &Option<String>,
    unit: &Option<String>,
    kind: MetricKind,
    time_unix_nano: u64,
    service_name: &str,
    value: Option<f64>,
    sum: Option<f64>,
    count: Option<u64>,
    min: Option<f64>,
    max: Option<f64>,
    quantiles: Option<Vec<(f64, f64)>>,
    buckets: Option<Vec<(f64, u64)>>,
    attributes: &[KeyValue],
    resource_attrs: &str,
    created_at: i64,
) -> MetricRow {
    MetricRow {
        name: name.to_string(),
        description: description.clone(),
        unit: unit.clone(),
        kind: format!("{:?}", kind).to_lowercase(),
        timestamp: (time_unix_nano / 1000) as i64, // nano to micro
        service_name: service_name.to_string(),
        value,
        sum,
        count: count.map(|c| c as i64),
        min,
        max,
        quantiles: quantiles.map(|q| serde_json::to_string(&q).unwrap_or_default()),
        buckets: buckets.map(|b| serde_json::to_string(&b).unwrap_or_default()),
        attributes: attrs_to_json(attributes),
        resource_attrs: resource_attrs.to_string(),
        created_at,
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
