use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::IngestConfig;
use crate::db::WriteDb;
use crate::schema::{SpanRow, SpanStatus};

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
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let request = ExportTraceServiceRequest::decode(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid OTLP: {}", e)))?;

    let rows = parse_otlp_request(&request, &state.config, &state.write_db)?;

    let count = rows.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting spans");

        state
            .write_db
            .insert_spans(&rows)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    let mut buf = Vec::new();
    ExportTraceServiceResponse {
        partial_success: None,
    }
    .encode(&mut buf)
    .ok();

    Ok((
        StatusCode::OK,
        [("content-type", "application/x-protobuf")],
        buf,
    )
        .into_response())
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
