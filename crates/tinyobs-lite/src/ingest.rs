use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

use crate::config::IngestConfig;
use crate::db::WriteDb;
use tinyobs_core::ingest as core_ingest;

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
    let format = core_ingest::detect_format(&headers);
    let request = core_ingest::decode_trace_request(format, body.as_ref())?;

    // Extract session map (first pass) and resolve unknowns from SQLite
    let (mut session_map, unknown_ids) =
        core_ingest::extract_session_map(&request, &state.config.session_attribute);

    if !unknown_ids.is_empty() {
        if let Ok(mappings) = state.write_db.lookup_session_ids(&unknown_ids) {
            session_map.extend(mappings);
        }
    }

    // Parse spans using core
    let spans = core_ingest::parse_spans(&request, &state.config.session_attribute, &session_map);

    let count = spans.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting spans");
        let rows: Vec<_> = spans.iter().map(|s| s.to_row()).collect();
        state
            .write_db
            .insert_spans(&rows)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(core_ingest::encode_trace_response(format))
}

/// OTLP logs ingestion endpoint (POST /v1/logs)
pub async fn receive_logs(
    State(state): State<IngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = core_ingest::detect_format(&headers);
    let request = core_ingest::decode_logs_request(format, body.as_ref())?;
    let logs = core_ingest::parse_logs(&request);

    let count = logs.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting logs");
        let rows: Vec<_> = logs.iter().map(|l| l.to_row()).collect();
        state
            .write_db
            .insert_logs(&rows)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(core_ingest::encode_logs_response(format))
}

/// OTLP metrics ingestion endpoint (POST /v1/metrics)
pub async fn receive_metrics(
    State(state): State<IngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = core_ingest::detect_format(&headers);
    let request = core_ingest::decode_metrics_request(format, body.as_ref())?;
    let metrics = core_ingest::parse_metrics(&request);

    let count = metrics.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting metrics");
        let rows: Vec<_> = metrics.iter().map(|m| m.to_row()).collect();
        state
            .write_db
            .insert_metrics(&rows)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(core_ingest::encode_metrics_response(format))
}
