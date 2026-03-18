//! OTLP ingest handlers for tinyobs-pro

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

use crate::clickhouse::TinyObsDB;
use crate::config::IngestConfig;
use tinyobs_core::backend::IngestBackend;
use tinyobs_core::ingest as core_ingest;

/// Shared state for the ingest handler
#[derive(Clone)]
pub struct IngestState {
    pub backend: TinyObsDB,
    pub config: Arc<IngestConfig>,
}

/// Health check endpoint
pub async fn health_check(State(state): State<IngestState>) -> impl IntoResponse {
    match state.backend.ping().await {
        Ok(true) => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// OTLP trace ingestion endpoint (POST /v1/traces)
pub async fn receive_traces(
    State(state): State<IngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = core_ingest::detect_format(&headers);
    let request = core_ingest::decode_trace_request(format, body.as_ref())?;

    // Extract session map — no external lookup for ClickHouse (session comes from attributes only)
    let (session_map, _) =
        core_ingest::extract_session_map(&request, &state.config.session_attribute);

    let spans = core_ingest::parse_spans(&request, &state.config.session_attribute, &session_map);

    let count = spans.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting spans to ClickHouse");
        state
            .backend
            .ingest_spans(spans)
            .await
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
        tracing::debug!(count, "Ingesting logs to ClickHouse");
        state
            .backend
            .ingest_logs(logs)
            .await
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
        tracing::debug!(count, "Ingesting metrics to ClickHouse");
        state
            .backend
            .ingest_metrics(metrics)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(core_ingest::encode_metrics_response(format))
}
