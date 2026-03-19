//! Shared server scaffolding: ingest handlers, telemetry setup, Application builder

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tracing::Subscriber;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt, EnvFilter, Registry};

use crate::backend::{IngestBackend, ManagedBackend};
use crate::config::IngestConfig;
use crate::ingest as core_ingest;

// ============================================================================
// Ingest state and handlers (generic over any IngestBackend + ManagedBackend)
// ============================================================================

/// Shared state for the ingest handler
#[derive(Clone)]
pub struct IngestState<B: IngestBackend + ManagedBackend + Clone + 'static> {
    pub backend: B,
    pub config: Arc<IngestConfig>,
}

/// Health check endpoint
pub async fn health_check<B: IngestBackend + ManagedBackend + Clone + 'static>(
    State(state): State<IngestState<B>>,
) -> impl IntoResponse {
    match state.backend.health_check().await.unwrap_or(false) {
        true => StatusCode::OK,
        false => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// OTLP trace ingestion endpoint (POST /v1/traces)
pub async fn receive_traces<B: IngestBackend + ManagedBackend + Clone + 'static>(
    State(state): State<IngestState<B>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = core_ingest::detect_format(&headers);
    let request = core_ingest::decode_trace_request(format, body.as_ref())?;

    let (session_map, _) =
        core_ingest::extract_session_map(&request, &state.config.session_attribute);

    let spans = core_ingest::parse_spans(&request, &state.config.session_attribute, &session_map);

    let count = spans.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting spans");
        state
            .backend
            .ingest_spans(spans)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(core_ingest::encode_trace_response(format))
}

/// OTLP logs ingestion endpoint (POST /v1/logs)
pub async fn receive_logs<B: IngestBackend + ManagedBackend + Clone + 'static>(
    State(state): State<IngestState<B>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = core_ingest::detect_format(&headers);
    let request = core_ingest::decode_logs_request(format, body.as_ref())?;
    let logs = core_ingest::parse_logs(&request);

    let count = logs.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting logs");
        state
            .backend
            .ingest_logs(logs)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(core_ingest::encode_logs_response(format))
}

/// OTLP metrics ingestion endpoint (POST /v1/metrics)
pub async fn receive_metrics<B: IngestBackend + ManagedBackend + Clone + 'static>(
    State(state): State<IngestState<B>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let format = core_ingest::detect_format(&headers);
    let request = core_ingest::decode_metrics_request(format, body.as_ref())?;
    let metrics = core_ingest::parse_metrics(&request);

    let count = metrics.len();
    if count > 0 {
        tracing::debug!(count, "Ingesting metrics");
        state
            .backend
            .ingest_metrics(metrics)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(core_ingest::encode_metrics_response(format))
}

/// Build an OTLP ingest router for any backend
pub fn ingest_router<B: IngestBackend + ManagedBackend + Clone + 'static>(state: IngestState<B>) -> Router {
    Router::new()
        .route("/v1/traces", post(receive_traces::<B>))
        .route("/v1/logs", post(receive_logs::<B>))
        .route("/v1/metrics", post(receive_metrics::<B>))
        .route("/health", get(health_check::<B>))
        .with_state(state)
}

// ============================================================================
// Telemetry setup
// ============================================================================

pub fn get_subscriber<Sink>(
    name: &str,
    env_filter: &str,
    sink: Sink,
) -> impl Subscriber + Send + Sync
where
    Sink: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));
    let formatting_layer = BunyanFormattingLayer::new(name.into(), sink);

    Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer)
}

pub fn get_subscriber_pretty(name: &str, env_filter: &str) -> impl Subscriber + Send + Sync {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));
    let _ = name;

    Registry::default()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().pretty())
}

pub fn init_subscriber(subscriber: impl Subscriber + Send + Sync) {
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}

/// Initialize telemetry based on TINYOBS_ENV
pub fn init_telemetry(name: &str) {
    let env = std::env::var("TINYOBS_ENV").unwrap_or_else(|_| "local".into());
    if env == "production" || env == "prod" {
        let subscriber = get_subscriber(name, "info", std::io::stdout);
        init_subscriber(subscriber);
    } else {
        let subscriber = get_subscriber_pretty(name, &format!("info,{}=debug", name.replace('-', "_")));
        init_subscriber(subscriber);
    }
}

