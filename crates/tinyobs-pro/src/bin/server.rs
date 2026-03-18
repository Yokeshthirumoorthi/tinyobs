//! TinyObs Pro Server Binary
//!
//! Single axum binary serving OTLP ingest and query API on one port,
//! backed by ClickHouse.

use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::Subscriber;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt, EnvFilter, Registry};

use tinyobs_core::backend::*;
use tinyobs_pro::clickhouse::ClickHouseBackend;
use tinyobs_pro::config::ProConfig;
use tinyobs_pro::ingest::{
    health_check, receive_logs, receive_metrics, receive_traces, IngestState,
};

// ============================================================================
// Query API types
// ============================================================================

#[derive(Debug, Deserialize)]
struct TraceQuery {
    service: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct LogQuery {
    service: Option<String>,
    severity: Option<String>,
    trace_id: Option<String>,
    body_contains: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct MetricQuery {
    service: Option<String>,
    name: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RawQueryRequest {
    sql: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    100
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    clickhouse: bool,
}

// ============================================================================
// Query API handlers
// ============================================================================

async fn api_health(
    State(backend): State<ClickHouseBackend>,
) -> Json<HealthResponse> {
    let ch_ok = backend.ping().await.unwrap_or(false);
    Json(HealthResponse {
        status: if ch_ok { "ok".to_string() } else { "degraded".to_string() },
        clickhouse: ch_ok,
    })
}

async fn api_list_traces(
    State(backend): State<ClickHouseBackend>,
    Query(q): Query<TraceQuery>,
) -> Result<Json<Vec<tinyobs_core::Span>>, (StatusCode, String)> {
    let filter = SpanFilter {
        service: q.service,
        limit: q.limit.unwrap_or(100),
        ..Default::default()
    };
    let spans = backend
        .query_spans(filter)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(spans))
}

async fn api_get_trace(
    State(backend): State<ClickHouseBackend>,
    Path(trace_id): Path<String>,
) -> Result<Json<Vec<tinyobs_core::Span>>, (StatusCode, String)> {
    let spans = backend
        .get_trace(&trace_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(spans))
}

async fn api_list_logs(
    State(backend): State<ClickHouseBackend>,
    Query(q): Query<LogQuery>,
) -> Result<Json<Vec<tinyobs_core::LogRecord>>, (StatusCode, String)> {
    let filter = LogFilter {
        service: q.service,
        severity: q.severity,
        trace_id: q.trace_id,
        body_contains: q.body_contains,
        limit: q.limit.unwrap_or(100),
        ..Default::default()
    };
    let logs = backend
        .query_logs(filter)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(logs))
}

async fn api_list_metrics(
    State(backend): State<ClickHouseBackend>,
    Query(q): Query<MetricQuery>,
) -> Result<Json<Vec<tinyobs_core::Metric>>, (StatusCode, String)> {
    let filter = MetricFilter {
        service: q.service,
        name: q.name,
        limit: q.limit.unwrap_or(100),
        ..Default::default()
    };
    let metrics = backend
        .query_metrics(filter)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(metrics))
}

async fn api_list_services(
    State(backend): State<ClickHouseBackend>,
) -> Result<Json<Vec<ServiceSummary>>, (StatusCode, String)> {
    let time_range = TimeRange::last_day();
    let services = backend
        .list_services(time_range)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(services))
}

async fn api_raw_query(
    State(backend): State<ClickHouseBackend>,
    Json(request): Json<RawQueryRequest>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
    let rows = backend
        .raw_query(&request.sql, request.limit)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(rows))
}

// ============================================================================
// Telemetry setup (same as lite)
// ============================================================================

fn get_subscriber<Sink>(name: &str, env_filter: &str, sink: Sink) -> impl Subscriber + Send + Sync
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

fn get_subscriber_pretty(name: &str, env_filter: &str) -> impl Subscriber + Send + Sync {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));
    let _ = name;
    Registry::default()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().pretty())
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize telemetry
    let env = std::env::var("TINYOBS_ENV").unwrap_or_else(|_| "local".into());
    if env == "production" || env == "prod" {
        let subscriber = get_subscriber("tinyobs-pro", "info", std::io::stdout);
        tracing::subscriber::set_global_default(subscriber)?;
    } else {
        let subscriber = get_subscriber_pretty("tinyobs-pro", "info,tinyobs_pro=debug");
        tracing::subscriber::set_global_default(subscriber)?;
    }

    // Load configuration
    let config = ProConfig::get_configuration().unwrap_or_else(|e| {
        tracing::warn!("Failed to load configuration: {}, using defaults", e);
        ProConfig::default()
    });
    tracing::info!("Starting with config: {:?}", config);

    // Create ClickHouse backend
    let backend = ClickHouseBackend::new(config.clickhouse.clone());

    // Wait for ClickHouse to be ready
    tracing::info!("Waiting for ClickHouse...");
    let mut attempts = 0;
    loop {
        if backend.ping().await.unwrap_or(false) {
            tracing::info!("ClickHouse is ready");
            break;
        }
        attempts += 1;
        if attempts > 60 {
            anyhow::bail!("ClickHouse not ready after 30s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // Build ingest state
    let ingest_state = IngestState {
        backend: backend.clone(),
        config: Arc::new(config.ingest.clone()),
    };

    // Ingest routes
    let ingest_router = Router::new()
        .route("/v1/traces", post(receive_traces))
        .route("/v1/logs", post(receive_logs))
        .route("/v1/metrics", post(receive_metrics))
        .route("/health", get(health_check))
        .with_state(ingest_state);

    // Query API routes
    let api_router = Router::new()
        .route("/api/traces", get(api_list_traces))
        .route("/api/traces/{trace_id}", get(api_get_trace))
        .route("/api/logs", get(api_list_logs))
        .route("/api/metrics", get(api_list_metrics))
        .route("/api/services", get(api_list_services))
        .route("/api/health", get(api_health))
        .route("/api/query", post(api_raw_query))
        .with_state(backend);

    // Merge into a single router
    let app = ingest_router.merge(api_router);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.application.port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("tinyobs-pro listening on :{}", config.application.port);

    axum::serve(listener, app).await?;

    Ok(())
}
