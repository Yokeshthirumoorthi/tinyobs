//! TinyObs Lite - Embedded ClickHouse (chdb) binary

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

use tinyobs::api_types::{HealthResponse, LogQuery, MetricQuery, RawQueryRequest, TraceQuery};
use tinyobs::backend::*;
use tinyobs::config::{ApplicationSettings, IngestConfig, StorageConfig};
use tinyobs::server::{self, IngestState};
use tinyobs::transport::chdb::ChdbTransport;

// ============================================================================
// Lite-specific config
// ============================================================================

#[derive(Debug, Clone, serde::Deserialize)]
struct LiteConfig {
    #[serde(default)]
    application: ApplicationSettings,
    #[serde(default)]
    storage: StorageConfig,
    #[serde(default)]
    ingest: IngestConfig,
}

impl Default for LiteConfig {
    fn default() -> Self {
        Self {
            application: ApplicationSettings::default(),
            storage: StorageConfig::default(),
            ingest: IngestConfig::default(),
        }
    }
}

impl LiteConfig {
    fn chdb_data_path(&self) -> String {
        format!("{}/chdb", self.storage.data_dir)
    }
}

// ============================================================================
// Lite-specific API types and handlers
// ============================================================================

type LiteBackend = ChBackend<ChdbTransport>;

async fn api_health(State(backend): State<LiteBackend>) -> Json<HealthResponse> {
    let ch_ok = backend.health_check().await.unwrap_or(false);
    Json(HealthResponse {
        status: if ch_ok { "ok".to_string() } else { "degraded".to_string() },
        backend: ch_ok,
    })
}

async fn api_list_traces(
    State(backend): State<LiteBackend>,
    Query(q): Query<TraceQuery>,
) -> Result<Json<Vec<tinyobs::Span>>, (StatusCode, String)> {
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
    State(backend): State<LiteBackend>,
    axum::extract::Path(trace_id): axum::extract::Path<String>,
) -> Result<Json<Vec<tinyobs::Span>>, (StatusCode, String)> {
    let spans = backend
        .get_trace(&trace_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(spans))
}

async fn api_list_logs(
    State(backend): State<LiteBackend>,
    Query(q): Query<LogQuery>,
) -> Result<Json<Vec<tinyobs::LogRecord>>, (StatusCode, String)> {
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
    State(backend): State<LiteBackend>,
    Query(q): Query<MetricQuery>,
) -> Result<Json<Vec<tinyobs::Metric>>, (StatusCode, String)> {
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
    State(backend): State<LiteBackend>,
) -> Result<Json<Vec<ServiceSummary>>, (StatusCode, String)> {
    let time_range = TimeRange::last_day();
    let services = backend
        .list_services(time_range)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(services))
}

async fn api_raw_query(
    State(backend): State<LiteBackend>,
    Json(request): Json<RawQueryRequest>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
    let rows = backend
        .raw_query(&request.sql, request.limit)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(rows))
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    server::init_telemetry("tinyobs");

    let config: LiteConfig = tinyobs::config::get_configuration().unwrap_or_else(|e| {
        tracing::warn!("Failed to load configuration: {}, using defaults", e);
        LiteConfig::default()
    });
    tracing::info!("Starting with config: {:?}", config);

    // Create data directories
    std::fs::create_dir_all(&config.storage.data_dir)?;

    // Create chdb transport + generic backend
    let transport = ChdbTransport::new(&config.chdb_data_path())?;
    let backend = ChBackend::new(transport);

    // Initialize OTEL schema tables
    backend.init_schema().await?;

    // Build ingest state
    let ingest_state = IngestState {
        backend: backend.clone(),
        config: Arc::new(config.ingest.clone()),
    };

    // API routes (same shape as pro)
    let api_router = Router::new()
        .route("/api/traces", get(api_list_traces))
        .route("/api/traces/{trace_id}", get(api_get_trace))
        .route("/api/logs", get(api_list_logs))
        .route("/api/metrics", get(api_list_metrics))
        .route("/api/services", get(api_list_services))
        .route("/api/health", get(api_health))
        .route("/api/query", post(api_raw_query))
        .with_state(backend.clone());

    let full_app = Router::new()
        .merge(server::ingest_router(ingest_state))
        .merge(api_router);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.application.port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("TinyObs listening on port {}", config.application.port);

    axum::serve(listener, full_app).await?;

    backend.stop_background_tasks().await?;
    Ok(())
}
