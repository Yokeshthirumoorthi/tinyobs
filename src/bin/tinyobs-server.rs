//! TinyObs Lite - Embedded ClickHouse (chdb) binary

use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use std::sync::Arc;

use tinyobs::backend::{ChBackend, ManagedBackend, TelemetryBackend};
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

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    data: Vec<serde_json::Value>,
}

async fn execute_query(
    State(backend): State<LiteBackend>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    let data = backend
        .raw_query(&request.sql, 1000)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    Ok(Json(QueryResponse { data }))
}

async fn list_traces(State(backend): State<LiteBackend>) -> Json<Vec<serde_json::Value>> {
    let data = backend
        .raw_query(
            r#"
            SELECT
                TraceId as trace_id,
                ServiceName as service_name,
                COUNT(*) as span_count,
                MIN(Timestamp) as started_at
            FROM otel_traces
            GROUP BY TraceId, ServiceName
            ORDER BY started_at DESC
            LIMIT 100
            "#,
            100,
        )
        .await
        .unwrap_or_default();

    Json(data)
}

async fn get_trace(
    State(backend): State<LiteBackend>,
    axum::extract::Path(trace_id): axum::extract::Path<String>,
) -> Json<Vec<tinyobs::Span>> {
    let spans = backend
        .get_trace(&trace_id)
        .await
        .unwrap_or_default();

    Json(spans)
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

    // Build ingest state
    let ingest_state = IngestState {
        backend: backend.clone(),
        config: Arc::new(config.ingest.clone()),
    };

    // Lite-specific API routes
    let api_router = Router::new()
        .route("/api/traces", get(list_traces))
        .route("/api/traces/{trace_id}", get(get_trace))
        .route("/query", post(execute_query))
        .with_state(backend.clone());

    let full_app = Router::new()
        .merge(server::ingest_router(ingest_state))
        .merge(api_router);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.application.port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("TinyObs listening on port {}", config.application.port);
    tracing::info!("  POST /v1/traces      - OTLP trace ingestion");
    tracing::info!("  POST /v1/logs        - OTLP logs ingestion");
    tracing::info!("  POST /v1/metrics     - OTLP metrics ingestion");
    tracing::info!("  GET  /health         - Health check");
    tracing::info!("  GET  /api/traces     - List traces");
    tracing::info!("  GET  /api/traces/:id - Get trace detail");
    tracing::info!("  POST /query          - Execute SQL query");

    axum::serve(listener, full_app).await?;

    backend.stop_background_tasks().await?;
    Ok(())
}
