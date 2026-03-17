//! Standalone TinyObs example
//!
//! Demonstrates running TinyObs as a standalone trace collector.
//!
//! Run with: `cargo run -p tinyobs --example standalone`

use anyhow::Result;
use axum::{extract::State, response::Json, routing::get, Router};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tinyobs::{Config, TinyObs};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Serialize, Deserialize)]
struct TraceSummary {
    trace_id: String,
    service_name: String,
    span_count: i64,
    started_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SpanDetail {
    trace_id: String,
    span_id: String,
    operation: String,
    service_name: String,
    duration_ns: i64,
}

// Custom handler using tinyobs.query()
async fn list_traces(State(tinyobs): State<TinyObs>) -> Json<Vec<TraceSummary>> {
    let traces = tinyobs
        .query::<TraceSummary>(
            r#"
            SELECT
                trace_id,
                service_name,
                COUNT(*) as span_count,
                MIN(start_time) as started_at
            FROM spans
            GROUP BY trace_id, service_name
            ORDER BY started_at DESC
            LIMIT 100
            "#,
            &[],
        )
        .unwrap_or_default();

    Json(traces)
}

async fn get_trace(
    State(tinyobs): State<TinyObs>,
    axum::extract::Path(trace_id): axum::extract::Path<String>,
) -> Json<Vec<SpanDetail>> {
    let spans = tinyobs
        .query::<SpanDetail>(
            r#"
            SELECT trace_id, span_id, operation, service_name, duration_ns
            FROM spans
            WHERE trace_id = ?
            ORDER BY start_time ASC
            "#,
            &[&trace_id],
        )
        .unwrap_or_default();

    Json(spans)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::new("info,tinyobs=debug"))
        .init();

    // Load or create config
    let config = Config::default();
    tracing::info!("Starting with config: {:?}", config);

    // Start TinyObs
    let handle = TinyObs::start(config).await?;
    let tinyobs = handle.clone_tinyobs();

    // Build custom API router with TinyObs state
    let api_router = Router::new()
        .route("/api/traces", get(list_traces))
        .route("/api/traces/{trace_id}", get(get_trace))
        .with_state(tinyobs.clone());

    // Build application: merge ingest router (has its own state) with custom API
    let app = Router::new()
        .merge(tinyobs.ingest_router())
        .merge(api_router);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], 4319));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("TinyObs listening on {}", addr);
    tracing::info!("  POST /v1/traces     - OTLP trace ingestion");
    tracing::info!("  GET  /health        - Health check");
    tracing::info!("  GET  /api/traces    - List traces (custom)");
    tracing::info!("  GET  /api/traces/:id - Get trace detail (custom)");

    axum::serve(listener, app).await?;

    // Graceful shutdown
    handle.shutdown().await?;

    Ok(())
}
