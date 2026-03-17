//! Standalone TinyObs example
//!
//! Demonstrates running TinyObs as a standalone trace collector with Z2P-style
//! telemetry (JSON logs for production, pretty logs for development).
//!
//! Run with: `cargo run -p tinyobs --example standalone`
//!
//! Environment variables:
//! - `TINYOBS_ENV`: Set to "production" for JSON logs, "local" for pretty logs (default: local)
//! - `RUST_LOG`: Override log level filtering

use anyhow::Result;
use axum::{extract::State, response::Json, routing::get, Router};
use serde::{Deserialize, Serialize};
use tinyobs::{startup::Application, telemetry, Config, TinyObs};

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
    // Initialize telemetry based on environment
    let env = std::env::var("TINYOBS_ENV").unwrap_or_else(|_| "local".into());
    if env == "production" || env == "prod" {
        let subscriber = telemetry::get_subscriber("tinyobs", "info", std::io::stdout);
        telemetry::init_subscriber(subscriber);
    } else {
        let subscriber = telemetry::get_subscriber_pretty("tinyobs", "info,tinyobs=debug");
        telemetry::init_subscriber(subscriber);
    }

    // Load configuration from files or use defaults
    let config = Config::get_configuration().unwrap_or_else(|e| {
        tracing::warn!("Failed to load configuration: {}, using defaults", e);
        Config::default()
    });
    tracing::info!("Starting with config: {:?}", config);

    // Build application using the startup module
    let app = Application::build(config).await?;
    let tinyobs = app.tinyobs();

    // Build custom API router with TinyObs state
    let api_router = Router::new()
        .route("/api/traces", get(list_traces))
        .route("/api/traces/{trace_id}", get(get_trace))
        .with_state(tinyobs.clone());

    // For this example, we run the server manually to add custom routes
    // In simpler cases, you could just use app.run_until_stopped()
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", app.port())).await?;

    let full_app = Router::new()
        .merge(tinyobs.ingest_router())
        .merge(api_router);

    tracing::info!("TinyObs listening on port {}", app.port());
    tracing::info!("  POST /v1/traces      - OTLP trace ingestion");
    tracing::info!("  GET  /health         - Health check");
    tracing::info!("  GET  /api/traces     - List traces (custom)");
    tracing::info!("  GET  /api/traces/:id - Get trace detail (custom)");

    axum::serve(listener, full_app).await?;

    // Graceful shutdown
    app.handle().clone_tinyobs();
    Ok(())
}
