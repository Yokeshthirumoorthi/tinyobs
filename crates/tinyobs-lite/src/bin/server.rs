use anyhow::Result;
use axum::{extract::State, http::StatusCode, response::Json, routing::{get, post}, Router};
use duckdb::ToSql;
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

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
    #[serde(default)]
    params: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    data: Vec<serde_json::Value>,
}

async fn execute_query(
    State(tinyobs): State<TinyObs>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    let param_refs: Vec<&dyn ToSql> = request.params.iter()
        .map(|s| s as &dyn ToSql)
        .collect();

    let data: Vec<serde_json::Value> = tinyobs
        .query(&request.sql, &param_refs)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    Ok(Json(QueryResponse { data }))
}

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
    let env = std::env::var("TINYOBS_ENV").unwrap_or_else(|_| "local".into());
    if env == "production" || env == "prod" {
        let subscriber = telemetry::get_subscriber("tinyobs", "info", std::io::stdout);
        telemetry::init_subscriber(subscriber);
    } else {
        let subscriber = telemetry::get_subscriber_pretty("tinyobs", "info,tinyobs=debug");
        telemetry::init_subscriber(subscriber);
    }

    let config = Config::get_configuration().unwrap_or_else(|e| {
        tracing::warn!("Failed to load configuration: {}, using defaults", e);
        Config::default()
    });
    tracing::info!("Starting with config: {:?}", config);

    let app = Application::build(config).await?;
    let (listener, handle, port) = app.into_parts();
    let tinyobs = handle.clone_tinyobs();

    let api_router = Router::new()
        .route("/api/traces", get(list_traces))
        .route("/api/traces/{trace_id}", get(get_trace))
        .route("/query", post(execute_query))
        .with_state(tinyobs.clone());

    let full_app = Router::new()
        .merge(tinyobs.ingest_router())
        .merge(api_router);

    tracing::info!("TinyObs listening on port {}", port);
    tracing::info!("  POST /v1/traces  - OTLP trace ingestion (protobuf & JSON)");
    tracing::info!("  POST /v1/logs    - OTLP logs ingestion (protobuf & JSON)");
    tracing::info!("  POST /v1/metrics - OTLP metrics ingestion (protobuf & JSON)");
    tracing::info!("  GET  /health     - Health check");
    tracing::info!("  GET  /api/traces     - List traces (custom)");
    tracing::info!("  GET  /api/traces/:id - Get trace detail (custom)");
    tracing::info!("  POST /query          - Execute SQL query");

    axum::serve(listener, full_app).await?;

    handle.shutdown().await?;
    Ok(())
}
