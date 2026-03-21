//! Basic TinyObs usage: create, init schema, ingest a span, query it back.
//!
//! Run with: cargo run --example basic --features lite

use tinyobs::backend::IngestBackend;
use tinyobs::{Span, SpanStatus, TinyObs};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create a TinyObs instance with embedded storage
    let tinyobs = TinyObs::lite("./example-data")?;

    // Initialize OTEL tables (safe to call multiple times)
    tinyobs.init_schema().await?;

    // Ingest a span programmatically
    let span = Span {
        trace_id: "abc123".to_string(),
        span_id: "def456".to_string(),
        parent_span_id: None,
        session_id: None,
        service_name: "my-service".to_string(),
        operation: "handle-request".to_string(),
        start_time: chrono::Utc::now().timestamp_micros(),
        duration_ns: 42_000_000, // 42ms
        status: SpanStatus::Ok,
        attributes: vec![
            ("http.method".to_string(), "GET".to_string()),
            ("http.url".to_string(), "/api/users".to_string()),
        ],
        resource_attrs: vec![("service.version".to_string(), "1.0.0".to_string())],
    };
    tinyobs.backend().ingest_span(span).await?;

    // Query it back
    let rows = tinyobs
        .query("SELECT TraceId, SpanName, Duration FROM otel_traces", 10)
        .await?;

    println!("Ingested spans:");
    for row in &rows {
        println!("  {}", serde_json::to_string(row)?);
    }

    // Clean up
    tinyobs.shutdown().await?;

    // Remove example data
    std::fs::remove_dir_all("./example-data").ok();

    Ok(())
}
