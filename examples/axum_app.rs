//! Embedding TinyObs in an existing Axum application.
//!
//! Run with: cargo run --example axum_app --features lite
//!
//! Then send OTLP traces to http://localhost:4318/v1/traces

use axum::{response::Json, routing::get, Router};
use std::net::SocketAddr;
use tinyobs::TinyObs;

async fn hello() -> Json<serde_json::Value> {
    Json(serde_json::json!({"message": "Hello from my app!"}))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let tinyobs = TinyObs::lite("./example-data")?;
    tinyobs.init_schema().await?;

    // Your app's routes
    let app_routes = Router::new().route("/hello", get(hello));

    // Merge tinyobs ingest routes with your app
    let app = Router::new()
        .merge(app_routes)
        .merge(tinyobs.ingest_router());

    let addr = SocketAddr::from(([0, 0, 0, 0], 4318));
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("Listening on {addr}");
    println!("  GET  /hello       - Your app endpoint");
    println!("  POST /v1/traces   - OTLP trace ingest");
    println!("  POST /v1/logs     - OTLP log ingest");
    println!("  POST /v1/metrics  - OTLP metric ingest");
    println!("  GET  /health      - Health check");

    axum::serve(listener, app).await?;

    tinyobs.shutdown().await?;
    Ok(())
}
