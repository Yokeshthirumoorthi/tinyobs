---
name: tinyobs-library
description: Use when helping users embed TinyObs as a Rust library crate, create custom tables, or extend with CLI/MCP tools
license: MIT
metadata:
  author: yokesh
  version: "0.1.0"
---

# Using TinyObs as a Rust Library

Add to `Cargo.toml`:

```toml
[dependencies]
tinyobs = { git = "https://github.com/Yokeshthirumoorthi/tinyobs.git" }
```

## Basic Usage

```rust
use tinyobs::TinyObs;

let tinyobs = TinyObs::lite("./data")?;
tinyobs.init_schema().await?;

// Embed OTLP ingest in your Axum app
let app = Router::new()
    .merge(tinyobs.ingest_router())
    .route("/my-route", get(my_handler));
```

## Custom Tables

TinyObs provides OTEL tables (otel_traces, otel_logs, otel_metrics). You can add your own:

```rust
tinyobs.execute("CREATE TABLE IF NOT EXISTS my_events (...) ENGINE = MergeTree() ORDER BY (...)").await?;
tinyobs.execute("INSERT INTO my_events VALUES (...)").await?;
let rows = tinyobs.query("SELECT * FROM my_events", 100).await?;
```

## Programmatic Ingestion

```rust
use tinyobs::backend::IngestBackend;
use tinyobs::{Span, SpanStatus};

let span = Span {
    trace_id: "abc123".to_string(),
    span_id: "def456".to_string(),
    parent_span_id: None,
    session_id: None,
    service_name: "my-service".to_string(),
    operation: "handle-request".to_string(),
    start_time: chrono::Utc::now().timestamp_micros(),
    duration_ns: 42_000_000,
    status: SpanStatus::Ok,
    attributes: vec![("key".to_string(), "value".to_string())],
    resource_attrs: vec![],
};
tinyobs.backend().ingest_span(span).await?;
```

## Extending CLI and MCP

TinyObs CLI commands and MCP tools are composable library modules:

```rust
// Add tinyobs CLI commands to your own CLI
use tinyobs::cli::base_commands;
let cli = Command::new("myapp")
    .subcommands(base_commands())
    .subcommand(my_custom_command());

// Add tinyobs MCP tools to your own MCP server
use tinyobs::mcp::TinyObsTools;
let tools = TinyObsTools::new(client);
```

Requires feature flags: `cli` for CLI, `mcp` for MCP, `client` for HTTP client.

## Public API Reference

| Method | Description |
|--------|-------------|
| `TinyObs::lite(path)` | Create with embedded storage |
| `init_schema()` | Create OTEL tables |
| `execute(sql)` | Run DDL/DML (custom tables) |
| `query(sql, limit)` | Query any table |
| `ingest_router()` | Axum router for OTLP endpoints |
| `health_check()` | Check backend health |
| `shutdown()` | Flush and stop |
| `backend()` | Access backend for `IngestBackend` methods |

## Examples

See `examples/` directory:
- `basic.rs` — minimal usage
- `custom_tables.rs` — extending with custom tables
- `axum_app.rs` — embedding in an Axum app

Run with: `cargo run --example basic --features lite`
