---
name: tinyobs-backend
description: Use when working on server code, backend traits, transports, SQL builders, OTLP ingest, or schema types
metadata:
  internal: true
---

# TinyObs Backend

## Architecture

```
OTLP Clients
    │ POST /v1/{traces,logs,metrics}
    ▼
Axum HTTP Server (port 4318)
    │ server.rs — shared ingest handlers
    │ ingest.rs — OTLP protobuf/JSON parsing → domain types
    ▼
ChBackend<T: Transport>  ← generic over transport
    │ backend/mod.rs — TelemetryBackend + IngestBackend traits
    │ ch.rs — SQL builders + row parsers (pure functions)
    ▼
┌─────────────────┬──────────────────┐
│ ChdbTransport   │ RemoteTransport  │
│ (lite)          │ (pro)            │
│ Embedded chdb   │ HTTP to remote   │
│ Arc<Mutex<Sess>>│ Inserter batching│
└─────────────────┴──────────────────┘
```

## Module Guide

| Module | Purpose |
|--------|---------|
| `src/lib.rs` | Public API, re-exports domain types |
| `src/schema.rs` | Domain types: `Span`, `LogRecord`, `Metric`, enums |
| `src/ingest.rs` | OTLP protobuf/JSON parsing, format detection |
| `src/server.rs` | Shared Axum handlers for ingest + telemetry init |
| `src/backend/mod.rs` | `TelemetryBackend`, `IngestBackend`, `ManagedBackend` traits + `ChBackend<T>` |
| `src/backend/types.rs` | Filter types, response types, `TimeRange` |
| `src/ch.rs` | ClickHouse SQL builders and row parsers (no DB calls) |
| `src/transport/mod.rs` | `Transport` trait (`query_json`, `execute`, `ping`) |
| `src/transport/chdb.rs` | Embedded chdb transport (lite) |
| `src/transport/remote.rs` | Remote ClickHouse transport + Inserter row types (pro) |
| `src/bin/tinyobs-server.rs` | Lite binary — chdb + API routes |
| `src/bin/tinyobs-pro-server.rs` | Pro binary — remote ClickHouse + ProBackend wrapper |

## Trait-Based Backend Pattern

The core abstraction is `ChBackend<T: Transport>`. All query/ingest logic lives in the generic impl. Only the transport layer differs between lite and pro. To add a new storage backend, implement the `Transport` trait.

Key traits in `src/backend/mod.rs`:
- `TelemetryBackend` — query spans, logs, metrics, services, schema
- `IngestBackend` — ingest spans, logs, metrics
- `ManagedBackend` — lifecycle (start/stop background tasks, health check)

## SQL Building

SQL construction is in `src/ch.rs` as pure functions — no database calls. Row parsing converts ClickHouse JSON responses to domain types. Always use the escaping helpers for string literals.

## Conventions

### Error Handling
- `anyhow::Result` for application errors
- `thiserror` for custom error types
- `.context("Failed to X")` for wrapping errors
- HTTP errors as `(StatusCode, String)` tuples

### Async Patterns
- Blocking chdb calls wrapped in `tokio::task::spawn_blocking()`
- Shared mutable state via `Arc<Mutex<T>>`
- `async_trait` for all trait methods

### Tests
- Unit tests are co-located in source files via `#[cfg(test)]`
- Run with `just test` or `cargo test`
