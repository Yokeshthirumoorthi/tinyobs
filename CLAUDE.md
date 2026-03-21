# CLAUDE.md

## Project Overview

TinyObs is a minimal observability backend that ingests OTLP telemetry (traces, logs, metrics) and stores them in ClickHouse. It ships as a Rust library and two server binaries:

- **tinyobs-server** (lite) — embedded ClickHouse via chdb, single binary, zero dependencies
- **tinyobs-pro-server** (pro) — connects to a remote ClickHouse instance, batched inserts

## Quick Reference

```bash
# Build
just build            # build lite (default features)
just build-pro        # build pro
just check            # cargo check (fast compile check)

# Test & Quality
just test             # run all tests
just clippy           # lint with clippy
just fmt              # format code
just fmt-check        # check formatting without modifying

# Run locally
just run-lite         # cargo run lite server
just run-pro          # cargo run pro server

# Docker
just docker-lite      # build + run lite in Docker
just docker-pro       # build + run pro in Docker
just reset            # remove lite container + volume, rebuild
just reset-pro        # remove pro container + volume, rebuild
```

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

## Key Conventions

### Feature Flags
- `lite` (default) — enables `chdb-rust` dep, builds `tinyobs-server`
- `pro` — enables `clickhouse` + `reqwest` deps, builds `tinyobs-pro-server`
- Build commands must use `--features pro --no-default-features` for pro builds

### Trait-Based Backend Pattern
The core abstraction is `ChBackend<T: Transport>`. All query/ingest logic lives in the generic impl. Only the transport layer differs between lite and pro. To add a new storage backend, implement the `Transport` trait.

### SQL Building
SQL construction is in `src/ch.rs` as pure functions — no database calls. Row parsing converts ClickHouse JSON responses to domain types. Always use the escaping helpers for string literals.

### Error Handling
- `anyhow::Result` for application errors
- `thiserror` for custom error types
- `.context("Failed to X")` for wrapping errors
- HTTP errors as `(StatusCode, String)` tuples

### Async Patterns
- Blocking chdb calls wrapped in `tokio::task::spawn_blocking()`
- Shared mutable state via `Arc<Mutex<T>>`
- `async_trait` for all trait methods

### Configuration
- `configuration/base.toml` + environment overlay (`local.toml` / `production.toml`)
- `TINYOBS_ENV` selects environment (default: `local`)
- `TINYOBS_*` env vars override config with `__` separator

### Tests
- Unit tests are co-located in source files via `#[cfg(test)]`
- Run with `just test` or `cargo test`

## HTTP Endpoints

### Shared (both lite and pro)
- `POST /v1/traces` — OTLP trace ingest
- `POST /v1/logs` — OTLP log ingest
- `POST /v1/metrics` — OTLP metric ingest

### Pro-specific
- `GET /api/health` — JSON health status
- `GET /api/traces?service=&limit=` — list traces
- `GET /api/traces/:id` — get spans for trace
- `GET /api/logs?severity=&trace_id=&body_contains=&limit=` — query logs
- `GET /api/metrics?service=&name=&limit=` — query metrics
- `GET /api/services` — list services (last 24h)
- `POST /api/query` — raw SQL with limit
