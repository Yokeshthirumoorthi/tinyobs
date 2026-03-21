---
name: tinyobs-api
description: Use when working on HTTP endpoints, API types, configuration, or feature flags
metadata:
  internal: true
---

# TinyObs API & Configuration

## HTTP Endpoints (both lite and pro)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/traces` | POST | OTLP trace ingest |
| `/v1/logs` | POST | OTLP log ingest |
| `/v1/metrics` | POST | OTLP metric ingest |
| `/api/health` | GET | JSON health status |
| `/api/traces?service=&limit=` | GET | List traces |
| `/api/traces/:id` | GET | Get spans for trace |
| `/api/logs?severity=&trace_id=&body_contains=&limit=` | GET | Query logs |
| `/api/metrics?service=&name=&limit=` | GET | Query metrics |
| `/api/services` | GET | List services (last 24h) |
| `/api/query` | POST | Raw SQL with limit |

## Shared API Types

`src/api_types.rs` contains request/response types shared by both server binaries and the client:
- `TraceQuery`, `LogQuery`, `MetricQuery` — query parameter structs
- `RawQueryRequest` — JSON body for `/api/query`
- `HealthResponse`, `QueryResponse` — response types

## Feature Flags

| Feature | Deps | Builds |
|---------|------|--------|
| `lite` (default) | chdb-rust | tinyobs-server |
| `pro` | clickhouse, reqwest | tinyobs-pro-server |
| `client` | reqwest | — (library only) |
| `cli` | client + clap | tinyobs-cli |
| `mcp` | client + rmcp | tinyobs-mcp |

Build commands for pro: `--features pro --no-default-features`

## Configuration

- `configuration/base.toml` + environment overlay (`local.toml` / `production.toml`)
- `TINYOBS_ENV` selects environment (default: `local`)
- `TINYOBS_*` env vars override config with `__` separator
- Config loading: `src/config.rs` — generic `get_configuration::<T>()` that merges files + env
