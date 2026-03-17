# TinyObs

Minimal observability storage: OTLP ingest + SQLite/Parquet + DuckDB query.

## Architecture

- `src/lib.rs` - Core library (Rust clients import this)
- `src/bin/server.rs` - HTTP server (Docker runs this for Python/other clients)

## Quick Start

```bash
# Build and run with Docker
just run

# Query spans
just query
just query sql='SELECT * FROM spans LIMIT 10'
```

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/traces` | POST | OTLP trace ingestion |
| `/health` | GET | Health check |
| `/api/traces` | GET | List traces |
| `/api/traces/:id` | GET | Get trace detail |
| `/query` | POST | Execute SQL query |

## Query API

```bash
curl -X POST http://localhost:4318/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) as count FROM spans"}'
```

## Configuration

Environment variables:
- `TINYOBS_ENV` - Set to `production` for JSON logs, `local` for pretty logs (default: local)
- `RUST_LOG` - Override log level filtering

## Development

```bash
# Run server locally
cargo run --bin tinyobs-server

# Build release binary
cargo build --release --bin tinyobs-server
```

## License

MIT
