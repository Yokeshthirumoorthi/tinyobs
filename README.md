# TinyObs

Minimal observability storage: OTLP ingest + SQLite/Parquet + DuckDB query.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              TinyObs                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   OTLP Span ──► POST /v1/traces ──► SQLite (hot, <10s)                 │
│                                           │                             │
│                                           ▼ (compaction, 5s)            │
│                                     Parquet files (cold)                │
│                                           │                             │
│                                           ▼ (merge, hourly)             │
│                                     Merged Parquet                      │
│                                           │                             │
│   Consumer SQL ──► tinyobs.query() ──► DuckDB ──► UNION ALL ──► Results │
│                                       (hot + cold)                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Components:**
- `src/lib.rs` - Core library (Rust clients import this)
- `src/bin/server.rs` - HTTP server (Docker runs this for Python/other clients)
- `src/db/` - SQLite write path + DuckDB read path
- `src/compaction/` - Background workers for SQLite → Parquet compaction
- `src/ingest.rs` - OTLP protobuf parsing and ingestion

## Installation

### Rust Projects (as a library)

Add to your `Cargo.toml`:

```toml
[dependencies]
tinyobs = { git = "https://github.com/Yokeshthirumoorthi/tinyobs.git" }
```

Then use it in your code:

```rust
use tinyobs::{TinyObs, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let handle = TinyObs::start(Config::default()).await?;
    let tinyobs = handle.clone_tinyobs();

    // Option 1: Merge the ingest router into your app
    let app = axum::Router::new()
        .merge(tinyobs.ingest_router())  // POST /v1/traces, GET /health
        .with_state(tinyobs.clone());

    // Option 2: Ingest spans directly
    tinyobs.ingest(span)?;
    tinyobs.ingest_batch(spans)?;

    // Query spans with SQL
    let results = tinyobs.query::<MyStruct>(
        "SELECT * FROM spans WHERE service_name = ?",
        &[&"my-service"]
    )?;

    handle.shutdown().await?;
    Ok(())
}
```

### Other Clients (Python, Node.js, etc.)

Run TinyObs as a standalone HTTP server using Docker, then send OTLP traces via HTTP.

## Quick Start with Docker

```bash
# Build and run
docker build -t tinyobs .
docker run -d --rm \
  -p 4318:4318 \
  -v tinyobs-data:/app/data \
  --name tinyobs \
  tinyobs

# Or use just (if installed)
just run
```

### Docker Build and Push

```bash
# Build the image
docker build -t tinyobs .

# Tag for registry
docker tag tinyobs tracing.paradise-grue.ts.net:5000/tinyobs

# Push to registry
docker push tracing.paradise-grue.ts.net:5000/tinyobs

# Or use just
just push
```

### Reset Data

```bash
docker rm -f tinyobs
docker volume rm tinyobs-data
just run  # or docker run command above
```

## HTTP Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/traces` | POST | OTLP trace ingestion (protobuf) |
| `/health` | GET | Health check |
| `/api/traces` | GET | List recent traces |
| `/api/traces/:id` | GET | Get spans for a specific trace |
| `/query` | POST | Execute arbitrary SQL query |

### Sending Traces (OTLP)

Configure your OpenTelemetry SDK to export to `http://localhost:4318/v1/traces`.

**Python example:**
```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

trace.set_tracer_provider(TracerProvider())
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))

tracer = trace.get_tracer(__name__)
with tracer.start_as_current_span("my-operation"):
    # your code here
    pass
```

**Node.js example:**
```javascript
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');

const exporter = new OTLPTraceExporter({
  url: 'http://localhost:4318/v1/traces',
});
```

### Querying Data

```bash
# Count all spans
curl -X POST http://localhost:4318/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) as count FROM spans"}'

# Get recent traces
curl http://localhost:4318/api/traces

# Get spans for a specific trace
curl http://localhost:4318/api/traces/abc123def456

# Custom SQL query
curl -X POST http://localhost:4318/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT service_name, COUNT(*) as cnt FROM spans GROUP BY service_name"}'
```

## Configuration

Environment variables:
- `TINYOBS_ENV` - Set to `production` for JSON logs, `local` for pretty logs (default: local)
- `RUST_LOG` - Override log level filtering (e.g., `info`, `debug`, `tinyobs=debug`)

## Development

```bash
# Run server locally
cargo run --bin tinyobs-server

# Build release binary
cargo build --release --bin tinyobs-server

# Run tests
cargo test
```

## License

MIT
