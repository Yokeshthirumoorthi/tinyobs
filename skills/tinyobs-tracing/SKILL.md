---
name: tinyobs-tracing
description: Use when helping users send OTLP traces, logs, or metrics to a TinyObs server from Python, Node.js, or any OpenTelemetry SDK
license: MIT
metadata:
  author: yokesh
  version: "0.1.0"
---

# Sending Telemetry to TinyObs

TinyObs accepts standard OTLP (OpenTelemetry Protocol) over HTTP on port 4318.

## Endpoint

```
http://<tinyobs-host>:4318/v1/traces    # traces
http://<tinyobs-host>:4318/v1/logs      # logs
http://<tinyobs-host>:4318/v1/metrics   # metrics
```

Default format: Protobuf. JSON is also supported via `Content-Type: application/json`.

## Python

```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

provider = TracerProvider()
exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)
with tracer.start_as_current_span("my-operation"):
    pass  # your code here
```

## Node.js

```javascript
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');

const exporter = new OTLPTraceExporter({
  url: 'http://localhost:4318/v1/traces',
});
```

## Rust (using opentelemetry-otlp)

```rust
use opentelemetry_otlp::WithExportConfig;

let exporter = opentelemetry_otlp::new_exporter()
    .http()
    .with_endpoint("http://localhost:4318");
```

## Docker Quick Start

```bash
# Run TinyObs
docker run -d --rm -p 4318:4318 -v tinyobs-data:/app/data --name tinyobs tinyobs

# Verify it's running
curl http://localhost:4318/health
```

## Querying Data

```bash
# List recent traces
curl http://localhost:4318/api/traces

# Get a specific trace
curl http://localhost:4318/api/traces/<trace_id>

# Raw SQL query
curl -X POST http://localhost:4318/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT ServiceName, COUNT(*) as cnt FROM otel_traces GROUP BY ServiceName"}'
```
