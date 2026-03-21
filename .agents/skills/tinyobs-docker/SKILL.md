---
name: tinyobs-docker
description: Use when working on Docker, deployment, registry push, or ops
metadata:
  internal: true
---

# TinyObs Docker & Deployment

## Dockerfiles

| File | Binary | Base Image |
|------|--------|------------|
| `Dockerfile` | tinyobs-server (lite) | debian:bookworm-slim |
| `tinyobs-pro/docker/Dockerfile` | tinyobs-pro-server | clickhouse/clickhouse-server:24.3 |

## Commands

```bash
just docker-lite      # build + run lite in Docker (port 4318)
just docker-pro       # build + run pro in Docker (port 4318)
just reset            # remove lite container + volume, rebuild
just reset-pro        # remove pro container + volume, rebuild
just push             # build + push lite to registry
just push-pro         # build + push pro to registry
```

## Pro Deployment

The pro Dockerfile bundles ClickHouse + tinyobs-pro into one image:
- `tinyobs-pro/docker/entrypoint.sh` — starts ClickHouse, waits for readiness, runs init SQL, starts tinyobs-pro
- `tinyobs-pro/docker/clickhouse-init.sql` — creates otel_traces, otel_logs, otel_metrics tables with indexes and 30-day TTL
- Volume: `/var/lib/clickhouse` for persistent data

## Registry

Images push to: `tracing.paradise-grue.ts.net:5000/tinyobs` and `tracing.paradise-grue.ts.net:5000/tinyobs-pro`
