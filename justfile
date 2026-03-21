# TinyObs justfile

# Show all available commands
help:
  @just --list

# ─── Build ────────────────────────────────────────────

# Build lite (default features)
build:
  cargo build --bin tinyobs-server

# Build pro
build-pro:
  cargo build --bin tinyobs-pro-server --features pro --no-default-features

# Build release lite
build-release:
  cargo build --release --bin tinyobs-server

# Build release pro
build-release-pro:
  cargo build --release --bin tinyobs-pro-server --features pro --no-default-features

# Fast compile check (no codegen)
check:
  cargo check --all-features

# ─── Test & Quality ───────────────────────────────────

# Run all tests
test:
  cargo test

# Run clippy lints
clippy:
  cargo clippy --all-features -- -D warnings

# Format code
fmt:
  cargo fmt

# Check formatting without modifying
fmt-check:
  cargo fmt -- --check

# ─── Run Locally ──────────────────────────────────────

# Run lite server locally
run-lite:
  cargo run --bin tinyobs-server

# Run pro server locally
run-pro:
  cargo run --bin tinyobs-pro-server --features pro --no-default-features

# ─── CLI & MCP ───────────────────────────────────────

# Build CLI
build-cli:
  cargo build --bin tinyobs-cli --features cli

# Build MCP server
build-mcp:
  cargo build --bin tinyobs-mcp --features mcp

# Run CLI with arguments (e.g. just cli health)
cli *ARGS:
  cargo run --bin tinyobs-cli --features cli -- {{ARGS}}

# Run MCP server
run-mcp:
  cargo run --bin tinyobs-mcp --features mcp

# ─── Docker (Lite) ────────────────────────────────────

# Build and run tinyobs lite in Docker
docker-lite:
  docker build -f Dockerfile -t tinyobs .
  docker run -d --rm \
    -p 4318:4318 \
    -v tinyobs-data:/app/data \
    --name tinyobs \
    tinyobs

# Reset lite: remove container + volume, rebuild
reset:
  docker rm -f tinyobs 2>/dev/null || true
  docker volume rm tinyobs-data 2>/dev/null || true
  just docker-lite

# Build and push lite to registry
push:
  docker build -f Dockerfile -t tinyobs .
  docker tag tinyobs tracing.paradise-grue.ts.net:5000/tinyobs
  docker push tracing.paradise-grue.ts.net:5000/tinyobs

# ─── Docker (Pro) ─────────────────────────────────────

# Build and run tinyobs-pro in Docker
docker-pro:
  docker build -f tinyobs-pro/docker/Dockerfile -t tinyobs-pro .
  docker run -d --rm \
    -p 4318:4318 \
    -v tinyobs-pro-data:/var/lib/clickhouse \
    --name tinyobs-pro \
    tinyobs-pro

# Reset pro: remove container + volume, rebuild
reset-pro:
  docker rm -f tinyobs-pro 2>/dev/null || true
  docker volume rm tinyobs-pro-data 2>/dev/null || true
  just docker-pro

# Build and push pro to registry
push-pro:
  docker build -f tinyobs-pro/docker/Dockerfile -t tinyobs-pro .
  docker tag tinyobs-pro tracing.paradise-grue.ts.net:5000/tinyobs-pro
  docker push tracing.paradise-grue.ts.net:5000/tinyobs-pro
