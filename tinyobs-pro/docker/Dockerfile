# Build stage — run from workspace root: docker build -f tinyobs-pro/docker/Dockerfile .
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# Copy manifest
COPY Cargo.toml Cargo.lock ./

# Create dummy sources to cache dependencies
RUN mkdir -p src && \
    echo "pub fn main() {}" > src/lib.rs && \
    mkdir -p src/bin && \
    echo "fn main() {}" > src/bin/tinyobs-pro-server.rs

# Build dependencies only
RUN cargo build --release --bin tinyobs-pro-server --features pro --no-default-features && \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build the actual binary
RUN touch src/lib.rs src/bin/tinyobs-pro-server.rs && \
    cargo build --release --bin tinyobs-pro-server --features pro --no-default-features

# Runtime stage — based on ClickHouse server image
FROM clickhouse/clickhouse-server:24.3

# Copy tinyobs-pro binary
COPY --from=builder /app/target/release/tinyobs-pro-server /usr/local/bin/tinyobs-pro-server

# Copy ClickHouse init SQL and entrypoint
COPY tinyobs-pro/docker/clickhouse-init.sql /docker-entrypoint-initdb.d/
COPY tinyobs-pro/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# OTLP ingest + Query API
EXPOSE 4318

# ClickHouse data persistence
VOLUME /var/lib/clickhouse

ENV RUST_LOG=info
ENV TINYOBS_ENV=production

ENTRYPOINT ["/entrypoint.sh"]
