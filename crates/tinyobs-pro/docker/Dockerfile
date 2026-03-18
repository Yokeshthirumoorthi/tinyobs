# Build stage — run from workspace root: docker build -f crates/tinyobs-pro/docker/Dockerfile .
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# Copy workspace manifests
COPY Cargo.toml Cargo.lock ./
COPY crates/tinyobs-core/Cargo.toml crates/tinyobs-core/Cargo.toml
COPY crates/tinyobs-pro/Cargo.toml crates/tinyobs-pro/Cargo.toml

# Create dummy sources to cache dependencies
RUN mkdir -p crates/tinyobs-core/src && \
    echo "pub fn main() {}" > crates/tinyobs-core/src/lib.rs && \
    mkdir -p crates/tinyobs-pro/src/bin && \
    echo "pub fn main() {}" > crates/tinyobs-pro/src/lib.rs && \
    echo "fn main() {}" > crates/tinyobs-pro/src/bin/server.rs && \
    mkdir -p crates/tinyobs-lite/src/bin && \
    printf '[package]\nname = "tinyobs"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\ntinyobs-core = { path = "../tinyobs-core" }\n' > crates/tinyobs-lite/Cargo.toml && \
    echo "pub fn main() {}" > crates/tinyobs-lite/src/lib.rs && \
    echo "fn main() {}" > crates/tinyobs-lite/src/bin/server.rs

# Build dependencies only
RUN cargo build --release -p tinyobs-pro && \
    rm -rf crates

# Copy actual source code
COPY crates/tinyobs-core crates/tinyobs-core
COPY crates/tinyobs-pro crates/tinyobs-pro

# Stub tinyobs-lite for workspace resolution
RUN mkdir -p crates/tinyobs-lite/src/bin && \
    printf '[package]\nname = "tinyobs"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\ntinyobs-core = { path = "../tinyobs-core" }\n' > crates/tinyobs-lite/Cargo.toml && \
    echo "pub fn main() {}" > crates/tinyobs-lite/src/lib.rs && \
    echo "fn main() {}" > crates/tinyobs-lite/src/bin/server.rs

# Build the actual binary
RUN touch crates/tinyobs-core/src/lib.rs crates/tinyobs-pro/src/lib.rs crates/tinyobs-pro/src/bin/server.rs && \
    cargo build --release --bin tinyobs-pro-server

# Runtime stage — based on ClickHouse server image
FROM clickhouse/clickhouse-server:24.3

# Copy tinyobs-pro binary
COPY --from=builder /app/target/release/tinyobs-pro-server /usr/local/bin/tinyobs-pro-server

# Copy ClickHouse init SQL and entrypoint
COPY crates/tinyobs-pro/docker/clickhouse-init.sql /docker-entrypoint-initdb.d/
COPY crates/tinyobs-pro/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# OTLP ingest + Query API
EXPOSE 4318 8080

# ClickHouse data persistence
VOLUME /var/lib/clickhouse

ENV RUST_LOG=info
ENV TINYOBS_ENV=production

ENTRYPOINT ["/entrypoint.sh"]
