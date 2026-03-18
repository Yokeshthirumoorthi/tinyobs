# Build stage — run from workspace root: docker build -f crates/tinyobs-lite/Dockerfile .
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# Copy workspace manifests
COPY Cargo.toml Cargo.lock ./
COPY crates/tinyobs-core/Cargo.toml crates/tinyobs-core/Cargo.toml
COPY crates/tinyobs-lite/Cargo.toml crates/tinyobs-lite/Cargo.toml

# Create dummy sources to cache dependencies
RUN mkdir -p crates/tinyobs-core/src && \
    echo "pub fn main() {}" > crates/tinyobs-core/src/lib.rs && \
    mkdir -p crates/tinyobs-lite/src/bin && \
    echo "pub fn main() {}" > crates/tinyobs-lite/src/lib.rs && \
    echo "fn main() {}" > crates/tinyobs-lite/src/bin/server.rs && \
    mkdir -p crates/tinyobs-pro/src/bin && \
    printf '[package]\nname = "tinyobs-pro"\nversion = "0.1.0"\nedition = "2021"\n' > crates/tinyobs-pro/Cargo.toml && \
    echo "pub fn main() {}" > crates/tinyobs-pro/src/lib.rs && \
    echo "fn main() {}" > crates/tinyobs-pro/src/bin/server.rs

# Pin crates to versions compatible with current rustc
RUN cargo update time --precise 0.3.36 && \
    cargo update comfy-table --precise 7.1.1

# Build dependencies only
RUN cargo build --release -p tinyobs && \
    rm -rf crates

# Copy actual source code
COPY crates/tinyobs-core crates/tinyobs-core
COPY crates/tinyobs-lite crates/tinyobs-lite

# Stub tinyobs-pro for workspace resolution
RUN mkdir -p crates/tinyobs-pro/src/bin && \
    printf '[package]\nname = "tinyobs-pro"\nversion = "0.1.0"\nedition = "2021"\n' > crates/tinyobs-pro/Cargo.toml && \
    echo "pub fn main() {}" > crates/tinyobs-pro/src/lib.rs && \
    echo "fn main() {}" > crates/tinyobs-pro/src/bin/server.rs

# Build the actual binary
RUN touch crates/tinyobs-core/src/lib.rs crates/tinyobs-lite/src/lib.rs crates/tinyobs-lite/src/bin/server.rs && \
    cargo build --release --bin tinyobs-server

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/tinyobs-server /app/tinyobs
COPY crates/tinyobs-lite/migrations ./migrations
COPY crates/tinyobs-lite/configuration ./configuration

RUN mkdir -p /app/data

EXPOSE 4318

ENV RUST_LOG=info
ENV TINYOBS_ENV=production

CMD ["/app/tinyobs"]
