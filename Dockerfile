# Build stage — tinyobs lite (embedded chdb)
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# Copy manifest
COPY Cargo.toml Cargo.lock ./

# Create dummy sources to cache dependencies
RUN mkdir -p src && \
    echo "pub fn main() {}" > src/lib.rs && \
    mkdir -p src/bin && \
    echo "fn main() {}" > src/bin/tinyobs-server.rs

# Build dependencies only
RUN cargo build --release --bin tinyobs-server && \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build the actual binary
RUN touch src/lib.rs src/bin/tinyobs-server.rs && \
    cargo build --release --bin tinyobs-server

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary
COPY --from=builder /app/target/release/tinyobs-server /app/tinyobs

# Copy configuration
COPY configuration ./configuration

# Create data directory
RUN mkdir -p /app/data

# Expose the default port
EXPOSE 4318

# Set environment variables
ENV RUST_LOG=info
ENV TINYOBS_ENV=production

CMD ["/app/tinyobs"]
