# Build stage
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy source to cache dependencies
RUN mkdir src && \
    echo "pub fn main() {}" > src/lib.rs && \
    mkdir -p src/bin && \
    echo "fn main() {}" > src/bin/server.rs

# Pin crates to versions compatible with current rustc
RUN cargo update time --precise 0.3.36 && \
    cargo update comfy-table --precise 7.1.1

# Build dependencies only
RUN cargo build --release --bin tinyobs-server && \
    rm -rf src

# Copy actual source code
COPY src ./src
COPY migrations ./migrations
COPY configuration ./configuration

# Build the actual binary
RUN touch src/lib.rs src/bin/server.rs && \
    cargo build --release --bin tinyobs-server

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary
COPY --from=builder /app/target/release/tinyobs-server /app/tinyobs

# Copy migrations
COPY migrations ./migrations

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
