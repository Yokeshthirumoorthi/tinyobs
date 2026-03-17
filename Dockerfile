# Build stage
FROM rust:1.83-bookworm AS builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy source to cache dependencies
RUN mkdir src && \
    echo "pub fn main() {}" > src/lib.rs && \
    mkdir examples && \
    echo "fn main() {}" > examples/standalone.rs

# Build dependencies only
RUN cargo build --release --example standalone && \
    rm -rf src examples

# Copy actual source code
COPY src ./src
COPY examples ./examples
COPY migrations ./migrations

# Build the actual binary
RUN touch src/lib.rs examples/standalone.rs && \
    cargo build --release --example standalone

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary
COPY --from=builder /app/target/release/examples/standalone /app/tinyobs

# Copy migrations
COPY migrations ./migrations

# Create data directory
RUN mkdir -p /app/data

# Expose the default port
EXPOSE 4319

# Set environment variables
ENV RUST_LOG=info

CMD ["/app/tinyobs"]
