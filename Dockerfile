# Build stage: heavy environment with compiler and source
FROM rust:1.99 AS builder
WORKDIR /app

# Copy dependency manifests first to leverage Docker layer caching
COPY Cargo.toml Cargo.lock ./
RUN cargo fetch

# Copy source code after dependencies are fetched
COPY . .
RUN cargo build --release

# Runtime stage: minimal environment for execution
FROM debian:trixie-slim
WORKDIR /app

# Install tools available to startup.command. These can be used by its
# startup script for Wake-on-LAN, SSH, or ICMP ping.
RUN apt-get update && apt-get install -y --no-install-recommends \
    wakeonlan \
    openssh-client \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Copy only the compiled binary from the builder stage
COPY --from=builder /app/target/release/wakemc /usr/local/bin/

# Run the binary directly without a shell
ENTRYPOINT ["wakemc"]