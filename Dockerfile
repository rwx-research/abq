FROM lukemathwalker/cargo-chef:latest-rust-1.59.0 AS chef
WORKDIR app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin abq

# Runtime image
FROM debian:bullseye-slim

# Run as "app" user
RUN useradd -ms /bin/bash app
USER app
WORKDIR /app

# Get compiled binaries from the builder
COPY --from=builder /app/target/release/abq /app/abq
COPY --from=builder /app/bin/abq_server_token /app/abq_server_token
COPY --from=builder /app/bin/deploy_prod /app/deploy_prod
COPY --from=builder /app/bin/health /app/health
COPY --from=builder /app/bin/deploy_staging /app/deploy_staging
COPY --from=builder /app/bin/health_staging /app/health_staging
