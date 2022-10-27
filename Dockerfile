FROM lukemathwalker/cargo-chef:latest-rust-1.62.1 AS chef
WORKDIR app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY .cargo ./.cargo
COPY bin/abq_api_key ./bin/abq_api_key
COPY bin/abq_server_token ./bin/abq_server_token
RUN cargo build --release --bin abq

# Runtime image
FROM debian:bullseye-slim

# Run as "app" user
RUN useradd -ms /bin/bash app
USER app
WORKDIR /app

# Get compiled binaries from the builder
COPY --from=builder /app/target/release/abq /app/abq
COPY bin/abq_user_token /app/abq_user_token
COPY bin/abq_admin_token /app/abq_admin_token
COPY bin/deploy_prod /app/deploy_prod
COPY bin/health /app/health
COPY bin/deploy_staging /app/deploy_staging
COPY bin/health_staging /app/health_staging
