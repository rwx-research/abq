# https://hub.docker.com/layers/rustlang/rust/nightly-bullseye-slim/images/sha256-bcfd21c359b6f64f40cb40a0284b2ed573695dc5910dd9db0367630fa285af4c
FROM rustlang/rust:nightly-bullseye@sha256:2ce116186d98dec86ea2c84eeeb3d5f12af456bca675b62872e1ac751c5696cf AS chef
RUN cargo +nightly install cargo-chef
WORKDIR app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

ENV RUSTFLAGS="-Zoom=panic"
RUN cargo +nightly chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this is the caching Docker layer!
ENV RUSTFLAGS="-Zoom=panic"
RUN cargo +nightly chef cook --release --recipe-path recipe.json

# Build application
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY .cargo ./.cargo

ENV RUSTFLAGS="-Zoom=panic"
RUN cargo +nightly build --release --bin abq

# Runtime image
FROM debian:bullseye-slim

# Run as "app" user
RUN useradd -ms /bin/bash app
USER app
WORKDIR /app

# Get compiled binaries from the builder
COPY --from=builder /app/target/release/abq /app/abq
COPY crates/abq_utils/data/cert/server.crt /app/server.crt
COPY crates/abq_utils/data/cert/server.key /app/server.key
COPY bin/abq_user_token /app/abq_user_token
COPY bin/abq_admin_token /app/abq_admin_token
COPY bin/abq_api_key /app/abq_api_key
COPY bin/deploy_prod /app/deploy_prod
COPY bin/health /app/health
COPY bin/deploy_staging /app/deploy_staging
COPY bin/health_staging /app/health_staging
