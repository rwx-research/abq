##
## Based on https://github.com/fly-apps/hello-rust/blob/main/Dockerfile

FROM rust:latest as builder

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY . ./

# Will build all dependent crates in release mode
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/app/target \
    cargo build -p abq --release

# Install the abq binary
RUN cargo install --path crates/abq_cli

# Runtime image
FROM debian:bullseye-slim

# Run as "app" user
RUN useradd -ms /bin/bash app

USER app
WORKDIR /app

# Get compiled binaries from the builder
COPY --from=builder /usr/local/cargo/bin/abq /app/abq
COPY --from=builder /usr/src/app/bin/deploy_prod /app/deploy_prod
COPY --from=builder /usr/src/app/bin/abq_server_token /app/abq_server_token
COPY --from=builder /usr/src/app/bin/health /app/health
