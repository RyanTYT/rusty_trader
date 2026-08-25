################################################################################
# Dockerfile.optimizer — builds the `optimize` binary (the optimizer crate,
# which depends on trading-app with --features backtest). Lighter than
# Dockerfile.backtest: no IBC/Xvfb (the optimizer assumes the data is
# pre-loaded via a prior `backtester` run, OR uses the Alpaca fallback).
#
# Context: the repo root (.) — needs both trading-app/ + optimizer/.
################################################################################

FROM debian:bullseye-slim AS build
WORKDIR /app

# Build deps (clang/lld for ibapi sync client).
RUN apt-get update && apt-get install -y \
    curl build-essential pkg-config libssl-dev ca-certificates \
    protobuf-compiler clang lld musl-dev git \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"
ENV SQLX_OFFLINE=true

# Copy trading-app (the lib dep) + its local deps + .sqlx cache + migrations.
COPY trading/trading-app/Cargo.toml trading/trading-app/Cargo.lock ./trading-app/
COPY trading/trading-app/crud_insertable ./trading-app/crud_insertable
COPY trading/trading-app/crud_insertable_macro ./trading-app/crud_insertable_macro
COPY trading/trading-app/crud_models ./trading-app/crud_models
COPY trading/trading-app/spmc-ring ./trading-app/spmc-ring
COPY trading/trading-app/.sqlx ./trading-app/.sqlx
COPY trading/trading-app/migrations ./trading-app/migrations

# Copy the optimizer crate.
COPY trading/optimizer/Cargo.toml ./optimizer/Cargo.toml
COPY trading/optimizer/Cargo.lock ./optimizer/Cargo.lock
COPY trading/optimizer/src ./optimizer/src

# Build (trading-app is built with --features backtest via the path dep).
RUN cd optimizer && cargo build --release --bin optimize \
    && cp target/release/optimize /bin/optimize

################################################################################
# Runtime stage — light (no IBC, no Xvfb).
FROM debian:bullseye-slim AS final

RUN apt-get update && apt-get install -y \
    postgresql-client ca-certificates libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /bin/optimize /bin/optimize
COPY trading/optimizer/entrypoint-optimize.sh /entrypoint-optimize.sh
RUN chmod +x /entrypoint-optimize.sh

ENV RUST_BACKTRACE=1
ENV RUST_LOG=info

CMD ["sh", "/entrypoint-optimize.sh"]
