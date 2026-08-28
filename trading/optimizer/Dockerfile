################################################################################
# Dockerfile.optimizer — builds the `optimize` binary (the optimizer crate,
# which depends on trading-app with --features backtest). Includes IBC + Xvfb
# (same as Dockerfile.backtest) because the optimizer's `load_market_data`
# calls `with_gateway_retry` → IBC → the IB Gateway GUI (needs a virtual X
# display) when the bars aren't pre-loaded.
#
# Context: the repo root (.) — needs trading-app/ (the lib dep + the IBC
# artifacts) + optimizer/.
#
# NB: `IBCLinux-3.21.2.zip` is gitignored (local) — must be present at
# `trading/trading-app/IBCLinux-3.21.2.zip` at build time (same as
# Dockerfile.backtest).
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

# ── Dep-caching layer ───────────────────────────────────────────────────────
# Copy ONLY manifests + the stable local path-dep crates (with their real
# src) + the .sqlx offline cache + migrations. Then stub a dummy entry point
# for trading-app's lib AND the optimizer's bin so cargo can compile ALL
# third-party deps (tokio, sqlx, ibapi, arc-swap, rayon, …) + the path-dep
# crates WITHOUT the real trading-app/optimizer source.
#
# trading-app is consumed as a path-dep LIB by the optimizer, so only its
# lib target is built here — its [[bin]] backtest is NOT built (and its
# src/bin/backtest.rs need not exist). An empty lib.rs compiles under any
# feature set; the `backtest` feature still pulls in arc-swap/atomic_float/
# rayon as optional deps, so they get cached too.
#
# This layer is a Docker layer-cache hit as long as Cargo.toml/Cargo.lock and
# the path-dep crates are unchanged → deps don't recompile on every source
# change. (Note: this helps cold builds and Cargo.toml changes; for the hot
# strategy-iteration loop you still recompile trading-app + optimizer from
# scratch each build unless target/ persists across builds — see notes.)
COPY trading/trading-app/Cargo.toml trading/trading-app/Cargo.lock ./trading-app/
COPY trading/trading-app/crud_insertable ./trading-app/crud_insertable
COPY trading/trading-app/crud_insertable_macro ./trading-app/crud_insertable_macro
COPY trading/trading-app/crud_models ./trading-app/crud_models
COPY trading/trading-app/spmc-ring ./trading-app/spmc-ring
COPY trading/trading-app/.sqlx ./trading-app/.sqlx
COPY trading/trading-app/migrations ./trading-app/migrations
COPY trading/optimizer/Cargo.toml ./optimizer/Cargo.toml
COPY trading/optimizer/Cargo.lock ./optimizer/Cargo.lock

# Stub dummy sources (empty trading-app lib + no-op optimizer bin), build to
# compile deps, then remove the stubs so the real source can be copied in.
RUN mkdir -p trading-app/src optimizer/src/bin && \
    echo "" > trading-app/src/lib.rs && \
    echo "fn main() {}" > optimizer/src/bin/optimize.rs && \
    cd optimizer && cargo build --release --bin optimize && \
    rm -rf trading-app/src optimizer/src

# ── Real source ─────────────────────────────────────────────────────────────
# Now copy the real trading-app + optimizer source. cargo sees all deps are
# already compiled (fingerprints match) → only trading-app + optimizer
# recompile from real source.
COPY trading/trading-app/src ./trading-app/src
COPY trading/optimizer/src ./optimizer/src

# Touch the optimizer bin to force a rebuild of the application code, then
# build the real binary and stage it for the runtime stage.
RUN cd optimizer && \
    touch src/bin/optimize.rs && \
    cargo build --release --bin optimize && \
    cp target/release/optimize /bin/optimize

################################################################################
# Runtime stage — IBC + Xvfb (same as Dockerfile.backtest, since the optimizer
# may need to populate data via IBKR when the bars aren't pre-loaded).
FROM debian:bullseye-slim AS final

# Install runtime deps: Xvfb + IBC libs (needed for the IBKR data-load step
# via with_gateway_retry) + postgresql-client (pg_isready) + curl.
RUN apt-get update && apt-get install -y \
    libxtst6 libxi6 libxrender1 libxrandr2 libfreetype6 libxext6 \
    libx11-6 libxfixes3 libxinerama1 libxcursor1 libgl1-mesa-glx \
    libasound2 libasound2-plugins libgbm1 libnss3 libxss1 \
    xdotool openbox \
    unzip xvfb \
    curl bash ca-certificates \
    postgresql-client \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install IBC (IB Gateway controller) — needed for the data-load step.
# with_gateway_retry (inside the optimize binary) boots IBC to seed
# market_data.historical_data; if IBC is absent, the loader falls back to Alpaca.
COPY trading/trading-app/IBCLinux-3.21.2.zip /IBCLinux-3.21.2.zip
RUN unzip /IBCLinux-3.21.2.zip -d /IBCLinux-3.21.2/; \
    chown -R root /IBCLinux-3.21.2/; \
    chmod -u+x -R /IBCLinux-3.21.2/
# The macOS config.ini + ibcstart.sh override the Linux IBC's (same workaround
# as Dockerfile.backtest).
COPY trading/trading-app/IBCMacos-3.20.0/config.ini /IBCLinux-3.21.2/config.ini
COPY trading/trading-app/IBCMacos-3.20.0/scripts/ibcstart.sh /IBCLinux-3.21.2/scripts/ibcstart.sh
RUN chmod -u+x -R /IBCLinux-3.21.2/

# Copy the optimize binary + entrypoint.
COPY --from=build /bin/optimize /bin/optimize
COPY trading/optimizer/entrypoint-optimize.sh /entrypoint-optimize.sh
RUN chmod +x /entrypoint-optimize.sh

ENV RUST_BACKTRACE=1
ENV RUST_LOG=info
ENV DISPLAY=:99

CMD ["sh", "/entrypoint-optimize.sh"]
