ARG RUST_VERSION=1.92.0
ARG APP_NAME=news-scraper

################################################################################
# Build Stage
FROM rust:${RUST_VERSION}-bookworm AS build
ARG APP_NAME
WORKDIR /app

# Install build dependencies (OpenSSL and pkg-config are usually needed for scrapers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

# 1. Dummy build for dependency caching
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release --locked && \
    rm -rf src target/release/.fingerprint/news-scraper*

# 2. Build actual source
COPY src ./src
RUN cargo build --locked --release && \
    cp target/release/$APP_NAME /bin/server

################################################################################
# Download Playwright Browsers
FROM mcr.microsoft.com/playwright:v1.43.0-jammy AS browser-helper

################################################################################
# Runtime Stage
FROM debian:trixie-slim AS final

# Install runtime dependencies
# We add the libraries required to run Chromium in headless mode
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    openssl \
    libssl3t64 \
    # Chromium dependencies
    libglib2.0-0t64 \
    libnss3 \
    libatk1.0-0t64 \
    libatk-bridge2.0-0t64 \
    libcups2t64 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2t64 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-privileged user
RUN useradd -ms /bin/bash appuser

# Copy executable
COPY --from=build --chown=appuser:appuser /bin/server /bin/

# Install npm for playwright; browsers come from the browser-helper stage below.
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g playwright@1.43.0

# Ensure the appuser has permissions to write to the directory 
# where Playwright downloads browsers (usually ~/.cache/ms-playwright)
ENV PLAYWRIGHT_BROWSERS_PATH=/home/appuser/.cache/ms-playwright
RUN mkdir -p $PLAYWRIGHT_BROWSERS_PATH && chown -R appuser:appuser /home/appuser
COPY --from=browser-helper /ms-playwright $PLAYWRIGHT_BROWSERS_PATH

RUN mkdir -p /data/scraped_articles && \
    chown -R appuser:appuser /data && \
    chmod -R 755 /data

USER appuser
WORKDIR /home/appuser
ENV HOME=/home/appuser

CMD ["/bin/server"]
