ARG RUST_VERSION=1.85.1
ARG APP_NAME=news-scraper

################################################################################
# Build Stage
FROM debian:bullseye-slim AS build
# FROM rust:${RUST_VERSION}-slim-bookworm AS build
ARG APP_NAME
WORKDIR /app

# Install build dependencies (OpenSSL and pkg-config are usually needed for scrapers)
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install rustup and Cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable

# Add Cargo to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# 1. Dummy build for dependency caching
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
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
FROM debian:bullseye-slim AS final

# Install runtime dependencies
# We add the libraries required to run Chromium in headless mode
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    openssl \
    libssl-dev \
    # Chromium dependencies
    libglib2.0-0 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
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
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-privileged user
RUN useradd -ms /bin/bash appuser

# Copy executable
COPY --from=build --chown=appuser:appuser /bin/server /bin/

# Install npm for playwright
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
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
