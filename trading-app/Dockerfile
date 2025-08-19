# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/go/dockerfile-reference/

# Want to help us make this template better? Share your feedback here: https://forms.gle/ybq9Krt8jtBL3iCk7

ARG RUST_VERSION=1.85.1
ARG APP_NAME=trading-app

################################################################################
# Create a stage for building the application.

# FROM rust:${RUST_VERSION}-alpine AS build
FROM debian:bullseye-slim AS build
ARG APP_NAME
WORKDIR /app

# Install host build dependencies.
# RUN apk add --no-cache clang lld musl-dev git \
#    openssl-dev \
#    pkgconfig \
#    make

# Install dependencies for building Rust
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

# COPY crud_models/. crud_models/
# COPY crud_insertable /crud_insertable
# COPY .sqlx /.sqlx
# Build the application.
# Leverage a cache mount to /usr/local/cargo/registry/
# for downloaded dependencies, a cache mount to /usr/local/cargo/git/db
# for git repository dependencies, and a cache mount to /app/target/ for
# compiled dependencies which will speed up subsequent builds.
# Leverage a bind mount to the src directory to avoid having to copy the
# source code into the container. Once built, copy the executable to an
# output directory before the cache mounted /app/target is unmounted.
ENV SQLX_OFFLINE=true
# RUN --mount=type=bind,source=.,target=/app \
#     --mount=type=cache,target=/target/ \
#     --mount=type=cache,target=/usr/local/cargo/git/db \
#     --mount=type=cache,target=/usr/local/cargo/registry/ \
#     # cargo build --locked --release && \
#     # cp ./target/release/$APP_NAME /bin/server
#     # cargo install sqlx-cli --locked --no-default-features --features postgres && \
#     cd app \
#     cargo build --locked --release && \
#     cp ../target/release/$APP_NAME /bin/server
RUN --mount=type=bind,source=.,target=/app \
    --mount=type=bind,source=Cargo.toml,target=/app/Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=/app/Cargo.lock \
    --mount=type=cache,target=/target \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry \
    WORKDIR=/app \
    cargo build --locked --release --target-dir /target && \
    cp /target/release/$APP_NAME /bin/server

################################################################################
# Create a new stage for running the application that contains the minimal
# runtime dependencies for the application. This often uses a different base
# image from the build stage where the necessary files are copied from the build
# stage.
#
# The example below uses the alpine image as the foundation for running the app.
# By specifying the "3.18" tag, it will use version 3.18 of alpine. If
# reproducibility is important, consider using a digest
# (e.g., alpine@sha256:664888ac9cfd28068e062c991ebcff4b4c7307dc8dd4df9e728bedde5c449d91).
# FROM alpine:3.18 AS final
FROM debian:bullseye-slim AS final

# Install all necessary dependencies for TWS
RUN apt-get update && apt-get install -y \
    libxtst6 libxi6 libxrender1 libxrandr2 libfreetype6 libxext6 \
    libx11-6 libxfixes3 libxinerama1 libxcursor1 libgl1-mesa-glx \
    unzip xvfb curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/go/dockerfile-user-best-practices/
# ARG UID=10001
# RUN adduser \
#     --disabled-password \
#     --gecos "" \
#     --home "/nonexistent" \
#     --shell "/sbin/nologin" \
#     --no-create-home \
#     --uid "${UID}" \
#     appuser
# USER appuser

# Install IBC properly
COPY IBCLinux-3.21.2.zip /IBCLinux-3.21.2.zip
RUN unzip /IBCLinux-3.21.2.zip -d /IBCLinux-3.21.2/; \
    chown -R root /IBCLinux-3.21.2/; \
    chmod -u+x -R /IBCLinux-3.21.2/;
COPY IBCMacos-3.20.0/config.ini /IBCLinux-3.21.2/config.ini
COPY IBCMacos-3.20.0/scripts/ibcstart.sh /IBCLinux-3.21.2/scripts/ibcstart.sh
RUN chmod -u+x -R /IBCLinux-3.21.2/

# Copy the entrypoint script
COPY --chmod=755 entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy the executable from the "build" stage.
COPY --from=build /bin/server /bin/
COPY migrations /migrations

# What the container should run when it is started.
# CMD ["/bin/server"]
CMD ["sh", "entrypoint.sh"]
