ARG RUST_VERSION=1.85.1
ARG APP_NAME=trading-app

################################################################################
# Create a stage for building the application.

FROM debian:bullseye-slim AS build
ARG APP_NAME
WORKDIR /app

# Install dependencies for building Rust
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    ca-certificates \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install rustup and Cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable

# Add Cargo to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

ENV SQLX_OFFLINE=true

# Copy manifests first (for better caching of dependencies)
COPY Cargo.toml Cargo.lock ./

# Copy local dependencies and migrations (needed at compile time)
COPY crud_insertable ./crud_insertable
COPY crud_insertable_macro ./crud_insertable_macro
COPY crud_models ./crud_models
COPY spmc-ring ./spmc-ring
COPY .sqlx ./.sqlx
COPY migrations ./migrations

# Create a dummy main.rs to build dependencies
RUN mkdir src && \
    echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs && \
    cargo build --release --features="${CARGO_FEATURES}" && \
    rm -rf src

# Now copy the actual source code
COPY src ./src

# Build the real application
# Touch src/main.rs to force rebuild of the application code
RUN touch src/main.rs && \
    cargo build --locked --release --features="${CARGO_FEATURES}" && \
    cp target/release/$APP_NAME /bin/server

################################################################################
# Runtime stage
FROM debian:bullseye-slim AS final

# Install all necessary dependencies for TWS
RUN apt-get update && apt-get install -y \
    libxtst6 libxi6 libxrender1 libxrandr2 libfreetype6 libxext6 \
    libx11-6 libxfixes3 libxinerama1 libxcursor1 libgl1-mesa-glx \
    unzip xvfb curl \
    && rm -rf /var/lib/apt/lists/*

# Install IBC properly
COPY IBCLinux-3.21.2.zip /IBCLinux-3.21.2.zip
RUN unzip /IBCLinux-3.21.2.zip -d /IBCLinux-3.21.2/; \
    chown -R root /IBCLinux-3.21.2/; \
    chmod -u+x -R /IBCLinux-3.21.2/;
COPY IBCMacos-3.20.0/config.ini /IBCLinux-3.21.2/config.ini
COPY IBCMacos-3.20.0/scripts/ibcstart.sh /IBCLinux-3.21.2/scripts/ibcstart.sh
RUN chmod -u+x -R /IBCLinux-3.21.2/

# Copy the entrypoint script and set permissions
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy the executable from the "build" stage.
COPY --from=build /bin/server /bin/
COPY migrations /migrations

CMD ["sh", "entrypoint.sh"]
