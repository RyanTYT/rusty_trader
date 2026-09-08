ARG RUST_VERSION=1.85.1
ARG APP_NAME=trading-app
################################################################################
# Bullseye went fully EOL on 2026-08-31 - security.debian.org/deb.debian.org
# have started pruning bullseye-security package files (that's the 404s),
# not just letting the Release file's signature expire. `debian/eol:bullseye`
# is Docker's official replacement image for exactly this situation: it's
# Bullseye pre-pointed at archive.debian.org, maintained by the same team
# that builds the regular debian image.
FROM debian/eol:bullseye AS build
ARG APP_NAME
WORKDIR /app

# debian/eol:bullseye redirects the main suite to archive.debian.org but
# still points bullseye-security at the live deb.debian.org/debian-security -
# whose package *index* still lists files that have already been deleted
# from the live pool now that Bullseye's LTS ended (2026-08-31). Drop
# bullseye-security entirely and pull everything from archive.debian.org
# instead, which has the full historical pool and actually still serves
# the files apt asks for. This means slightly older (pre-final-security-
# patch) builds of a handful of packages, not the live -security versions.
RUN echo "deb http://archive.debian.org/debian bullseye main contrib non-free" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian bullseye-updates main contrib non-free" >> /etc/apt/sources.list

# Without bullseye-security, archive.debian.org's main+updates indexes have
# newer runtime libs (libc6, libssl1.1, perl-base) than the -dev/perl builds
# available there support - those later -dev builds only ever shipped via
# -security. libc6/libssl1.1/perl-base are built as an exact-version-matched
# set with their -dev/perl counterparts, so pin the runtime libs down to the
# versions that actually have a matching -dev build (archive.debian.org is a
# frozen snapshot, so these version numbers are permanent, not a moving
# target) before installing the -dev/full packages that need them.
RUN apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --allow-downgrades \
      libc6=2.31-13+deb11u11 \
      libssl1.1=1.1.1w-0+deb11u1 \
      perl-base=5.32.1-4+deb11u3 && \
    apt-get install -y --no-install-recommends \
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
ENV CARGO_FEATURES=hotpath,hotpath-alloc,hotpath-cpu
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
FROM debian/eol:bullseye AS final

# debian/eol:bullseye redirects the main suite to archive.debian.org but
# still points bullseye-security at the live deb.debian.org/debian-security -
# whose package *index* still lists files that have already been deleted
# from the live pool now that Bullseye's LTS ended (2026-08-31). Drop
# bullseye-security entirely and pull everything from archive.debian.org
# instead, which has the full historical pool and actually still serves
# the files apt asks for. This means slightly older (pre-final-security-
# patch) builds of a handful of packages, not the live -security versions.
RUN echo "deb http://archive.debian.org/debian bullseye main contrib non-free" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian bullseye-updates main contrib non-free" >> /etc/apt/sources.list

# Install all necessary dependencies for TWS
RUN apt-get -o Acquire::Check-Valid-Until=false update && apt-get install -y \
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
