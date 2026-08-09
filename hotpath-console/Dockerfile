FROM rust:1.85.1-slim-bullseye AS build
RUN cargo install hotpath --features=tui --version '^0.23'

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/cargo/bin/hotpath /usr/local/bin/hotpath
ENTRYPOINT ["hotpath"]
CMD ["console"]
