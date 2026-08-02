# syntax=docker/dockerfile:1
FROM rust:1.96-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked \
    && cp /src/target/release/comradex /tmp/comradex

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /tmp/comradex /usr/local/bin/comradex
USER 65532:65532
ENTRYPOINT ["comradex"]
CMD ["--config", "/config/comradex.toml", "serve"]
