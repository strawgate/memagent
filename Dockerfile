# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS builder
# Allow overriding RUSTFLAGS at build time (e.g. --build-arg RUSTFLAGS="-C target-cpu=x86-64-v3").
# The default is empty to produce portable binaries that run on any x86-64 host.
ARG RUSTFLAGS=""
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN --mount=type=cache,target=/src/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    CARGO_BUILD_RUSTC_WRAPPER="" \
    RUSTFLAGS="${RUSTFLAGS}" \
    cargo build --release -p ffwd --bin ff && \
    strip target/release/ff && \
    cp target/release/ff /ff

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /ff /usr/local/bin/ff
USER nonroot
EXPOSE 9090
# Health checks should be configured at the orchestrator level (e.g. k8s
# liveness/readiness probes) since distroless images have no shell.
ENTRYPOINT ["ff"]
