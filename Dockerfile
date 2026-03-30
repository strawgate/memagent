# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN --mount=type=cache,target=/src/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    CARGO_BUILD_RUSTC_WRAPPER="" \
    cargo build --release --bin logfwd && \
    strip target/release/logfwd && \
    cp target/release/logfwd /logfwd

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /logfwd /usr/local/bin/logfwd
EXPOSE 9090
# Health checks should be configured at the orchestrator level (e.g. k8s
# liveness/readiness probes) since distroless images have no shell.
ENTRYPOINT ["logfwd"]
