FROM rust:1-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates/ crates/
COPY benches/ benches/
ENV RUSTFLAGS="-C target-cpu=native"
RUN cargo build --release --bin logfwd

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/logfwd /usr/local/bin/logfwd
ENTRYPOINT ["logfwd"]
