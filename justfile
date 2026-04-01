# logfwd development task runner
# Install: cargo install just
# Usage:  just --list

# Default recipe: run all checks (same as CI)
default: ci

# Format all Rust code
fmt:
    cargo fmt

# Check formatting without modifying files
fmt-check:
    cargo fmt --check

# Run clippy lints
clippy:
    cargo clippy -- -D warnings

# Run all tests
test:
    cargo test

# Run all tests with nextest (parallel, faster)
nextest:
    cargo nextest run

# Lint everything: format, clippy, TOML, deny (matches CI Lint job)
lint: fmt-check clippy toml-check deny

# Full CI suite: lint + test (run before pushing)
ci: lint test

# Check TOML formatting (Cargo.toml, etc.)
toml-check:
    taplo check

# Format TOML files
toml-fmt:
    taplo fmt

# Audit dependencies for vulnerabilities, licenses, and duplicates
deny:
    cargo deny check

# Build the diagnostics dashboard (Preact + TypeScript → single HTML file)
# Requires Node.js. Output: crates/logfwd-io/src/dashboard.html
# Must run before cargo build/test/clippy (CI does this automatically).
dashboard:
    cd dashboard && npm install --prefer-offline && npm run build



# Build release binary
build:
    cargo build --release

# Build release binary with Profile-Guided Optimisation (PGO).
# Runs a training workload automatically; output binary is target/release/logfwd-pgo.
# Requires llvm-profdata on PATH (e.g. `sudo apt install llvm`).
build-pgo:
    #!/usr/bin/env bash
    set -euo pipefail
    PGO_DIR=$(mktemp -d)
    echo "==> Step 1: build with PGO instrumentation (profile-generate)"
    RUSTFLAGS="-Cprofile-generate=${PGO_DIR}" cargo build --release -p logfwd
    echo "==> Step 2: run training workload to collect profiles"
    LOGFWD=./target/release/logfwd cargo run -p logfwd-competitive-bench --release -- \
        --lines 500000 --mode binary --cpus 1 --memory 1g \
        --scenarios passthrough,json_parse,filter
    echo "==> Step 3: merge raw profiles"
    llvm-profdata merge -output="${PGO_DIR}/merged.profdata" "${PGO_DIR}"/*.profraw
    echo "==> Step 4: rebuild with PGO profile applied"
    RUSTFLAGS="-Cprofile-use=${PGO_DIR}/merged.profdata" cargo build --release -p logfwd
    cp target/release/logfwd target/release/logfwd-pgo
    echo "PGO binary written to target/release/logfwd-pgo"

# Run criterion microbenchmarks
bench:
    cargo bench -p logfwd-bench

# Run competitive benchmarks (binary mode, local dev)
bench-competitive *ARGS:
    cargo run -p logfwd-competitive-bench --release -- {{ARGS}}

# Run competitive benchmarks in Docker with profiling
bench-docker:
    cargo build --release -p logfwd
    cargo build --release --features dhat-heap -p logfwd
    cp target/release/logfwd target/release/logfwd-dhat
    cargo build --release -p logfwd
    LOGFWD=./target/release/logfwd cargo run -p logfwd-competitive-bench --release -- \
        --lines 5000000 --docker --cpus 1 --memory 1g --markdown \
        --profile ./profiles --dhat-binary ./target/release/logfwd-dhat

# Run a local File -> OTLP profile with pprof-rs.
# Outputs a temp directory containing config.yaml, logs.json, pipeline.log,
# blackhole.log, and flamegraph.svg.
profile-otlp-local lines="500000" seconds="6":
    #!/usr/bin/env bash
    set -euo pipefail
    ROOT=$(mktemp -d /tmp/logfwd-pprof.XXXXXX)
    PORT=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')

    echo "==> Build cpu-profiling binary"
    RUSTC_WRAPPER= cargo build --release --features cpu-profiling -p logfwd

    mkdir -p "${ROOT}/bin"
    cp target/release/logfwd "${ROOT}/bin/logfwd-prof"

    echo "==> Generate test data ({{lines}} lines)"
    "${ROOT}/bin/logfwd-prof" --generate-json "{{lines}}" "${ROOT}/logs.json"

    printf '%s\n' \
      "input:" \
      "  type: file" \
      "  path: ${ROOT}/logs.json" \
      "  format: json" \
      "transform: |" \
      "  SELECT * FROM logs" \
      "output:" \
      "  type: otlp" \
      "  endpoint: http://127.0.0.1:${PORT}" \
      "  protocol: http" \
      "  compression: zstd" \
      > "${ROOT}/config.yaml"

    echo "==> Start blackhole on ${PORT}"
    "${ROOT}/bin/logfwd-prof" --blackhole "127.0.0.1:${PORT}" > "${ROOT}/blackhole.log" 2>&1 &
    BLACKHOLE_PID=$!
    cleanup() {
        kill -TERM "${BLACKHOLE_PID}" 2>/dev/null || true
    }
    trap cleanup EXIT

    echo "==> Run profiled pipeline for {{seconds}}s"
    pushd "${ROOT}" >/dev/null
    "${ROOT}/bin/logfwd-prof" --config "${ROOT}/config.yaml" > pipeline.log 2>&1 &
    PIPELINE_PID=$!
    sleep "{{seconds}}"
    kill -TERM "${PIPELINE_PID}"
    wait "${PIPELINE_PID}" || true
    popd >/dev/null

    echo "==> Output directory: ${ROOT}"
    ls -lah "${ROOT}"
    if [ -f "${ROOT}/flamegraph.svg" ]; then
        du -h "${ROOT}/flamegraph.svg"
    else
        echo "flamegraph.svg missing"
        exit 1
    fi

# Generate microbenchmark report (markdown)
bench-report:
    cargo run -p logfwd-bench

# Run low-and-slow rate-ingest benchmark (logfwd only, measures memory and CPU at each eps)
bench-rate *ARGS:
    cargo build --release -p logfwd
    LOGFWD=./target/release/logfwd cargo run -p logfwd-competitive-bench --release -- --rate-bench {{ARGS}}

# Install development tools
install-tools:
    cargo install taplo-cli cargo-deny
    @echo "Optional: cargo install cargo-nextest inferno"
    @echo "Install just: https://just.systems/man/en/installation.html"

# Set up git pre-commit hook
install-hooks:
    @echo '#!/bin/sh' > .git/hooks/pre-commit
    @echo 'set -e' >> .git/hooks/pre-commit
    @echo 'cargo fmt --check' >> .git/hooks/pre-commit
    @echo 'cargo clippy -- -D warnings' >> .git/hooks/pre-commit
    @chmod +x .git/hooks/pre-commit
    @echo "Pre-commit hook installed (.git/hooks/pre-commit)"

# Remove git pre-commit hook
uninstall-hooks:
    @rm -f .git/hooks/pre-commit
    @echo "Pre-commit hook removed"

