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

# Run Kani formal verification proofs (logfwd-core only)
# Requires: cargo install --locked kani-verifier && cargo kani setup
kani:
    RUSTC_WRAPPER="" cargo kani -p logfwd-core -Z function-contracts -Z mem-predicates -Z stubbing

# Run all tests with nextest (parallel, faster output)
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



# Extended testing: 10K proptest cases + turmoil simulation
test-extended:
    PROPTEST_CASES=10000 cargo nextest run --profile ci
    cargo test -p logfwd --features turmoil --test turmoil_sim

# Build release binary
build:
    cargo build --release

# ---------------------------------------------------------------------------
# End-to-end pipeline benchmarks (bench/scenarios/*.yaml)
# ---------------------------------------------------------------------------

# Helper: run a single pipeline for N seconds, print stats from diagnostics.
[private]
_bench-run name config seconds="10" diag="http://127.0.0.1:9090":
    #!/usr/bin/env bash
    set -euo pipefail
    LOGFWD=./target/release/logfwd
    $LOGFWD --config {{config}} &
    PID=$!
    sleep {{seconds}}
    STATS=$(curl -s {{diag}}/api/stats 2>/dev/null || echo '{}')
    kill $PID 2>/dev/null; wait $PID 2>/dev/null || true
    echo "$STATS" | python3 -c "
    import sys,json; d=json.load(sys.stdin)
    up=d.get('uptime_sec',0)
    li=d.get('input_lines',0)
    lps=li/up if up>0 else 0
    print(f'  {\"{{name}}\":.<20s} {li:>12,} lines  {lps:>12,.0f} lines/sec  ({up:.1f}s)')
    " 2>/dev/null || echo "  {{name}}: no stats"

# Helper: run sender+receiver pair, report receiver stats.
[private]
_bench-pair name rx_config tx_config seconds="10":
    #!/usr/bin/env bash
    set -euo pipefail
    LOGFWD=./target/release/logfwd
    $LOGFWD --config {{rx_config}} &
    RX=$!; sleep 1
    $LOGFWD --config {{tx_config}} &
    TX=$!; sleep {{seconds}}
    STATS=$(curl -s http://127.0.0.1:9091/api/stats 2>/dev/null || echo '{}')
    kill $TX $RX 2>/dev/null; wait $TX $RX 2>/dev/null || true
    echo "$STATS" | python3 -c "
    import sys,json; d=json.load(sys.stdin)
    up=d.get('uptime_sec',0)
    li=d.get('input_lines',0)
    lps=li/up if up>0 else 0
    print(f'  {\"{{name}}\":.<20s} {li:>12,} lines  {lps:>12,.0f} lines/sec  ({up:.1f}s)')
    " 2>/dev/null || echo "  {{name}}: no stats"

# Self-contained: generator → SQL filter → null (no network)
bench-self seconds="10":
    @echo "==> Self benchmark (generator → filter → null)"
    just _bench-run self bench/scenarios/self-bench.yaml {{seconds}}

# Throughput ceiling: generator → SELECT * → null (no filter, no network)
bench-ceiling-self seconds="10":
    @echo "==> Ceiling benchmark (generator → passthrough → null)"
    just _bench-run ceiling bench/scenarios/self-ceiling.yaml {{seconds}}

# TCP end-to-end
bench-tcp seconds="10":
    @echo "==> TCP benchmark (generator → tcp → tcp → null)"
    just _bench-pair tcp bench/scenarios/tcp-receiver.yaml bench/scenarios/tcp-sender.yaml {{seconds}}

# UDP end-to-end
bench-udp seconds="10":
    @echo "==> UDP benchmark (generator → udp → udp → null)"
    just _bench-pair udp bench/scenarios/udp-receiver.yaml bench/scenarios/udp-sender.yaml {{seconds}}

# OTLP end-to-end
bench-otlp seconds="10":
    @echo "==> OTLP benchmark (generator → otlp → otlp_receiver → null)"
    just _bench-pair otlp bench/scenarios/otlp-receiver.yaml bench/scenarios/otlp-sender.yaml {{seconds}}

# Elasticsearch end-to-end: starts ES in Docker, sends generator output to it, then stops ES.
# Requires Docker.  Skips gracefully if Docker or the ES image is unavailable.
bench-es seconds="10":
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v docker &>/dev/null; then
        echo "==> Docker not found — skipping ES benchmark"
        exit 0
    fi
    echo "==> Starting Elasticsearch (Docker)"
    docker compose -f examples/elasticsearch/docker-compose.yml up -d
    echo "==> Waiting for Elasticsearch health (up to 120s)..."
    for i in $(seq 1 60); do
        if curl -sf http://localhost:9200/_cluster/health > /dev/null 2>&1; then
            echo "    Elasticsearch ready (${i}×2s)"
            break
        fi
        sleep 2
    done
    if ! curl -sf http://localhost:9200/_cluster/health > /dev/null 2>&1; then
        echo "ERROR: Elasticsearch did not become healthy in time"
        docker compose -f examples/elasticsearch/docker-compose.yml down
        exit 1
    fi
    cargo build --release -p logfwd
    echo "==> ES benchmark (generator → elasticsearch)"
    just _bench-run es bench/scenarios/es-sender.yaml {{seconds}}
    echo "==> Stopping Elasticsearch"
    docker compose -f examples/elasticsearch/docker-compose.yml down

# Elasticsearch end-to-end using streaming request bodies.
# Requires Docker. Skips gracefully if Docker or the ES image is unavailable.
bench-es-streaming seconds="10":
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v docker &>/dev/null; then
        echo "==> Docker not found — skipping streaming ES benchmark"
        exit 0
    fi
    echo "==> Starting Elasticsearch (Docker)"
    docker compose -f examples/elasticsearch/docker-compose.yml up -d
    echo "==> Waiting for Elasticsearch health (up to 120s)..."
    for i in $(seq 1 60); do
        if curl -sf http://localhost:9200/_cluster/health > /dev/null 2>&1; then
            echo "    Elasticsearch ready (${i}×2s)"
            break
        fi
        sleep 2
    done
    if ! curl -sf http://localhost:9200/_cluster/health > /dev/null 2>&1; then
        echo "ERROR: Elasticsearch did not become healthy in time"
        docker compose -f examples/elasticsearch/docker-compose.yml down
        exit 1
    fi
    cargo build --release -p logfwd
    echo "==> ES streaming benchmark (generator → elasticsearch)"
    just _bench-run es-streaming bench/scenarios/es-sender-streaming.yaml {{seconds}}
    echo "==> Stopping Elasticsearch"
    docker compose -f examples/elasticsearch/docker-compose.yml down

# Run all pipeline benchmarks (alias: bench-pipelines)
bench-e2e seconds="10":
    just bench-pipelines {{seconds}}

[private]
bench-pipelines seconds="10":
    @echo "logfwd pipeline benchmarks ({{seconds}}s each)"
    @echo "================================================"
    cargo build --release -p logfwd
    just bench-self {{seconds}}
    just bench-tcp {{seconds}}
    just bench-udp {{seconds}}
    just bench-otlp {{seconds}}

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

# Run Tier 1 criterion benchmarks (fast, ~30s — composed functions, no heavy I/O)
bench:
    cargo bench -p logfwd-bench --bench pipeline --bench output_encode --bench full_chain

# Run throughput ceiling benchmark (generator → scan → null, no transform)
bench-ceiling:
    cargo bench -p logfwd-bench --bench throughput_ceiling

# Run all criterion benchmarks (Tier 1 + Tier 2 — includes I/O and batch scaling, ~2-5min)
# Excludes elasticsearch_arrow which requires a running ES instance.
bench-full:
    cargo bench -p logfwd-bench --bench pipeline --bench output_encode --bench full_chain --bench builder_compare --bench batch_formation --bench file_io --bench throughput_ceiling

# Run system-level benchmarks (pipeline, contention, backpressure — requires running services)
bench-system:
    @echo "System-level benchmarks: pipeline end-to-end with real I/O"
    just bench-pipelines

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

# Profile FramedInput / format processing overhead and print a markdown report.
bench-framed-input *ARGS:
    cargo run -p logfwd-bench --release --bin framed_input_profile -- {{ARGS}}

# Allocation-focused FramedInput profiling (dhat-backed, slower; no throughput numbers).
bench-framed-input-alloc *ARGS:
    cargo run -p logfwd-bench --release --features dhat-heap --bin framed_input_profile -- --alloc-only {{ARGS}}

# Run low-and-slow rate-ingest benchmark (logfwd only, measures memory and CPU at each eps)
bench-rate *ARGS:
    cargo build --release -p logfwd
    LOGFWD=./target/release/logfwd cargo run -p logfwd-competitive-bench --release -- --rate-bench {{ARGS}}

# Run sustained-load memory profiler (generator → SQL → null, default 5 minutes).
# Use --quick (30s) for CI or --medium (120s) for quick checks.
bench-memory *ARGS:
    cargo run -p logfwd-bench --release --bin memory-profile -- {{ARGS}}

# Install development tools
install-tools:
    cargo install taplo-cli cargo-deny cargo-audit cargo-nextest
    @echo "Optional: cargo install inferno"
    @echo "Install just: https://just.systems/man/en/installation.html"

# Set up git pre-commit hook (works from any worktree)
install-hooks:
    #!/usr/bin/env bash
    set -euo pipefail
    HOOKS_DIR=$(git rev-parse --git-common-dir)/hooks
    mkdir -p "$HOOKS_DIR"
    printf '#!/bin/sh\nset -e\nRUSTC_WRAPPER="" cargo fmt --check\nRUSTC_WRAPPER="" cargo clippy -- -D warnings\nRUSTC_WRAPPER="" cargo check --all-targets\n' \
        > "$HOOKS_DIR/pre-commit"
    chmod +x "$HOOKS_DIR/pre-commit"
    echo "Pre-commit hook installed ($HOOKS_DIR/pre-commit)"

# Remove git pre-commit hook
uninstall-hooks:
    #!/usr/bin/env bash
    HOOKS_DIR=$(git rev-parse --git-common-dir)/hooks
    rm -f "$HOOKS_DIR/pre-commit"
    echo "Pre-commit hook removed"
