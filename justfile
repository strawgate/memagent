# ff development task runner
# Install: cargo install just
# Usage:  just --list

# Default recipe: run quick CI checks (default-members path).
# Use `just ci-all` for full workspace CI checks (includes datafusion).

# Limit all parallelism to 2 vCPU to avoid starving other processes.
# This caps cargo compilation, test execution, rayon workers, and any Tokio
# runtime that reads TOKIO_WORKER_THREADS (including Pipeline::run()).
# Override with: JOBS=8 just test
export CARGO_BUILD_JOBS := env("JOBS", "2")
export RUST_TEST_THREADS := env("JOBS", "2")
export NEXTEST_TEST_THREADS := env("JOBS", "2")
export TOKIO_WORKER_THREADS := env("JOBS", "2")
export RAYON_NUM_THREADS := env("JOBS", "2")
default: ci

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

# One-command bootstrap: install toolchain, dev tools, git hooks, and fetch deps.
# Run this after cloning the repo. Safe to re-run — idempotent.
setup:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> Checking Rust toolchain (rust-toolchain.toml)"
    if command -v rustup &>/dev/null; then
        rustup show active-toolchain
    else
        echo "ERROR: rustup not found — install from https://rustup.rs" >&2
        exit 1
    fi
    echo ""
    echo "==> Installing dev tools"
    if command -v mise &>/dev/null; then
        echo "    mise detected — running mise install"
        mise install
    else
        echo "    mise not found — falling back to cargo install"
        just install-tools
    fi
    echo ""
    echo "==> Fetching Cargo dependencies"
    cargo fetch
    echo ""
    echo "==> Installing git hooks"
    just install-hooks
    echo ""
    echo "==> Building diagnostics dashboard (Node.js)"
    if command -v node &>/dev/null; then
        just dashboard
    else
        echo "    SKIP: node not found — dashboard build requires Node.js 22+"
        echo "    The dashboard is optional for most Rust development."
    fi
    echo ""
    echo "==> Setup complete. Run 'just doctor' to verify, or 'just ci' to test."

# Verify that all required dev tools are installed and report versions.
doctor:
    #!/usr/bin/env bash
    set -uo pipefail
    MISSING_REQUIRED=0; MISSING_OPTIONAL=0
    check() {
        local name="$1" cmd="$2" required="${3:-true}"
        if command -v "$cmd" &>/dev/null; then
            ver=$("$cmd" --version 2>/dev/null | head -1)
            printf "  %-18s %s\n" "✓ $name" "$ver"
        elif [ "$required" = "true" ]; then
            printf "  %-18s %s\n" "✗ $name" "MISSING (required)"
            MISSING_REQUIRED=1
        else
            printf "  %-18s %s\n" "- $name" "not found (optional)"
            MISSING_OPTIONAL=1
        fi
    }
    echo "ff development environment"
    echo "==============================="
    echo ""
    echo "Required:"
    check "rustc"     rustc
    check "cargo"     cargo
    check "rustfmt"   rustfmt
    check "clippy"    cargo-clippy
    check "just"      just
    check "nextest"   cargo-nextest
    echo ""
    echo "Recommended:"
    check "taplo"     taplo     false
    check "cargo-deny" cargo-deny false
    check "cargo-machete" cargo-machete false
    echo ""
    echo "Optional:"
    check "node"      node      false
    check "npm"       npm       false
    check "docker"    docker    false
    check "mise"      mise      false
    check "sccache"   sccache   false
    check "java"      java      false
    check "kani"      cargo-kani false
    echo ""
    if [ "$MISSING_REQUIRED" -ne 0 ]; then
        echo "Some required tools are missing. Run: just setup"
        exit 1
    elif [ "$MISSING_OPTIONAL" -ne 0 ]; then
        echo "All required tools present. Some optional tools missing — see mise.toml."
    else
        echo "All tools present."
    fi

# Format all Rust code
fmt:
    cargo fmt

# Check formatting without modifying files
fmt-check:
    cargo fmt --check

# Clippy — default-members only (skips datafusion, ~30s)
clippy:
    RUSTFLAGS="-W clippy::unwrap_used -W clippy::expect_used -W clippy::indexing_slicing" cargo clippy -- -D warnings

# Clippy — full workspace including datafusion (~3min, CI uses this)
clippy-all:
    cargo clippy --workspace -- -D warnings

# Tests — default-members only (skips datafusion)
test:
    FFWD_DISABLE_DEFAULT_CHECKPOINTS=1 cargo nextest run --profile ci

# Tests — full workspace (CI uses this)
test-all:
    FFWD_DISABLE_DEFAULT_CHECKPOINTS=1 cargo nextest run --workspace --profile ci

# Run semantic lints via dylint (hot_path_no_alloc, and any future
# semantic lints defined in crates/ffwd-lints/).
# Requires: cargo install --locked cargo-dylint dylint-link
#           rustup toolchain install nightly-2025-09-18 --component llvm-tools-preview --component rustc-dev
dylint:
    cargo dylint --path crates/ffwd-lints -- --workspace

# Run required Kani formal verification proofs for production crates
# Requires: cargo install --locked kani-verifier && cargo kani setup
kani:
    just kani-required

# Run local Miri checks for proof-heavy pure crates.
# Requires:
#   rustup component add --toolchain nightly miri rust-src
miri-setup:
    rustup component add --toolchain nightly miri rust-src

miri:
    just miri-setup
    RUSTC_WRAPPER="" cargo +nightly miri setup
    just miri-core
    just miri-types
    just miri-io-buffered

miri-core:
    RUSTC_WRAPPER="" MIRIFLAGS="-Zmiri-strict-provenance" cargo +nightly miri test -p ffwd-core --lib

miri-types:
    RUSTC_WRAPPER="" MIRIFLAGS="-Zmiri-strict-provenance" cargo +nightly miri test -p ffwd-types --lib

miri-io-buffered:
    RUSTC_WRAPPER="" MIRIFLAGS="-Zmiri-strict-provenance" cargo +nightly miri test -p ffwd-io framed::tests::poll_into_miri_shared_buffer_alias_regression --lib

# Run the required Kani crate set enforced by CI guardrails.
kani-required:
    RUSTC_WRAPPER="" cargo kani -p ffwd-kani -Z function-contracts -Z mem-predicates -Z stubbing
    RUSTC_WRAPPER="" cargo kani -p ffwd-core -Z function-contracts -Z mem-predicates -Z stubbing
    RUSTC_WRAPPER="" cargo kani -p ffwd-arrow --lib -Z function-contracts -Z mem-predicates -Z stubbing
    RUSTC_WRAPPER="" cargo kani -p ffwd-types -Z function-contracts -Z mem-predicates -Z stubbing
    RUSTC_WRAPPER="" cargo kani -p ffwd-io -Z function-contracts -Z mem-predicates -Z stubbing
    RUSTC_WRAPPER="" cargo kani -p ffwd-output -Z function-contracts -Z mem-predicates -Z stubbing
    RUSTC_WRAPPER="" cargo kani -p ffwd-runtime -Z function-contracts -Z mem-predicates -Z stubbing
    RUSTC_WRAPPER="" cargo kani -p ffwd-diagnostics -Z function-contracts -Z mem-predicates -Z stubbing
    RUSTC_WRAPPER="" cargo kani -p ffwd -Z function-contracts -Z mem-predicates -Z stubbing

# Validate the non-core Kani boundary contract.
kani-boundary:
    python3 scripts/verify_kani_boundary_contract.py

# Validate that CI's TLC matrix covers expected tla/*.cfg files.
tlc-matrix-contract:
    python3 scripts/verify_tlc_matrix_contract.py

# Validate proptest regression-file and persistence policy.
proptest-regressions:
    python3 scripts/verify_proptest_regressions.py

# Validate CI/just verification trigger contracts stay in sync.
verification-trigger-contract:
    python3 scripts/verify_verification_trigger_contract.py

# Run structural verification checks.
verify:
    cargo xtask verify

# Regenerate config support tables from the shared docspec registry.
generate-config-docs:
    cargo xtask generate-config-docs

# Run all lightweight verification guardrails enforced in CI.
verification-guardrail: kani-boundary tlc-matrix-contract proptest-regressions verification-trigger-contract

# Download tla2tools.jar to .tools/ if missing.
tla-setup:
    #!/usr/bin/env bash
    set -euo pipefail
    JAR="{{justfile_directory()}}/.tools/tla2tools.jar"
    if [[ -f "$JAR" ]]; then
        echo "tla2tools.jar already present at $JAR"
        exit 0
    fi
    mkdir -p "$(dirname "$JAR")"
    URL="${TLA2TOOLS_URL:-https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar}"
    echo "Downloading tla2tools.jar from $URL"
    TMP="${JAR}.tmp.$$"
    curl -fsSL "$URL" -o "$TMP"
    mv "$TMP" "$JAR"
    echo "Installed $JAR"

# Run TLC with a given model and cfg from tla/.
tlc model config:
    #!/usr/bin/env bash
    set -euo pipefail
    just tla-setup
    JAR="{{justfile_directory()}}/.tools/tla2tools.jar"
    JAVA_BIN="${JAVA_BIN:-}"
    if [[ -n "$JAVA_BIN" ]] && ! "$JAVA_BIN" -version >/dev/null 2>&1; then
        JAVA_BIN=""
    fi
    if [[ -z "$JAVA_BIN" ]]; then
        for candidate in /opt/homebrew/opt/openjdk/bin/java /usr/local/opt/openjdk/bin/java java; do
            if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -version >/dev/null 2>&1; then
                JAVA_BIN="$candidate"
                break
            fi
        done
    fi
    if [[ -z "$JAVA_BIN" ]]; then
        echo "Java runtime not found. Install OpenJDK (e.g. 'brew install openjdk') or set JAVA_BIN."
        exit 1
    fi
    cd "{{justfile_directory()}}/tla"
    "$JAVA_BIN" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC "{{model}}" -config "{{config}}"

# Tail reducer state machine model.
tlc-tail:
    just tlc MCTailLifecycle.tla TailLifecycle.cfg

# Verify each feature flag compiles independently (cargo-hack).
# Requires: cargo install cargo-hack
hack-check:
    cargo hack check --each-feature --no-dev-deps -p ffwd -p ffwd-runtime -p ffwd-io -p ffwd-config

# Lint — fast (default-members, skips datafusion)
lint: fmt-check otlp-codegen-check workspace-inheritance-guard public-api-error-guard production-panic-guard clippy toml-check

# Lint — full workspace (CI uses this)
lint-all: fmt-check verification-guardrail otlp-codegen-check workspace-inheritance-guard public-api-error-guard production-panic-guard clippy-all toml-check deny

# Quick CI — fast lint + test (default-members, no datafusion)
ci: lint test

# Compatibility alias used by pre-commit hooks.
check: ci

# Full CI — everything including datafusion and TLA+ tail verification (run before pushing)
ci-all: lint-all test-all tlc-tail

# Check TOML formatting (Cargo.toml, etc.)
toml-check:
    taplo check

# Check generated OTLP code drift.
otlp-codegen-check:
    python3 scripts/generate_otlp_fast_encoder.py --check
    python3 scripts/generate_otlp_projection.py --check

# Guardrail: inherited dependencies must not override default-features locally.
workspace-inheritance-guard:
    python3 scripts/check_workspace_inherited_default_features.py

# Guardrail: no Box<dyn Error> in public library signatures.
public-api-error-guard:
    python3 scripts/check_no_box_dyn_error.py

# Guardrail: no panic!/todo!/unimplemented! in production runtime/output paths.
production-panic-guard:
    python3 scripts/check_no_panic_in_production.py

# Format TOML files
toml-fmt:
    taplo fmt

# Audit dependencies for vulnerabilities, licenses, and duplicates
deny:
    cargo deny check

# Build the diagnostics dashboard (Preact + TypeScript → single HTML file)
# Requires Node.js. Output: crates/ffwd-diagnostics/src/dashboard.html
# Must run before cargo build/test/clippy (CI does this automatically).
dashboard:
    cd dashboard && npm install --prefer-offline && npm run build



# Extended testing: 10K proptest cases + turmoil simulation + ignored benchmarks/scaling tests
test-extended:
    PROPTEST_CASES=10000 cargo nextest run --profile ci
    cargo nextest run --profile ci --run-ignored ignored-only
    cargo test -p ffwd --features turmoil --test turmoil_sim

# Run turmoil simulation including Porcupine linearizability checker integration.
test-linearizability:
    cargo test -p ffwd --features turmoil --test turmoil_sim linearizability::porcupine_checker_accepts_runtime_history

# ---------------------------------------------------------------------------
# Mutation testing (cargo-mutants)
# ---------------------------------------------------------------------------

# Run mutation testing on a single crate (default: ffwd-core).
# Requires: cargo install cargo-mutants cargo-nextest
# Config:   .cargo/mutants.toml (exclusions, timeout, nextest)
mutants crate="ffwd-core":
    cargo mutants -p {{crate}}

# Run mutation testing only on code changed vs origin/main (fast, CI-friendly).
mutants-diff crate="ffwd-core":
    #!/usr/bin/env bash
    set -euo pipefail
    git rev-parse --verify origin/main >/dev/null
    git diff origin/main...HEAD | cargo mutants -p {{crate}} --in-diff -

# Build release binary (full package, includes DataFusion SQL)
build:
    cargo build --release -p ffwd

# Build a fast local dev binary without DataFusion SQL.
# Useful for tighter compile/edit loops; this is NOT the release artifact.
build-dev-lite:
    cargo build --release -p ffwd --no-default-features

# ---------------------------------------------------------------------------
# End-to-end pipeline benchmarks (bench/scenarios/*.yaml)
# ---------------------------------------------------------------------------

# Helper: run a single pipeline for N seconds, print stats from diagnostics.
[private]
_bench-run name config seconds="10" diag="http://127.0.0.1:9090":
    #!/usr/bin/env bash
    set -euo pipefail
    FF=./target/release/ff
    $FF run --config {{config}} &
    PID=$!
    sleep {{seconds}}
    STATS=$(curl -s {{diag}}/admin/v1/stats 2>/dev/null || echo '{}')
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
    FF=./target/release/ff
    $FF run --config {{rx_config}} &
    RX=$!;

    # Poll /ready until the diagnostics HTTP server is up (503 → 200).
    # /ready returns 200 once diagnostics registers the pipeline, which happens
    # just before pipeline.run_async() spawns and binds receiver sockets.
    # The extra sleep 1 after the check gives run_async() time to complete
    # socket binding before the sender process starts connecting.
    READY=0
    for i in {1..10}; do
        if curl --fail --silent --connect-timeout 1 --max-time 1 http://127.0.0.1:9091/ready >/dev/null; then
            READY=1
            break
        fi
        sleep 1
    done
    if [ "$READY" -ne 1 ]; then
        echo "receiver pipeline did not become ready after 10 retries (~20s)" >&2
        kill $RX 2>/dev/null || true
        wait $RX 2>/dev/null || true
        exit 1
    fi
    # Give run_async() time to bind receiver sockets after /ready returns.
    sleep 1

    $FF run --config {{tx_config}} &
    TX=$!; sleep {{seconds}}
    STATS=$(curl -s http://127.0.0.1:9091/admin/v1/stats 2>/dev/null || echo '{}')
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
    cargo build --release -p ffwd
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
    cargo build --release -p ffwd
    echo "==> ES streaming benchmark (generator → elasticsearch)"
    just _bench-run es-streaming bench/scenarios/es-sender-streaming.yaml {{seconds}}
    echo "==> Stopping Elasticsearch"
    docker compose -f examples/elasticsearch/docker-compose.yml down

# Run all pipeline benchmarks (alias: bench-pipelines)
bench-e2e seconds="10":
    just bench-pipelines {{seconds}}

[private]
bench-pipelines seconds="10":
    @echo "ff pipeline benchmarks ({{seconds}}s each)"
    @echo "================================================"
    cargo build --release -p ffwd
    just bench-self {{seconds}}
    just bench-tcp {{seconds}}
    just bench-udp {{seconds}}
    just bench-otlp {{seconds}}

# Run Tier 1 criterion benchmarks (fast, ~30s — composed functions, no heavy I/O)
bench:
    cargo bench -p ffwd-bench --bench pipeline --bench output_encode --bench full_chain

# Run throughput ceiling benchmark (generator → scan → null, no transform)
bench-ceiling:
    cargo bench -p ffwd-bench --bench throughput_ceiling

# Run all criterion benchmarks (Tier 1 + Tier 2 — includes I/O and batch scaling, ~2-5min)
# Excludes elasticsearch_arrow which requires a running ES instance.
bench-full:
    cargo bench -p ffwd-bench --bench pipeline --bench output_encode --bench full_chain --bench builder_compare --bench batch_formation --bench file_io --bench throughput_ceiling

# Run system-level benchmarks (pipeline, contention, backpressure — requires running services)
bench-system:
    @echo "System-level benchmarks: pipeline end-to-end with real I/O"
    just bench-pipelines

# Run a local File -> OTLP profile with pprof-rs.
# Outputs a temp directory containing config.yaml, logs.json, pipeline.log,
# blackhole.log, and flamegraph.svg.
profile-otlp-local lines="500000" seconds="6":
    #!/usr/bin/env bash
    set -euo pipefail
    ROOT=$(mktemp -d /tmp/ff-pprof.XXXXXX)
    PORT=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')

    echo "==> Build cpu-profiling binary"
    RUSTC_WRAPPER= cargo build --release --features cpu-profiling -p ffwd

    mkdir -p "${ROOT}/bin"
    cp target/release/ff "${ROOT}/bin/ff-prof"

    echo "==> Generate test data ({{lines}} lines)"
    "${ROOT}/bin/ff-prof" generate-json "{{lines}}" "${ROOT}/logs.json"

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
    "${ROOT}/bin/ff-prof" blackhole "127.0.0.1:${PORT}" > "${ROOT}/blackhole.log" 2>&1 &
    BLACKHOLE_PID=$!
    cleanup() {
        kill -TERM "${BLACKHOLE_PID}" 2>/dev/null || true
    }
    trap cleanup EXIT

    echo "==> Run profiled pipeline for {{seconds}}s"
    pushd "${ROOT}" >/dev/null
    "${ROOT}/bin/ff-prof" run --config "${ROOT}/config.yaml" > pipeline.log 2>&1 &
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

# Run OTLP I/O Criterion benchmarks (stage-separated: parser, decode, encode, compression, e2e).
bench-otlp-io *ARGS:
    cargo bench -p ffwd-bench --bench otlp_io -- {{ARGS}}

# Run OTLP I/O benchmarks with fast local iteration settings.
bench-otlp-io-fast *ARGS:
    cargo bench -p ffwd-bench --bench otlp_io -- --warm-up-time 1 --measurement-time 2 --sample-size 10 {{ARGS}}

# Run source metadata attachment benchmarks.
bench-source-metadata *ARGS:
    cargo bench -p ffwd-bench --bench source_metadata -- {{ARGS}}

# Run source metadata attachment benchmarks with fast local iteration settings.
bench-source-metadata-fast *ARGS:
    cargo bench -p ffwd-bench --bench source_metadata -- --warm-up-time 1 --measurement-time 2 --sample-size 10 {{ARGS}}

# Profile OTLP decode/encode CPU with the normal allocator (flamegraph, per-mode timings).
profile-otlp-io *ARGS:
    cargo run -p ffwd-bench --release --features bench-tools --bin otlp_io_profile -- {{ARGS}}

# Profile OTLP decode/encode allocation counts with stats_alloc instrumentation.
profile-otlp-io-alloc *ARGS:
    cargo run -p ffwd-bench --release --features bench-tools,otlp-profile-alloc --bin otlp_io_profile -- {{ARGS}}

# Generate microbenchmark report (markdown)
bench-report:
    cargo run -p ffwd-bench --features bench-tools

# Profile FramedInput / format processing overhead and print a markdown report.
bench-framed-input *ARGS:
    cargo run -p ffwd-bench --release --features bench-tools --bin framed_input_profile -- {{ARGS}}

# Allocation-focused FramedInput profiling (dhat-backed, slower; no throughput numbers).
bench-framed-input-alloc *ARGS:
    cargo run -p ffwd-bench --release --features bench-tools,dhat-heap --bin framed_input_profile -- --alloc-only {{ARGS}}

# Run sustained-load memory profiler (generator → SQL → null, default 5 minutes).
# Use --quick (30s) for CI or --medium (120s) for quick checks.
bench-memory *ARGS:
    cargo run -p ffwd-bench --release --features bench-tools --bin memory-profile -- {{ARGS}}

# Profile file output (JSON lines serialization + file I/O) CPU, memory, or per-stage breakdown.
# Modes: breakdown (default), cpu (flamegraph), alloc (allocation counts).
# Examples:
#   just profile-file-output                              # breakdown, narrow schema
#   just profile-file-output --schema wide                # breakdown, wide schema
#   just profile-file-output --mode cpu --schema wide     # CPU flamegraph, wide
profile-file-output *ARGS:
    cargo run -p ffwd-bench --release --features bench-tools --bin file_output_profile -- {{ARGS}}

# Profile file output allocation counts (requires stats_alloc instrumented allocator).
profile-file-output-alloc *ARGS:
    cargo run -p ffwd-bench --release --features bench-tools,otlp-profile-alloc --bin file_output_profile -- --mode alloc {{ARGS}}

# Start a local OTLP blackhole receiver using main CLI devour wrapper.
bench-devour-otlp listen="127.0.0.1:4318":
    cargo run -p ffwd --release -- devour --mode otlp --listen {{listen}}

# Blast generated OTLP data to a receiver endpoint using main CLI blast wrapper.
bench-blast-otlp endpoint="http://127.0.0.1:4318/v1/logs" duration="15":
    cargo run -p ffwd --release -- blast --destination otlp --endpoint {{endpoint}} --duration-secs {{duration}}

# Install development tools
install-tools:
    cargo install taplo-cli cargo-deny cargo-audit cargo-nextest
    @echo "Optional: cargo install inferno"
    @echo "Install just: https://just.systems/man/en/installation.html"

# Install recommended VS Code extensions from .vscode/extensions.json
install-extensions:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v code &>/dev/null; then
        echo "ERROR: 'code' CLI not found. Open VS Code and run:"
        echo "  Cmd/Ctrl+Shift+P → 'Shell Command: Install code command in PATH'"
        exit 1
    fi
    grep -oE '"[a-zA-Z0-9_-]+\.[a-zA-Z0-9._-]+"' .vscode/extensions.json \
        | tr -d '"' \
        | while read -r ext; do
            echo "Installing $ext …"
            code --install-extension "$ext" --force
        done
    echo "Done."

# Set up git pre-commit hook (works from any worktree)
install-hooks:
    #!/usr/bin/env bash
    set -euo pipefail
    HOOKS_DIR=$(git rev-parse --git-common-dir)/hooks
    mkdir -p "$HOOKS_DIR"
    printf '#!/bin/sh\nset -e\njust fmt-check\njust clippy\njust check\n' \
        > "$HOOKS_DIR/pre-commit"
    chmod +x "$HOOKS_DIR/pre-commit"
    echo "Pre-commit hook installed ($HOOKS_DIR/pre-commit)"

# Remove git pre-commit hook
uninstall-hooks:
    #!/usr/bin/env bash
    HOOKS_DIR=$(git rev-parse --git-common-dir)/hooks
    rm -f "$HOOKS_DIR/pre-commit"
    echo "Pre-commit hook removed"
