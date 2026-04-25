# Developing logfwd

## Fast Start

If you are new, do this first:

```bash
just ci
just test
# Optional crate-local shortcut during focused iteration:
# cargo test -p logfwd-core
```

Then read only the section needed for your task:
- crate map: `Workspace layout`
- command semantics: `Build, test, lint, bench, fuzz`
- performance profiling: `Local CPU profiling (macOS)`
- historical gotchas: `Things that will bite you`

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| [Rust](https://rustup.rs) | Latest stable (managed by `rust-toolchain.toml`) | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| [just](https://just.systems) | 1.40+ | `cargo install just` or `brew install just` |
| [cargo-nextest](https://nexte.st) | 0.9+ | `cargo install cargo-nextest` |

Optional but recommended:

| Tool | Purpose | Install |
|------|---------|---------|
| [mise](https://mise.jdx.dev) | Manages all tool versions from `mise.toml` | [mise.jdx.dev](https://mise.jdx.dev/getting-started.html) |
| [taplo](https://taplo.tamasfe.dev) | TOML linting (`just toml-check`) | `cargo install taplo-cli` |
| [cargo-deny](https://embarkstudios.github.io/cargo-deny/) | License/advisory audit (`just deny`) | `cargo install cargo-deny` |
| [Node.js](https://nodejs.org) 22+ | Dashboard build (`just dashboard`) | via mise, nvm, or nodejs.org |
| [Docker](https://www.docker.com) | Local services (`docker-compose.dev.yml`) | docker.com |
| [sccache](https://github.com/mozilla/sccache) | Compile caching | `cargo install sccache --locked` |
| [OpenJDK](https://openjdk.org) | Run TLC model checks (`just tlc-tail`) | `brew install openjdk` (macOS) |
| Miri nightly components | Local UB checks (`just miri`) | `just miri-setup` |

All tool versions are declared in `mise.toml`. If you have [mise](https://mise.jdx.dev) installed, `mise install` sets everything up automatically. Otherwise, install the tools listed above manually.

## Setup

```bash
git clone https://github.com/strawgate/fastforward.git && cd fastforward
just setup     # install tools, fetch deps, set up git hooks
just doctor    # verify everything is installed
just ci        # fast lint + test (~30s)
```

`just setup` is idempotent — safe to re-run at any time.

### Local services (optional)

Some benchmarks and examples need Elasticsearch or an OTLP receiver:

```bash
docker compose -f docker-compose.dev.yml up -d   # start services
docker compose -f docker-compose.dev.yml down     # stop
```

| Service | URL | Used by |
|---------|-----|---------|
| Elasticsearch | `http://localhost:9200` | `just bench-es`, `examples/elasticsearch/` |
| OTLP blackhole | `http://localhost:4318` | OTLP end-to-end tests |

## Workspace layout

```
crates/
  logfwd/              Binary crate. CLI entrypoints and compatibility re-exports.
  logfwd-runtime/      Runtime orchestration. Pipeline loop, worker pool, processors.
  logfwd-core/         Proven kernel. Scanner, parsers, pipeline state machine, OTLP encoding. no_std.
  logfwd-arrow/        Arrow integration. ScanBuilder impls, SIMD backends, RecordBatch builders.
  logfwd-config/       YAML config parsing and validation.
  logfwd-config-wasm/  WASM bindings for the config validator (browser/Node.js).
  logfwd-io/           I/O layer. File tailing, TCP/UDP/OTLP inputs, checkpointing, diagnostics.
  logfwd-transform/    DataFusion SQL transforms, UDFs (grok, regexp_extract, geo_lookup).
  logfwd-output/       Output sinks (OTLP, Elasticsearch, Loki, JSON lines, stdout).
  logfwd-types/        Shared value types, state-machine semantics, diagnostics.
  logfwd-diagnostics/  Diagnostics control plane: HTTP endpoints, dashboard, readiness.
  logfwd-bench/        Criterion benchmarks for the scanner pipeline.
  logfwd-competitive-bench/  Comparative benchmarks vs other log agents.
  logfwd-test-utils/   Shared test utilities.
  logfwd-kani/         Verification oracles and Kani proof helpers.
  logfwd-lint-attrs/   Custom proc-macro lint attributes (no_panic, pure).
  logfwd-lints/        Custom Clippy-style lint passes.
  logfwd-ebpf-proto/   eBPF log capture protocol definitions (experimental).
  logfwd-otap-proto/   OTAP protocol definitions.
  logfwd-proto-build/  Protobuf build scripts.
```

## Build, test, lint, bench, fuzz

```bash
just ci                      # lint + test (fast — skips datafusion)
just ci-all                  # full workspace including datafusion
just test                    # tests (default-members only, ~30s)
just test-all                # tests (full workspace, ~3min)
just lint                    # fmt + clippy + toml
just lint-all                # full: fmt + clippy + toml + deny + kani-boundary
just miri-setup              # install nightly Miri components locally
just miri                    # Miri checks for logfwd-core and logfwd-types
just tlc-tail                # run TailLifecycle TLA+ model check
just build                   # release logfwd binary (includes DataFusion SQL)
just build-dev-lite          # dev-only fast binary (no DataFusion SQL)
cargo test -p logfwd-core    # single crate (fastest iteration)
just fuzz scanner 300        # fuzz a target for 300s (nightly)
```

## Benchmark suites

Use the smallest benchmark surface that answers the question:

```bash
just bench                         # Tier 1 Criterion suite
just bench-competitive --lines 1000000 --scenarios passthrough,json_parse,filter
just profile-otlp-local            # end-to-end CPU flamegraph on macOS
just bench-framed-input -- --lines 200000 --iterations 5
just bench-framed-input-alloc -- --lines 200000
```

- `just bench` is the default regression net for performance-sensitive PRs.
- `just bench-competitive` is for product-level comparisons against other agents.
- `profile-*` and `bench-framed-input*` are for hotspot analysis, not headline numbers.
- Nightly benchmark reports land as GitHub issues with the `benchmark` label.
  Use `gh issue list --label benchmark --state open` to inspect the current reports.

> **Why two tiers?** The workspace `default-members` excludes `logfwd-transform`
> (datafusion) and `logfwd` (binary). Bare `cargo check` / `just clippy` skip
> them (~30s vs ~3min). Use `--workspace`, `-p logfwd`, or the `-all` just
> targets when you need the full build. CI always uses `--workspace`.
> Adding the `ci:full` PR label includes the slower verification lanes in
> subsequent CI runs (for example, after pushing a commit or rerunning CI):
> `miri`, macOS tests, and the deeper TLA/TLC sweeps.

## Test filtering and proptest regressions

Common loops when iterating on a single test or investigating a
proptest failure:

```bash
# Run one test by pattern (matches test name substring).
cargo nextest run -p logfwd-core --lib scanner::handles_empty_line

# Run all tests in a module.
cargo nextest run -p logfwd-arrow --lib scanner::

# Raise proptest case count for a suspected edge case. The default
# is 256; use 10_000+ when you think a rare shape is being missed.
PROPTEST_CASES=10000 cargo nextest run -p logfwd-arrow --lib

# Re-run a minimized regression that was pinned into a regression file.
# `crates/*/proptest-regressions/` holds previously-shrunk failing seeds;
# proptest reads them automatically when the matching test runs.
cargo nextest run -p logfwd-arrow --lib scanner::round_trip
```

Turmoil tests use deterministic seeds: set `TURMOIL_SEED=<N>` to
reproduce a scenario from a failing CI run.

## Reading criterion benchmark output

`just bench` writes HTML + JSON under `target/criterion/`. A quick
cheat sheet for the files you'll actually care about:

| File | Purpose |
|---|---|
| `target/criterion/report/index.html` | Top-level summary across all benchmarks in the run. |
| `target/criterion/<bench>/<param>/report/index.html` | Per-case detail: median, confidence interval, violin plot, raw iterations. |
| `target/criterion/<bench>/<param>/change/estimates.json` | Change vs previous run — `mean.point_estimate` is the percent delta. |
| `target/criterion/<bench>/<param>/new/sample.json` | Raw sample timings for the current run. Load this if you want to compute a custom statistic. |

Rules of thumb when reading results:

- **Ignore single-run noise under ~2%.** Criterion's noise floor on
  typical workstation hardware is 1–3 %; changes within that band are
  usually scheduler jitter, not regressions.
- **Check the confidence interval** (p5–p95 band on the violin plot).
  A wide band means high variance — re-run with `--warm-up-time 5
  --measurement-time 10` for more stable numbers.
- **For perf PRs**, quote `mean.point_estimate` from
  `change/estimates.json` in the PR body, plus the 95 % CI. Don't
  paste the HTML into the PR — link the relevant section of the
  markdown report (`just bench` generates one alongside the HTML).

## DataFusion in Dev vs Release

`logfwd` now has a feature-gated SQL engine:

- **Release/full package (default):** includes DataFusion.
  - `cargo build --release -p logfwd`
  - `just build`
- **Dev-lite (opt-in):** skips DataFusion for faster local iteration.
  - `cargo build --release -p logfwd --no-default-features`
  - `just build-dev-lite`

Use the dev-lite build only when you're intentionally working on non-SQL paths.
If your config uses `transform:` SQL or `enrichment:`, run the full/default build.

## Compile caching with sccache

[sccache](https://github.com/mozilla/sccache) caches Rust compilation artefacts to speed up builds. The project is configured to use it (`.cargo/config.toml` sets `rustc-wrapper = "sccache"`).

**CI and Copilot agents:** sccache is installed automatically — no action needed.

**Local development:** install sccache once:

```bash
cargo install sccache --locked
```

After that, every `cargo build` / `cargo test` / `just clippy` will use the cache automatically via the project's `.cargo/config.toml`.

To temporarily **disable** sccache (e.g. for debugging):

```bash
RUSTC_WRAPPER="" cargo build
```

## Faster linking (optional)

Linking is often 30–50 % of incremental rebuild time. Installing a faster
linker can significantly speed up the edit-compile cycle.

**Linux — mold (recommended):**

```bash
# Ubuntu/Debian
sudo apt install mold
# Then add to your personal ~/.cargo/config.toml (not the repo config):
# [target.x86_64-unknown-linux-gnu]
# rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

**macOS — lld via Homebrew:**

```bash
brew install llvm
# Then add to your personal ~/.cargo/config.toml:
# [target.aarch64-apple-darwin]
# rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

This is deliberately not set in the repo's `.cargo/config.toml` to avoid
breaking builds for developers who have not installed the linker.

## Local CPU profiling (macOS)

The `cpu-profiling` feature works locally on macOS, but the shutdown path matters:
the profiled `logfwd` process must receive `SIGTERM` directly so it can build and
write `flamegraph.svg` before exiting.

The easiest way to run the full File -> OTLP path locally is:

```bash
just profile-otlp-local
```

This recipe:

- builds `logfwd` with `--features cpu-profiling`
- generates a JSON input file
- starts a local OTLP blackhole on a fresh port
- runs `logfwd` with a file input and OTLP output
- sends `SIGTERM` to the real `logfwd` child process after a short run
- leaves a temp directory containing `config.yaml`, `logs.json`, `pipeline.log`,
  `blackhole.log`, and `flamegraph.svg`

Useful variants:

```bash
just profile-otlp-local 1000000 10
```

For FramedInput-specific profiling (newline framing, remainder handling, format
processing before the scanner), use:

```bash
just bench-framed-input -- --lines 200000 --iterations 5 --flamegraph /tmp/framed-input.svg
just bench-framed-input-alloc -- --lines 200000
```

Caveats:

- Avoid reusing a diagnostics port from another local run; the helper recipe
  omits diagnostics entirely to keep the profile loop simple.
- If the `cpu-profiling` release build fails with `No space left on device`,
  run `RUSTC_WRAPPER= cargo clean` and retry. The profiled release build is
  large because `release` keeps debug info for flamegraphs.
- Killing a wrapper shell is not sufficient; the `SIGTERM` must reach the
  actual `logfwd` process.

---

## Performance change workflow

When a change is motivated by performance — or unintentionally affects
the hot path — the following loop is the repo standard. No perf claim
lands in a PR or commit message without a corresponding `criterion`
artifact to back it.

1. **Baseline.** Before touching code, run the relevant `criterion`
   bench on `main` and save the report. `just bench` runs the Tier 1
   suite (`pipeline`, `output_encode`, `full_chain`). For more
   targeted Criterion runs, invoke the specific `--bench` target directly.
   Helper and profiling binaries in `logfwd-bench` are gated behind
   `--features bench-tools`; prefer the `just profile-*` recipes, or pass
   that feature explicitly for direct `cargo run -p logfwd-bench --bin ...`
   commands.
2. **Profile to find the actual hotspot.** On macOS: `just profile-otlp-local`
   produces a flamegraph. For FramedInput-specific work:
   `just bench-framed-input -- --flamegraph /tmp/framed-input.svg`.
   Optimize what the profile shows, not what you *think* is slow.
3. **Measure allocations separately.** `just bench-framed-input-alloc`
   and the `cpu-profiling` feature cover allocation-sensitive paths.
   Heap churn and wall-clock are different axes — changes that trade
   one for the other should say so explicitly.
4. **Change the code.** Prefer the smallest change that moves the
   measured bottleneck. Avoid micro-optimizations outside the hotspot.
5. **Re-benchmark and compare.** Run the same bench on the change.
   Include the before/after numbers (and percent delta) in the PR body.
6. **Check for regressions elsewhere.** A win in the scanner can lose
   in the encoder. `just bench` covers the Tier 1 suite quickly;
   use it as a regression net before merge.
7. **Update `dev-docs/` if the change alters a documented perf
   characteristic.** Buffer lifecycle, hot-path rules, and any relevant
   research/design notes under `dev-docs/research/`. See
   `dev-docs/CHANGE_MAP.md` for co-change requirements.

If the change touches `unsafe` SIMD in `logfwd-arrow`, verify it
against the scalar fallback with proptest (`cargo test -p logfwd-arrow`
exercises the equivalence proptests). Miri does not cover `logfwd-arrow`
— `just miri` runs only the `logfwd-core` and `logfwd-types` suites.
For SIMD invariants enforced at the type level, `just kani-boundary`
verifies the scanner contract.

---

## Things that will bite you

Hard-won lessons from building the scanner and builder pipeline.
See also `dev-docs/ARCHITECTURE.md` for pipeline data flow.

### The deferred builder pattern exists because incremental null-padding is broken

`StreamingBuilder` collects `(row, value)` records during scanning and bulk-builds Arrow columns at `finish_batch`. This seems roundabout — why not write directly to Arrow builders?

Because maintaining column alignment across multiple type builders (str, int, float) per field is a coordination nightmare. When you write an int, you must pad the str and float builders with null. When `end_row` fires, pad all unwritten fields. When a new field appears mid-batch, back-fill all prior rows. We tried this (`IndexedBatchBuilder`); proptest found column length mismatches on multi-line NDJSON with varying field sets.

The deferred pattern is correct by construction: each column is built independently. Gaps are nulls. Columns can never mismatch.

Canonical design note: [dev-docs/research/columnar-batch-builder.md](dev-docs/research/columnar-batch-builder.md).

### Chunk-level SIMD classification beats per-line SIMD

We tried three approaches:
1. **Per-line SIMD**: load 16 bytes, compare for `"` and `\`. Slower than scalar on short strings.
2. **sonic-rs DOM**: SIMD JSON parser builds a DOM per line. The DOM allocation is the bottleneck.
3. **Chunk-level streaming classification**: one portable SIMD pass (via `wide` crate) over the entire buffer, detecting 10 structural characters simultaneously. Then string and nesting scans use the retained per-block masks.

Approach 3 wins everywhere because classification is amortized across all strings and per-string lookup is O(1).

### The prefix_xor escape detection has a subtle correctness requirement

The simdjson `prefix_xor` algorithm detects escaped quotes by computing backslash-run parity. It works for **consecutive** backslashes (`\\\"` = escaped quote). But for **non-consecutive** backslashes like `\n\"`, `prefix_xor` gives wrong results because it counts ALL backslashes, not per-run.

Our implementation iterates each backslash: mark next byte as escaped, skip escaped backslashes. Fast because most JSON has zero or few backslashes. The carry between 64-byte blocks must be handled.

### The scanner assumes UTF-8 input

`from_utf8_unchecked` throughout the scanner and builders. JSON is UTF-8 by spec, so this holds in practice. But the scanner does NOT validate — non-UTF-8 input is UB. The fuzz target guards against this; production code currently doesn't. See issue #76.

### HashMap field lookup was 60% of total scan time

Profiling showed `get_or_create_field` dominating — SipHash + probe per field per row. Fix: `resolve_field` does the HashMap lookup once per batch. Subsequent rows use the returned index directly. The `ScanBuilder` trait's `resolve_field` + `append_*_by_idx` pattern encodes this.

### StringViewArray memory reporting is misleading

Arrow's `get_array_memory_size()` counts the backing buffer for every column sharing it. If 5 string columns point into the same buffer, reported memory is 5x actual. The `StreamingBuilder` produces shared-buffer columns; memory reports overcount significantly.

### Source-byte accounting must happen before payload normalization

If an input mutates payloads before framing or scanning, diagnostics must still
charge bytes from the original source payload, not the normalized buffer. Two
easy ways to get this wrong are synthetic newline insertion and compressed
request decoding. Capture source bytes at the receiver boundary first, then
carry them explicitly through `InputEvent::Data` or `InputEvent::Batch`.

### TLS client CA settings must not silently downgrade mTLS

For server-side inputs, `client_ca_file` is meaningful only when
`require_client_auth` is true. Accepting a client CA while leaving client auth
disabled looks like mTLS is configured but still accepts unauthenticated clients.
Reject that combination during startup, and normalize optional certificate paths
once before validation and file loading.

### Arrow IPC compression is just a flag

Compressed Arrow IPC is `StreamWriter` with `IpcWriteOptions::try_with_compression(Some(CompressionType::ZSTD))`. Any `RecordBatch` can be compressed. No special builder needed.

### Line Capture Can Dominate Table Memory

The `body` column can store the full input line and is often larger than all parsed columns combined. Line capture is disabled unless configured (raw inputs default to `line_field_name: body`).

### Always use `just clippy`, never bare `cargo clippy`

CI runs `cargo clippy -- -D warnings` (all warnings are errors). Bare `cargo clippy` only shows warnings, so code that looks clean locally fails in CI. The `just clippy` recipe matches CI exactly. Additionally, conditional SIMD compilation means warnings differ between aarch64 (macOS) and x86_64 (CI Linux).

### proptest finds bugs unit tests can't

Every time we thought the scanner was correct, proptest broke it. Escapes crossing 64-byte boundaries, fields in different orders, duplicate keys with different types. Run `PROPTEST_CASES=2000` minimum.

Oracle tests compare against sonic-rs as ground truth. Our scanner does first-writer-wins for duplicate keys; sonic-rs does last-writer-wins. Both valid per RFC 8259; oracle tests skip duplicate-key inputs.

### Two scan modes serve different purposes

- **`Scanner::scan_detached`**: produces self-contained `StringArray` columns. Input buffer can be freed. For persistence and compression.
- **`Scanner::scan`**: zero-copy `StringViewArray` views into `bytes::Bytes` buffer. 20% faster. Buffer must stay alive. For real-time query-then-discard.

Both use the same `StreamingBuilder` (which implements `ScanBuilder`), sharing the generic `scan_streaming()` loop.

### Receiver socket binding races paired bench runs

In `_bench-pair`, the receiver diagnostics HTTP server (`/ready`) can return `200 OK` before `run_async()` has finished binding the actual TCP/UDP receiver sockets. Starting the sender immediately after the readiness check causes intermittent connection-refused failures in CI.

Fix: poll `http://127.0.0.1:9091/ready` (which returns 503 until at least one pipeline is registered), then sleep 1 second to let `run_async()` complete socket binding before spawning the sender. Without this grace period, the race window is ~1 ms and triggers roughly 10 % of the time on loaded CI runners. (See PR #1320.)
