---
title: "Contributing"
description: "Build, test, and contribute to FastForward"
---

## Quick reference

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # install Rust
cargo install just                                                  # install task runner
just install-tools                                                  # install dev tools

just build          # release binary
just test           # all tests
just lint           # fmt + clippy + toml + deny + typos
just ci             # full CI suite
just bench          # Criterion microbenchmarks
cargo test -p logfwd-core                  # core crate only (fastest)
cargo test -p logfwd-io -- tail            # specific test subset
```

## Workspace layout

```
crates/
  logfwd/                    Binary crate. CLI, async pipeline orchestration.
  logfwd-runtime/            Async runtime. Pipeline lifecycle, worker pool, checkpoint I/O.
  logfwd-core/               Proven kernel. Scanner, parsers, state machine, OTLP encoding. no_std.
  logfwd-arrow/              Arrow integration. ScanBuilder impls, SIMD backends, RecordBatch builders.
  logfwd-config/             YAML config parsing and validation.
  logfwd-types/              Shared types used across crates (pipeline lifecycle, config enums).
  logfwd-io/                 I/O layer. File tailing, TCP/UDP/OTLP/HTTP inputs, checkpointing.
  logfwd-diagnostics/        Diagnostics server, HTML dashboard, metrics.
  logfwd-transform/          DataFusion SQL transforms, UDFs (grok, regexp_extract, geo_lookup).
  logfwd-output/             Output sinks (OTLP, Elasticsearch, Loki, JSON lines, stdout, file, TCP, UDP).
  logfwd-bench/              Criterion benchmarks and profiling tools.
  logfwd-competitive-bench/  Comparative benchmarks vs other log agents.
  logfwd-test-utils/         Shared test utilities.
  logfwd-ebpf-proto/         eBPF log capture protocol definitions (experimental).
  logfwd-otap-proto/         OTAP protocol definitions.
  logfwd-proto-build/        Protobuf code generation build scripts.
```

## Build, test, lint, bench, fuzz

```bash
just test                    # All tests
just lint                    # fmt + clippy + toml + deny + typos
cargo test -p logfwd-core    # Core crate only (fastest iteration)

RUSTFLAGS="-C target-cpu=native" cargo bench --bench scanner -p logfwd-core

cd crates/logfwd-core && cargo +nightly fuzz run scanner -- -max_total_time=300
```

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

Caveats:

- Avoid reusing a diagnostics port from another local run; the helper recipe
  omits diagnostics entirely to keep the profile loop simple.
- If the `cpu-profiling` release build fails with `No space left on device`,
  run `RUSTC_WRAPPER= cargo clean` and retry. The profiled release build is
  large because `release` keeps debug info for flamegraphs.
- Killing a wrapper shell is not sufficient; the `SIGTERM` must reach the
  actual `logfwd` process.

---

## Things that will bite you

:::caution
Hard-won lessons from building the scanner and builder pipeline. Read these before modifying core code. See also `dev-docs/ARCHITECTURE.md` for pipeline data flow.
:::

### The deferred builder pattern exists because incremental null-padding is broken

`StreamingBuilder` collects `(row, value)` records during scanning and bulk-builds Arrow columns at `finish_batch`. This seems roundabout — why not write directly to Arrow builders?

Because maintaining column alignment across multiple type builders (str, int, float) per field is a coordination nightmare. When you write an int, you must pad the str and float builders with null. When `end_row` fires, pad all unwritten fields. When a new field appears mid-batch, back-fill all prior rows. We tried this (`IndexedBatchBuilder`); proptest found column length mismatches on multi-line NDJSON with varying field sets.

The deferred pattern is correct by construction: each column is built independently. Gaps are nulls. Columns can never mismatch.

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
