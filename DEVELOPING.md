# Developing logfwd

## Workspace layout

```
crates/
  logfwd/              Binary crate. CLI, async pipeline orchestration.
  logfwd-core/         Proven kernel. Scanner, parsers, pipeline state machine, OTLP encoding. no_std.
  logfwd-arrow/        Arrow integration. ScanBuilder impls, SIMD backends, RecordBatch builders.
  logfwd-config/       YAML config parsing and validation.
  logfwd-io/           I/O layer. File tailing, TCP/UDP/OTLP inputs, checkpointing, diagnostics.
  logfwd-transform/    DataFusion SQL transforms, UDFs (grok, regexp_extract, geo_lookup).
  logfwd-output/       Output sinks (OTLP, Elasticsearch, Loki, JSON lines, stdout).
  logfwd-bench/        Criterion benchmarks for the scanner pipeline.
  logfwd-competitive-bench/  Comparative benchmarks vs other log agents.
  logfwd-test-utils/   Shared test utilities.
  logfwd-ebpf-proto/   eBPF log capture protocol definitions (experimental).
```

## Build, test, lint, bench, fuzz

```bash
just dev                     # Dev build (regenerates dashboard asset if needed)
just build                   # Release build (regenerates dashboard asset if needed)
just test                    # All tests (uses nextest for parallel execution)
just lint                    # fmt + clippy + toml + deny + typos
just bench                   # Criterion microbenchmarks (bench profile, full optimisations)
cargo test -p logfwd-core    # Core crate only (fastest iteration)

RUSTFLAGS="-C target-cpu=native" cargo bench --bench scanner -p logfwd-core

cd crates/logfwd-core && cargo +nightly fuzz run scanner -- -max_total_time=300
```

## Dashboard asset

The diagnostics dashboard is a generated file:

- source: `dashboard/`
- generated HTML: `crates/logfwd-io/src/dashboard.html`
- committed: **no**

Use `just dashboard` to regenerate it manually. The main `just` entry points
that compile Rust (`just dev`, `just build`, `just test`, `just clippy`,
`just bench`, and the end-to-end benchmark/profile commands) regenerate it
automatically when the dashboard sources or config changed.

CI release/dev jobs do the same with the shared `build-dashboard` action so
cross-compiles can embed the generated HTML without checking the built asset
into git.

## Build performance

### Why builds are slow

This workspace depends on **DataFusion** (v48) and **Arrow** (v55) — extremely
large crates with thousands of generic instantiations.  At the default
`opt-level = 0` (debug mode), these dependencies generate enormous amounts of
LLVM IR which makes both compilation and linking slow.  Cold builds
(no cache, first checkout) were observed to take 10–15 minutes on an average
developer machine purely because of these two dependency trees.

Three mitigations are in place:

| Mitigation | Where | Effect |
|-----------|-------|--------|
| `debug = "line-tables-only"` | `[profile.dev]` and `[profile.test]` | Reduces debug-binary size from ~180 MB to ~60 MB (DataFusion debug info is enormous) |
| `[profile.dev.package."*"] opt-level = 1` | `Cargo.toml` | Compiles all dependencies at opt-level 1 instead of 0 — one cheap LLVM pass shrinks IR and object files dramatically, cutting cold build time and link time without expensive IPO |
| `rustc-wrapper = "sccache"` | `.cargo/config.toml` | Caches compiled artefacts so unchanged deps are never recompiled on subsequent builds |

### Dependency opt-level override

`[profile.dev.package."*"]` sets `opt-level = 1` for **all** packages
(dependencies and workspace crates alike) in dev and test profiles.
Cargo's profile inheritance means `[profile.test]` picks this up automatically.

This setting matters most for:
- **Cold builds** (first checkout, no sccache): eliminates the worst of
  DataFusion's IR explosion.
- **After `cargo update`** or feature changes: re-compilation of changed deps
  is much faster.
- **Incremental rebuilds of your own code**: neutral — sccache caches the
  compiled workspace artefacts, so only the file you edited is recompiled.

### Benchmarks are intentionally separate

Benchmark commands are kept separate from normal build/test commands so they
always run with full optimisations:

- `just bench` uses Cargo's `bench` profile (`[profile.bench]` inherits `release`)
- `just bench-build` builds the fully optimised `target/release/logfwd` binary
  used by the end-to-end pipeline benchmarks
- `just bench-self|bench-tcp|bench-udp|bench-otlp|bench-e2e` all go through
  that release build path

### Compile caching with sccache

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

Hard-won lessons from building the scanner and builder pipeline.
See also `dev-docs/ARCHITECTURE.md` for pipeline data flow.

### The deferred builder pattern exists because incremental null-padding is broken

`StreamingBuilder` collects `(row, value)` records during scanning and bulk-builds Arrow columns at `finish_batch`. This seems roundabout — why not write directly to Arrow builders?

Because maintaining column alignment across multiple type builders (str, int, float) per field is a coordination nightmare. When you write an int, you must pad the str and float builders with null. When `end_row` fires, pad all unwritten fields. When a new field appears mid-batch, back-fill all prior rows. We tried this (`IndexedBatchBuilder`); proptest found column length mismatches on multi-line NDJSON with varying field sets.

The deferred pattern is correct by construction: each column is built independently. Gaps are nulls. Columns can never mismatch.

### Chunk-level SIMD classification beats per-line SIMD

We tried three approaches:
1. **Per-line SIMD**: load 16 bytes, compare for `"` and `\`. Slower than scalar on short strings.
2. **sonic-rs DOM**: SIMD JSON parser builds a DOM per line. The DOM allocation is the bottleneck.
3. **Chunk-level classification** (`StructuralIndex`): one portable SIMD pass (via `wide` crate) over the entire buffer, detecting 10 structural characters simultaneously. Then `scan_string` is a single `trailing_zeros` bit-scan.

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

### `keep_raw` costs 65% of table memory

The `_raw` column stores the full JSON line. Larger than all other columns combined. Default is `keep_raw: false`.

### Always use `just clippy`, never bare `cargo clippy`

CI runs `cargo clippy -- -D warnings` (all warnings are errors). Bare `cargo clippy` only shows warnings, so code that looks clean locally fails in CI. The `just clippy` recipe matches CI exactly. Additionally, conditional SIMD compilation means warnings differ between aarch64 (macOS) and x86_64 (CI Linux).

### proptest finds bugs unit tests can't

Every time we thought the scanner was correct, proptest broke it. Escapes crossing 64-byte boundaries, fields in different orders, duplicate keys with different types. Run `PROPTEST_CASES=2000` minimum.

Oracle tests compare against sonic-rs as ground truth. Our scanner does first-writer-wins for duplicate keys; sonic-rs does last-writer-wins. Both valid per RFC 8259; oracle tests skip duplicate-key inputs.

### Two scan modes serve different purposes

- **`Scanner::scan_detached`**: produces self-contained `StringArray` columns. Input buffer can be freed. For persistence and compression.
- **`Scanner::scan`**: zero-copy `StringViewArray` views into `bytes::Bytes` buffer. 20% faster. Buffer must stay alive. For real-time query-then-discard.

Both use the same `StreamingBuilder` (which implements `ScanBuilder`), sharing the generic `scan_streaming()` loop.
