# Building a proven kernel crate in Rust with formal verification

**A `logfwd-core` crate can enforce purity, safety, and formal verification through a combination of `#![no_std]` + `alloc`, Kani proofs gated behind `#[cfg(kani)]`, bolero for unified test/fuzz/proof harnesses, cargo-vet for per-crate dependency auditing, and a layered CI pipeline.** The approach is battle-tested: AWS's s2n-quic uses this exact pattern with bolero + Kani across dozens of modules in production. The migration from logfwd-core should proceed as five small PRs, starting with the lowest-dependency pure-logic modules and using `pub use` re-exports to maintain backward compatibility throughout.

---

## The kernel crate's Cargo.toml and source-level configuration

The proven kernel needs two layers of defense: structural enforcement via `#![no_std]` (which physically prevents access to `std::io`, `std::fs`, `std::net`, and `std::thread`) and lint enforcement via Cargo.toml's `[lints]` section (stable since Rust 1.74).

**With `#![no_std]` + `extern crate alloc`**, you get `Vec`, `String`, `Box`, `Rc`, `Arc`, `BTreeMap`, `BTreeSet`, `VecDeque`, `BinaryHeap`, and `Cow` — but **not `HashMap` or `HashSet`**. These are `std`-only because they rely on OS-sourced random seeds for HashDoS protection. If you need hash maps, add `hashbrown` — it's the same implementation backing `std::HashMap` since Rust 1.36, packaged for `no_std`. Its default hasher (`foldhash`) lacks HashDoS resistance, which is fine for a kernel processing trusted internal data.

Is `no_std` worth it when you need `alloc` anyway? **Yes.** It provides structural enforcement — the compiler itself blocks IO access, not just lints. It also makes the kernel usable in WASM, embedded, and other constrained environments. The one catch: your entire dependency tree must also be `no_std`-compatible, and the compiler won't warn you otherwise. Verify with CI: `cargo build --target thumbv6m-none-eabi -p logfwd-core`.

Here is the concrete Cargo.toml for the kernel crate, modeled on s2n-quic's pattern:

```toml
[package]
name = "logfwd-core"
version = "0.0.0"
edition = "2021"

[features]
default = []
alloc = []
std = ["alloc"]
testing = ["std", "generator"]
generator = ["bolero-generator"]

[dependencies]
memchr = { version = "2", default-features = false }
hashbrown = { version = "0.15", default-features = false, features = ["allocator-api2"] }
bolero-generator = { version = "0.13", default-features = false, optional = true }

[dev-dependencies]
bolero = "0.13"
bolero-generator = "0.13"

[package.metadata.kani]
flags = { tests = true }
unstable = { stubbing = true }

[lints.rust]
unsafe_code = "forbid"
missing_docs = "warn"
trivial_casts = "warn"
trivial_numeric_casts = "warn"
unused_qualifications = "warn"

[lints.rust.unexpected_cfgs]
level = "warn"
check-cfg = ['cfg(kani)', 'cfg(kani_slow)']

[lints.clippy]
pedantic = { level = "warn", priority = -1 }
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
indexing_slicing = "deny"
print_stdout = "deny"
print_stderr = "deny"
dbg_macro = "deny"
todo = "deny"
exit = "deny"
std_instead_of_core = "warn"
std_instead_of_alloc = "warn"
module_name_repetitions = "allow"
must_use_candidate = "allow"
```

And the corresponding `src/lib.rs` header:

```rust
#![no_std]
extern crate alloc;
use alloc::vec::Vec;
use alloc::string::String;
use alloc::collections::BTreeMap;
```

The `[lints]` section replaces the old `#![forbid(...)]` / `#![deny(...)]` attributes in source files. **`forbid` is stronger than `deny`** — it cannot be overridden by `#[allow(...)]` in inner code. Use `forbid` for `unsafe_code` (non-negotiable in the kernel) and `deny` for clippy restrictions where you might occasionally need a targeted override.

For workspace-wide lint inheritance, define `[workspace.lints]` in the root Cargo.toml and add `[lints] workspace = true` in each member. However, the kernel should have its own stricter configuration since you cannot currently override workspace lints per-member — it's all or nothing.

---

## Kani harnesses, bolero unification, and the proof workflow

Kani harnesses belong **in the same source file as the code they verify**, wrapped in a `#[cfg(kani)]` module. This is the convention from both the Rust standard library verification project and AWS's s2n-quic. The `cfg(kani)` gate ensures `cargo build` and `cargo test` never see Kani code — the `kani` cfg is only set when running `cargo kani`.

The most powerful pattern is **bolero-unified harnesses** that serve as proptest, fuzz, and formal verification targets simultaneously:

```rust
#[test]
#[cfg_attr(kani, kani::proof, kani::solver(cadical), kani::unwind(9))]
fn chunk_classify_never_panics() {
    bolero::check!()
        .with_type::<(Vec<u8>, u8)>()
        .cloned()
        .for_each(|(input, flags)| {
            let result = classify_chunk(&input, flags);
            assert!(result.is_valid());
        });
}
```

This single harness runs three different ways. `cargo test` executes it as a randomized property test. `cargo bolero test chunk_classify_never_panics` runs it with libfuzzer. `cargo kani --tests --harness chunk_classify_never_panics` sends it through CBMC for exhaustive verification. Under Kani, bolero redirects `with_type()` to `kani::any()`, which symbolically represents *all possible values* — the `for_each` body runs exactly once covering the entire input space.

For proofs that use Kani-specific features like `kani::assume` or `kani::proof_for_contract`, place them in a dedicated module:

```rust
#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof_for_contract(parse_int_fast)]
    fn verify_parse_int_fast() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);
        parse_int_fast(&bytes[..len]);
    }
}
```

**Kani function contracts** (`#[kani::requires(...)]`, `#[kani::ensures(...)]`, enabled with `-Z function-contracts`) are more powerful than the third-party `contracts` crate for verified code. They integrate directly with CBMC's model checker and support `#[kani::proof_for_contract(fn_name)]` to verify contracts exhaustively. The `contracts` crate generates runtime assertions — useful for defense-in-depth in the core crate but redundant in kernel code that already has Kani proofs.

**Running Kani locally** requires Linux x86_64 or macOS (Intel/Apple Silicon). Install with `cargo install --locked kani-verifier && cargo kani setup`, which downloads CBMC and the Kani compiler toolchain (~2-3 GB). For IDE support without red squiggles, use `#[cfg_attr(not(rust_analyzer), cfg(kani))]` or add kani as an optional dependency with a feature flag for rust-analyzer.

---

## Enforcing dependency discipline with cargo-vet and cargo-deny

**cargo-vet is the right tool for per-crate dependency enforcement.** Its `[policy]` table lets you set different audit criteria for different workspace members:

```toml
# supply-chain/config.toml
[policy.logfwd-core]
criteria = "safe-to-deploy"
notes = "All kernel dependencies must be fully audited"

[policy.logfwd-core]
dependency-criteria = { bolero-generator = ["safe-to-run"] }
notes = "bolero-generator is only used for test/fuzz generation"
```

This means every dependency of `logfwd-core` must meet the `safe-to-deploy` criteria — either through your own audits or imported audits from trusted organizations. Mozilla, Google, Bytecode Alliance, and Embark Studios all publish audit sets you can import. Firefox uses this exact pattern with a 773-line `config.toml` that sets per-crate policies across their entire Rust dependency tree.

**cargo-deny operates at the workspace level**, not per-member. It cannot natively express "crate X may only depend on Y and Z." The workaround is running `cargo deny check --manifest-path crates/logfwd-core/Cargo.toml` with a stricter deny.toml specific to the kernel. For the kernel, use an allow-list approach:

```toml
# kernel-deny.toml
[bans]
multiple-versions = "deny"
wildcards = "deny"
allow = [
    "logfwd-core",
    "memchr",
    "hashbrown",
    "bolero-generator",
]
```

A simpler CI check for dependency growth uses `cargo metadata`:

```bash
#!/usr/bin/env bash
CURRENT=$(cargo metadata --no-deps -p logfwd-core --format-version 1 \
  | jq -r '.packages[0].dependencies[].name' | sort -u)
ALLOWED=$(cat crates/logfwd-core/allowed-deps.txt)
NEW=$(comm -13 <(echo "$ALLOWED") <(echo "$CURRENT"))
if [ -n "$NEW" ]; then
  echo "❌ New kernel dependencies: $NEW"; exit 1
fi
```

Google Fuchsia goes further with a **graded UB risk scale** (ub-risk-0 through ub-risk-4) where only designated unsafe reviewers can certify crates at each level. Their critical-path crates must be ub-risk-1 or lower. For logfwd-core, this level of formality isn't necessary, but the principle is sound: the kernel's dependencies deserve stricter scrutiny than the rest of the workspace.

---

## The CI pipeline: from lints to formal verification

A complete pipeline for the kernel has seven parallel jobs, running in roughly **20-40 minutes total**:

```yaml
name: Kernel Verification
on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo fmt --check -p logfwd-core
      - run: cargo clippy -p logfwd-core -- -D warnings
      - run: cargo build -p logfwd-core --target thumbv6m-none-eabi  # verify no_std

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo test -p logfwd-core --all-features
      - run: cargo test -p logfwd-core --all-features -- --ignored  # long proptests

  kani:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: model-checking/kani-github-action@v1
        with:
          working-directory: crates/logfwd-core
          args: --tests -Z function-contracts

  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: taiki-e/install-action@cargo-llvm-cov
      - run: cargo llvm-cov -p logfwd-core --fail-under-lines 100 --fail-under-functions 100

  semver:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: obi1kenobi/cargo-semver-checks-action@v2
        with:
          package: logfwd-core

  deps:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo install cargo-deny cargo-vet
      - run: cargo deny check --manifest-path crates/logfwd-core/Cargo.toml -c kernel-deny.toml
      - run: cargo vet
      - run: ./scripts/check-kernel-deps.sh

  proof-coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: ./scripts/check-proof-coverage.sh logfwd-core
```

**Kani proof timing** depends on harness complexity. Individual simple proofs complete in seconds; a crate with 20-30 proofs typically finishes in **5-15 minutes** on standard CI hardware. Kani results cannot be cached (CBMC re-solves every run), but you can cache the Kani installation and Cargo build artifacts. The official GitHub Action (`model-checking/kani-github-action@v1`) handles installation automatically.

**Mutation testing with cargo-mutants** belongs on a weekly schedule, not every PR — it runs `cargo test` against hundreds of mutated source variants and takes **30-60 minutes** for a small crate. It's valuable alongside Kani because it tests a different property: Kani proves your assertions hold for all inputs, while mutation testing checks that your test suite actually detects behavioral changes. Run with `cargo mutants -vV --in-place --package logfwd-core` and use `--shard K/N` to split across CI workers.

The proof-coverage check script extracts public functions from rustdoc JSON and verifies each has a corresponding `kani::proof_for_contract` or `kani::proof` harness:

```bash
#!/usr/bin/env bash
set -euo pipefail
PUB_FNS=$(grep -rn '^[[:space:]]*pub fn ' crates/logfwd-core/src/ \
  | grep -v '#\[cfg(test)\]' | grep -oP 'pub fn \K\w+' | sort -u)
PROVED=$(grep -rn 'proof_for_contract\|kani::proof' crates/logfwd-core/src/ \
  | grep -oP 'proof_for_contract\(\K[^)]+|fn \K\w+' | sort -u)
UNPROVED=$(comm -23 <(echo "$PUB_FNS") <(echo "$PROVED"))
if [ -n "$UNPROVED" ]; then
  echo "❌ Public functions without Kani proofs:"; echo "$UNPROVED"; exit 1
fi
```

**Proptest regression files** should be committed to version control. When proptest discovers a failing case, it persists the seed in `proptest-regressions/` (default path) so collaborators and CI replay it. Additionally, convert each discovered failure into a permanent `#[test]` unit test — the regression file captures the seed, but an explicit test case documents the edge case for future maintainers.

---

## Custom lints with dylint and other enforcement tools

For rules too nuanced for clippy's built-in lints, **dylint** (by Trail of Bits) lets you write project-specific lints loaded from dynamic libraries. A "no IO calls" lint would implement `LateLintPass` to inspect function bodies for calls to `std::io`, `std::fs`, and `std::net` paths. However, if the kernel crate already uses `#![no_std]`, this lint is redundant — the compiler handles it structurally. Dylint shines for rules like "no floating-point arithmetic in this module" or "all error types must implement `Display`."

Dylint requires nightly Rust (for the `rustc_private` feature) and uses `clippy_utils` for lint utilities. Setup: `cargo install cargo-dylint dylint-link`, then `cargo dylint --all` in CI. Define lint libraries in workspace metadata:

```toml
[workspace.metadata.dylint]
libraries = [{ path = "./lints/kernel-purity" }]
```

**cargo-semver-checks** detects public API surface changes — removed functions, changed signatures, altered visibility — but not behavioral contract changes. It uses rustdoc JSON comparison and found violations in **1 in 6 of the top 1,000 crates**. For the kernel, it catches when a function's type signature changes but won't detect if you weaken a precondition. That gap is exactly what Kani contracts fill.

**No built-in lint exists to deny `async`/`await`** at the crate level. With `#![no_std]`, `async` functions are technically available (they're a language feature, not a library feature), but they're useless without an executor. You can ban async runtime entry points via `clippy::disallowed_methods` in `clippy.toml`:

```toml
disallowed-methods = [
    { path = "tokio::runtime::Runtime::block_on", reason = "No async in kernel" },
    { path = "tokio::spawn", reason = "No async in kernel" },
]
```

---

## Migration plan (superseded — see dev-docs/PHASES.md)

The migration follows a principle: **move lowest-dependency, purest-logic modules first**, using `pub use` re-exports to maintain backward compatibility at every step. Always use `pub use`, never `pub type` — the latter is a semver hazard for unit structs and tuple structs that breaks construction syntax.

The workspace layout uses a flat structure under `crates/`:

```toml
# Root Cargo.toml (virtual manifest)
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.dependencies]
logfwd-core = { path = "crates/logfwd-core" }
memchr = "2"
serde = { version = "1", features = ["derive"] }
```

**PR 1 — Scaffold and utility functions.** Create the workspace structure and empty `logfwd-core` crate. Move `parse_int_fast` and `parse_float_fast` from `scan_config.rs`. Add `pub use logfwd_kernel::{parse_int_fast, parse_float_fast}` in logfwd-core. This is the lowest-risk PR that establishes the build infrastructure, lint configuration, and Kani pipeline. All existing tests pass unchanged.

**PR 2 — Pure types and classification.** Move `format.rs` and `chunk_classify.rs` entirely. These are data types and byte-pattern classification — inherently pure. Re-export with `pub use logfwd_kernel::format::*` and `pub use logfwd_kernel::chunk_classify::*` in logfwd-core. Other modules that depend on these types continue working through re-exports.

**PR 3 — CRI parsing.** Move `cri.rs` entirely. CRI line parsing is pure string/byte parsing that depends on `format.rs` types (already in kernel from PR 2). Re-export from logfwd-core.

**PR 4 — Scanner split.** This is the first split module. Define a `ParsedLine` struct in `logfwd-core/src/scanner_parse.rs` as the interface boundary — a plain Rust struct containing only primitive types and `Vec`/`String`. Move all parsing logic into the kernel. Leave Arrow `ArrayBuilder` dispatch in `logfwd-core/src/scanner.rs`, which consumes `ParsedLine` structs:

```rust
// logfwd-core/src/scanner_parse.rs
pub struct ParsedLine<'a> {
    pub timestamp: Option<i64>,
    pub severity: Option<u8>,
    pub body: &'a [u8],
    pub attributes: Vec<(&'a [u8], &'a [u8])>,
}

pub fn parse_log_line(line: &[u8], format: &Format) -> ParsedLine<'_> { /* ... */ }
```

**PR 5 — OTLP split.** Same pattern: define `OtlpLogRecord` in `logfwd-core/src/otlp_wire.rs` for the decoded wire format, move protobuf decoding there, leave `RecordBatch` assembly in logfwd-core. The struct approach (vs. a trait-based `ParseSink`) is simpler and produces values that proptest oracles can directly inspect.

During migration, **existing fuzz targets and proptest tests continue working** through re-exports. Once all five PRs land and the kernel is stable, fuzz targets testing pure parsing logic can optionally move to the kernel crate; tests exercising Arrow integration stay in logfwd-core. The `http` crate (pure types, no IO) + `hyper` (client/server with IO) is the canonical example of this pattern in the Rust ecosystem.

## Conclusion

The kernel crate's guarantees rest on four reinforcing layers. First, `#![no_std]` structurally prevents IO access — no lint to circumvent, no `#[allow]` escape hatch. Second, `forbid(unsafe_code)` plus strict clippy restriction lints catch panics, unwraps, and debug artifacts at compile time. Third, Kani proofs exhaustively verify function contracts against all possible inputs, with bolero providing a unified interface that also serves proptest and fuzzing. Fourth, CI enforces that every public function has a proof, coverage stays at 100%, dependencies don't grow unchecked, and cargo-vet ensures every dependency is audited. These layers are not redundant — each catches failures the others miss. The `no_std` enforcement catches IO that lints might miss; Kani catches logical errors that tests might miss; mutation testing catches assertions that Kani's property specifications might not cover. Together they make the kernel crate the most trustworthy component in the system.