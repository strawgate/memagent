# Crate Boundary and Dependency Integrity — Full Review Guidance

You are reviewing crate boundary integrity and dependency rules for logfwd.
This workspace uses strict structural constraints on each crate, enforced by
CI and code review together. Fail this check if any boundary is violated.
These rules exist because `logfwd-core` is the "proven kernel" — a formally
verified `no_std` component whose proofs are only valid if the crate remains
free of IO, threads, allocation-heavy patterns, and external dependencies.
Relaxing any rule is an architectural regression, not a convenience.


## logfwd-core Absolute Invariants — Every Rule Is a Hard Fail

### Rule 1: `#![no_std]` + `extern crate alloc`

`logfwd-core` must compile to `thumbv6m-none-eabi` (bare metal ARM Cortex-M0,
no operating system, no heap allocator unless explicitly provided). CI enforces
this with:

```
cargo build -p logfwd-core --target thumbv6m-none-eabi
```

Any import of `std::` (not `alloc::`) is a build failure on that target. Allowed:
`alloc::vec::Vec`, `alloc::string::String`, `alloc::borrow::Cow`,
`alloc::collections::BTreeMap`. Not allowed: `std::collections::HashMap`,
`std::io::*`, `std::sync::*`, `std::thread::*`, `std::fs::*`. If a PR introduces
any `std::` import into `logfwd-core`, reject it and require the `alloc::` equivalent
or a restructuring that moves IO to a satellite crate.

### Rule 2: `#![forbid(unsafe_code)]`

This attribute makes it a compile error to use `unsafe` anywhere in `logfwd-core`,
including with `#[allow(unsafe_code)]`. It cannot be overridden. Any `unsafe` block
in `logfwd-core` is a hard compiler error; no PR can sneak one in without removing
the crate-level attribute. If a PR removes or weakens `#![forbid(unsafe_code)]` in
`logfwd-core`, reject it immediately and escalate — this is an architectural
regression that invalidates the Kani proofs (Kani proofs of safe Rust do not cover
unsafe code paths).

### Rule 3: Only `memchr` and `wide` as dependencies

`logfwd-core`'s `[dependencies]` section in `Cargo.toml` must contain exactly
`memchr` and `wide`. No other dependency is permitted, regardless of how small or
well-audited it seems. Adding `serde`, `bytes`, `regex`, or any other crate violates
the minimal-dependency invariant that makes `logfwd-core` auditable, provable, and
embeddable. CI enforces this with a dependency allowlist check. If a PR adds any
other dependency to `logfwd-core`, reject it. If there is a legitimate need (e.g.,
a specific utility), the dependency belongs in `logfwd-arrow` or another satellite
crate, and `logfwd-core` should expose a trait that the satellite implements.

### Rule 4: No panics in logfwd-core

`CRATE_RULES.md` specifies that `clippy::unwrap_used`, `clippy::panic`, and
`clippy::indexing_slicing` must be denied for `logfwd-core`, but these are
review-enforced rather than currently in the workspace `Cargo.toml`. The workspace
lint config uses `clippy::pedantic = warn`; the per-crate deny rules for
`logfwd-core` are enforced by code review.

**This rule applies to new changes.** Existing `.unwrap()` and `panic!()` calls in
the codebase are known violations that pre-date enforcement. Flag only PRs that
*introduce* new `.unwrap()`, `panic!()`, or unchecked direct slice indexing
(`slice[i]` without a prior bounds check or `.get()`). Require `.get(i)?` or
`.get(i).ok_or(Error::OutOfBounds)?` instead. If a PR adds these patterns and CI
passes without objection, the lint is not yet enforced at the compiler level —
that is a separate issue (#todo: add per-crate deny to `Cargo.toml`), but it does
not make the new violation acceptable.

### Rule 5: Every public function has a Kani proof

With the same three exemptions as the Formal Verification Coverage check: async
functions, heap-heavy `Vec`/`HashMap` code, and trivial getters/setters with no
logic. CI runs a proof coverage script. For proof quality requirements, see the
Formal Verification Coverage check.

### Rule 6: No IO, no threads, no async

These are structurally blocked by `no_std` (the relevant `std` APIs do not exist on
`thumbv6m-none-eabi`). Flag any creative workaround: `extern "C"` function calls to
OS APIs, raw system call wrappers, conditional compilation that enables IO only on
non-embedded targets. The point of `no_std` is that the kernel is structurally
incapable of performing IO — a PR that works around this intent violates the
architectural contract.


## Dependency Direction — No Cycles, No Upward Deps

The core rule is that dependencies must only flow toward lower-level crates.
The actual dependency graph (verify against `Cargo.toml`):

- `logfwd-core` is the foundation: no sibling crate deps, only `memchr` and `wide`.
- `logfwd-arrow` depends on `logfwd-core`.
- `logfwd-io` depends on `logfwd-core` and `logfwd-arrow`.
- `logfwd-transform` depends on `logfwd-core`, `logfwd-arrow`, and `logfwd-io`.
- `logfwd-output` depends on `logfwd-core`, `logfwd-arrow`, `logfwd-io`, and `logfwd-config`.
- `logfwd-config` depends on `serde`; it does not depend on other workspace crates.
- `logfwd` (binary) depends on all other crates.

Hard violations that must be rejected regardless of current graph state:
- `logfwd-core` depending on ANY sibling crate.
- `logfwd-arrow` depending on `logfwd-io`, `logfwd-transform`, `logfwd-output`, or `logfwd`.
- `logfwd-config` depending on any crate that processes data (`logfwd-arrow`, `logfwd-io`, `logfwd-transform`, `logfwd-output`).
- Any circular dependency.
- Any dependency that goes from a lower-level crate to a higher-level one (e.g., `logfwd-io` depending on `logfwd-transform`).

The binary crate must contain ONLY async orchestration: config loading, channel
wiring, signal handling, spawning tasks. No parsing logic, no encoding logic, no
SQL. If a PR adds business logic to `logfwd/src/pipeline.rs` or `logfwd/src/main.rs`
that belongs in a library crate, flag it and require extraction.


## Per-Crate Structural Rules

Rather than maintaining a complete allowed-dependency list here (which will go
stale as dependencies are added), verify new dependencies against the principles
in `CRATE_RULES.md`. The structural rules that ARE invariant and must be enforced:

**logfwd-arrow:** `unsafe` is permitted only for SIMD implementations. Every `unsafe`
block must have a `SAFETY` comment. Any `unsafe` for non-SIMD purposes is a violation.
The workspace arrow dependency (`arrow = { workspace = true }`) includes the
`ipc_compression` feature; do not add a separate arrow dep with different features.

**logfwd-io:** IO is expected here. Tests must use temp directories (via `tempfile`
or similar) rather than real filesystem paths to avoid test interference.

**logfwd-transform:** DataFusion is the SQL engine. Do not add a second SQL engine.
Enrichment tables must implement Arrow's `RecordBatchReader` for compatibility.

**logfwd-output:** Transport and serialization must remain separate. The OTLP
protobuf encoder lives in `logfwd-core`; `logfwd-output` only handles transport.
Do not move encoding logic into `logfwd-output`.

**logfwd-config:** All config parsing and validation happens here. Config structs
must be validated at deserialization time so the rest of the codebase receives
already-validated types.


## Workspace Dependency Management

New dependencies must be added to `[workspace.dependencies]` in the root `Cargo.toml`
with an explicit version. Per-crate `Cargo.toml` entries must use `workspace = true`
and may specify `features = [...]` but must not re-specify the version. This ensures
all crates in the workspace use the same version of every shared dependency.

For any new dependency added to the workspace, require:
1. What functionality it provides that justifies adding it.
2. What crate(s) will use it.
3. Its license (must be in the approved list below).
4. Whether it introduces any new transitive dependencies with non-approved licenses or known advisories.

Major version bumps of arrow, datafusion, tokio, or opentelemetry-otlp require:
a summary of breaking changes in the PR description, all affected call sites
updated in the same PR, and the corresponding reference doc in `dev-docs/references/`
updated.


## Cargo Deny Compliance

Approved licenses: MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause,
BSD-3-Clause, ISC, Unicode-3.0, Zlib, OpenSSL, CC0-1.0, BSL-1.0, MPL-2.0.
Any dependency with a license outside this list requires explicit discussion.
No new dependencies with RustSec advisories (RUSTSEC-*) for vulnerabilities or
unsoundness. `deny.toml`'s skip list is for unavoidable transitive conflicts only;
any PR that adds a new entry to the skip list to allow a duplicate must explain
why the duplication is unavoidable.


## Workspace Lint Inheritance and Build Configuration

Every crate (new and existing) must have `[lints] workspace = true`. Per-crate lint
overrides are forbidden — adjustments go in root `[workspace.lints]`. New crates must
use `edition = "2024"` and inherit `rust-version` from the workspace (currently 1.85).
No new crate may use a lower edition or a lower `rust-version`.


## Adding a New Crate — The Five-Step Checklist

If this PR adds a new crate, all five of the following must be present; flag any
that are missing:

1. A new section in `dev-docs/CRATE_RULES.md` stating the crate's one-sentence
   purpose and its allowed dependency list, with enforcement method for each rule.
2. A per-crate `AGENTS.md` file inside the new crate directory listing its purpose
   and rules for AI agents.
3. The crate added to the workspace members list in root `Cargo.toml`.
4. CI checks added for any structural rules that can be automated (if the crate has
   a dependency allowlist, a CI step that verifies it).
5. The crate's dependency direction documented in `ARCHITECTURE.md` under the
   "Crate boundaries" section.
