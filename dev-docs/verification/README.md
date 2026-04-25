# Verification Docs

Canonical landing page for verification structure, ownership, and maintenance.

Use this directory together with:

- [`../VERIFICATION.md`](../VERIFICATION.md) for policy: which technique to use and when.
- [`../../tla/README.md`](../../tla/README.md) for the TLA+ spec inventory and TLC commands.
- [`traceability-matrix.md`](traceability-matrix.md) for current seam-to-evidence mapping.
- [`kani-boundary-contract.toml`](kani-boundary-contract.toml) for non-core Kani ownership.
- [`fuzz-manifest.toml`](fuzz-manifest.toml) for trust-boundary fuzz target inventory.

## Current Inventory

As of April 2026, the verification surface is large enough that structure matters:

- Kani: ~234 proof harnesses across core and proof-bearing non-core seams
- `#[cfg(kani)]` modules: ~52
- TLA+: 7 primary specs under [`../../tla`](../../tla)
- TLC configs: 21 safety/liveness/coverage/thorough configs
- Turmoil simulation files: 17 under [`../../crates/ffwd/tests/turmoil_sim`](../../crates/ffwd/tests/turmoil_sim)
- Verification guardrail scripts: 5 under [`../../scripts`](../../scripts)
- Non-core Kani seams in boundary contract: 39

That scale is workable, but only if ownership stays explicit and duplication stays under control.

## Canonical Entry Points

### Human-facing

- Policy and thresholds: [`../VERIFICATION.md`](../VERIFICATION.md)
- TLA+ model catalog: [`../../tla/README.md`](../../tla/README.md)
- Seam ownership and evidence map: [`traceability-matrix.md`](traceability-matrix.md)

### Machine-facing

- Structural verification checks: [`../../xtask/src/main.rs`](../../xtask/src/main.rs)
- Non-core Kani contract: [`kani-boundary-contract.toml`](kani-boundary-contract.toml)
- Fuzz inventory: [`fuzz-manifest.toml`](fuzz-manifest.toml)
- CI verification trigger and TLC coverage contracts:
  - [`../../scripts/verify_kani_boundary_contract.py`](../../scripts/verify_kani_boundary_contract.py)
  - [`../../scripts/verify_tlc_matrix_contract.py`](../../scripts/verify_tlc_matrix_contract.py)
  - [`../../scripts/verify_tla_coverage.py`](../../scripts/verify_tla_coverage.py)
  - [`../../scripts/verify_proptest_regressions.py`](../../scripts/verify_proptest_regressions.py)
  - [`../../scripts/verify_verification_trigger_contract.py`](../../scripts/verify_verification_trigger_contract.py)

### Local runners

- `just verify` -> `cargo xtask verify`
- `just verification-guardrail`
- `just kani`
- `just miri`
- `just tlc model config`
- `just tlc-tail`

### CI runners

- Structural verification in [`../../.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
- TLC execution through the shared [`../../.github/actions/run-tlc/action.yml`](../../.github/actions/run-tlc/action.yml)

## Current Layout

The repo currently uses four verification storage patterns:

1. In-file proofs near production code
   Example: `#[cfg(kani)] mod verification`
2. Top-level verification assets by framework
   Example: [`../../tla`](../../tla)
3. Verification manifests where a machine-readable inventory already pays for itself
   Example: `kani-boundary-contract.toml`, `fuzz-manifest.toml`
4. Runtime/simulation evidence under test trees
   Example: [`../../crates/ffwd/tests/turmoil_sim`](../../crates/ffwd/tests/turmoil_sim)

This is a good base. The main weakness is that some verification facts are duplicated across prose, TOML, CI YAML, `just`, and `xtask` logic.

## Problems To Fix

1. ~~Structural policy is only partially machine-enforced.~~
   **Resolved.** [`cargo xtask verify`](../../xtask/src/main.rs) now exits with a non-zero code on any violation, causing CI to fail.
2. Verification ownership is partly duplicated.
   Kani seams already have a useful TOML contract, while TLC suite inventory is still mostly encoded in workflow and script wiring.
3. Local TLC ergonomics lag CI.
   CI runs a meaningful TLC matrix; local usage still requires knowing config names and commands.
4. Trace validation exists as a runtime prototype, but not yet as a spec-linked workflow.
5. New metadata could become a problem of its own.
   A manifest is only worth adding if it replaces existing duplicated truth.

## Recommended Target Shape

Do not split verification into a separate repository. Keep the assets in-repo, but tighten structure conservatively.

### 1. Keep proof code close to owning modules

Continue the current pattern for:

- Kani proofs attached to production modules
- loom-style concurrency tests when introduced
- small local reducers extracted from async/runtime shells

This matches Tokio's `cfg(loom)` layout and keeps proof ownership close to the code being proved.

### 2. Keep heavyweight design specs as top-level subsystems

Continue keeping TLA+ under [`../../tla`](../../tla), but treat it as a first-class subsystem with:

- one shared runner surface
- clear CI/local parity
- explicit ownership in docs

This is closer to CCF and etcd than to a casual examples folder.

### 3. Make `dev-docs/verification/` the metadata hub

Use this directory for:

- contributor-facing navigation
- the manifests that already pay for themselves
- generated or validated summaries
- organization guidance

Recommended near-term contents:

```text
dev-docs/verification/
  README.md
  kani-boundary-contract.toml
  fuzz-manifest.toml
  traceability-matrix.md
```

Possible later addition if it eliminates duplication:

```text
  tla-manifest.toml
```

### 4. Treat CI as a renderer of verification policy, not the policy itself

The policy should live in code, docs, and a very small number of manifests.
CI should consume that policy, not become the only place where it exists.

Avoid making `.github/workflows/ci.yml` the only authoritative place where:

- TLC suites are enumerated
- required proof-bearing crates are listed
- ownership rules are encoded

## Manifest Direction

The repo already has a good pattern in [`kani-boundary-contract.toml`](kani-boundary-contract.toml).
Keep using that pattern where it clearly pays for itself. Do not add more manifests unless they replace duplicated truth.

### Keep now

- `kani-boundary-contract.toml` because scripts and docs already depend on it
- `fuzz-manifest.toml` because trust-boundary inventory is a real machine-consumed list

### Consider later

Add `tla-manifest.toml` only if we decide to make one source of truth for:

- which specs exist
- which configs belong to each spec
- which suites are required in CI
- which code paths each spec owns

If we do that, the manifest should replace some current hard-coding in `ci.yml`, `justfile`, and `xtask`.

### Do not add yet

- `simulation-manifest.toml`

The simulation and trace-validation surface is still evolving. Adding a manifest there now would likely create metadata churn instead of reducing it.

## Runner Direction

Adopt one verification-oriented runner surface instead of scattering logic across `just`, CI, and scripts.

Recommended direction:

- keep `just` as the user entrypoint
- move only duplicated suite enumeration into `xtask`
- let CI call `cargo xtask verify-*` or `cargo xtask tlc --suite ...`

Recommended future commands:

```text
cargo xtask verify-structure
cargo xtask verify-guardrails
cargo xtask verify-doc-sync
cargo xtask tlc --suite pr
cargo xtask tlc --suite nightly
cargo xtask verification-inventory
```

This would align local and CI execution more closely with the CCF-style wrapper approach without forcing extra metadata everywhere.

## Trace Validation Direction

The repo already has a prototype bridge in:

- [`../../crates/ffwd/tests/turmoil_sim/trace_validation.rs`](../../crates/ffwd/tests/turmoil_sim/trace_validation.rs)
- [`../../crates/ffwd/tests/turmoil_sim/trace_bridge.rs`](../../crates/ffwd/tests/turmoil_sim/trace_bridge.rs)

The next step should be to connect trace validation to a spec-backed model, following the shape used by CCF and etcd:

- runtime emits trace artifacts
- a validator constrains a spec using those artifacts
- failing validation means implementation/spec drift, not just local invariant failure

Do not replace the current Rust-side validator yet. Extend it with spec linkage.

## What Not To Do

- Do not move all verification assets into a separate repository.
- Do not create a second copy of verification ownership in issue bodies only.
- Do not split `tla/` into subdirectories yet; the current flat layout is still manageable.
- Do not add manifests unless they delete or replace duplicated truth.
- Do not add more markdown summaries unless they are generated or validated from the durable source of truth.

## Recommended Execution Plan

### Phase 1: Immediate cleanup

1. Add this README as the canonical landing page.
2. Keep linking contributors here from [`../README.md`](../README.md).
3. Treat [`traceability-matrix.md`](traceability-matrix.md) as the human view and manifests as the durable source of truth where manifests already exist.

### Phase 2: Runner and enforcement cleanup

1. Split [`../../xtask/src/main.rs`](../../xtask/src/main.rs) into submodules
2. Add better local TLC runner parity with CI
3. ~~Promote `cargo xtask verify` from warning-only to failing mode once known gaps are intentionally classified~~ **Done.** `cargo xtask verify` now hard-fails in CI (non-zero exit code on any structural violation).

### Phase 3: Selective manifest completion

1. Decide whether TLA suite inventory is duplicated enough to justify `tla-manifest.toml`
2. If yes, add only `tla-manifest.toml`
3. Use it to delete or simplify duplicated TLC suite enumeration

### Phase 4: Spec-linked trace validation

1. Define the first trace-backed TLA validation slice
2. Wire a single scenario through runtime trace -> spec validation
3. Expand only after the bridge is stable and debuggable

## Maintenance Rules

- If a PR adds a new proof-worthy seam, update the owning manifest in the same PR.
- If a PR adds a new TLA+ spec or config, update local runner parity in the same PR.
- If a TLA manifest exists, update it in the same PR too.
- If a PR changes verification ownership, update:
  - [`../VERIFICATION.md`](../VERIFICATION.md)
  - the relevant manifest(s)
  - [`traceability-matrix.md`](traceability-matrix.md)
- Before adding a new manifest, first identify which duplicated facts it will delete or replace.
