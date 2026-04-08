# Dependency and Structure Audit (2026-04-08)

## Scope

Audit focus:

- Workspace dependency hygiene (`Cargo.toml`, `Cargo.lock`, `deny.toml`)
- Crate boundary / dependency-direction integrity
- Build/test command semantics drift between docs and actual tooling

## Commands Run

- `cargo check --workspace`
- `cargo tree -d --workspace`
- `cargo deny check`
- `cargo update -p fastrand --dry-run`
- metadata and manifest inspection with `cargo metadata` + direct `Cargo.toml` reads

## Findings (Prioritized)

### 1. P2: Yanked crate in lockfile (`fastrand 2.4.0`)

- Evidence: `cargo deny check` reports a yanked crate.
- Lockfile location: `Cargo.lock` shows `fastrand 2.4.0` at line 1566.
- Validation: `cargo update -p fastrand --dry-run` resolves cleanly to `2.4.1`.

References:

- `Cargo.lock:1566`
- `Cargo.lock:1567`

Recommendation:

- Update lockfile with `cargo update -p fastrand`.
- Re-run `cargo deny check` in CI to verify warning is gone.

---

### 2. P2: Build docs claim `just ci` is full CI, but it is fast-tier only

- `book/src/development/building.md` says `just ci` runs the full CI suite.
- `justfile` defines `ci` as quick/default-members path (no DataFusion), and `ci-all` as full workspace.
- This can give false confidence that DataFusion/full-workspace checks ran locally.

References:

- `book/src/development/building.md:19`
- `justfile:58`
- `justfile:61`

Recommendation:

- Update docs to describe:
  - `just ci` = fast dev CI tier
  - `just ci-all` = full CI-equivalent tier

---

### 3. P2: Crate-boundary guidance doc has stale dependency graph entries

- `docs/ci/crate-boundary-and-dependency-integrity.md` says:
  - `logfwd-transform` depends on `logfwd-io`
  - `logfwd-output` depends on `logfwd-io`
- Current manifests do not match:
  - `crates/logfwd-transform/Cargo.toml` has no `logfwd-io` dependency.
  - `crates/logfwd-output/Cargo.toml` has no `logfwd-io` dependency.
- This creates reviewer confusion because the policy doc claims “actual dependency graph.”

References:

- `docs/ci/crate-boundary-and-dependency-integrity.md:94`
- `docs/ci/crate-boundary-and-dependency-integrity.md:95`
- `crates/logfwd-transform/Cargo.toml:11`
- `crates/logfwd-output/Cargo.toml:11`

Recommendation:

- Update the policy document to reflect current manifests.
- Optionally add a lightweight graph-check script in CI to prevent doc drift.

---

### 4. P3: Workspace dependency declaration style is inconsistent

- Several crates directly version dependencies that are already listed in `[workspace.dependencies]` (for example `tokio`, `bytes`, `backon`, `globset`, `sonic-rs`), rather than using `workspace = true`.
- This is not a correctness bug today, but increases drift risk and makes version coordination harder over time.

Examples:

- `crates/logfwd/Cargo.toml` (`tokio`)
- `crates/logfwd-io/Cargo.toml` (`bytes`)
- `crates/logfwd-transform/Cargo.toml` (`tokio`, `bytes`)
- `crates/logfwd-bench/Cargo.toml` (`tokio`, `bytes`, `backon`, `globset`, `sonic-rs`)

Recommendation:

- Gradually normalize shared dependencies to `workspace = true` + local `features = [...]` where needed.
- Enforce with a manifest lint script (warn-only initially).

---

### 5. P3: `deny.toml` has stale allowed-license entries not currently encountered

- `cargo deny check` reports unmatched allowances for:
  - `Unicode-DFS-2016`
  - `OpenSSL`
  - `MPL-2.0`
- Low risk, but adds policy noise.

References:

- `deny.toml:17`
- `deny.toml:19`
- `deny.toml:22`

Recommendation:

- Prune unmatched entries or leave a short comment that these are intentionally pre-approved for expected future deps.

## What Passed

- `cargo check --workspace` succeeded.
- No dependency cycles or upward crate-direction violations were observed in current manifests.
- `cargo deny check` passed (warnings only; no hard failures in advisories/bans/licenses/sources).

## Suggested Follow-up Order

1. Fix yanked crate (`fastrand`) in lockfile.
2. Fix doc mismatches (`building.md`, crate-boundary CI guidance).
3. Decide and document policy on workspace dependency normalization.
4. Prune or annotate stale `deny.toml` license allowances.
