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

### 1. P2: Yanked crate in lockfile (`fastrand 2.4.0`) — resolved

- Historical evidence (at audit time): `cargo deny check` reported `fastrand 2.4.0` as yanked.
- Current state: `Cargo.lock` now contains `fastrand 2.4.1`.
- Validation command: `cargo update -p fastrand --dry-run`.

References:

- `Cargo.lock` package stanza for `name = "fastrand"` (current `version = "2.4.1"`).
- `cargo deny check` output from this audit run.

Recommendation:

- Keep this item as a historical audit note.
- Continue enforcing with `cargo deny check` in CI.

---

### 2. P2: Build docs claim `just ci` is full CI, but it is fast-tier only — resolved

- Historical observation (audit snapshot): docs text and `justfile` semantics diverged.
- Current state: `book/src/development/building.md` now matches `justfile` (`ci` fast-tier, `ci-all` full-tier).

References:

- `book/src/development/building.md` command table (`just ci` / `just ci-all`)
- `justfile` target `ci`
- `justfile` target `ci-all`

Recommendation:

- Keep docs and `justfile` semantics aligned as part of normal CI/docs review.

---

### 3. P2: Crate-boundary guidance doc had stale dependency graph entries — resolved

- Historical observation (audit snapshot): dependency graph text was stale.
- Current state: crate-boundary docs were corrected to match manifests.

References:

- `docs/ci/crate-boundary-and-dependency-integrity.md` section: actual dependency graph
- `crates/logfwd-transform/Cargo.toml` dependencies section
- `crates/logfwd-output/Cargo.toml` dependencies section

Recommendation:

- Keep policy docs synchronized with manifest changes.
- Optionally add a lightweight graph-check script in CI to prevent future drift.

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

1. Keep `cargo deny check` in CI and monitor for new yanked advisories.
2. Add light anti-drift checks for docs that mirror tool/manifest semantics.
3. Decide and document policy on workspace dependency normalization.
4. Prune or annotate stale `deny.toml` license allowances.
