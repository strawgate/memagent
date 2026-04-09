# Workstream 4 — Edition 2024 Dependency-Inheritance Readiness

Date: 2026-04-09
Scope: workspace-wide Cargo manifests (`Cargo.toml` at root + crate manifests)
Mode: targeted research + policy proposal

---

## 1) Bounded orientation pass (completed)

I completed the required orientation pass over:

- `README.md` (workspace purpose, performance positioning, deployment shape)
- `DEVELOPING.md` (build tiers, CI expectations, workflow constraints)
- `dev-docs/CRATE_RULES.md` (crate boundary + dependency intent)
- `dev-docs/CHANGE_MAP.md` (how dependency/boundary changes propagate)

I then audited:

- root `Cargo.toml`
- all crate manifests under `crates/**/Cargo.toml`
- edition/rust-version settings
- inherited dependency usage patterns (`workspace = true`)
- `default-features` behavior at workspace root and crate level

---

## 2) Edition 2024 risk model (what can go wrong)

This workstream focuses on dependency inheritance traps in Edition 2024, especially around `default-features`.

### Core pitfall

The historically confusing pattern is attempting to override feature-default behavior on an inherited dependency at the crate site (for example by adding `default-features = false` next to `workspace = true`). In modern Cargo/Edition 2024 migration guidance, these cases are treated much more strictly because they are easy to misread and often do not do what maintainers think.

### Practical failure modes

1. **Silent intent drift**: crate manifest appears to disable defaults for an inherited dependency, but effective behavior is controlled by workspace definition.
2. **Migration breaks**: a future Cargo/edition bump turns warning-class behavior into hard error, or changes lint strictness in CI.
3. **Cross-crate inconsistency**: one crate uses inherited dep while another pins version/features directly, causing accidental feature unification side effects.
4. **Feature-surface expansion**: a dependency defaults-on in one path and defaults-off in another path, producing larger compile/runtime surface than expected.

---

## 3) Workspace snapshot: current state

### Edition settings

- Workspace uses `edition = "2024"` at `[workspace.package]`.
- Member crates mostly inherit edition/rust-version from workspace.
- A few non-workspace-member eBPF helper manifests and fuzz manifest still use 2021; they are out of the main workspace dependency-inheritance path.

### Workspace dependency policy currently encoded

Root `[workspace.dependencies]` defines 25 shared deps. Of these, the following explicitly pin defaults off:

- `arrow` (`default-features = false`, custom features)
- `backon` (`default-features = false`)
- `datafusion` (`default-features = false`, selective expression features)
- `opentelemetry-otlp` (`default-features = false`, HTTP/reqwest/trace/metrics)
- `thiserror` (`default-features = false`)

This is already a strong centralization pattern.

### Inherited dependency usage findings

I scanned every workspace crate manifest for inherited dependency patterns and the specific anti-pattern `workspace = true` combined with local `default-features`.

#### Hard-failure check

- **No current hard-failure candidates found** for the known Edition 2024 inheritance pitfall.
- Specifically: **zero occurrences** of inherited dependencies declared with both `workspace = true` and local `default-features` override.

#### Style/maintainability observations

Not hard failures, but worth guarding:

1. **Mixed inheritance style for common deps**
   - Some crates use workspace inheritance for `tokio`; others pin direct `tokio = { version = "1", ... }`.
   - This is legal, but weakens centralized policy and increases drift risk.

2. **Feature duplication in inheriting crates**
   - Example: `serde` already has `derive` at workspace level, but one crate restates `features = ["derive"]` on inherited serde.
   - This is harmless due to feature unification, but adds noise and can mask intent.

3. **Non-workspace manifests (fuzz/eBPF helper bins) use direct dependency specs**
   - Acceptable due to tooling/scope isolation, but should be explicitly documented as exceptions to avoid accidental policy bypass.

---

## 4) Hard failures vs style-only recommendations

### Hard failures (current)

- **None identified** in the main workspace manifests for Edition 2024 inherited-default-features behavior.

### Potential future hard failures

Could occur if contributors add patterns like:

```toml
serde = { workspace = true, default-features = false }
```

or

```toml
arrow = { workspace = true, default-features = true }
```

These should be treated as prohibited patterns under workspace policy.

### Style-only / policy-quality improvements

- Standardize on workspace inheritance for shared strategic dependencies (`tokio`, `serde`, `thiserror`, `arrow`, `datafusion`, `opentelemetry*`) unless a crate has a documented exception.
- Reduce redundant feature restatements when workspace already encodes the required baseline.

---

## 5) Cross-Crate Impact Map

This map captures how a change to inherited dependency policy fans out.

| Dependency | Workspace Baseline | Crates inheriting (representative) | Impact if baseline changes | Risk class |
|---|---|---|---|---|
| `arrow` | defaults off + `ipc_compression` | `logfwd-arrow`, `logfwd-runtime`, `logfwd-io`, `logfwd-output`, `logfwd`, test-utils/bench variants | Compilation surface + Arrow codec behavior + benches/tests | High |
| `datafusion` | defaults off + selective expression features | `logfwd-transform`, `logfwd-bench` | SQL transform functionality + dev/full build split | High |
| `opentelemetry-otlp` | defaults off + HTTP reqwest stack | primarily `logfwd-runtime` | Export transport stack and binary footprint | Medium-High |
| `backon` | defaults off + tokio sleep | `logfwd-runtime` (inherited), bench may direct-pin variant | Retry behavior compile surface | Medium |
| `thiserror` | defaults off | config/runtime/io/transform/output | Usually low runtime impact; compile behavior consistency | Medium |
| `tokio` | workspace version only (defaults on unless crate overrides) | many crates inherit, some direct-pin | Async runtime feature drift if not centralized | Medium |
| `serde` | workspace includes `derive` | many crates inherit | Build consistency; minor feature drift risk | Low-Medium |

### Fanout rules implied by this map

- Any change to workspace dependency defaults/features must be treated as a **crate-boundary change** and validated across all affected crates.
- For `arrow`, `datafusion`, and `opentelemetry-otlp`, require full-workspace CI path (`just ci-all`) before merge.

---

## 6) Proposed policy text (docs-ready)

The following text is intended to be copy-ready for `dev-docs/CRATE_RULES.md` (or a dedicated dependency policy doc).

> ### Workspace dependency inheritance policy (Edition 2024)
>
> 1. Shared dependencies must be declared in `[workspace.dependencies]` and inherited via `{ workspace = true }` in member crates.
> 2. Member crates **must not** set `default-features` on an inherited dependency.
>    - If default-feature behavior must change, update the workspace dependency declaration instead.
> 3. Member crates may add crate-local `features = [...]` only when required by that crate’s behavior.
> 4. Dependencies that intentionally do not inherit from workspace (tooling-only crates, fuzz targets, eBPF side projects) must include a short comment documenting why.
> 5. PRs that modify workspace dependency default/features for `arrow`, `datafusion`, `tokio`, `opentelemetry*`, or `serde` must run full-workspace checks and include a cross-crate impact note.

### Enforcement-level statement

- Rule #2 should be treated as **hard error policy** (CI fail).
- Rules #3–#4 are **style+clarity policy** (warn first, then tighten).

---

## 7) Lint/check opportunities

### A. Fast manifest guard script (recommended immediate)

Add a CI script that fails on:

- any dependency table entry containing both `workspace = true` and `default-features = ...`

Implementation options:

- Python `tomllib` script in `scripts/` (fast, deterministic)
- integrate into existing lint/ci recipes (`just lint-all` / `just ci`)

### B. Optional normalization checker (warn-only initially)

Warn when high-value shared deps are direct-pinned in member crates instead of inherited:

- `tokio`, `serde`, `thiserror`, `arrow`, `datafusion`, `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`

Allowlist exceptions:

- fuzz manifests
- eBPF side manifests
- explicitly documented performance bench-only crates when needed

### C. cargo-deny/metadata integration

If desired, add a metadata audit step that emits a machine-readable report of effective default-feature state for core shared deps across workspace members. This helps detect accidental expansion early.

---

## 8) Rollout plan (minimize breakage)

### Phase 0 — Document + announce

- Add policy text to contributor docs.
- Add one short section in `DEVELOPING.md` under dependency conventions.

### Phase 1 — Non-blocking CI warning

- Introduce script as warn-only (e.g., prints violations but does not fail).
- Collect any existing exceptions and annotate with rationale.

### Phase 2 — Blocking for hard rule only

- Flip CI to fail on `workspace=true + default-features` combinations.
- Keep normalization checks warn-only.

### Phase 3 — Tighten consistency where low risk

- Migrate direct-pinned shared deps to workspace inheritance where practical.
- Preserve deliberate exceptions for isolated tooling/fuzz/eBPF manifests.

### Phase 4 — Periodic drift audit

- Add a quarterly (or release-train) dependency policy audit task.
- Include report snippet in release engineering checklist.

---

## 9) Top 5 fixes or guardrails

1. **Add CI hard-check** banning `default-features` on inherited dependencies (`workspace = true`).
2. **Codify dependency inheritance policy text** in contributor docs (copy-ready block above).
3. **Add warn-only normalization checker** for high-value shared deps that should inherit from workspace.
4. **Document exception classes** (fuzz/eBPF/tooling crates) so intentional non-inheritance is explicit.
5. **Require cross-crate impact note + full CI-all run** for workspace dependency feature/default changes.

---

## 10) Confidence and remaining unknowns

Confidence: **High** on current-state audit findings in workspace manifests and immediate policy recommendations.

Remaining unknowns:

1. Exact future Cargo diagnostics severity for every inheritance edge case may evolve; policy should prefer explicit CI script guarantees over relying on tool messaging.
2. Some non-workspace helper manifests may eventually be promoted into workspace membership; if that happens, they must be re-audited under this policy.
3. Potential transitive feature-surface effects (especially through DataFusion/Arrow ecosystems) still warrant occasional `cargo tree -e features` spot checks after major version bumps.

---

Recommendation: **Adopt-Now (Hard-Guard + Phased-Normalization)**
