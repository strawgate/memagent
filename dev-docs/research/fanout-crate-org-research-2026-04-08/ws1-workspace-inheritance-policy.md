# Workstream 1 — Workspace Inheritance Policy Hardening

Date: 2026-04-09
Scope: root workspace + all discovered `Cargo.toml` manifests under `crates/`
Status: Proposed (research output; not yet enforced)
Owner: Build/Infra maintainers
Review cadence: Revalidate each release cut or when workspace members/manifests change
Supersedes: N/A
Adoption target: After Phase 1 CI readiness

## Bounded orientation pass (completed)

Read as requested:

- `README.md`
- `DEVELOPING.md`
- `dev-docs/ARCHITECTURE.md`
- `dev-docs/CRATE_RULES.md`
- `dev-docs/CODE_STYLE.md`
- `dev-docs/CHANGE_MAP.md`

Inspected root + crate manifests, including at least one from each requested category:

- core: `crates/logfwd-core/Cargo.toml`
- runtime: `crates/logfwd-runtime/Cargo.toml`
- io: `crates/logfwd-io/Cargo.toml`
- output: `crates/logfwd-output/Cargo.toml`
- bench: `crates/logfwd-bench/Cargo.toml`, `crates/logfwd-competitive-bench/Cargo.toml`
- test-utils: `crates/logfwd-test-utils/Cargo.toml`
- plus config/transform/binary/proto/ebpf side manifests

---

## Objective restated

Define a strict but practical policy for:

1. `workspace.package`
2. `workspace.dependencies`
3. `workspace.lints`
4. per-crate `lints.workspace = true`
5. manifest hygiene rules (including sub-workspaces and research/fuzz/experimental manifests)

---

## Current state audit

### Evidence and reproducibility

Last verified: 2026-04-09
Evidence source: `python scripts/check_workspace_inheritance.py --report markdown`
Regenerate:

```bash
python scripts/check_workspace_inheritance.py --report markdown > /tmp/workspace-inheritance-report.md
```

Snapshot notes below are informative and may drift; CI-generated report output is the source of truth.

## 1) Root workspace inheritance surface

### Present today

- `[workspace.package]` exists, but only includes:
  - `edition = "2024"`
  - `rust-version = "1.85"`
- `[workspace.dependencies]` exists and is broad, but not consistently consumed via `{ workspace = true }`.
- `[workspace.lints]` exists (clippy + rust).

### Implication

The repo already has the right primitives, but policy enforcement is partial and currently relies on convention.

---

## 2) Workspace member compliance (`[lints] workspace = true`)

All current root workspace members have:

```toml
[lints]
workspace = true
```

No gaps were found among members listed in root `Cargo.toml` at verification time.

---

## 3) `workspace.dependencies` adoption gaps (members)

The following member crates pin versions directly even though the same dependency name exists in `[workspace.dependencies]`:

- `crates/logfwd-arrow`: `bytes = "1"`
- `crates/logfwd-io`: `bytes = "1"`
- `crates/logfwd-transform`: `bytes = "1"`, `tokio = { version = "1", ... }`
- `crates/logfwd-output`: `tokio = { version = "1", ... }`
- `crates/logfwd-runtime`: `tokio = { version = "1", ... }`
- `crates/logfwd`: `tokio = { version = "1", ... }`
- `crates/logfwd-bench`: `bytes = "1"`, `tokio = { version = "1", ... }`, `backon = { version = "1", ... }`, `globset = "0.4"`, `sonic-rs = "0.5"`
- `crates/logfwd-core` (dev-deps): `bytes = "1"`, `sonic-rs = "0.5"`

### Observations

- Most are semver-equivalent with workspace pins, so risk is currently low.
- Drift risk is still real when root moves versions and member manifests are missed.
- Several crates already use workspace inheritance correctly (`serde`, `tracing`, `thiserror`, etc.), so this is a consistency rather than capability problem.

---

## 4) Non-member / nested workspace manifests (critical policy edge)

These manifests are **not** governed by root workspace lints/inheritance and represent the biggest policy loophole:

1. `crates/logfwd-core/fuzz/Cargo.toml`
   - Declares its own `[workspace]`
   - Uses `edition = "2021"`
   - No `[lints] workspace = true`
   - Pins `arrow = "54"` while root uses `arrow = "55"`
2. `crates/logfwd-ebpf-proto/xdp-syslog/udp-bench/Cargo.toml`
   - Declares its own `[workspace]`
   - Uses `edition = "2021"`
   - No lint inheritance from root
3. `crates/logfwd-ebpf-proto/pipe-capture/Cargo.toml`
   - Declares nested `[workspace]`
   - Uses `edition = "2021"`
   - No root lint inheritance
4. `crates/logfwd-ebpf-proto/pipe-capture/pipe-capture-common/Cargo.toml`
   - Standalone package manifest, no explicit lint policy
5. `crates/logfwd-ebpf-proto/pipe-capture/pipe-capture-ebpf/Cargo.toml`
   - Standalone package manifest, no explicit lint policy

### Why this matters

Even if root workspace members are clean, these paths can reintroduce lint and dependency drift, especially for security updates and rustc policy transitions.

---

## 5) Manifest hygiene findings

### Good

- Root members consistently use `edition.workspace = true` and `rust-version.workspace = true`.
- Root members consistently set `publish = false`.
- Root members consistently include `[lints] workspace = true`.

### Weak points

- Inconsistent use of `{ workspace = true }` for shared dependencies.
- Multiple isolated sub-workspaces with independent policy surface.
- No explicit CI guard that enforces inheritance policy across all root members.
- No documented exception mechanism for intentional divergence (e.g., fuzz pinned to old Arrow ABI).

---

## Cross-Crate Impact Map

## A) Root policy knobs and direct blast radius

### `[workspace.package]`

- Affects all 15 root workspace members inheriting edition/rust-version.
- If expanded (e.g., `license`, `repository`, `homepage`), all members can inherit metadata and avoid duplication.

### `[workspace.dependencies]`

- Affects all members that opt into `{ workspace = true }`.
- Highest-value targets: `tokio`, `bytes`, `backon`, `globset`, `sonic-rs` currently duplicated in member manifests.

### `[workspace.lints]`

- Already affects all 15 root members via `lints.workspace = true`.
- Does **not** affect nested workspaces unless they adopt local lint blocks or are re-homed.

## B) Crate-group impact

- Runtime path (`logfwd`, `logfwd-runtime`, `logfwd-io`, `logfwd-output`, `logfwd-transform`): highest sensitivity to tokio/bytes policy changes.
- Core/proof path (`logfwd-core`, `logfwd-arrow`, `logfwd-types`): moderate sensitivity; mostly dependency alignment.
- Bench/research path (`logfwd-bench`, `logfwd-competitive-bench`, `logfwd-core/fuzz`, `udp-bench`, `pipe-capture*`): highest exception rate; should be explicitly segmented rather than implicitly drifting.

## C) CI/check impact

- A strict policy can be enforced for root workspace members with low false positives.
- Sub-workspaces need explicit opt-in/out lists to avoid noisy failures for intentional experiments.

---

## Proposed strict workspace inheritance policy

## Policy Rule 1 — Mandatory lint inheritance for root workspace members

For every crate listed in root `[workspace].members`:

- Must contain:

```toml
[lints]
workspace = true
```

- Must not define local lint overrides except in pre-approved allowlist files.

**Rationale:** matches `dev-docs/CODE_STYLE.md` explicit guidance.

---

## Policy Rule 2 — Dependency inheritance default

For root workspace members:

- If dependency key exists in root `[workspace.dependencies]`, member must use `{ workspace = true }`.
- Feature augmentation is allowed:

```toml
tokio = { workspace = true, features = ["rt-multi-thread", "macros"] }
```

- Exception requires annotation in manifest comment and inclusion in a central exception list.

---

## Policy Rule 3 — Expand `workspace.package` for metadata hygiene

Add at root (recommended baseline):

```toml
[workspace.package]
edition = "2024"
rust-version = "1.85"
version = "0.1.0"
license = "Apache-2.0"
repository = "https://github.com/strawgate/memagent"
```

Then migrate members from duplicated literals to `version.workspace = true` where appropriate.

If publishing remains disabled (`publish = false`) this still improves metadata consistency and future-proofing.

---

## Policy Rule 4 — Sub-workspace classification (governed vs sandboxed)

Every non-root workspace manifest must be tagged as one of:

1. **Governed sub-workspace**: must define local lint policy + documented dependency policy.
2. **Sandbox/experimental**: explicitly exempt, but must be excluded from strict CI checks.

No unclassified sub-workspaces.

---

## Policy Rule 5 — Machine enforcement in CI

Introduce a manifest policy check script (e.g., `scripts/check_workspace_inheritance.py`) with two scopes:

- **Strict scope (default, blocking):** root workspace members.
- **Advisory scope (non-blocking):** nested/sandbox manifests.

Checks:

1. root members have `[lints] workspace = true`
2. root members use `edition.workspace = true` and `rust-version.workspace = true`
3. for keys present in `[workspace.dependencies]`, disallow version literals unless allowlisted
4. report unused workspace dependencies as warning only

Canonical exception registry:

- File: `dev-docs/policy/workspace-inheritance-exceptions.toml`
- Schema:
  - `[[exception]]`
  - `crate = "crates/logfwd-core/fuzz"`
  - `dependency = "arrow"`
  - `reason = "fuzz ABI compatibility"`
  - `tracking_issue = "https://github.com/strawgate/memagent/issues/<id>"`
  - `expires = "YYYY-MM-DD"`
- Ownership: Build/Infra maintainers
- CI rule: expired exceptions fail strict checks

False-positive risk:

- Low for lint/edition checks.
- Medium for dependency inheritance rule (feature-combination edge cases, intentional forked versions).
- Mitigation: explicit allowlist + inline justification comments.

---

## Ready-to-apply manifest snippets

## Root `Cargo.toml` (policy-hardening example)

```toml
[workspace.package]
edition = "2024"
rust-version = "1.85"
version = "0.1.0"
license = "Apache-2.0"
repository = "https://github.com/strawgate/memagent"

[workspace.dependencies]
bytes = "1"
tokio = { version = "1" }
# ... existing workspace deps ...
```

## Member manifest (runtime crate) example

```toml
[package]
name = "logfwd-runtime"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish = false

[dependencies]
bytes = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "signal", "time", "sync"] }

[lints]
workspace = true
```

## Exception annotation example

```toml
[dependencies]
# workspace-inheritance-exception: pinned for fuzz ABI compatibility with toolchain X (issue #NNNN)
arrow = { version = "54", default-features = false }
```

---

## Adoption sequence

## Phase 0 (immediate, low-risk)

1. Document the policy in `dev-docs/CRATE_RULES.md` (or a new dedicated manifest policy doc).
2. Convert semver-identical duplicated deps to `{ workspace = true }` in root members:
   - `bytes` and `tokio` first.
3. Add a non-blocking CI audit script that prints violations + suggested fixes.

## Phase 1 (medium-risk rollout)

1. Enforce blocking checks for root members only.
2. Add explicit exception list for known intentional divergences.
3. Evaluate and classify sub-workspaces (`fuzz`, `pipe-capture`, `udp-bench`) as governed vs sandboxed.

## Phase 2 (policy completion)

1. Expand `workspace.package` metadata and migrate members to `version.workspace = true`.
2. Decide whether any sub-workspaces should be absorbed into root workspace governance.
3. Promote selected advisory checks to blocking after one release cycle of clean runs.

---

## Immediate low-risk changes

1. Replace duplicated `bytes = "1"` and `tokio = { version = "1", ... }` with workspace inheritance in root members.
2. Add script-based audit (warning mode) for root member manifests.
3. Add manifest exception comment format and centralized allowlist file.

## Medium-risk changes needing rollout

1. Blocking CI enforcement for dependency inheritance.
2. `workspace.package` expansion with `version.workspace = true` migration.
3. Governance decision for nested sub-workspaces (fuzz/eBPF research).

## Watch-items

1. Fuzz/tooling compatibility when aligning Arrow versions.
2. Nested workspace independence requirements for eBPF toolchains.
3. Potential feature-resolution shifts when converting to workspace dependencies with local features.

---

## Recommendation

Recommendation: **Adopt strict inheritance for root workspace members now, and treat nested workspaces as explicitly classified exceptions until separately governed.**

This yields high consistency with low disruption and avoids conflating production policy with research sandboxes.

---

## Top 5 actionable changes

1. Add CI script to enforce lint/edition/rust-version inheritance for all root workspace members.
2. Convert all root member `tokio` and `bytes` usages to `{ workspace = true }` with per-crate feature lists.
3. Add documented dependency inheritance exception mechanism (allowlist + inline comment convention).
4. Classify `logfwd-core/fuzz`, `pipe-capture*`, and `udp-bench` as governed or sandboxed in docs.
5. Expand `[workspace.package]` metadata and plan `version.workspace = true` migration.

---

## What evidence would change your recommendation

1. If nested sub-workspaces are promoted to first-class release artifacts, they should move from exception model to full governance immediately.
2. If any crate requires incompatible major versions that cannot be feature-resolved safely, dependency inheritance should allow more permanent carve-outs.
3. If CI runtime or false-positive burden from strict checks is materially high, staged enforcement should remain warning-only for longer.
4. If workspace metadata (license/repository/version) is intentionally heterogeneous by design, `workspace.package` expansion should be limited.
5. If future cargo capabilities provide better native policy enforcement, custom scripts should be reduced in favor of built-in checks.
