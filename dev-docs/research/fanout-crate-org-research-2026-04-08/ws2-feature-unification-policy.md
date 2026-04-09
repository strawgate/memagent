# Workstream 2: Feature Unification and Dependency Surface Policy

> **Status:** Proposed
> **Date:** 2026-04-09
> **Context:** Fanout workstream analysis of workspace feature topology and dependency declaration patterns, with a concrete policy for predictable builds.

## Scope and orientation baseline

This workstream covers feature and dependency-surface behavior for the workspace configured with resolver `"3"` and edition `2024`.
The orientation pass reviewed:
`README.md`, `DEVELOPING.md`, `dev-docs/ARCHITECTURE.md`, `dev-docs/CRATE_RULES.md`, and `dev-docs/CHANGE_MAP.md`.
It then inspected root `Cargo.toml` and representative crate manifests:
`logfwd`, `logfwd-runtime`, `logfwd-transform`, `logfwd-io`, `logfwd-output`, and `logfwd-bench`.

## Executive summary

The workspace already has good foundational controls:
resolver v3, constrained high-risk workspace dependencies (`arrow`, `datafusion`, `opentelemetry-otlp`), and explicit default-member partitioning for fast dev loops.
The primary remaining risks are not version drift but feature-shape drift from local dependency declarations, especially when crates bypass workspace definitions for `tokio`, `bytes`, HTTP clients, and proto dependencies.

The highest-confidence policy direction is:
standardize dependency declaration authority at the workspace for all shared/runtime-sensitive crates and reserve crate-local overrides for narrowly defined exception classes.
This reduces accidental default-feature activation and makes dependency behavior auditable from one place.

## Current feature/dependency topology

### Workspace-level anchors (already strong)

- Resolver is pinned to `"3"`, which isolates features more predictably than older resolvers.
- Workspace centralizes several critical dependencies and explicitly disables defaults where risk is high:
  - `arrow` with `default-features = false`.
  - `datafusion` with `default-features = false` and selected expression/protection features.
  - `opentelemetry-otlp` with `default-features = false` and explicit protocol/transport features.
  - `backon` with `default-features = false`.
  - `thiserror` with `default-features = false`.
- Workspace `default-members` intentionally exclude heavy crates (`logfwd-transform`, `logfwd`), matching documented fast/full workflows.

### Crate-level feature topology (key nodes)

- `logfwd` and `logfwd-runtime` both define `default = ["datafusion"]`.
  - `logfwd` disables default features for `logfwd-runtime` and forwards `datafusion` explicitly.
  - This is correct and intentional for a dev-lite path via `--no-default-features`.
- `logfwd-transform` is optional in runtime and binary crates, acting as the SQL feature payload.
- `logfwd-bench` unconditionally depends on `logfwd-transform` and `datafusion`, effectively always “full stack.”

### Declaration pattern variability (main policy target)

Across crates, several dependencies are declared locally with explicit versions/features instead of inheriting from workspace entries, including:

- `tokio` (some crates use `workspace = true`, others use local `version = "1"` plus per-crate feature lists).
- `bytes` (workspace in some crates, literal version in others).
- `reqwest`/`ureq`, `opentelemetry-proto`, `prost`, and `axum` are crate-local only.
- Multiple path dependencies include explicit `version = "0.1.0"`, which is acceptable but adds churn surface.

This mixed style does not automatically break builds, but it increases cognitive load and raises the chance of accidental feature/default expansion when new crates or contributors copy local patterns.

## Risk and ambiguity map

### 1) `default-features = false` is inconsistent outside workspace-critical crates

Current usage is strongest where it matters most (`arrow`, `datafusion`, OTLP, HTTP clients in output, `axum`, and some core deps), but policy is implicit rather than explicit.
When policy is implicit, new dependencies can be added with defaults enabled accidentally.

**Risk profile:** medium-high over time.
**Failure mode:** hidden transitive pulls (TLS backends, heavy codecs, extra runtime integrations).

### 2) Mixed workspace inheritance vs local declarations obscures ownership

When the same ecosystem crate (`tokio`, `bytes`) is sometimes workspace-managed and sometimes local, maintainers must inspect many files to answer:
“What exact feature envelope is intended across runtime-critical crates?”

**Risk profile:** medium.
**Failure mode:** non-obvious feature unions across crates and harder root-cause on build/runtime changes.

### 3) Feature-forwarding is centralized in only two crates but not codified as policy

The `datafusion` behavior is intentionally forwarded through `logfwd-runtime` and `logfwd`.
This is good architecture, but the rule is fragile unless documented as a policy invariant.

**Risk profile:** medium.
**Failure mode:** future crate introduces parallel transform toggles and diverges behavior from facade/runtime.

### 4) Benchmark crate can accidentally become precedent for production manifests

`logfwd-bench` includes broad, research-oriented dependencies and local declarations.
Without policy guardrails, contributors may copy these patterns into production crates.

**Risk profile:** medium-low but persistent.
**Failure mode:** production crates inherit broad defaults from bench-style manifests.

## Cross-Crate Impact Map

| Policy decision area | Directly affected crates | Potential side effects | Suggested guardrail |
|---|---|---|---|
| Centralize shared deps in `[workspace.dependencies]` | `logfwd`, `logfwd-runtime`, `logfwd-io`, `logfwd-output`, `logfwd-transform`, `logfwd-bench` | Manifest churn; short-term merge conflicts | Add policy table + cargo-deny/manifest lint checks in CI |
| Standardize `tokio` declaration model | runtime, binary, output, transform, test-utils, bench | Accidental removal of required per-crate runtime features | Require explicit crate-local feature lists even with `workspace = true` base version |
| Define mandatory `default-features = false` classes | all crates using network/transport/proto/large frameworks | Potential missing feature at compile time during migration | Migrate in ordered batches with `just ci-all` checkpoints |
| Codify feature forwarding boundaries (`datafusion`, `turmoil`) | `logfwd`, `logfwd-runtime`, `logfwd-transform` | Downstream behavior regressions for users relying on current flags | Add “feature forwarding contract” section in crate rules + compile checks |
| Separate bench exception policy from production policy | `logfwd-bench`, `logfwd-competitive-bench` | None functionally; mostly contributor behavior | Mark bench crates as exception zone with stricter comment requirements |

## Proposed policy: Feature Unification and Dependency Surface

### Policy objective

Keep build behavior predictable and auditable by making dependency authority explicit, minimizing implicit default features, and constraining crate-local exceptions.

### Decision matrix

#### A) When to define dependencies in workspace

Define in `[workspace.dependencies]` when any of the following are true:

1. Dependency appears in 2+ crates.
2. Dependency has high default-feature risk (runtime, network/TLS, async stacks, SQL/compute, proto/serialization).
3. Dependency is part of crate-boundary contracts or performance-sensitive paths.
4. Dependency version must stay synchronized for tooling, semver, or security patching.

**Examples in this repo that should be workspace-owned (if not already):**
`tokio`, `bytes`, `prost`, `opentelemetry-proto`, `reqwest`, `ureq`.

#### B) When crate-local overrides are allowed

Crate-local declaration/override is allowed only when one of these is true:

1. Truly crate-unique dependency (single crate, no near-term reuse expected).
2. Target-specific dependency (`cfg(...)`) that is crate-specific.
3. Bench/research-only dependency that must not influence production envelopes.
4. Temporary migration exception with a documented TODO + owner + expiry issue.

For shared dependencies, local overrides of version are disallowed by default.
Feature additions are allowed only if they are crate-local behavioral needs and are documented in the crate manifest comment block.

#### C) When `default-features = false` is required

`default-features = false` is required for:

1. Networking and transport clients/servers (`reqwest`, `ureq`, `axum`, OTLP transports).
2. Heavy data/compute frameworks (`arrow`, `datafusion`).
3. Serialization/proto stacks where defaults may enable unused protocols/codegen paths.
4. Dependencies known to ship optional runtime integrations or TLS stacks by default.

`default-features = false` is optional when:

- The crate is tiny, deterministic, and has no meaningful optional behavior that affects runtime or build graph.
- The dependency is strictly dev-only and low-risk.

## Manifest patterns

### Good/default pattern (workspace authority + explicit local features)

```toml
# Root Cargo.toml
[workspace.dependencies]
tokio = { version = "1", default-features = false }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }
bytes = { version = "1", default-features = false }
```

```toml
# Crate Cargo.toml
[dependencies]
# Inherit version and default-feature stance from workspace.
# Add only crate-local feature needs.
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "sync"] }
reqwest = { workspace = true, features = ["http2", "gzip", "json", "stream"] }
bytes = { workspace = true }
```

Why this is preferred:
one authority point for version/default policy, plus explicit per-crate capability needs.

### Exception-case pattern (crate-local, justified and bounded)

```toml
[dependencies]
# Exception: benchmark-only protocol experiment.
# Tracked by issue <issue-id-required>. MUST be replaced before merge.
# Include rationale, scope (bench/test/prod), and reevaluation owner.
experimental-client = { version = "0.9", default-features = false, features = ["zstd"] }
```

Required for exception cases:
comment with rationale, scope (bench/test/prod), and tracking issue for reevaluation.

## Additional enforcement recommendations

1. Add a short “Dependency Surface Policy” section to `dev-docs/CRATE_RULES.md` with mandatory rules and exceptions.
2. Add CI check script to flag duplicate shared deps declared with local versions when workspace entry exists.
3. Add CI check script to flag high-risk deps without `default-features = false` unless allowlisted.
4. Add a small manifest template snippet in `DEVELOPING.md` to reduce copy/paste drift.
5. Add a pre-PR checklist item: “Did this dependency belong in workspace?”

## Migration order (short, low-risk sequence)

1. **Policy codification first:** document rules in `CRATE_RULES.md` and contributor guidance.
2. **Shared runtime deps:** move/normalize `tokio` and `bytes` declarations to workspace authority pattern.
3. **Transport/proto deps:** normalize `reqwest`, `ureq`, `prost`, `opentelemetry-proto` policy and defaults.
4. **Bench exceptions:** mark and isolate exceptions in benchmark crates with comments/issues.
5. **Automated enforcement:** add CI guard scripts after manifests are normalized to prevent regression.

Recommendation: **Adopt Workspace-Authority with Explicit Exception Gates (WA-EEG)**

Canonical rules are defined above in **Decision matrix** and **Additional enforcement recommendations**.
Execution sequence is defined in **Migration order (short, low-risk sequence)**.
