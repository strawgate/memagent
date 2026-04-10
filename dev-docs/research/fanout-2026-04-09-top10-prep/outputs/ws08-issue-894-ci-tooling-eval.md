# WS08 #894 CI Tooling Enhancements Evaluation (2026-04-09)

> **Status:** Completed
> **Date:** 2026-04-09
> **Context:** Fanout workstream output evaluating CI tooling options for issue #894.

## Scope and method

This workstream re-evaluates remaining `#894` tooling candidates against the current mainline CI posture and delivery constraints:

- Keep CI predictable and maintainable.
- Prefer high-signal checks with low runtime/maintenance cost.
- Avoid adding checks that duplicate existing safeguards.

Inputs reviewed:

- Root CI workflow and companion workflows under `.github/workflows/`
- Workspace dependency + lint policy in `Cargo.toml`
- Tooling entrypoints in `justfile`
- Lockfile / dependency footprint in `Cargo.lock`
- Production guidance in `dev-docs/references/production-patterns.md`

## Current baseline (what already exists)

Mainline already includes:

- Strong lint lane (fmt, taplo, typos, actionlint, clippy, rustdoc warnings, cargo-deny, cargo-machete).
- Tiered test lanes (Linux required, macOS conditional).
- Kani/TLA+/turmoil specialized verification lanes.
- Coverage lane (`cargo llvm-cov nextest --workspace`) marked non-blocking.
- Existing `cargo-machete` check already active in CI lint lane.

This means any new addition must provide *incremental signal* beyond the above.

## Decision table (keep/defer/drop)

| Candidate | Recommendation | Why | CI runtime/cost estimate | Notes |
|---|---|---|---|---|
| `cargo-semver-checks` | **Drop for now** | Every workspace crate is `publish = false`, so no public crates.io compatibility contract is currently enforced externally; low incremental value vs existing compile + doc + verification checks. | ~6–12 min/job plus cache churn on rustdoc JSON and baseline artifacts. | Reconsider if any crate becomes publishable or external API stability policy changes. |
| `cargo-hakari` | **Defer** | Workspace already uses resolver `"3"`, centralized `[workspace.dependencies]`, and explicit defaults policy; savings are uncertain vs added workspace-hack maintenance. | ~1–3 min/job for validate + occasional lockfile churn and review overhead. | Best revisited if compile times regress materially or dependency graph grows significantly. |
| Feature-pruning direction | **Keep and implement now** | High signal, low cost: enforce a concrete invariant that avoids confusing inheritance behavior (`workspace=true` + local `default-features`). | **<1s** Python guardrail in lint path. | Implemented in this change (script + CI step + just recipes) and one manifest cleanup. |
| `divan` viability | **Defer (evaluate in bench-only slice)** | Current bench stack already exists (`logfwd-bench`, criterion-oriented workflows); replacing benchmark framework now increases churn without direct CI quality gain. | If added as CI gate: typically +3–8 min depending on benchmark set; also risk of noisy perf variance. | Keep criterion path for now; consider `divan` only if benchmark ergonomics become a concrete pain point. |

## Required-item deep notes

### 1) `cargo-semver-checks`

**Finding:** not justified as a required CI gate today.

Evidence in repo:

- All workspace packages inspected are `publish = false`, including core/runtime/arrow/output/binary crates.
- CI already runs robust compile/lint/doc checks and formal methods lanes that catch many practical breakages.

Risk/ROI:

- Signal is strongest for published libraries with compatibility promises.
- For an internal monorepo where all crates are unpublished, this is mainly defensive overhead.

**Decision:** **Drop** from near-term CI plan; track as conditional-on-publishing.

### 2) `cargo-hakari`

**Finding:** potential compile savings are uncertain on this graph; likely moderate maintenance tax.

Evidence in repo:

- Workspace already uses resolver 3 and centralized root dependency policy.
- Existing CI uses caching + scoped jobs; build cost is dominated by large stacks (DataFusion/Kani/etc.), where hakari impact can be marginal.

Risk/ROI:

- Introduces additional generated artifacts / review noise.
- Useful when feature unification overhead is a measurable pain point; not clearly demonstrated yet.

**Decision:** **Defer** pending measured compile-regression trigger.

### 3) Feature-pruning direction

**Finding:** strong candidate for immediate improvement.

Problem addressed:

- Inheritance confusion when a crate uses `workspace = true` while also setting local `default-features` creates ambiguity and drift risk.

Action implemented:

- Added `scripts/check_workspace_inherited_default_features.py`.
- Wired check into CI lint workflow.
- Wired check into `just lint` and `just lint-all`.
- Cleaned one manifest instance (`crates/logfwd-io/Cargo.toml`) to satisfy guardrail.

Expected impact:

- Prevents a recurring dependency-policy footgun with negligible runtime cost.
- Keeps feature-surface policy explicit and centralized.

**Decision:** **Keep** and now **landed**.

### 4) `divan` viability

**Finding:** viable as a benchmark framework, but not justified as immediate CI/tooling migration.

Evidence in repo:

- Existing bench infrastructure is already extensive and integrated.
- Current objective favors low-risk CI improvements over framework migration churn.

Risk/ROI:

- Benchmark framework migration can cause duplicated harnesses and result drift during transition.
- CI perf jobs are susceptible to noise; adding another framework now does not increase correctness signal.

**Decision:** **Defer**; evaluate in isolated bench ergonomics follow-up.

## Sequencing recommendation

1. **Now (done):** add lightweight feature-inheritance guardrail.
2. **Next (optional):** collect compile-time telemetry for 2 weeks (job-level trend) before deciding on hakari.
3. **Later/conditional:** semver-checks only when any crate transitions to `publish = true` or external API guarantees become contractual.
4. **Bench track only:** prototype `divan` in a non-blocking bench experiment before any CI integration decision.

## Implemented change summary

- New script: `scripts/check_workspace_inherited_default_features.py`
- CI lint step: run workspace inheritance guardrail.
- Task runner: `workspace-inheritance-guard` recipe added; lint recipes include it.
- Manifest cleanup: removed local `default-features = false` override from inherited `chrono` in `crates/logfwd-io` dev-dependencies.

## Runtime/cost estimate summary

| Change | Added wall-time (per CI run) | Maintenance cost |
|---|---:|---|
| Workspace inheritance guardrail (implemented) | ~0.1–0.8s | Very low (single Python script). |
| `cargo-semver-checks` gate (not added) | ~6–12 min | Medium (baseline management + occasional false-positive triage). |
| `cargo-hakari` gate (not added) | ~1–3 min | Medium (workspace-hack regeneration churn). |
| `divan` CI benchmark lane (not added) | ~3–8 min | Medium-high (benchmark maintenance + noise management). |

## Final recommendation status

- `cargo-semver-checks`: **DROP (for current mainline state)**
- `cargo-hakari`: **DEFER (needs compile-regression evidence)**
- feature-pruning direction: **KEEP (implemented)**
- `divan` viability: **DEFER (bench-only exploration first)**
