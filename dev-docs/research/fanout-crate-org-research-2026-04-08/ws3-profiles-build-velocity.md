# Workstream 3 — Build Profile Strategy for Monorepo Velocity

Date: 2026-04-08
Scope: Cargo profile policy for local dev/test/release/bench + CI in this workspace.

## 1) Bounded orientation pass (completed)

Read during orientation:

- `README.md` (project goals, performance claims, user expectations)
- `DEVELOPING.md` (actual build/test commands, two-tier workflow, dev-lite path)
- `dev-docs/ARCHITECTURE.md` (crate roles, hot paths, runtime composition)
- `dev-docs/CHANGE_MAP.md` (what to co-update when workflow semantics change)
- Root `Cargo.toml` (workspace members, default-members, current profiles)
- `.cargo/config.toml` (global rustflags and wrapper settings)
- `justfile` (canonical developer/CI commands)

## 2) Current state audit

## 2.1 Existing profile configuration (root `Cargo.toml`)

- `dev`: only `debug = "line-tables-only"`
- `test`: only `debug = "line-tables-only"`
- `release`:
  - `opt-level = 3`
  - `lto = "thin"`
  - `codegen-units = 1`
  - `overflow-checks = true`
  - `debug = true`
- `bench`: inherits `release`

This is a coherent **safety-first + profileability-first** baseline, but it currently conflates two distinct needs:

1. shipping/benchmarking max-performance artifacts, and
2. profiled/diagnostic-friendly release artifacts.

## 2.2 Build workflow context that affects profile strategy

From docs + task runner:

- Workspace intentionally uses `default-members` to exclude DataFusion-heavy crates for faster default loops.
- `just ci` is fast-path (default-members), while `just ci-all` is full workspace.
- `just build-dev-lite` already exists as an intentional non-SQL build path.
- `JOBS` defaults to 2 for fairness/reproducibility in shared environments.

Interpretation: this repo already embraces **multi-lane iteration**; profiles should explicitly mirror that instead of relying on one release profile for all advanced uses.

## 2.3 Targeted timing experiment (local, cold-ish)

Commands run from repository root:

- `cargo check` → **152.568s**
- `cargo check --workspace` → **383.260s**

Observed delta:

- Full workspace check is ~2.5x slower than default-members check.
- The incremental cost is dominated by DataFusion and crates that pull it in (`logfwd-transform`, benches, and binary full feature set).

This validates that profile/policy decisions for heavy crates matter disproportionately.

## 2.4 Likely bottlenecks / misalignment

1. **`release` is doing two jobs at once.**
   `debug = true` and `overflow-checks = true` are excellent for flamegraphs/safety diagnostics, but they are not usually the fastest production artifact settings.

2. **`bench` inherits diagnostic-heavy `release`.**
   Benchmarks are likely under-reporting absolute ceiling due to extra checks/debug info inherited from release.

3. **No explicit CI profile lane.**
   CI behavior is mostly command-driven today (`just ci` vs `ci-all`) rather than profile-driven. This works, but makes profile semantics less explicit and can drift.

4. **No per-package profile specialization for heavy dependencies.**
   DataFusion and related transitive graph are known compile-time hotspots; there is no current package-level profile tuning.

5. **Release-like builds for tuning are manually encoded in recipes.**
   PGO exists in a recipe, but profile intent is not centralized in Cargo profile names.

---

## 3) Proposed profile policy

Policy goal: keep current fast local behavior and safety expectations, while making purpose-built lanes explicit.

## 3.1 Recommended defaults

### A) Keep current `dev` + `test` debug posture

- Keep `debug = "line-tables-only"` for both `dev` and `test`.
- Do **not** aggressively optimize dev/test by default.

Why:
- Preserves debuggability and verifier/test loops.
- Avoids accidental compile-time blowups for local iteration.

### B) Split release intent into two named lanes

Keep existing `release` as **diagnostic release** (rename intent in docs, optionally keep name for compatibility), and add:

- `profile.release-prod` (new): production/perf artifact lane
  - inherits `release`
  - `debug = "none"`
  - `overflow-checks = false`
  - retain `lto = "thin"`, `codegen-units = 1`
- `profile.release-debug` (new): explicit profiling/diagnostic lane
  - inherits `release`
  - `debug = true`
  - `overflow-checks = true`

Implementation choice:
- Either keep existing `release` as today and add `release-prod`, or flip and make `release` fast while adding `release-debug` for profiling. I recommend **keeping compatibility** short term:
  - keep current `release`
  - add `release-prod`
  - migrate CI/release packaging command to `--profile release-prod`

### C) Decouple `bench` from diagnostic release defaults

Set:
- `profile.bench` inherits `release-prod`
- `debug = "line-tables-only"` (optional compromise if stack traces in benches are needed)

Rationale:
- Bench should represent deployment-like throughput/latency more closely.
- If profiling benches is needed, use `--profile release-debug` or dedicated profiling recipe.

## 3.2 Optional per-package profile overrides (opt-in phase)

These are optional and should be introduced one at a time with measurement.

1. `profile.dev.package.datafusion*` and major heavy dependencies:
   - `debug = false`
   - possibly `codegen-units = 256` (compile speed over runtime)

2. `profile.test.package.datafusion*`:
   - `debug = false`

Expected effect:
- Less debug metadata generation for dependencies that contributors rarely step through.
- Potentially improved compile times and lower disk pressure.

Risk:
- Harder debugging *inside* those dependencies.
- If enabled too broadly, may hamper diagnosing dependency internals.

Guardrail:
- Limit overrides to known heavy third-party crates, not workspace crates.

## 3.3 CI-specific guidance

Use command lanes + profile lanes together:

- Fast signal CI (`lint + default-members tests`): unchanged commands.
- Full correctness CI (`--workspace`): unchanged commands.
- Performance-sensitive CI (binary artifact, perf smoke, benchmark compare): switch to explicit profiles:
  - `cargo build -p logfwd --profile release-prod`
  - optional profiling jobs use `--profile release-debug`

Also recommend:

- Set `CARGO_INCREMENTAL=0` in CI for deterministic compile behavior and stable cache characteristics.
- Keep sccache enabled as documented.
- Keep `JOBS` policy explicit per CI class (2 for shared runners; larger on dedicated perf runners).

---

## 4) Cross-Crate Impact Map

## 4.1 High-impact crates for profile policy

- `crates/logfwd` — binary packaging target, feature gate for DataFusion.
- `crates/logfwd-transform` — DataFusion entry point; major compile-time driver.
- `crates/logfwd-bench` and `crates/logfwd-competitive-bench` — sensitive to bench/release profile alignment.
- `crates/logfwd-runtime` — runtime hot path; affected by overflow-check and LTO/codegen posture in release-like profiles.

## 4.2 Medium-impact crates

- `crates/logfwd-output`, `crates/logfwd-io`, `crates/logfwd-arrow`, `crates/logfwd-core` — built frequently in default-members and full lanes; will inherit global dev/test/release choices.

## 4.3 Workflow/docs files needing synchronized changes if policy is adopted

- `Cargo.toml` (profile definitions)
- `justfile` (build/bench commands selecting new profiles)
- `DEVELOPING.md` (developer lane guidance)
- Possibly benchmark docs under `book/src/architecture/performance.md` if benchmark methodology changes due to profile lane split.

This aligns with `dev-docs/CHANGE_MAP.md` guidance: behavior/workflow changes must update contributor docs and performance docs together.

---

## 5) New profile knobs: expected benefits and regression risks

### Knob 1: `profile.release-prod` with `overflow-checks = false`, `debug = none`

- Benefit: faster runtime; smaller binaries; more realistic production perf.
- Risk: less runtime safety checking vs current release defaults; harder postmortem symbolization without external symbols.

### Knob 2: `profile.release-debug` (explicit)

- Benefit: removes ambiguity; keeps profiling and debug workflows first-class.
- Risk: small cognitive overhead (more profiles to choose from).

### Knob 3: `profile.bench` inherits `release-prod`

- Benefit: benchmark numbers track deploy reality better.
- Risk: historical benchmark trend discontinuity when profile semantics change.

### Knob 4: dependency-only `debug=false` overrides in dev/test for DataFusion graph

- Benefit: reduced compile time and artifact size in full-workspace loops.
- Risk: more difficult dependency-level debugging.

### Knob 5: CI `CARGO_INCREMENTAL=0`

- Benefit: reproducibility and more predictable CI timings.
- Risk: slower incremental rebuilds in long-lived CI workers if cache setup is unusual.

---

## 6) Suggested rollout plan

Phase 1 (low risk, high clarity)
1. Add `release-prod` and `release-debug` profiles.
2. Point artifact build and performance jobs to `release-prod`.
3. Keep existing local defaults unchanged.

Phase 2 (measurement-driven)
4. Switch `bench` to inherit from `release-prod`.
5. Re-baseline benchmark dashboards.

Phase 3 (optional, guarded)
6. Trial dependency-level debug overrides for DataFusion graph on a branch.
7. Keep only if full workspace check/test time shows meaningful improvement (target 10%+).

---

## 7) Validation plan (verify gains without regressions)

For each phase, compare before/after across the same runner class:

1. Build velocity metrics
   - `cargo check` (default-members)
   - `cargo check --workspace`
   - `cargo test -p logfwd-core`
   - `cargo nextest run --workspace --profile ci`

2. Artifact/perf metrics
   - Binary size for `logfwd` release artifacts.
   - Throughput/latency from existing pipeline benches (`just bench-self`, `just bench-ceiling-self`, `just bench-otlp`).

3. Reliability/safety guardrails
   - `just ci` and `just ci-all` must pass unchanged.
   - At least one profiling workflow (`just profile-otlp-local`) remains functional and documented.

4. Acceptance thresholds
   - No regression in fast-loop commands (`cargo check`, default-members tests).
   - Measurable improvement in production/perf artifact lane.
   - No loss of required debugging ergonomics for known workflows.

---

Recommendation: **Dual-Lane Profiles (Prod + Diagnostic) with Bench Alignment**

Top 5 profile changes
1. Add `profile.release-prod` (fast production lane).
2. Add `profile.release-debug` (explicit profiling lane).
3. Move `profile.bench` to inherit `release-prod`.
4. Keep `dev/test` as debug-friendly line tables by default.
5. Optionally add dependency-only (`datafusion*`) debug suppression in `dev/test` after measurement.

Validation plan
- Run paired before/after timing, artifact size, and bench throughput comparisons using existing just/cargo commands; accept only if build speed and perf improve without breaking ci/profiling workflows.
