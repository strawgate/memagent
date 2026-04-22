# Config POC Fan-In Synthesis

Date: 2026-04-19

## Inputs

Fan-out manifest:

- `dev-docs/research/fanout-2026-04-19-config-pocs/fanout-manifest.json`

Collected artifacts:

- `dev-docs/research/fanout-2026-04-19-config-pocs/cloud-artifacts/artifact-index.json`

Cloud tasks:

| Workstream | Attempts | Task | Final state |
|---|---:|---|---|
| `config` crate source-framework POC | 2 | `task_e_69e468f3e710832e92d7dc369a208946` | READY |
| `figment` / `confique` / `twelf` framework comparison | 3 | `task_e_69e468f94380832ebab7dc9ac279008c` | READY |
| typed env coercion with `serde_with` / local helpers | 3 | `task_e_69e468fca9b8832e85035d667845b21a` | READY |
| validation crates `garde` / `validator` / `validify` | 2 | `task_e_69e468ff8248832eaee63478399b43cb` | READY |
| diagnostics/schema/glob with `serde_path_to_error` / `schemars` / `globset` | 2 | `task_e_69e4690278a4832eb351822850ffd16f` | READY |

The fan-out asked for implementation-oriented POCs where feasible, not just surveys.
Every task produced final diffs, and the collector saved all requested per-attempt diffs.

## Convergence

The attempts converged on one architectural direction:

**Keep `logfwd-config` as the config authority. Do not replace the loader with a broad config framework. Improve it with narrowly scoped crates and local helpers.**

Strong points of agreement:

- The current shape is right: parse YAML, expand env in the parsed value tree, deserialize into strict Serde types, normalize into `Config`, then run semantic validation.
- No published config framework replaces FastForward's domain validation: duplicate listeners, duplicate output paths, feedback-loop checks, Loki label collisions, transport-specific rules, and base-path-sensitive validation remain custom.
- Raw text env substitution is unacceptable for YAML configs because env values containing `#`, `:`, `{`, `[`, or spaces can mutate YAML syntax.
- Any framework integration still needs custom env-in-file semantics or a deliberate redesign.
- The highest-confidence immediate external crate is `serde_path_to_error`, not a config framework.

## Disagreements

### `figment`: adopt now vs defer

The framework workstream recommended `figment` as a layering/provenance augmentation, while the `config` workstream recommended only limited augmentation ideas.

This is not a true conflict. The difference is scope:

- `figment` looks best if the product needs layered providers or provenance.
- It does not solve file-embedded `${VAR}` expansion and may introduce YAML behavior drift because its YAML provider is not the repo's current `serde_yaml_ng` path.
- Therefore `figment` should not be a near-term loader replacement. It is a future provider/provenance experiment after the core semantics are stable.

### Typed env coercion: replace quote markers vs hybrid

Typed env coercion attempts agreed the model is promising but not ready as a direct replacement.

The best target model is:

- env expansion in YAML values produces strings, never YAML source fragments
- typed fields accept either native YAML scalars or strings parsed into the field type
- partial interpolation stays string-only
- quote-style marker machinery can eventually shrink or disappear

But replacing current behavior requires annotating or wrapping all numeric/bool fields that can receive env values. Without full coverage, existing valid configs regress.

### `globset`: exact matching vs conservative safety

`globset` is mature and already in the workspace, but exact glob matching is not identical to the current conservative "could this output path match this input glob?" safety check.

Adopt it only if the final design preserves conservative rejection, for example:

- use `globset` for exact known-path matches
- keep a conservative fallback for uncertain or unsupported patterns
- add regression tests for every current feedback-loop case before replacing custom logic

## Repo Fit

The fan-out fits the local repo constraints:

- `logfwd-config` already uses strict Serde types and centralized semantic validation.
- The codebase's `dev-docs/CHANGE_MAP.md` requires config behavior changes to update parser/validation, docs, examples as needed, and negative tests.
- Current local follow-up work already moves `effective-config` away from raw env text expansion, which the `config` POC independently validated as the right direction.
- The workspace already depends on `globset`; adopting it in `logfwd-config` would not introduce a new published crate, but it would still change semantics and needs a test matrix.
- `serde_path_to_error` is small and targeted enough to fit the current architecture.

The cloud POC diffs should not be merged directly. Several intentionally added dev/prototype dependencies and test spikes. Use them as evidence and implementation sketches.

## Evidence Quality

Decision-grade:

- **Reject full `config` crate replacement.** The POC demonstrated that `config` does not expand file-embedded `${VAR}` and raw text pre-expansion breaks YAML-significant env values.
- **Keep custom semantic validation.** Every workstream found that cross-object config rules remain repo-specific.
- **Adopt `serde_path_to_error` next.** The diagnostics workstream implemented a small loader-only integration and tests for nested field path reporting.

Directional but promising:

- **Typed env coercion hybrid.** POCs showed local helpers work for `usize`, `u64`, `bool`, and option cases, but production adoption needs a field inventory and compatibility tests.
- **Selective `garde`.** The validation POC showed `garde` can make local field constraints clearer, but adoption should follow newtypes/`TryFrom` for scalar invariants and should not absorb cross-object rules.
- **`figment` augmentation.** It is the best future candidate for provider layering/provenance, but only after current semantics are locked down.

Still needs more proof:

- **`schemars`.** It may help docs/config tooling, but schema fidelity for internally tagged enums, aliases, custom deserializers, and generated examples needs a separate tool-focused POC.
- **`globset`.** Useful but not a drop-in replacement for conservative "could match" logic.

## Ranked Recommendation

### 1. Adopt `serde_path_to_error`

Recommendation: **adopt now**

Why:

- small integration surface
- immediate user-facing diagnostic gain
- no config syntax change
- no runtime pipeline behavior change
- aligns with keeping Serde as the typed config core

Implementation notes:

- Add `serde_path_to_error` as a workspace dependency.
- Wrap the `RawConfig` deserialization step in `load.rs`.
- Preserve existing `ConfigError` shape unless a better typed diagnostic variant is worth adding.
- Add tests for nested paths under both simple and `pipelines` layouts.

### 2. Keep the YAML-aware env expansion fix and docs

Recommendation: **keep and finish**

Why:

- fan-out independently confirmed raw text substitution is unsafe
- current local follow-up fixes `effective-config` for env values containing `#`
- docs now need to remain explicit about exact unquoted placeholders, quoted placeholders, partial interpolation, and duplicate key behavior

Implementation notes:

- Keep `Config::expand_env_yaml_str` or equivalent.
- Route all user-visible expanded config output through the YAML-aware path.
- Keep regression tests for env values containing YAML syntax.

### 3. Prototype typed env coercion as a staged migration

Recommendation: **hybrid, not immediate replacement**

Why:

- target-typed string parsing is the cleanest long-term way to avoid quote-style detection
- local helpers are probably enough; avoid adding `serde_with` unless it clearly reduces real code
- broad field coverage is required before changing documented behavior

Implementation sequence:

1. Inventory numeric/bool/duration fields that should accept env strings.
2. Add local `string_or_native` Serde helpers for representative `usize`, `u64`, `bool`, and `Option<T>` fields.
3. Add tests using env strings with `#`, `:`, `{`, `[`, leading/trailing spaces, and `"08"`.
4. Keep current AST scalar coercion until helper coverage is complete.
5. Revisit docs only after compatibility is proven.

### 4. Improve validation with newtypes first, `garde` selectively later

Recommendation: **selective adoption only**

Why:

- cross-object validation must stay centralized
- newtypes/`TryFrom` are stronger for values that should be impossible post-parse
- `garde` is useful only for leaf/local constraints where annotations improve readability without hiding business rules

Implementation sequence:

1. Classify validation rules into local field, variant-specific, cross-object, and runtime/dry-run buckets.
2. Convert high-value scalar invariants to newtypes or `TryFrom` where the internal model benefits.
3. Only add `garde` after a small production-quality patch proves better clarity and error formatting.

### 5. Defer `figment`

Recommendation: **defer until layering/provenance is a product need**

Why:

- best broad framework candidate
- POC confirmed strict fields, enum tagging, typed env overlays, and layered parsing can work
- still does not solve in-file env expansion
- may introduce YAML behavior drift if it bypasses `serde_yaml_ng`
- dependency cost and migration complexity are not justified for current needs

Use `figment` later if:

- config needs explicit layered provider semantics
- operator-facing source provenance becomes important
- a parity suite proves no YAML/config behavior drift

### 6. Defer `schemars`; prototype `globset` carefully

Recommendation: **future scoped POCs**

For `schemars`:

- use only for build-time docs/schema/config-builder tooling at first
- validate representative internally tagged enums and compatibility aliases

For `globset`:

- use only if conservative safety is preserved
- add tests before replacing `is_glob_match_possible`
- consider exact-match `globset` plus custom conservative fallback

## Integration Plan

### Phase 0: Preserve current local fixes

Keep the current local follow-up work:

- YAML-aware effective config expansion
- `#` env value regression test
- normalized glob path comparison
- config reference docs for env semantics
- workspace dependency cleanup for `serde_yaml_ng`

### Phase 1: Add `serde_path_to_error`

Target files:

- `Cargo.toml`
- `crates/logfwd-config/Cargo.toml`
- `crates/logfwd-config/src/load.rs`
- `crates/logfwd-config/tests/validation_gaps.rs`

Verification:

- `cargo test -p logfwd-config --test validation_gaps`
- `cargo test -p logfwd-config --lib load::tests`
- `cargo clippy -p logfwd-config -- -D warnings`

### Phase 2: Typed env helper pilot

Target files:

- `crates/logfwd-config/src/serde_helpers.rs`
- `crates/logfwd-config/src/types.rs`
- focused tests in `validation_gaps.rs` or a dedicated integration test

Start with low-risk fields where env use is plausible and tests already cover behavior, such as workers and timeout fields.

### Phase 3: Validation refactor plan

Do not add `garde` yet.

First produce a small classification patch or memo:

- local scalar invariant candidates for newtypes
- variant-specific checks that should stay near variants
- cross-object checks that stay in `validate.rs`
- runtime/dry-run checks that stay in CLI/runtime validation

### Phase 4: Optional framework/tooling POCs

Only after phases 1-3:

- `figment` provider/provenance POC if layered config becomes a real requirement
- `schemars` schema generation POC if docs/config-builder tooling needs it
- `globset` replacement POC with conservative fallback and regression matrix

## Decision

Proceed with:

1. `serde_path_to_error` integration.
2. Keep the current YAML-aware env expansion direction.
3. Plan typed env coercion as a compatibility-preserving migration.

Do not proceed with:

1. Full replacement by `config`.
2. Full replacement by `figment`, `confique`, or `twelf`.
3. Broad validation-derive migration.
4. Schema generation or glob replacement before tighter scoped tests.
