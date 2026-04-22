# Validation Crate POC: `garde`, `validator`, and `validify`

You are handling one workstream inside a larger Codex Cloud fanout for this repository.

Assume you are operating as a principal engineer with full autonomy to investigate this workstream.
You may inspect the repo deeply, run tests or benchmarks, prototype code, edit files, and use web or doc research when that materially improves the result.
Do not stop at a shallow summary if the right answer requires code inspection, experiments, or comparison of multiple approaches.

## Reality model

Assume you can rely on only:

- the committed repository state on the launched branch
- this prompt text

Assume you cannot rely on:

- any local branch, worktree, or uncommitted file from the launcher
- hidden thread context from prior tasks
- repo-local fanout artifacts unless they are committed on the launched branch

## Objective

Determine whether a published Rust validation crate should be adopted for field-level config validation while keeping custom cross-pipeline semantic validation.

Candidate crates:

- `garde`
- `validator`
- `validify`
- any better current published crate you find

You MUST implement a POC with the best candidate if feasible.

## Why this workstream exists

PR #2244 added many validation checks manually:

- duplicate listen addresses across pipelines per transport
- duplicate file output paths
- file input/output feedback loops
- worker count bounds
- invalid TCP/UDP output format rejection
- duplicate named inputs/outputs
- Loki static/dynamic label collisions

Some checks are inherently cross-object and should remain custom. Others are local field constraints and may be cleaner as derive-based validation, newtypes, or `TryFrom` conversion.

We need to know whether a validation crate improves signal, maintainability, and diagnostics, or just adds annotation noise.

## Mode

Mode: prototype.

Autonomy: investigate + prototype + test.

Compare the strongest crate against a "parse, don't validate" approach using newtypes and Serde `try_from`.

## Required execution checklist

- MUST read:
  - `crates/logfwd-config/src/types.rs`
  - `crates/logfwd-config/src/validate.rs`
  - `crates/logfwd-config/tests/validation_gaps.rs`
  - `dev-docs/CODE_STYLE.md`
  - `dev-docs/CHANGE_MAP.md`
- MUST research current docs for `garde`, `validator`, and `validify`.
- MUST classify existing validation checks into:
  - local field validation
  - variant-specific validation
  - cross-object semantic validation
  - runtime/dry-run validation
- MUST prototype the best validation crate on at least two local field constraints, such as:
  - `workers` range `1..=1024`
  - required listen/path rules for input variants
  - URL/endpoint shape where applicable
- MUST compare with a newtype/`TryFrom` approach for one of those constraints.
- MUST evaluate error message quality and whether validation errors can include config path/context.
- MUST run focused tests for the POC or explain exact blockers.
- MUST write the deliverable file listed below.
- MUST end with `Recommendation: adopt`, `Recommendation: selective adopt`, or `Recommendation: do not adopt`.

## Branch-specific context you must treat as canonical

The launcher had a local follow-up patch not visible to cloud. Treat this as context only:

- validation path normalization was tightened for file feedback-loop detection
- docs are being updated for env substitution semantics

Your workstream should focus on validation architecture, not on re-solving those exact local edits.

## Deliverable

Write one repo-local output at:

`dev-docs/research/fanout-2026-04-19-config-pocs/04-validation-crates-poc.md`

Include:

- classification table of current validation checks
- POC summary and changed files
- tests run and results
- recommendation on crate adoption vs newtypes/custom validation
- migration sequence if adoption is recommended

## Constraints

- Do not move cross-pipeline checks into awkward derive annotations if that makes them harder to reason about.
- Do not scatter business rules so future contributors cannot find the validation contract.
- Respect the repo's code style: no production unwraps, focused tests, and clear error messages.
- Preserve strict validation behavior.

## Success criteria

This workstream is useful if fan-in gets a clear split between "library-worthy local validation" and "keep custom semantic validation," backed by a code experiment.

End with:

- `Recommendation: <label>`
- `Primary rationale: <1-3 bullets>`
- `Alternatives considered: <short list>`
- `What would change my mind: <specific evidence>`
