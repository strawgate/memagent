# Config Framework POC: `figment`, `confique`, and `twelf`

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

Compare published higher-level Rust config crates that might be a better fit than the current custom loader:

- `figment`
- `confique`
- `twelf`
- any other credible published crate you find during current research

You MUST build at least one executable POC with the strongest candidate and explain why you selected it for the POC.

## Why this workstream exists

FastForward currently has a dedicated `logfwd-config` crate with strict Serde structs and custom semantic validation. That may still be correct, but there are published crates that offer typed layered config, env support, metadata/provenance, templates, or CLI/env/file merging.

We need a serious challenge to the current direction: if a published crate can give us a cleaner, safer, more maintainable design, find it and show it in code.

## Mode

Mode: prototype.

Autonomy: investigate + prototype + test.

You should compare 2-3 credible options, choose the strongest, then implement a small POC.

## Required execution checklist

- MUST read:
  - `crates/logfwd-config/src/load.rs`
  - `crates/logfwd-config/src/types.rs`
  - `crates/logfwd-config/src/validate.rs`
  - `crates/logfwd/src/main.rs`
  - `book/src/content/docs/configuration/reference.mdx`
  - `Cargo.toml`
- MUST research current docs for `figment`, `confique`, and `twelf`.
- MUST compare them on:
  - strict unknown-field rejection
  - support for nested enums with `type`
  - env var support inside file values vs env overlay
  - typed env parsing semantics
  - provenance/error reporting
  - support for config templates or schema-like metadata
  - incremental adoption cost
  - dependency and compile-time footprint
- MUST prototype the strongest candidate against representative FastForward config.
- MUST test or reason concretely about:
  - simple layout: top-level `input` + `output`
  - advanced layout: `pipelines`
  - multiple inputs/outputs
  - env values in strings and typed fields
  - whether the crate can preserve current fail-fast behavior for missing env vars
- MUST write the deliverable file listed below.
- MUST end with a decisive recommendation, even if the recommendation is to reject all candidates.

## Branch-specific context you must treat as canonical

The launcher had a local follow-up patch not visible to cloud. Treat this as context only:

- local code is moving away from raw text substitution and toward AST-based YAML expansion
- `effective-config` should print parseable expanded YAML where `#` in env values remains data, not a YAML comment
- current docs are being clarified around exact unquoted placeholders using YAML scalar typing, quoted placeholders remaining strings, and partial interpolation remaining strings

Your job is allowed to challenge this direction. If a crate gives a cleaner alternative, make the case with code.

## Deliverable

Write one repo-local output at:

`dev-docs/research/fanout-2026-04-19-config-pocs/02-framework-comparison-poc.md`

Include:

- candidate shortlist and why each was included or excluded
- POC details for the strongest candidate
- commands run and results
- migration sketch if adoption is recommended
- what would remain custom no matter which crate is used

## Constraints

- Do not recommend a framework just because it is elegant in isolation. It must fit this repo's existing config surface.
- Do not weaken validation behavior or unknown-field rejection.
- Do not require a disruptive user-facing config syntax change unless the safety/maintenance payoff is compelling.
- If one candidate requires replacing `serde_yaml_ng`, call out risk and compatibility issues.

## Success criteria

This workstream is useful if it either finds a credible replacement/augmentation path or convincingly rules one out with enough evidence that fan-in can stop revisiting the same question.

End with:

- `Recommendation: <label>`
- `Primary rationale: <1-3 bullets>`
- `Alternatives considered: <short list>`
- `What would change my mind: <specific evidence>`
