# Typed Env Coercion POC: `serde_with` and Serde Helpers

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

Prototype a safer long-term env expansion model that avoids source-text YAML substitution and avoids relying on YAML quote-style detection.

Primary candidate crates/patterns:

- `serde_with`, especially `DisplayFromStr` or related adapters
- custom Serde helper deserializers in `crates/logfwd-config/src/serde_helpers.rs`
- any published crate that directly supports "deserialize string or native scalar into typed field" cleanly

## Why this workstream exists

The current env behavior after PR #2244 is intentionally YAML-aware:

- exact unquoted `${VAR}` can become YAML null/bool/number
- exact quoted `${VAR}` remains a string
- partial interpolation remains a string
- mapping keys can expand, with duplicate-key detection

This works, but preserving the distinction between quoted and unquoted placeholders requires a pre-scan because `serde_yaml_ng::Value` does not retain quote style. That is a maintenance smell.

A potentially better model:

- expand env placeholders in the parsed YAML tree as strings, never as YAML syntax fragments
- let typed field deserializers parse strings into `usize`, `bool`, durations, URLs, etc.
- keep partial interpolation as string-only
- eliminate quote-style marker machinery if possible

Your job is to test whether that model is feasible and better.

## Mode

Mode: prototype.

Autonomy: investigate + prototype + test.

Compare at least two implementation shapes:

- published helper crate such as `serde_with`
- local Serde visitor/helper functions

## Required execution checklist

- MUST read:
  - `crates/logfwd-config/src/load.rs`
  - `crates/logfwd-config/src/serde_helpers.rs`
  - `crates/logfwd-config/src/types.rs`
  - `crates/logfwd-config/tests/validation_gaps.rs`
  - `book/src/content/docs/configuration/reference.mdx`
- MUST identify all or a representative high-risk subset of numeric/bool typed fields that currently benefit from unquoted env scalar coercion.
- MUST prototype a string-or-native scalar deserializer for at least:
  - `usize`
  - `u64` or `u32`
  - `bool`
  - `Option<T>` behavior, if relevant
- MUST test env values containing YAML-significant characters such as `#`, `:`, `{`, `[`, and leading/trailing spaces.
- MUST test that an env value `"08"` does not accidentally become a different YAML number interpretation.
- MUST test that invalid env strings fail with useful field-level errors.
- MUST compare dependency impact of `serde_with` vs local helpers.
- MUST write the deliverable file listed below.
- MUST end with a recommendation label:
  - `Recommendation: replace quote-marker coercion`
  - `Recommendation: keep current AST coercion`
  - `Recommendation: hybrid`

## Branch-specific context you must treat as canonical

The launcher had a local follow-up patch not visible to cloud. Treat this as context only:

- added a YAML-aware `effective-config` expansion API
- fixed a bug where raw effective config substitution could turn `/var/log/my app #1.log` into YAML where `#1.log` became a comment
- documented current semantics as unquoted exact placeholders using YAML scalar typing

If your POC shows a better target-typed coercion model, explain how docs and compatibility should migrate.

## Deliverable

Write one repo-local output at:

`dev-docs/research/fanout-2026-04-19-config-pocs/03-typed-env-coercion-poc.md`

Include:

- POC implementation summary
- changed files, if any
- tests added or attempted
- list of config fields that would need helper annotations or wrapper types
- compatibility implications for existing configs
- a staged migration plan if recommended

## Constraints

- Do not break existing documented configs without explicitly calling it out.
- Do not require users to wrap every env var in a new syntax unless the alternative is materially unsafe.
- Avoid adding a large dependency for a trivial helper unless it clearly improves correctness or schema/docs integration.
- Keep all behavior in `logfwd-config`; runtime crates should not learn env expansion details.

## Success criteria

This workstream is useful if fan-in can decide whether the quote-marker machinery is a necessary compromise or a temporary bridge toward typed field coercion.

End with:

- `Recommendation: <label>`
- `Primary rationale: <1-3 bullets>`
- `Alternatives considered: <short list>`
- `What would change my mind: <specific evidence>`
