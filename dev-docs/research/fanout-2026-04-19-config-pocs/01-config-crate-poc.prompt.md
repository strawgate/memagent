# Config Source Framework POC: `config`

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

Determine whether the published Rust `config` crate should replace or augment FastForward's current YAML config loading path in `crates/logfwd-config`.

You MUST produce an executable POC if it is feasible within this task. The POC can be a small alternate loader module, ignored prototype test, integration test, or focused spike branch change. The point is to learn from code, not just summarize docs.

## Why this workstream exists

PR #2244 fixed several config validation gaps and added YAML-aware env expansion. The remaining architectural question is whether FastForward should keep its custom Serde/YAML loader or adopt a published config source framework.

The current loader shape is roughly:

- parse YAML into `serde_yaml_ng::Value`
- expand `${VAR}` placeholders inside the YAML value tree
- deserialize into strict Serde structs with `deny_unknown_fields`
- normalize into `Config`
- run semantic validation across pipelines, inputs, outputs, labels, paths, and transport settings

The desired answer is not "can `config` parse YAML?" The desired answer is whether `config` improves this repo without weakening strictness, error quality, env semantics, docs, binary size, build time, or maintainability.

## Mode

Mode: prototype.

Autonomy: investigate + prototype + test.

Compare the primary `config`-crate approach against at least one credible alternative, which may be "keep the current loader and borrow only a small idea."

## Required execution checklist

- MUST read:
  - `crates/logfwd-config/src/load.rs`
  - `crates/logfwd-config/src/types.rs`
  - `crates/logfwd-config/src/validate.rs`
  - `crates/logfwd-config/tests/validation_gaps.rs`
  - `book/src/content/docs/configuration/reference.mdx`
  - `dev-docs/CHANGE_MAP.md`
- MUST research the current published `config` crate docs and version enough to avoid stale assumptions.
- MUST attempt a POC that loads representative FastForward YAML through `config` and extracts the existing typed config model or a near equivalent.
- MUST test these edge cases in the POC or explain exactly why they block adoption:
  - `deny_unknown_fields` behavior
  - tagged enum behavior for input/output `type`
  - `${VAR}` in whole scalar, partial string, and mapping key positions
  - quoted env values containing YAML-significant characters such as `#`, `:`, `{`, `[`
  - duplicate mapping key after env expansion
  - base-path-sensitive validation for file paths
- MUST report compile-time/dependency impact at least qualitatively; use `cargo tree -p logfwd-config` or similar if useful.
- MUST write the deliverable file listed below.
- MUST end with `Recommendation: adopt`, `Recommendation: augment only`, or `Recommendation: do not adopt`.
- MUST explain what evidence would change your recommendation.

## Branch-specific context you must treat as canonical

The launcher had a local follow-up patch not visible to cloud. Treat this as context, not as code you can assume exists:

- added `Config::expand_env_yaml_str`, which parses YAML, expands env vars on the YAML tree using the same rules as `Config::load_str`, and serializes canonical YAML
- changed `effective-config` to use this YAML-aware expansion instead of raw text substitution
- added a regression for `/var/log/my app #1.log` so `#1` survives effective config
- normalized glob input path comparisons before feedback-loop checks
- documented exact env placeholder semantics

If your POC would conflict with or supersede this local direction, say so explicitly.

## Deliverable

Write one repo-local output at:

`dev-docs/research/fanout-2026-04-19-config-pocs/01-config-crate-poc.md`

Include:

- summary of POC implementation
- changed files, if any
- commands run and results
- comparison table: current loader vs `config`
- recommendation and migration plan if applicable

## Constraints

- Do not hand-wave domain validation. FastForward still needs custom cross-pipeline validation even if a source framework is adopted.
- Do not trade strict config rejection for convenience.
- Do not assume runtime paths exist; config validation must work for output paths not yet created.
- Keep the hot path out of scope; config loading is startup/admin behavior.
- If you leave code changed, the change must be clearly prototype-scoped or production-quality with tests.

## Success criteria

This workstream is useful if fan-in can decide whether `config` is a serious candidate from concrete code evidence, not from a generic crate survey.

End with:

- `Recommendation: <label>`
- `Primary rationale: <1-3 bullets>`
- `Alternatives considered: <short list>`
- `What would change my mind: <specific evidence>`
