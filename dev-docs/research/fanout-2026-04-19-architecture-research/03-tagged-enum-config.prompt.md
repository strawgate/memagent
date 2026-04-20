# Research: Tagged Enum Config Migration — Plan and Compatibility Analysis

> **Status:** Completed
> **Date:** 2026-04-19
> **Context:** Architecture research fanout — workstream synthesis

You are handling one workstream inside a larger architecture research fanout for the FastForward (formerly logfwd) log forwarder.

## Objective

Produce a concrete migration plan for replacing the flat `InputConfig`/`OutputConfig` structs with per-type tagged enums (issue #1101). The key deliverable is a migration path that maintains backward compatibility with existing user configs while eliminating the "silent field acceptance" bug class.

## Why this workstream exists

Issue #1101 is the root cause of 13+ bugs where config fields valid for one output type are silently accepted by all types. The current `InputConfig` and `OutputConfig` are single flat structs with every possible field as `Option<T>`, shared across all input/output types.

This causes:
- Fields for OTLP accepted silently on TCP output (and ignored at runtime)
- Typos in enum fields silently select wrong defaults via `match _ =>` fallthrough
- No way to validate that a config is complete for its specific type
- Config builder (WASM) can't generate type-specific forms

The fix direction is clear: use `serde(tag = "type")` to make each input/output type its own struct. But the migration is complex because:
- ~20 output config feature issues (#2194-#2200) are actively adding fields to the flat struct
- The config builder WASM crate depends on the config types
- Existing user configs must continue to parse (backward compat)
- PR #2203 just landed shared config types (TLS, Retry, Batch, Compression) — these should be reused
- WU #2068 exists for this work but needs a concrete plan before execution

## Mode

research

## Required execution checklist

- You MUST read these files before writing anything:
  - `crates/logfwd-config/src/types.rs` (current InputConfig/OutputConfig flat structs)
  - `crates/logfwd-config/src/lib.rs` (config loading and structure)
  - `crates/logfwd-config/src/validate.rs` (current validation — what it catches and misses)
  - `crates/logfwd-config/src/shared.rs` (shared config types from PR #2203)
  - `crates/logfwd-config/src/load.rs` (YAML loading)
  - `crates/logfwd-config-wasm/src/lib.rs` (WASM config builder dependency)
  - `crates/logfwd-runtime/src/pipeline/input_build.rs` (how config becomes runtime objects)
  - `crates/logfwd-output/src/factory.rs` (output factory — how config selects sink type)
  - `book/src/content/docs/configuration/reference.mdx` (user-facing config docs)
  - `examples/use-cases/` (sample configs that must continue to work)
- You MUST catalog every field in InputConfig and OutputConfig, grouped by which types actually use them
- You MUST produce the proposed enum variants with their fields (showing what moves where)
- You MUST identify backward-compatibility risks:
  - Fields that exist in user configs today that would be rejected by strict typing
  - YAML key ordering or structure changes
  - `serde(tag = "type")` implications for the existing `type: otlp` / `type: file` pattern
- You MUST analyze the impact on:
  - Config validation (what new errors appear)
  - Config builder WASM crate
  - Runtime input/output construction
  - Example configs in the repo
  - The 7 open output config feature issues (#2194-#2199, #2200)
- You MUST produce a phased migration plan that:
  - Phase 1 can land without breaking any existing user config
  - Each phase is a reviewable PR (not a big-bang rewrite)
  - Uses `serde(untagged)` or dual-parse as a compatibility shim if needed
- You MUST end with a concrete recommendation

After completing the required work, use your judgment to explore:
- Whether `serde(tag = "type")` or `serde(untagged)` with manual dispatch is better for backward compat
- How to handle the shared fields (name, type, pipelines) that exist on every config variant
- Whether a proc macro could auto-generate the validation that the flat struct currently needs manually

## Required repo context

Read at least these:
- `crates/logfwd-config/src/types.rs`
- `crates/logfwd-config/src/lib.rs`
- `crates/logfwd-config/src/validate.rs`
- `crates/logfwd-config/src/shared.rs`
- `crates/logfwd-config/src/load.rs`
- `crates/logfwd-config-wasm/src/lib.rs`
- `crates/logfwd-runtime/src/pipeline/input_build.rs`
- `crates/logfwd-output/src/factory.rs`
- `book/src/content/docs/configuration/reference.mdx`
- At least 5 example configs from `examples/use-cases/`

## Deliverable

Write one repo-local output at:

`dev-docs/research/tagged-enum-config-migration-plan.md`

Include:
1. Current field inventory (every field, which types use it, which types silently ignore it)
2. Proposed enum variants with field assignments
3. Backward compatibility analysis with specific risk scenarios
4. Impact analysis on WASM builder, runtime construction, examples, open feature issues
5. Phased migration plan (Phase 1: non-breaking, Phase 2: strict mode opt-in, Phase 3: remove flat struct)
6. Serde strategy recommendation (tagged vs untagged vs dual-parse)
7. Relationship to #2068 (WU), #2194-#2200 (output config features), #1107 (--validate)

## Constraints

- Ground everything in the actual codebase
- Do not implement code changes — this is a research deliverable
- Existing user configs MUST continue to parse in Phase 1 (no breaking changes)
- The shared config types from PR #2203 MUST be reused (do not propose duplicating them)
- Performance of config parsing is not a concern — correctness and compatibility are

## Success criteria

- A reader can see exactly which fields move to which enum variants
- Backward compatibility risks are concrete (not hypothetical)
- The phased plan is realistic and each phase is PR-sized
- The recommendation on serde strategy is decisive with clear rationale

## Decision style

End with: "Use {serde strategy} because {reasons}. Phase 1 should {scope}. The open config feature issues should {proceed as-is / wait / adapt}."
