# Research: InputConfig V2 Tagged Enum Migration (Issue #1101)

## Context

The output config was successfully migrated to `OutputConfigV2` — a `#[serde(tag = "type")]` tagged enum with per-output-type structs. The input config (`InputConfig`) is still a flat struct with ~15 `Option<>` fields where only one is set at a time. This is the remaining half of #1101.

## What to investigate

1. Read the current InputConfig:
   - `crates/logfwd-config/src/types.rs` — `InputConfig` struct (around line 628+)
   - Count all `Option<>` fields and their types
   - List all input type variants that exist

2. Read how InputConfig is consumed:
   - `crates/logfwd-runtime/src/pipeline/` — how inputs are built from config
   - `crates/logfwd/src/commands.rs` — CLI config handling
   - `crates/logfwd/src/validate.rs` — validation
   - `crates/logfwd-config/src/validate.rs` — config validation
   - Grep for `InputConfig` across the workspace to find all consumers

3. Study the OutputConfigV2 migration as a template:
   - `crates/logfwd-config/src/types.rs` — `OutputConfigV2` enum
   - `crates/logfwd-config/src/compat.rs` — compatibility layer
   - How was the V1→V2 migration handled? Was there a compat parser?

4. Design InputConfigV2:
   - What variants? (File, Tcp, Udp, Otlp, Http, Generator, HostMetrics, Journald, S3, Sensor, etc.)
   - What fields are per-variant vs shared?
   - How to handle the `format` field (CRI/JSON/raw) — is it per-input-type or shared?
   - Compat strategy: support both flat and tagged during migration?

5. Estimate scope:
   - How many call sites need updating?
   - What's the risk of breaking user configs?
   - Can it be done incrementally?

## Deliverable

Write a research report to `dev-docs/research/fanout-2026-04-21/ws03-inputconfig-v2-report.md` containing:

1. Current InputConfig inventory: all fields, all consumers
2. Proposed InputConfigV2 enum design
3. Migration strategy (big-bang vs incremental, compat parser)
4. Call site impact analysis with file paths
5. User config migration guide
6. Implementation plan: ordered steps
7. Risk assessment

Do NOT implement code changes. Research and design only.
