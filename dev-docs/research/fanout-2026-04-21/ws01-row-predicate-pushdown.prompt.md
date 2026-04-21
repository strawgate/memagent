# Research: Row Predicate Pushdown Design (Issue #1257)

## Context

logfwd already has **field-level pushdown** — the scanner skips fields not in `ScanConfig::wanted_fields`. But there is no **row-level predicate pushdown** — every row is fully scanned and built into a RecordBatch even if a SQL WHERE clause will discard it immediately.

For common filters like `WHERE level = 'error'` or `WHERE severity_number >= 17`, skipping rows at scan time could eliminate most Arrow builder work.

## What to investigate

1. Read the scanner pipeline end-to-end:
   - `crates/logfwd-core/src/json_scanner.rs` — `scan_streaming()`, line ranges, field extraction
   - `crates/logfwd-arrow/src/streaming_builder/mod.rs` — `begin_row()`, `end_row()`, field appending
   - `crates/logfwd-arrow/src/scanner.rs` — `scan()` and `scan_detached()` orchestration
   - `crates/logfwd-core/src/scan_config.rs` — `ScanConfig`, `wanted_fields`

2. Understand the DataFusion SQL integration:
   - `crates/logfwd-transform/src/` — how SQL transforms consume RecordBatches
   - Can DataFusion push predicates down to a custom TableProvider?

3. Identify which predicate types are feasible:
   - Equality on string fields (`level = 'error'`)
   - Comparison on numeric fields (`severity_number >= 17`)
   - IS NULL / IS NOT NULL
   - Simple AND/OR combinations

4. Design where filtering should happen:
   - Option A: In `json_scanner.rs` — skip lines before field extraction
   - Option B: In `streaming_builder` — skip `begin_row()`/`end_row()` for filtered rows
   - Option C: In the CPU worker — filter RecordBatch after scan but before SQL transform
   - Which gives the best cost/benefit?

## Deliverable

Write a research report to `dev-docs/research/fanout-2026-04-21/ws01-row-predicate-pushdown-report.md` containing:

1. Architecture proposal with diagrams (text-based)
2. Which predicates can be pushed down and which cannot
3. Where in the pipeline filtering should happen (with file paths and line numbers)
4. Expected performance gain estimate (what fraction of work is skipped)
5. Implementation plan: ordered list of changes with file paths
6. Risks and non-goals

Do NOT implement code changes. Research and design only.
