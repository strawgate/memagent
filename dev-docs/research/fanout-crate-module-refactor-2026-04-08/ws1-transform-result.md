# Workstream 1 Result: `logfwd-transform` module split

## Summary

Refactored `crates/logfwd-transform/src/lib.rs` monolith into internal modules with clear ownership while preserving crate public API (`SqlTransform`, `QueryAnalyzer`, `TransformError`, `conflict_schema`, and existing public `enrichment`/`error`/`udf` modules).

No intended behavior changes.

## Module boundaries chosen

### Module Map

- `crates/logfwd-transform/src/lib.rs`
  - Crate entrypoint and public surface wiring.
  - Declares/re-exports public API and internal modules only.
- `crates/logfwd-transform/src/query_analyzer.rs`
  - SQL AST parsing/inspection.
  - Referenced-column collection.
  - `SELECT *`/`EXCEPT` handling.
  - filter-hint extraction (`severity`/`facility`) from WHERE for pushdown.
  - `scan_config()` derivation.
- `crates/logfwd-transform/src/cast_udf.rs`
  - Internal scalar cast UDF implementations: `int()` and `float()`.
- `crates/logfwd-transform/src/sql_transform.rs`
  - `SqlTransform` lifecycle.
  - SessionContext cache management + schema-hash invalidation.
  - per-batch `logs` and enrichment-table registration.
  - query execution and result batch concatenation.
  - synchronous wrapper and plan validation.
- `crates/logfwd-transform/src/tests.rs`
  - Existing crate-level transform/analyzer regression and integration-style unit tests moved out of `lib.rs` to keep production source focused.

## Alternative considered

**Alternative:** keep all tests in `lib.rs` and split only runtime code into modules.

**Why not chosen:** this would leave `lib.rs` still very large and noisy, reducing the reviewability win for future transform-flow changes. Moving tests into `tests.rs` keeps runtime/module boundaries obvious and preserves test coverage without semantic changes.

## Changed file list

- `crates/logfwd-transform/src/lib.rs`
- `crates/logfwd-transform/src/query_analyzer.rs` (new)
- `crates/logfwd-transform/src/cast_udf.rs` (new)
- `crates/logfwd-transform/src/sql_transform.rs` (new)
- `crates/logfwd-transform/src/tests.rs` (new)
- `crates/logfwd-transform/src/udf/regexp_extract.rs` (single internal-path update for `IntCastUdf` reference)

## Validation commands and outcomes

- `cargo check -p logfwd-transform` ✅ pass
- `cargo test -p logfwd-transform` ✅ pass

## Recommendation label

**Recommendation: ship**

The split is coherent by responsibility, preserves external API/behavior, and full crate checks/tests pass.

## What evidence would change my mind

I would revise this recommendation if any of the following appears in follow-up review:

1. measurable perf regression in transform hot-path benchmarks attributable to module boundary changes,
2. newly observed behavioral divergence in schema evolution / conflict-column normalization paths,
3. downstream crate breakage due to assumptions about previous in-file visibility of internals.
