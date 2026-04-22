# Workstream D results (2026-04-20)

## Summary

Implemented property-based testing coverage for output JSON serialization paths and added a reusable Arrow `RecordBatch` strategy.

## Changes made

- Added shared `arb_record_batch` strategy at `crates/logfwd-test-utils/src/arrow.rs`.
  - Supports these Arrow data types in a generated batch schema:
    - `Utf8`
    - `Int64`
    - `Float64`
    - `Boolean`
    - `Null`
    - `Timestamp(Nanosecond)`
    - `List<Int64>`
    - `Struct<{inner_text: Utf8, inner_int: Int64}>`
- Exported the new module from `crates/logfwd-test-utils/src/lib.rs` (`pub mod arrow;`).
- Added proptests to `crates/logfwd-output/src/row_json.rs`:
  1. `row_json_produces_valid_json`
  2. `row_json_field_count_matches_schema`
  3. `row_json_null_handling`
  4. `row_json_paths_agree`
  5. `row_json_special_floats` (unit test, NaN/±Infinity => `null`)
- Added proptests to `crates/logfwd-output/src/json_lines.rs`:
  1. `ndjson_lines_are_valid_json`
  2. `ndjson_no_embedded_newlines`

## Notes

- `row_json_paths_agree` compares `write_row_json` and `write_row_json_resolved` outputs directly for each row over arbitrary generated batches.
- NDJSON tests validate one-line-per-row framing and JSON parseability per line.
