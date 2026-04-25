# ffwd-arrow Agent Guide

Read `README.md` in this directory for crate constraints and module map.

Key rules:
- Implements core's ScanBuilder trait for Arrow RecordBatch output
- proptest verifies scanner output matches expected behavior
- Column naming: bare names for single-type fields, StructArray for conflicts
- See `src/conflict_schema.rs` for `is_conflict_struct()` and `normalize_conflict_columns()`
