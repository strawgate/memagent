# Type Suffix Redesign Research

Date: 2026-03-31
Context: #445 — `_str`/`_int`/`_float` suffix convention is root cause of 11 bugs

## Problem

One JSON key can currently produce up to 3 Arrow columns (`status_int`,
`status_float`, `status_str`). The output layer dispatches on column
**name suffix** to determine serialization format. This breaks when:

- SQL transforms rename columns (suffix lost)
- SQL aggregates produce new columns (no suffix)
- User field names collide with suffixes
- Schema evolves across batches (different suffix sets)

## Revised Understanding

The multi-column approach (`status_int` + `status_str`) is actually
correct for type fidelity — it preserves round-trip JSON → Arrow → JSON
without losing type information. The bugs are NOT caused by having
multiple columns. They're caused by:

1. Output sinks dispatch on column **name suffix** instead of Arrow DataType
2. The SQL rewriter is incomplete and can never handle all SQL patterns
3. Column deduplication in `build_col_infos` uses suffix strings, not DataType
4. None of this is properly wired up — it's promised but broken

## Design: Fix the Dispatch, Not the Schema

### Core rule

Keep the multi-column internal representation. Fix how it's consumed.

### Fix 1: Output sinks dispatch on Arrow DataType

`write_row_json` currently parses column name suffixes to decide how
to serialize. Replace with `field.data_type()` dispatch (matching OTLP):

```rust
match col.data_type() {
    DataType::Int64 => { /* write as JSON number */ }
    DataType::Float64 => { /* write as JSON number */ }
    _ => { /* write as JSON string */ }
}
```

This alone fixes #430, #444, #428.

### Fix 2: Replace SQL rewriter with schema aliasing

Instead of rewriting SQL ASTs to add suffixes, register the MemTable
with bare column names (alias `status_int` → `status` for the int
variant). DataFusion handles type resolution natively. User writes:

```sql
SELECT level, status FROM logs WHERE status > 400
```

DataFusion sees `status` as Int64 (the alias) and resolves correctly.
This eliminates the 772-line rewriter. Fixes #415, #429, #531.

### Fix 3: Column deduplication uses DataType

`build_col_infos()` currently deduplicates by parsing suffix strings.
Replace with DataType-based priority: Int64 > Float64 > Utf8 for
the "primary" column when a field has multiple typed columns.
Fixes #404, #442.

### What stays the same

- Scanner still emits `append_str_by_idx` / `append_int_by_idx` etc.
- Builders still create `_int` / `_float` / `_str` columns internally
- Multi-column representation preserves type fidelity for round-tripping
- OTLP sink already dispatches on DataType (no change needed)

## What gets deleted

- `crates/logfwd-transform/src/rewriter.rs` — 772 lines, entire file
- `parse_column_name()` in `logfwd-output/src/lib.rs` — suffix parsing
- `strip_type_suffix()` in `logfwd-transform/src/lib.rs`
- Name-based type dispatch in `write_row_json()`

## What gets changed

| File | Change |
|------|--------|
| `logfwd-output/lib.rs` | DataType dispatch in write_row_json |
| `logfwd-output/lib.rs` | DataType-based dedup in build_col_infos |
| `logfwd-output/stdout.rs` | DataType dispatch for console format |
| `logfwd-transform/lib.rs` | Schema aliasing instead of rewriting |
| `rewriter.rs` | **DELETE** |

## Bugs this fixes

| Fix | Bugs resolved |
|-----|---------------|
| DataType dispatch in output | #430, #444, #428 |
| Schema aliasing (delete rewriter) | #415, #429, #531 |
| DataType-based dedup | #404, #442 |
| Escape handling fix | #410 |

## Key insight

The multi-column representation is fine. The bugs are in the
**consumption** layer, not the **production** layer. The builders
produce correct typed columns. The output sinks and SQL transform
consume them incorrectly by parsing column names instead of checking
Arrow DataType.
