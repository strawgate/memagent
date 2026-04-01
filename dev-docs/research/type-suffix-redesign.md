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

## Design: Single Column Per Field with Type Promotion

### Core rule

One JSON key = one Arrow column. Column name = JSON key name (no suffix).
Arrow DataType determined by observed values, with promotion on conflict.

### Type promotion rules

```
Int64 + Int64      → Int64
Float64 + Float64  → Float64
Int64 + Float64    → Float64    (widening)
Int64 + Utf8       → Utf8       (universal fallback)
Float64 + Utf8     → Utf8       (universal fallback)
Utf8 + anything    → Utf8
```

This matches `arrow-json`'s strategy and DataFusion's type coercion.

### How it works

The `ScanBuilder` trait stays unchanged. `append_int_by_idx` /
`append_float_by_idx` / `append_str_by_idx` communicate the observed
JSON type. The builder tracks a `ResolvedType` per field:

```rust
enum ResolvedType {
    Unknown,     // no values seen yet
    Int,         // all values were integers
    Float,       // all values were floats (or int promoted to float)
    Str,         // all values were strings
    PromotedToStr, // mixed types — everything becomes string
}
```

At `finish_batch()`:
- `Int` → emit `Int64` column
- `Float` → emit `Float64` column (int values widened)
- `Str` → emit `Utf8View` column (zero-copy in StreamingBuilder)
- `PromotedToStr` → emit `Utf8View` column (int/float values formatted)

### Output dispatch

All output sinks dispatch on `field.data_type()`, not column name:

```rust
match col.data_type() {
    DataType::Int64 => { /* write as JSON number */ }
    DataType::Float64 => { /* write as JSON number */ }
    _ => { /* write as JSON string */ }
}
```

The OTLP sink already does this. The JSON sink (`write_row_json`) and
console format (`stdout.rs`) need updating.

### SQL transforms

Column names are bare JSON keys. User SQL works naturally:

```sql
SELECT level, status, msg FROM logs WHERE status > 400
```

No rewriter needed. DataFusion resolves types from the Arrow schema.
The `int()` and `float()` UDFs remain for explicit casting when the
user knows a Utf8 column should be numeric.

### Zero-copy preservation

For the common case (homogeneous types), StreamingBuilder's
`StringViewArray` is still zero-copy into the input `Bytes` buffer.
Promotion only costs string formatting for the rare mixed-type case.

## What gets deleted

- `crates/logfwd-transform/src/rewriter.rs` — 772 lines, entire file
- `parse_column_name()` in `logfwd-output/src/lib.rs`
- `strip_type_suffix()` in `logfwd-transform/src/lib.rs`
- `field_type_map_from_schema()` and `FieldTypeMap`/`FieldTypes` types
- Suffix-based deduplication in `build_col_infos()`
- Name-based type dispatch in `write_row_json()`

## What gets changed

| File | Change |
|------|--------|
| `streaming_builder.rs` | Single column per field, type promotion |
| `storage_builder.rs` | Same |
| `logfwd-output/lib.rs` | DataType dispatch |
| `otlp_sink.rs` | Remove `parse_column_name` usage |
| `stdout.rs` | Bare column name lookup |
| `logfwd-transform/lib.rs` | Delete `strip_type_suffix` |
| `rewriter.rs` | **DELETE** |
| All test files | `"status_int"` → `"status"`, etc. |

## Bugs this fixes

#415, #429, #430, #404, #442, #444, #531, #410 — all 8 open bugs
from the suffix convention are resolved by this design.

## Migration

No external API, no backward compatibility needed. Single PR with
coordinated changes across all crates. Tests updated in the same PR.

## Cross-batch schema consistency

First batch pins each field's resolved type. Subsequent batches
promote to match. If a field was `Int64` in batch 1 but gets a string
value in batch 2, it promotes to `Utf8` for batch 2. DataFusion
handles schema differences across batches because it creates a fresh
session per batch.
