# Column Type Design

Date: 2026-04-01
Context: #445 — column naming, type fidelity, and SQL UX

## Core Requirements

1. **Per-document type fidelity.** A field that enters as int `200` in
   document A and string `"OK"` in document B must exit with those exact
   types. `SELECT *` round-trips the data.

2. **Clean-schema inputs stay clean.** OTLP, Arrow IPC, and other typed
   inputs already have per-column types with no conflicts. These should
   pass through with bare column names — no suffixes, no views, no overhead.

3. **Mixed-type inputs are handled gracefully.** JSON can have `status: 200`
   in one row and `status: "OK"` in another. The system must preserve both
   types without promotion.

4. **SQL is usable.** Users write SQL against column names. Bare names
   should work for the common case. Typed access is available when needed.

5. **Schema stability.** The user's SQL is fixed but the batch schema
   varies. SQL-referenced columns must always exist (padded with nulls
   if absent from a batch).

## Design: Suffix Only On Conflict

### The rule

When the builder finalizes a batch, each field gets:

- **No type conflict** (field is always int, or always string, etc.):
  bare column name, native DataType.
  - `status` always int → column `status` (Int64)
  - `level` always string → column `level` (Utf8)

- **Type conflict** (field has multiple types across rows in the batch):
  suffixed columns for each observed type.
  - `status` is int in some rows, string in others →
    `status_int` (Int64) + `status_str` (Utf8)

### Why this works for all input types

| Input source | Type conflicts? | Column names |
|---|---|---|
| OTLP / Arrow IPC | Never (typed per-column) | Bare: `status`, `level` |
| CSV / raw / syslog | Never (all strings) | Bare: `status`, `level` |
| JSON (consistent) | No (field is always same type) | Bare: `status` (Int64), `level` (Utf8) |
| JSON (mixed types) | Yes | `status_int` + `status_str`, `level` (Utf8) |
| Mixed pipeline | Depends on batch | Bare when clean, suffixed on conflict |

Clean-schema inputs (OTLP, CSV, consistent JSON) get bare names and
native SQL. No suffixes, no views, no overhead. The suffix machinery
only activates when there's an actual type conflict in the batch.

### SQL interface

**Bare names (common case):**
```sql
-- Works when columns are bare (OTLP, CSV, consistent JSON):
SELECT status, level FROM logs WHERE status > 400
```

**Suffixed names (type conflict case):**
```sql
-- When a field has multiple types, user accesses typed columns:
SELECT status_int, status_str FROM logs WHERE status_int > 400
```

**View layer for type-conflict batches:**
When suffixed columns exist, a DataFusion view provides bare-name access:
```sql
-- Auto-generated view:
CREATE VIEW logs AS SELECT
  status_str AS status,      -- bare name = string representation
  status_int,                -- typed access
  status_str,
  level_str AS level,        -- single-type field, alias only
  ...
FROM _raw_logs
```

`SELECT status` returns the string representation. `SELECT status_int`
returns the integer column. `SELECT *` returns all columns — the output
layer groups by bare field name and picks the non-null typed value per
row for serialization.

The view is only created when suffixed columns are detected in the
batch schema. For clean-schema batches, the table is registered
directly as `logs`.

### Output serialization

The output layer already dispatches on Arrow DataType (#568), not column
name suffix. For round-trip serialization:

1. Group columns by bare field name (strip `_int`/`_float`/`_str` suffix)
2. For each row, find the first non-null value across the column group
3. Serialize using the DataType of that column:
   - Int64 → JSON number: `"status": 200`
   - Float64 → JSON number: `"status": 1.5`
   - Utf8 → JSON string: `"status": "OK"`

JSON output key is always the bare field name. Never `status_int`.

### Schema stability

The user's SQL is fixed but batch schemas vary. Solution:

- At config time, `QueryAnalyzer` extracts `referenced_columns` from
  the user's SQL. The suffix convention gives us the DataType.
- Before registering each batch as a MemTable, pad with null columns
  for any SQL-referenced columns missing from this batch.
- This is **stateless** — guaranteed columns are derived from the SQL,
  not from runtime data.

For `SELECT *` (no explicit column references), no padding is needed.

### Builder implementation

```rust
// In StreamingBuilder::finish() / StorageBuilder::finish():
if fc.has_int && !fc.has_float && !fc.has_str {
    // Single type: bare name
    columns.push(Column::new(field_name, DataType::Int64, int_values));
} else if fc.has_str && !fc.has_int && !fc.has_float {
    // Single type: bare name
    columns.push(Column::new(field_name, DataType::Utf8View, str_values));
} else if fc.has_float && !fc.has_int && !fc.has_str {
    // Single type: bare name
    columns.push(Column::new(field_name, DataType::Float64, float_values));
} else {
    // Type conflict: suffixed names
    if fc.has_int {
        columns.push(Column::new(format!("{field_name}_int"), DataType::Int64, int_values));
    }
    if fc.has_float {
        columns.push(Column::new(format!("{field_name}_float"), DataType::Float64, float_values));
    }
    if fc.has_str {
        columns.push(Column::new(format!("{field_name}_str"), DataType::Utf8View, str_values));
    }
}
```

## What gets deleted

- `crates/logfwd-transform/src/rewriter.rs` — 772 lines, dead code (#602)
- `strip_type_suffix()` in `logfwd-transform/src/lib.rs`
- Name-based type dispatch in `write_row_json()` (already replaced by #568)

## What gets changed

| Component | Change |
|---|---|
| `StreamingBuilder` | Suffix only on conflict (bare name when single type) |
| `StorageBuilder` | Same |
| `build_col_infos()` | Group by bare name using DataType, not suffix parsing |
| `SqlTransform` | Generate view when suffixed columns exist; pad missing columns |
| `COLUMN_NAMING.md` | Update to reflect bare-name-first convention |
| `SCANNER_CONTRACT.md` | Update column naming section |
| All tests | Update expected column names |

## Bugs this fixes

| Fix | Bugs resolved |
|---|---|
| DataType dispatch in output | #430, #444, #428 |
| Delete dead rewriter | #415, #429, #531 |
| DataType-based dedup | #404, #442 |
| Bare names for clean schemas | Improves SQL UX for OTLP/CSV pipelines |

## Implementation phases

1. **Delete dead code** — rewriter.rs, strip_type_suffix(). Zero risk.
2. **Suffix only on conflict** — builder change. Bare names when single
   type, suffixed only on actual type conflict.
3. **Output grouping** — `build_col_infos()` uses DataType-based grouping.
   Handles both bare (single column) and suffixed (multi-column) fields.
4. **Schema stability** — pad SQL-referenced columns with nulls.
5. **View layer** — auto-generate view when suffixed columns exist.
   Bare name = string representation. Typed columns available.
