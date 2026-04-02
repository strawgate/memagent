# Column Type Design

Context: #445

## Requirements

1. **Per-document type fidelity.** A field that enters as int `200` in
   document A and string `"OK"` in document B must exit with those exact
   types. `SELECT *` round-trips the data.

2. **Clean-schema inputs stay clean.** OTLP, Arrow IPC, CSV, and other
   typed inputs pass through with bare column names — no suffixes, no
   views, no overhead.

3. **Mixed-type inputs are handled gracefully.** JSON can have
   `status: 200` in one row and `status: "OK"` in another. The system
   preserves both types without promotion.

4. **SQL is usable.** Bare names work for the common case. Typed access
   is available when needed.

5. **Schema stability.** SQL-referenced columns always exist, padded
   with nulls if absent from a batch.

## Design: Suffix Only On Conflict

When the builder finalizes a batch, each field gets:

- **No type conflict** (field is always int, or always string, etc.):
  bare column name, native DataType.
  - `status` always int → column `status` (Int64)
  - `level` always string → column `level` (Utf8)

- **Type conflict** (field has multiple types across rows in the batch):
  suffixed columns for each observed type.
  - `status` is int in some rows, string in others →
    `status_int` (Int64) + `status_str` (Utf8)

### Input compatibility

| Input source | Type conflicts? | Column names |
|---|---|---|
| OTLP / Arrow IPC | Never (typed per-column) | Bare: `status`, `level` |
| CSV / raw / syslog | Never (all strings) | Bare: `status`, `level` |
| JSON (consistent) | No | Bare: `status` (Int64), `level` (Utf8) |
| JSON (mixed types) | Yes | `status_int` + `status_str`, `level` (Utf8) |
| Mixed pipeline | Depends on batch | Bare when clean, suffixed on conflict |

### SQL interface

**Bare names (common case):**
```sql
SELECT status, level FROM logs WHERE status > 400
```

**Suffixed names (type conflict case):**
```sql
SELECT status_int, status_str FROM logs WHERE status_int > 400
```

**View layer for type-conflict batches:** when suffixed columns exist,
a DataFusion view provides bare-name access. The bare name resolves to
the string representation. Typed columns remain available.

```sql
CREATE VIEW logs AS SELECT
  status_str AS status,
  status_int,
  status_str,
  level_str AS level,
  ...
FROM _raw_logs
```

The view is only created when suffixed columns are detected in the batch
schema. For clean-schema batches, the table is registered directly.

### Output serialization

The output layer dispatches on Arrow DataType, not column name suffix.
For round-trip serialization:

1. Group columns by bare field name (strip `_int`/`_float`/`_str` suffix
   if present).
2. For each row, find the first non-null value across the column group.
3. Serialize using the DataType of that column:
   - Int64 → JSON number: `"status": 200`
   - Float64 → JSON number: `"status": 1.5`
   - Utf8 → JSON string: `"status": "OK"`

JSON output key is always the bare field name.

### Schema stability

At config time, `QueryAnalyzer` extracts `referenced_columns` from the
user's SQL. The suffix convention gives us the DataType. Before
registering each batch as a MemTable, pad with null columns for any
SQL-referenced columns missing from the batch.

This is stateless — guaranteed columns are derived from the SQL at
config time, not from runtime data. For `SELECT *` (no explicit column
references), no padding is needed.

### Builder logic

```rust
if fc.has_int && !fc.has_float && !fc.has_str {
    // Single type: bare name
    columns.push((field_name, DataType::Int64, int_values));
} else if fc.has_str && !fc.has_int && !fc.has_float {
    columns.push((field_name, DataType::Utf8View, str_values));
} else if fc.has_float && !fc.has_int && !fc.has_str {
    columns.push((field_name, DataType::Float64, float_values));
} else {
    // Type conflict: suffixed names
    if fc.has_int   { columns.push((format!("{field_name}_int"),   DataType::Int64,    int_values));   }
    if fc.has_float { columns.push((format!("{field_name}_float"), DataType::Float64,  float_values)); }
    if fc.has_str   { columns.push((format!("{field_name}_str"),   DataType::Utf8View, str_values));   }
}
```
