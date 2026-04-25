# Column Type Design

> **Status:** Active
> **Date:** 2026-03-31
> **Context:** StructArray conflict column design for mixed-type inputs; C3 not yet implemented.

Context: #445, #625

## Requirements

1. **Per-document type fidelity.** A field that enters as int `200` in
   document A and string `"OK"` in document B must exit with those exact
   types. `SELECT *` round-trips the data.

2. **Clean-schema inputs stay clean.** OTLP, Arrow IPC, CSV, and other
   typed inputs pass through with bare column names — no overhead.

3. **Mixed-type inputs are handled gracefully.** JSON can have
   `status: 200` in one row and `status: "OK"` in another. The system
   preserves both types without promotion.

4. **SQL is usable.** Bare names work for the common case. Typed access
   is available when needed.

5. **Schema stability.** SQL-referenced columns always exist, padded
   with nulls if absent from a batch.

## Design: StructArray Conflict Columns

When the builder finalizes a batch, each field gets:

- **No type conflict** (field is always int, or always string, etc.):
  bare column name, native DataType.
  - `status` always int → column `status` (Int64)
  - `level` always string → column `level` (Utf8View)

- **Type conflict** (field has multiple types across rows in the batch):
  a single Arrow `StructArray` column with child fields named after the
  observed types.
  - `status` is int in some rows, string in others →
    `status: Struct { int: Int64, str: Utf8View }`

A conflict struct is detected structurally by `is_conflict_struct()`: an
Arrow `Struct` type whose child fields are all named from
`{"int", "float", "str", "bool"}`. No schema metadata key is required.

### Input compatibility

| Input source | Type conflicts? | Column names |
|---|---|---|
| OTLP / Arrow IPC | Never (typed per-column) | Bare: `status`, `level` |
| CSV / raw / syslog | Never (all strings) | Bare: `status`, `level` |
| JSON (consistent) | No | Bare: `status` (Int64), `level` (Utf8View) |
| JSON (mixed types) | Yes | `status: Struct { int: Int64, str: Utf8View }` |
| Mixed pipeline | Depends on batch | Bare when clean, struct on conflict |

This is almost entirely a JSON problem. Protobuf, Avro, Parquet, and
OTLP all have per-field schemas that eliminate type conflicts at the
source. The typed-variant machinery is invisible for non-JSON inputs.

### SQL layer

**Problem:** When a conflict batch has `status: Struct { int, str }`,
SQL `SELECT status FROM logs` either fails or returns the struct — not
the per-row coalesced value.

**Solution:** `normalize_conflict_columns()` in `ffwd-transform` detects
conflict structs via `is_conflict_struct()` and replaces each in-place with
a synthesized flat `Utf8` column before handing the batch to DataFusion:

```
COALESCE(CAST(status.int AS Utf8), CAST(status.float AS Utf8), status.str)
```

This column is for SQL resolution only. Output sinks never see it — they
operate on the original `RecordBatch` before normalization.

**Bare names (common case — no conflict):**
```sql
SELECT status, level FROM logs WHERE status > 400
```

**Mixed-type case:**
```sql
-- status is Utf8 (synthesized), use int() / float() UDFs for numeric ops
SELECT status, level FROM logs WHERE int(status) > 400
SELECT status, level FROM logs WHERE status = 'ERROR'
```

The `int()` / `float()` / `str()` UDFs inspect the struct's children
directly if the column is a conflict struct, or read the flat column
otherwise. They work correctly on both single-type and conflict batches.

### Output serialization

Output sinks receive the original `RecordBatch` (before SQL normalization).
They use `build_col_infos()` from `ffwd-output`, which returns a
`Vec<ColInfo>` — one per logical field. Each `ColInfo` carries
`json_variants: Vec<ColVariant>` (int > float > str priority) and
`str_variants: Vec<ColVariant>` (str > int > float).

```rust
pub enum ColVariant {
    /// Column is a flat (non-struct) Arrow array.
    Flat { col_idx: usize, dt: DataType },
    /// Column is a child of a conflict StructArray.
    StructField { struct_col_idx: usize, field_idx: usize, dt: DataType },
}

pub struct ColInfo {
    pub field_name: Arc<str>,
    pub json_variants: Vec<ColVariant>,  // priority order for JSON emission
    pub str_variants:  Vec<ColVariant>,  // priority order for string coercion
}
```

For each row, `write_row_json()` picks the first non-null variant from
`json_variants` and emits the correctly-typed JSON field. Output is
`"status": 200` or `"status": "error"` — identical to the original document.

Struct conflict columns are skipped in sinks that cannot represent typed
variants (e.g., OTLP attribute encoding — `DataType::Struct(_) => continue`
in `resolve_batch_columns()`). Those sinks rely on a per-output SQL
transform (planned architecture) to reshape the struct before emission.

### Builder logic

```rust
let conflict = (fc.has_int as u8) + (fc.has_float as u8) + (fc.has_str as u8) > 1;

if conflict {
    // Build child arrays and assemble StructArray
    let mut child_fields: Vec<Arc<Field>> = Vec::new();
    let mut child_arrays: Vec<ArrayRef> = Vec::new();
    if fc.has_int   { child_fields.push(Field::new("int",   DataType::Int64,   true)); child_arrays.push(...); }
    if fc.has_float { child_fields.push(Field::new("float", DataType::Float64, true)); child_arrays.push(...); }
    if fc.has_str   { child_fields.push(Field::new("str",   DataType::Utf8View,true)); child_arrays.push(...); }
    let struct_validity: Vec<bool> = (0..num_rows)
        .map(|i| child_arrays.iter().any(|arr| !arr.is_null(i)))
        .collect();
    let struct_arr = StructArray::new(Fields::from(child_fields.clone()), child_arrays,
        Some(NullBuffer::from(struct_validity)));
    schema_fields.push(Field::new(name, DataType::Struct(Fields::from(child_fields)), true));
    arrays.push(Arc::new(struct_arr) as ArrayRef);
} else {
    // Single type: bare name, zero overhead
    if fc.has_int   { schema_fields.push(Field::new(name, DataType::Int64,    true)); arrays.push(int_arr);   }
    if fc.has_float { schema_fields.push(Field::new(name, DataType::Float64,  true)); arrays.push(float_arr); }
    if fc.has_str   { schema_fields.push(Field::new(name, DataType::Utf8View, true)); arrays.push(str_arr);   }
}
```

No schema metadata is written or read for conflict detection.

### Schema stability

**Current behavior:** `normalize_conflict_columns` synthesizes the bare
Utf8 column from the conflict struct before each batch is registered as a
DataFusion MemTable. SQL references to bare names always resolve. This is
stateless — no per-batch schema tracking needed.

**Planned (#625):** At config time, a `QueryAnalyzer` will extract
`referenced_columns` from the user's SQL. An `AnalyzerRule` +
`TableProvider` will route each batch through schema-padding (null columns
for any SQL-referenced columns missing from the batch) and conflict
normalization, replacing the direct MemTable registration used today.

## Implementation Phases

### Phase 10 (complete — PR #684)
- Builders emit bare names for single-type fields, `__int`/`__str`/`__float`
  (double underscore) for conflicts
- `normalize_conflict_columns` synthesizes bare Utf8 column for SQL
- Dead `rewriter.rs` deleted

### Phase 10b (complete — PR #713)
- Standardized double-underscore suffixes in `StreamingBuilder`
- `ffwd.conflict_groups` schema metadata stamped in builders
- `strip_conflict_suffix` / `suffix_order` updated in `conflict_schema.rs`
  and `json_extract.rs`

### Phase 10c (complete — PR #760)
- **Replaced** flat `__int`/`__str`/`__float` columns + `ffwd.conflict_groups`
  metadata with a single Arrow `StructArray` conflict column per field
- `is_conflict_struct()` structural detection replaces metadata parsing
- `normalize_conflict_columns()` rewrites struct → flat Utf8 for SQL path
- `ColVariant` / `ColInfo` / `build_col_infos()` / `write_row_json()` added
  to `ffwd-output` for type-preserving output serialization
- `json_extract` UDF updated: step 1 skips structs, step 2 extracts from
  struct children
- OTLP sink skips struct columns instead of emitting empty-string attributes
- All sinks updated; 500+ tests passing

### Future: Type hints (#625 follow-on)
A user can declare in config:
```yaml
schema:
  status: int
```
This pins `status` to Int64 across all batches. The scanner uses this hint
to coerce values at scan time, eliminating the conflict columns entirely.
Satisfies C8 (config determines behavior, not data).
