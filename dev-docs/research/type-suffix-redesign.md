# Column Type Design

Context: #445, #625

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
  - `level` always string → column `level` (Utf8View)

- **Type conflict** (field has multiple types across rows in the batch):
  double-underscore suffixed columns for each observed type.
  - `status` is int in some rows, string in others →
    `status__int` (Int64) + `status__str` (Utf8View)

Double underscores (`__`) are used to minimize collision with real field
names (a JSON field literally named `status_int` is common; `status__int`
is not).

### Input compatibility

| Input source | Type conflicts? | Column names |
|---|---|---|
| OTLP / Arrow IPC | Never (typed per-column) | Bare: `status`, `level` |
| CSV / raw / syslog | Never (all strings) | Bare: `status`, `level` |
| JSON (consistent) | No | Bare: `status` (Int64), `level` (Utf8View) |
| JSON (mixed types) | Yes | `status__int` + `status__str`, `level` (Utf8View) |
| Mixed pipeline | Depends on batch | Bare when clean, suffixed on conflict |

This is almost entirely a JSON problem. Protobuf, Avro, Parquet, and
OTLP all have per-field schemas that eliminate type conflicts at the
source. The typed-variant machinery is invisible for non-JSON inputs.

### Schema metadata

When the builder emits conflict columns, it also stamps the Arrow schema
with a metadata key listing the conflict groups:

```
schema.metadata["logfwd.conflict_groups"] = "status:int,str;duration:float,int"
```

Format: semicolon-separated groups, each group is `base_name:type1,type2`
where types are `int`, `float`, `str`. The metadata is the authoritative
record — all downstream code (output sinks, tests) reads this key rather
than parsing column names.

For batches with no conflicts (the common case), the key is absent.
Zero overhead.

### SQL layer

**Problem:** When a conflict batch has `status__int` and `status__str`
but no bare `status` column, SQL `SELECT status FROM logs` fails.

**Solution:** `normalize_conflict_columns()` in `logfwd-transform` reads
the `logfwd.conflict_groups` metadata and adds a synthesized bare
`status: Utf8` column before handing the batch to DataFusion:

```
COALESCE(CAST(status__int AS Utf8), CAST(status__float AS Utf8), status__str)
```

This column is for SQL resolution only. Output sinks ignore it (see below).

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

**Direct typed-variant access (power users):**
```sql
SELECT status__int, status__str FROM logs WHERE status__int > 400
```

### Output serialization

Output sinks **must not** emit `status__int`/`status__str` as separate
fields, and **must not** use the synthesized bare `status: Utf8` column
(it's string-only and loses type fidelity).

Instead, each output sink uses `ConflictGroups` from `logfwd-output`:

```rust
pub enum TypedValue<'a> {
    Int(i64),
    Float(f64),
    Str(&'a str),
    Null,
}

pub struct ConflictGroup {
    pub base: String,             // "status"
    pub int_col:   Option<usize>, // column index of status__int
    pub float_col: Option<usize>,
    pub str_col:   Option<usize>,
    pub synth_col: Option<usize>, // index of synthesized bare col — skip
}

pub struct ConflictGroups {
    pub groups:    Vec<ConflictGroup>,
    pub skip_cols: HashSet<usize>, // variant + synth cols — skip in normal iteration
}

impl ConflictGroups {
    /// Parse from Arrow schema metadata.
    pub fn from_schema(schema: &Schema) -> Self { ... }

    /// Return the non-null typed value for this group at this row.
    /// Priority: int > float > str.
    pub fn typed_value_for_row<'a>(
        &self, batch: &'a RecordBatch, group: &ConflictGroup, row: usize,
    ) -> TypedValue<'a> { ... }
}
```

Each output sink's row-emission loop:

```rust
let groups = ConflictGroups::from_schema(batch.schema());

// Normal columns
for (i, field) in schema.fields().iter().enumerate() {
    if groups.skip_cols.contains(&i) { continue; }
    emit_column(field.name(), batch.column(i), row);
}

// Conflict groups (type-preserving, correct field names)
for group in &groups.groups {
    match groups.typed_value_for_row(batch, group, row) {
        TypedValue::Int(v)   => emit_int(group.base, v),
        TypedValue::Float(v) => emit_float(group.base, v),
        TypedValue::Str(v)   => emit_str(group.base, v),
        TypedValue::Null     => {},
    }
}
```

This produces `"status": 200` or `"status": "error"` per row with correct
types — identical to what the original JSON document contained.

### Builder logic

```rust
let conflict = (fc.has_int as u8) + (fc.has_float as u8) + (fc.has_str as u8) > 1;

if !conflict {
    // Single type: bare name, zero overhead
    if fc.has_int   { columns.push((field_name.clone(), DataType::Int64,    int_values));   }
    if fc.has_float { columns.push((field_name.clone(), DataType::Float64,  float_values)); }
    if fc.has_str   { columns.push((field_name.clone(), DataType::Utf8View, str_values));   }
} else {
    // Type conflict: double-underscore suffixed names
    if fc.has_int   { columns.push((format!("{field_name}__int"),   DataType::Int64,    int_values));   }
    if fc.has_float { columns.push((format!("{field_name}__float"), DataType::Float64,  float_values)); }
    if fc.has_str   { columns.push((format!("{field_name}__str"),   DataType::Utf8View, str_values));   }
    // Stamp schema metadata (accumulated and set on schema at finish_batch)
    conflict_groups.push((field_name.clone(), observed_types));
}
```

### Schema stability

**Current behavior:** `normalize_conflict_columns` synthesizes the bare
Utf8 column from conflict variants before each batch is registered as a
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

### Phase 10b (complete — this PR)
- Double-underscore suffixes (`__int`/`__str`/`__float`) in
  `StreamingBuilder`, `StorageBuilder`
- `strip_conflict_suffix` in `conflict_schema.rs` uses `__` prefix
- `suffix_order` in `json_extract.rs` updated
- All tests updated (scanner_conformance, compliance_data, scanner.rs, etc.)
- `logfwd.conflict_groups` schema metadata stamped in builders

### Phase 10c: ConflictGroups output abstraction
- Add `ConflictGroups` + `TypedValue` to `logfwd-output`
- Update OTLP sink (type-preserving per-row emission)
- Update JSON Lines / TCP / UDP sinks (same)
- Update stdout sink
- Tests: conflict batch round-trips correctly through each sink

### Future: Type hints (#625 follow-on)
A user can declare in config:
```yaml
schema:
  status: int
```
This pins `status` to Int64 across all batches. The scanner uses this hint
to coerce values at scan time, eliminating the conflict columns entirely.
Satisfies C8 (config determines behavior, not data).
