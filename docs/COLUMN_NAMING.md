# Column Naming and Schema Mapping

`logfwd` automatically maps input fields to typed Arrow columns. Column
names match the original field names whenever possible.

## How Column Names Work

### Single-type fields (common case)

When a field has a consistent type across all rows in a batch, the column
uses the bare field name with the native Arrow type:

| JSON value | Column name | Arrow type |
|---|---|---|
| `"status": 200` | `status` | `Int64` |
| `"level": "INFO"` | `level` | `Utf8View` |
| `"duration": 1.5` | `duration` | `Float64` |

SQL works naturally:
```sql
SELECT status, level FROM logs WHERE status > 400
```

This is the normal case for OTLP inputs, CSV inputs, and JSON sources
with consistent types.

### Mixed-type fields (type conflict)

When the same field appears as different types across rows in a batch —
for example `"status": 200` in one row and `"status": "OK"` in another —
`logfwd` creates a single **conflict struct column** using an Arrow
`StructArray`:

| Column name | Arrow type | Child fields |
|---|---|---|
| `status` | `Struct { int: Int64, str: Utf8View }` | `int` — rows where status was an integer; `str` — rows where status was a string |

Each child field is nullable — rows where the field had a different type
contain null in that child. The struct row itself is non-null if any child
is non-null.

A conflict struct is detected structurally: an Arrow `Struct` whose child
fields are all named from `{"int", "float", "str", "bool"}`.

For SQL access, `normalize_conflict_columns()` replaces the struct column
in-place with a synthesized flat `Utf8` column
(`COALESCE(CAST(int AS Utf8), CAST(float AS Utf8), str)`) before handing
the batch to DataFusion. Use the `int()` and `float()` UDFs to access
typed children:

```sql
-- Access the (string-coalesced) value:
SELECT status FROM logs

-- Filter on the integer child:
SELECT * FROM logs WHERE int(status) > 400

-- String comparison:
SELECT * FROM logs WHERE status = 'OK'
```

No Arrow schema metadata key is used — the struct layout itself is the
authoritative signal.

### Type children

| Child field name | JSON type | Arrow type |
|---|---|---|
| `str` | String, boolean, nested object/array | `Utf8View` |
| `int` | Integer | `Int64` |
| `float` | Float | `Float64` |

Conflict struct columns only appear when there is a type conflict within a
batch. If a field is always one type across all rows, it uses a bare name
with the native Arrow type.

## Output Round-Tripping

Output sinks use the `ColVariant`/`ColInfo` abstraction in `logfwd-output`,
not the column name, to dispatch on the Arrow DataType:

- `Int64` → JSON number: `"status": 200`
- `Float64` → JSON number: `"duration": 1.5`
- `Utf8View` → JSON string: `"level": "INFO"`

For conflict struct columns, the output layer emits one field per row with
the correct per-row type from the matching child array. The output JSON key
is always the bare field name (`status`, not a child path).

This means `SELECT * FROM logs` round-trips documents with their original
types intact.

## Special Columns

| Column | Description |
|---|---|
| `_raw` | The original raw byte line (unparsed) |
| `_timestamp` | Timestamp from the CRI header as an RFC 3339 string (CRI inputs only) |
| `_stream` | CRI stream name — `stdout` or `stderr` (CRI inputs only) |

## Schema Stability

If your SQL references a column that doesn't exist in a particular batch
(e.g., `WHERE int(status) > 400` but this batch has no integer status
values), the column is automatically padded with nulls. Your SQL will
never fail due to a missing column — it will simply return no matching
rows for that column.

This is derived from your SQL at config time, not accumulated at runtime.

## Cross-Batch Type Variation

A field can be single-type in one batch (bare `status: Int64`) and
conflict-type in a later batch (`status: Struct { int: Int64, str: Utf8View }`).
After `normalize_conflict_columns()`, both batches expose `status` as Utf8
to DataFusion, so SQL semantics are consistent across batches.

To always get numeric semantics regardless of batch type, use the typed UDFs:

```sql
-- Always numeric — works on both clean and conflict batches:
WHERE int(status) > 400
```

Per-batch fidelity (C1) is implemented today — within a single batch the
scanner preserves every observed type faithfully.

Full cross-batch schema stability (C3) is **not yet implemented**. It is
tracked in [#625](https://github.com/strawgate/memagent/issues/625) and will
require a `TableProvider` approach that advertises referenced columns as
stable `Utf8` and rewrites `CAST(status AS BIGINT)` to read the struct's
`int` child directly.
