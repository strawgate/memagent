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
| `"level": "INFO"` | `level` | `Utf8` |
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
`logfwd` creates separate typed variant columns using **double-underscore**
suffixes:

| Column name | Arrow type | Contains |
|---|---|---|
| `status__int` | `Int64` | Rows where status was an integer |
| `status__str` | `Utf8` | Rows where status was a string |
| `status__float` | `Float64` | Rows where status was a float |

Each typed variant column is nullable — rows where the field had a
different type contain null.

A synthesized bare `status: Utf8` column is also added so that SQL
referencing `status` resolves without error. Use the `int()` and
`float()` UDFs to access the typed variants:

```sql
-- Access the bare (string-coalesced) value:
SELECT status FROM logs

-- Filter on the integer variant:
SELECT * FROM logs WHERE int(status) > 400

-- Get the string variant explicitly:
SELECT * FROM logs WHERE status = 'OK'
```

The conflict group is recorded in the Arrow schema under the
`logfwd.conflict_groups` metadata key (format: `"status:int,str"`). This
key is the authoritative signal that conflict columns exist — user fields
that happen to end in `__int` or `__str` are not treated as conflict
columns unless the metadata key is present.

### Type suffixes

| Suffix | JSON type | Arrow type |
|---|---|---|
| `__str` | String, boolean, nested object/array | `Utf8` / `Utf8View` |
| `__int` | Integer | `Int64` |
| `__float` | Float | `Float64` |

Double-underscore suffixes only appear when there is a type conflict
within a batch. If a field is always one type across all rows, it uses
a bare name with no suffix.

## Output Round-Tripping

Output serialization uses the Arrow DataType and the `ConflictGroups`
abstraction, not the column name:
- `Int64` → JSON number: `"status": 200`
- `Float64` → JSON number: `"duration": 1.5`
- `Utf8` / `Utf8View` → JSON string: `"level": "INFO"`

For conflict-group fields, the output layer emits one field per row with
the correct per-row type from the typed variant column. The output JSON
key is always the bare field name (suffixes stripped).

This means `SELECT * FROM logs` round-trips documents with their original
types intact.

## Special Columns

| Column | Description |
|---|---|
| `_raw` | The original raw byte line (unparsed) |
| `_file_str` | The absolute path of the file being tailed |
| `_time` | The internal timestamp assigned to the log line |

## Schema Stability

If your SQL references a column that doesn't exist in a particular batch
(e.g., `WHERE int(status) > 400` but this batch has no integer status
values), the column is automatically padded with nulls. Your SQL will
never fail due to a missing column — it will simply return no matching
rows for that column.

This is derived from your SQL at config time, not accumulated at runtime.

## Cross-Batch Type Variation

A field can be single-type in one batch (bare `status: Int64`) and
conflict-type in a later batch (suffixed `status__int` + `status__str`).
The synthesized bare `status: Utf8` column is only present in conflict
batches, so `WHERE status > 400` has different semantics (numeric vs.
string comparison) across batches.

To write SQL that works correctly regardless of whether a batch has a
type conflict, use the typed UDFs:

```sql
-- Always numeric — works on both clean and conflict batches:
WHERE int(status) > 400
```

Full cross-batch schema stability (C3) requires the `#625` TableProvider
approach, which advertises referenced columns as stable `Utf8` and
rewrites `CAST(status AS BIGINT)` to read `status__int` directly.
