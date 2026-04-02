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
`logfwd` creates separate typed columns:

| Column name | Arrow type | Contains |
|---|---|---|
| `status_int` | `Int64` | Rows where status was an integer |
| `status_str` | `Utf8` | Rows where status was a string |

Each typed column is nullable — rows where the field had a different type
contain null.

```sql
-- Filter on the integer version:
SELECT status_int FROM logs WHERE status_int > 400

-- Get the string version:
SELECT status_str FROM logs WHERE status_str = 'OK'
```

### Type suffixes

| Suffix | JSON type | Arrow type |
|---|---|---|
| `_str` | String, boolean, nested object/array | `Utf8` / `Utf8View` |
| `_int` | Integer | `Int64` |
| `_float` | Float | `Float64` |

Suffixes only appear when there's a type conflict. If a field is always
one type, it has a bare name with no suffix.

## Output Round-Tripping

Output serialization uses the Arrow DataType, not the column name:
- `Int64` → JSON number: `"status": 200`
- `Float64` → JSON number: `"duration": 1.5`
- `Utf8` → JSON string: `"level": "INFO"`

The output JSON key is always the bare field name (suffix stripped).
This means `SELECT *` round-trips documents with their original types.

## Special Columns

| Column | Description |
|---|---|
| `_raw` | The original raw byte line (unparsed) |
| `_file_str` | The absolute path of the file being tailed |
| `_time` | The internal timestamp assigned to the log line |

## Schema Stability

If your SQL references a column that doesn't exist in a particular batch
(e.g., `WHERE status_int > 400` but this batch has no integer status
values), the column is automatically padded with nulls. Your SQL will
never fail due to a missing column — it will simply return no matching
rows for that column.

This is derived from your SQL at config time, not accumulated at runtime.
