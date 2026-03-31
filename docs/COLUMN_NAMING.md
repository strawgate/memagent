# Column Naming and Schema Mapping

`logfwd` uses a zero-copy scanner that automatically maps JSON fields to typed columns. To avoid ambiguity and enable efficient SQL transforms, all JSON fields are suffixed with their detected type.

## Type Suffixes

When you write a SQL transform, reference your JSON fields using these suffixes:

| Suffix | JSON Type | DataFusion Type | Example |
|--------|-----------|-----------------|---------|
| `$str` | String | `Utf8` or `Utf8View` | `level$str` |
| `$int` | Integer | `Int64` | `status$int` |
| `$float` | Float | `Float64` | `duration_ms$float` |

## Why Suffixes?

NDJSON (Newline Delimited JSON) is schema-less. A field like `status` might be an integer in one line (`200`) and a string in another (`"200"`). 

To process these efficiently in Arrow's strictly-typed `RecordBatch` format without a full pre-scan, `logfwd` extracts both versions into separate columns.

## Example

Given this JSON:

```json
{"timestamp":"2024-01-15T10:30:00Z", "level":"INFO", "status":200, "duration":1.5}
```

The available columns in your SQL `SELECT` will be:
- `timestamp$str`
- `level$str`
- `status$int` (and `status$str` if it looks like a string elsewhere)
- `duration$float` (and `duration$str`)

## Automatic "Smart" Coalesce

If you want to handle fields that might be either `int` or `str` gracefully, you can use `COALESCE`:

```sql
SELECT COALESCE(CAST(status$int AS VARCHAR), status$str) AS status FROM logs
```

## Special Columns

| Column | Description |
|--------|-------------|
| `_raw` | The original raw byte line (unparsed) |
| `_file$str` | The absolute path of the file being tailed |
| `_time` | The internal timestamp assigned to the log line |
