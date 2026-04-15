# SQL Transforms

logfwd uses Apache DataFusion to run SQL queries on your log data. Every log
line becomes a row in a virtual `logs` table.

## Column naming

The JSON scanner creates columns using **bare field names** with native Arrow types:
- `level` — Utf8View (string)
- `status` — Int64 (integer)
- `latency_ms` — Float64 (float)

Use bare names directly in SQL:

```sql
SELECT * FROM logs WHERE level = 'ERROR'
```

When a field has mixed types across rows (e.g., `status` is sometimes an int,
sometimes a string), the builder emits a `StructArray` conflict column
(`status: Struct { int: Int64, str: Utf8View }`).
Before SQL execution, conflict columns are normalized to flat `Utf8` columns
via `COALESCE(CAST(int AS Utf8), CAST(float AS Utf8), str)`.
Use `int(status)` or `float(status)` for numeric operations on these columns.

## Custom UDFs

logfwd registers the following custom scalar functions in addition to
[DataFusion's built-in functions](https://datafusion.apache.org/user-guide/sql/scalar_functions.html).

### `json(column, key)` — extract a string value from JSON

```
json(column: Utf8, key: Utf8) → Utf8
```

Extracts a field from a raw JSON string column and returns its value as a
string. Integer and float values are coerced to their string representation.
Returns `NULL` when the key is not present.

```sql
SELECT json(_raw, 'host') AS host FROM logs
```

### `json_int(column, key)` — extract an integer value from JSON

```
json_int(column: Utf8, key: Utf8) → Int64
```

Extracts a field from a raw JSON string column and returns its value as a
64-bit integer. Returns `NULL` when the key is not present or the value is not
a JSON number (e.g. a quoted string `"200"` returns `NULL`).

```sql
SELECT json_int(_raw, 'status') AS status FROM logs
  WHERE json_int(_raw, 'status') >= 500
```

### `json_float(column, key)` — extract a float value from JSON

```
json_float(column: Utf8, key: Utf8) → Float64
```

Extracts a field from a raw JSON string column and returns its value as a
64-bit float. Integer values are promoted to float. Returns `NULL` when the
key is not present or the value is a quoted string.

```sql
SELECT json_float(_raw, 'duration') AS duration_sec FROM logs
  WHERE json_float(_raw, 'duration') > 1.5
```

### `grok(column, pattern)` — grok pattern extraction

```
grok(column: Utf8, pattern: Utf8) → Struct
```

Applies a Logstash-style grok pattern to each row and returns a struct with
one Utf8 field per named capture (`%{PATTERN:name}`). Non-matching rows
produce `NULL` struct fields.

Built-in patterns: `IP`, `IPV4`, `IPV6`, `NUMBER`, `INT`, `BASE10NUM`,
`WORD`, `NOTSPACE`, `SPACE`, `DATA`, `GREEDYDATA`, `QUOTEDSTRING`, `UUID`,
`MAC`, `URIPATH`, `URIPATHPARAM`, `URI`, `TIMESTAMP_ISO8601`, `DATE`,
`TIME`, `LOGLEVEL`, `HOSTNAME`, `EMAILADDRESS`.

```sql
SELECT grok(message, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}')
  FROM logs
```

### `regexp_extract(column, pattern, group)` — regex capture group extraction

```
regexp_extract(column: Utf8, pattern: Utf8, group: Int64) → Utf8
```

Returns the capture group at the given index (1-based), or the full match
when `group` is 0. Returns `NULL` when the pattern does not match or the
group index is out of range.

```sql
SELECT regexp_extract(message, 'status=(\d+)', 1) AS status_code FROM logs
```

### `geo_lookup(ip)` — GeoIP enrichment

```
geo_lookup(ip: Utf8) → Struct
```

Looks up an IP address in the configured MaxMind database and returns a struct
with fields: `country_code` (Utf8), `country_name` (Utf8), `city` (Utf8),
`region` (Utf8), `latitude` (Float64), `longitude` (Float64), `asn` (Int64),
`org` (Utf8). All fields are nullable — private/unresolvable IPs return
`NULL`. Requires a `geo_database` enrichment in the pipeline config.

Use `get_field()` to extract individual fields:

```yaml
enrichment:
  - type: geo_database
    path: /data/GeoLite2-City.mmdb
transform: |
  SELECT *, get_field(geo, 'city') AS city, get_field(geo, 'country_code') AS country
  FROM (SELECT *, geo_lookup(source_ip) AS geo FROM logs)
```

## Examples

### Filter by severity
```yaml
transform: "SELECT * FROM logs WHERE level IN ('ERROR', 'WARN')"
```

### Drop fields
```yaml
transform: "SELECT * EXCEPT (request_id, trace_id) FROM logs"
```

### Rename fields
```yaml
transform: "SELECT duration_ms AS latency_ms, * EXCEPT (duration_ms) FROM logs"
```

### Computed fields
```yaml
transform: |
  SELECT *,
    CASE WHEN duration_ms > 200 THEN 'slow' ELSE 'fast' END AS speed
  FROM logs
```

### Aggregation
```yaml
transform: |
  SELECT level, COUNT(*) as count, AVG(duration_ms) as avg_latency
  FROM logs
  GROUP BY level
```
