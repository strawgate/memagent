---
title: "SQL Transforms"
description: "Filter, reshape, and enrich logs with DataFusion SQL"
---

FastForward runs SQL queries on your log data using Apache DataFusion. Every log
line becomes a row in a virtual `logs` table.

## Column naming

The JSON scanner creates columns using **bare field names** and natural SQL-friendly types:
- `level` — string
- `status` — integer
- `latency_ms` — float

Use bare names directly in SQL:

```sql
SELECT * FROM logs WHERE level = 'ERROR'
```

When a field has mixed types across rows (for example, `status` is sometimes a
number and sometimes a string), FastForward normalizes it to a text column so
queries continue to work predictably. Use `int(status)` or `float(status)` when
you need numeric operations on those mixed-type fields.

## Custom UDFs

FastForward registers the following custom scalar functions in addition to
[DataFusion's built-in functions](https://datafusion.apache.org/user-guide/sql/scalar_functions.html).

### `json(column, key)` — extract a string value from JSON

```
json(column: Utf8, key: Utf8) → Utf8
```

Extracts a field from a raw JSON string column and returns its value as a
string. Integer and float values are coerced to their string representation.
Returns `NULL` when the key is not present.

```sql
SELECT json(body, 'host') AS host FROM logs
```

### `json_int(column, key)` — extract an integer value from JSON

```
json_int(column: Utf8, key: Utf8) → Int64
```

Extracts a field from a raw JSON string column and returns its value as a
64-bit integer. Returns `NULL` when the key is not present or the value is not
a JSON number (e.g. a quoted string `"200"` returns `NULL`).

```sql
SELECT json_int(body, 'status') AS status FROM logs
  WHERE json_int(body, 'status') >= 500
```

### `json_float(column, key)` — extract a float value from JSON

```
json_float(column: Utf8, key: Utf8) → Float64
```

Extracts a field from a raw JSON string column and returns its value as a
64-bit float. Integer values are promoted to float. Returns `NULL` when the
key is not present or the value is a quoted string.

```sql
SELECT json_float(body, 'duration') AS duration_sec FROM logs
  WHERE json_float(body, 'duration') > 1.5
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
SELECT grok(body, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}')
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
SELECT regexp_extract(body, 'status=(\d+)', 1) AS status_code FROM logs
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
    format: mmdb
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

## What's next

| Topic | Where to go |
|-------|-------------|
| See all YAML options | [YAML Reference](/configuration/reference/) |
| Understand the scanner | [Scanner Deep Dive](/learn/scanner/) (interactive) |
| Deploy to production | [Kubernetes DaemonSet](/deployment/kubernetes/) |
| Debug transform issues | [Troubleshooting](/troubleshooting/) |
