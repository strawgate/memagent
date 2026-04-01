# SQL Transforms

logfwd uses Apache DataFusion to run SQL queries on your log data. Every log
line becomes a row in a virtual `logs` table.

## Column naming

The JSON scanner creates typed columns with suffixes:
- `field_str` — string value (Utf8)
- `field_int` — integer value (Int64)
- `field_float` — float value (Float64)

The SQL rewriter automatically translates bare column names, so you can write:

```sql
SELECT * FROM logs WHERE level = 'ERROR'
-- automatically becomes: WHERE level_str = 'ERROR'
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

### Regular expressions
```yaml
transform: |
  SELECT *, regexp_extract(message, 'status=(\d+)', 1) AS status_code
  FROM logs
```

### Grok patterns
```yaml
transform: |
  SELECT *, grok(message, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}')
  FROM logs
```

### GeoIP enrichment

`geo_lookup(ip)` returns a struct with fields: `country_code`, `country_name`,
`city`, `region`, `latitude`, `longitude`, `asn`, `org`. Use `get_field()` to
extract individual fields.

```yaml
enrichment:
  - type: geo_database
    path: /data/GeoLite2-City.mmdb
transform: |
  SELECT *, get_field(geo, 'city') AS city, get_field(geo, 'country_code') AS country
  FROM (SELECT *, geo_lookup(source_ip) AS geo FROM logs)
```
