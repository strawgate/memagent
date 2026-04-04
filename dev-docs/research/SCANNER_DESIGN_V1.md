# Scanner and Transform Design

> **Status:** Historical
> **Date:** 2026-03
> **Context:** Design rationale from the v1 Arrow-first pipeline, superseded by current implementation.

Design rationale for the Arrow-first pipeline. The implementations are in
`scanner.rs`, `batch_builder.rs`, `transform.rs`, and `output.rs`.

## Why Arrow-First, No Fast Path

Rust benchmark (scan_bench.rs, 8192 lines × 13 fields × 310 bytes/line):

```
Scan-only 3 fields:             74 ns/line   13.5M lines/sec
Scan-and-build all → Arrow:    399 ns/line    2.5M lines/sec
Scan-and-build 3 (pushdown):   342 ns/line    2.9M lines/sec
Arrow construction overhead:    93 ns/line
```

Arrow construction is 93 ns. DataFusion passthrough is 85 ns. Combined: 178 ns
on top of scanning. Not worth maintaining a separate fast path to save 178 ns
when the total pipeline is 500-900 ns. One code path, one implementation per feature.

## Scanner Design (scanner.rs)

The scanner uses memchr to find JSON quotes, extracts key-value pairs, and
writes values directly into BatchBuilder's Arrow column builders. No intermediate
data structure. ScanConfig controls field pushdown.

Key performance choices:
- **Field pushdown**: `ScanConfig.wanted_fields` skips non-requested fields (values
  are parsed over but not appended to builders). `SELECT timestamp, level, service`
  only builds 3 columns instead of 13.
- **No UTF-8 validation** in the scan loop. Work on raw bytes. Arrow StringBuilder
  handles validation at construction time.
- **Field index caching**: HashMap<Vec<u8>, usize> maps field name bytes to builder
  index. Populated on first batch, stable afterward.
- **No per-line allocation**: All builders pre-allocated with capacity hints, reused
  via `begin_batch()`.

## Typed Column Model (batch_builder.rs)

Each JSON field produces 1-3 Arrow columns based on observed value types:

| JSON value | Column created | Arrow type |
|-----------|---------------|------------|
| `"hello"` | `message_str` | Utf8 |
| `42` | `status_int` | Int64 |
| `3.14` | `rate_float` | Float64 |
| `true`/`false` | `flag_str` | Utf8 ("true"/"false") |
| `null` | (NULL in all typed columns) | - |
| `{...}` / `[...]` | `payload_str` | Utf8 (JSON string) |

When a field has consistent types across a batch: one column.
When types conflict (e.g., `status=500` then `status="error"`): both `status_int`
and `status_str`, with NULLs in non-matching rows.

Memory overhead of multi-typed columns: 1.03x (measured). Null bitmaps are 1 bit/row.

## SQL Rewriter (NOT YET BUILT)

Translates user-facing SQL to internal typed-column SQL. Required because users
write `level` but the RecordBatch has `level_str`.

### Rewrite Rules

```
1. Bare column in SELECT:
   duration_ms → COALESCE(CAST(duration_ms_int AS VARCHAR),
                          CAST(duration_ms_float AS VARCHAR),
                          duration_ms_str) AS duration_ms
   level → level_str AS level  (single type, no coalesce)

2. Bare column in WHERE with string literal:
   WHERE level = 'ERROR' → WHERE level_str = 'ERROR'

3. Bare column in WHERE with numeric literal:
   WHERE status > 400 → WHERE status_int > 400
   (string rows get NULL → correctly excluded)

4. int() function:
   int(duration_ms) → COALESCE(duration_ms_int,
                               CAST(duration_ms_float AS BIGINT),
                               TRY_CAST(duration_ms_str AS BIGINT))

5. float() function:
   float(duration_ms) → COALESCE(CAST(duration_ms_int AS DOUBLE),
                                 duration_ms_float,
                                 TRY_CAST(duration_ms_str AS DOUBLE))

6. EXCEPT:
   EXCEPT (duration_ms) → EXCEPT (duration_ms_int, duration_ms_str, duration_ms_float)

7. SELECT *:
   Expand to all fields with coalesced aliases.

8. ORDER BY bare column:
   ORDER BY duration_ms → ORDER BY duration_ms_str
   (string ordering by default; use int(duration_ms) for numeric)

9. Aggregation:
   AVG(duration_ms) → error: "use AVG(float(duration_ms))"
   COUNT(duration_ms) → COUNT(duration_ms_str)  (counts non-null)
```

### Implementation

Use `sqlparser::parser::Parser` to parse the AST. Walk Expr nodes, rewrite column
references using the FieldTypeMap (populated from BatchBuilder after first batch).
Re-serialize to SQL string. ~500-800 lines.

**FieldTypeMap**: `HashMap<String, Vec<TypeTag>>` where TypeTag is Str/Int/Float.
Populated from BatchBuilder's discovered fields after `finish_batch()`.

### Schema Evolution

1. Parse SQL at startup → extract column references (before data arrives)
2. First batch → scanner discovers field names + types
3. Build FieldTypeMap, run rewriter, compile DataFusion plan (~1ms)
4. Cache plan for subsequent batches
5. New field appears → extend FieldTypeMap, recompile plan (~1ms, rare)

## Output Type Preservation (output.rs)

Outputs read typed columns for correct serialization:

- `status_int` non-null → OTLP `int_value: 500`, JSON `"status": 500`
- `rate_float` non-null → OTLP `double_value: 0.95`, JSON `"rate": 0.95`
- `level_str` non-null → OTLP `string_value: "ERROR"`, JSON `"level": "ERROR"`

When both `status_int` and `status_str` exist for a row, prefer: int > float > str.
The `parse_column_name()` helper strips the type suffix for output field names.

For JSON output with `_raw` column: if no transform modified fields, memcpy the
original JSON line directly (passthrough optimization).

## Buffer Lifecycle

```
ChunkAccumulator yields OwnedChunk (read buffer)
    ↓
CRI parser → messages into json_batch buffer
    ↓
Scanner reads json_batch → writes into Arrow column builders
    ↓
OwnedChunk reclaimed here (Arrow builders own their data)
    ↓
BatchBuilder.finish_batch() → RecordBatch
    ↓
DataFusion executes SQL on RecordBatch
    ↓
Output sinks serialize from RecordBatch → network/file
```

The read buffer is released after Arrow column construction. DataFusion and
outputs operate on independently-owned Arrow memory. No lifetime coupling
between the reader and the rest of the pipeline.
