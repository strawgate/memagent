# Raw-First Architecture

> **Status:** Historical
> **Date:** 2026-03
> **Context:** Design decision for readers producing raw lines with SQL as schema declaration.

Context: #445, #625

## Design

Readers produce raw lines. The query engine handles parsing.
The user's SQL is the schema declaration.

## Layers

### 1. Readers (inputs)

Readers frame input into lines and produce a minimal schema:

```
_raw    (Utf8)  — the original line, unmodified
_time   (Int64) — ingestion timestamp (nanoseconds)
_source (Utf8)  — source identifier (file path, TCP addr, etc.)
```

Readers do NOT parse content. They don't know or care if the line is
JSON, CSV, syslog, or garbage. They just find line boundaries and
put each line into `_raw`.

For OTLP/Arrow inputs that arrive pre-structured, the columns pass
through directly — no `_raw` wrapping needed.

### 2. Query engine (DataFusion SQL)

The user writes SQL that extracts fields from `_raw` using UDFs:

```sql
-- JSON extraction (our SIMD scanner under the hood)
SELECT json(_raw, 'status') as status,
       json(_raw, 'level') as level
FROM logs
WHERE json_int(_raw, 'status') > 400

-- CSV extraction
SELECT csv(_raw, 1) as status,
       csv(_raw, 2) as level
FROM logs

-- Passthrough (no parsing)
SELECT _raw FROM logs
```

UDFs:
- `json(_raw, 'key')` → Utf8 (string value of the field)
- `json_int(_raw, 'key')` → Int64 (parse as int, NULL if not int)
- `json_float(_raw, 'key')` → Float64 (parse as float, NULL if not)
- `csv(_raw, index)` → Utf8 (field by position)
- `kv(_raw, 'key')` → Utf8 (key=value extraction)

The user's SQL determines the output schema. No automatic type
detection. No suffix convention. No conflict resolution.

### 3. Output (sinks)

The output receives whatever RecordBatch DataFusion produces.
Column types come from the user's SQL (or from the input for
pre-structured data).

For `SELECT * FROM logs` (passthrough): the output gets `_raw` and
emits it directly. Perfect round-trip — the original line goes out
unchanged.

For SQL with extraction: the output gets the columns the user asked
for, with the types the user specified. `json_int(...)` produces Int64,
`json(...)` produces Utf8.

## How this solves every constraint

| Constraint | Solution |
|---|---|
| **C1: Round-trip fidelity** | `_raw` is the original line. Passthrough emits it unchanged. |
| **C2: Bare field names** | User chooses names via `AS` aliases. |
| **C3: Schema stability** | Schema is defined by SQL, not by data content. |
| **C4: Clean SQL** | `json(_raw, 'status')` is natural. |
| **C5: Mixed types** | User picks the type: `json_int` vs `json` vs `json_float`. |
| **C6: Mixed pipelines** | All inputs produce `_raw`. OTLP passes through as-is. |
| **C7: Downstream compatibility** | Output schema is explicit from SQL. |
| **C8: Config determines schema** | The SQL IS the schema declaration. |

## Performance

The `json()` UDF wraps our SIMD scanner. Benchmarks show:
- SIMD scanner: ~200 MB/s regardless of field count
- DataFusion json_get (re-parses per field): 12-100 MB/s
- SIMD via StringArray (what the UDF does): ~200 MB/s

The UDF parses once per row, extracts all requested fields. Multiple
`json(_raw, 'f1'), json(_raw, 'f2')` calls in the same SELECT share
the parse via DataFusion's common subexpression elimination, or we
implement a multi-field extraction UDF.

## What about pre-structured inputs (OTLP)?

OTLP inputs arrive as typed Arrow columns — `status` (Int64),
`level` (Utf8), etc. These bypass `_raw` entirely and register
directly with DataFusion. The user writes normal SQL:

```sql
SELECT status, level FROM logs WHERE status > 400
```

No UDFs needed. No `_raw`. Clean types. The raw-first architecture
only applies to text-based inputs.

## What gets deleted

- `crates/logfwd-transform/src/rewriter.rs` — already deleted (dead code)
- The scanner-before-DataFusion pipeline step — scanner becomes a UDF
- `parse_column_name()` / `strip_type_suffix()` — no suffixes exist
- `build_col_infos()` column grouping — each column is what the user asked for
- The entire type suffix convention

## What stays

- The SIMD scanner code (logfwd-core) — now wrapped as UDFs
- DataFusion SQL transform
- Output sinks dispatching on DataType

## Migration path

1. Wrap scanner as `json()` / `json_int()` / `json_float()` UDFs
2. Register UDFs in SqlTransform
3. Update default SQL from `SELECT * FROM logs` to include `_raw`
4. For backwards compat: a `json_expand(_raw)` UDF that returns all
   fields as a struct (equivalent to current auto-extraction behavior)
