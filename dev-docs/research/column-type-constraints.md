# Column Type System — Constraint Analysis

> **Status:** Historical
> **Date:** 2026-03
> **Context:** Constraint analysis for column type handling across input/output formats.

Context: #445. This document enumerates all constraints before choosing a design.

## Actors and their expectations

### 1. Input sources (producers)

| Input | What it produces | Type info |
|---|---|---|
| JSON (NDJSON) | Per-value types, can vary across rows | `status: 200` (int), `status: "OK"` (str) in same batch |
| CSV / raw / syslog | Everything is a string | No type info at all |
| OTLP / Arrow IPC | Per-column types, fixed schema | `status` is Int64. No per-row variance. |
| CRI | Wraps one of the above | Depends on inner format |

### 2. SQL transform (consumer + producer)

- Receives a RecordBatch, runs user SQL, produces a RecordBatch
- The user's SQL is fixed at config time
- The batch schema can vary between batches
- Column names referenced in SQL must exist in every batch
- DataFusion is strict: column not found = error, type mismatch = error
- `SELECT *` must work and should round-trip data faithfully

### 3. Output sinks (consumers)

| Output | What it expects | Name sensitivity | Type sensitivity |
|---|---|---|---|
| JSON (NDJSON) | Bare field names, native JSON types | `"status": 200` not `"status_int": 200` | Int→number, Str→quoted string |
| OTLP | Attribute names, typed values | Bare names | Int→IntValue, Str→StringValue |
| Elasticsearch | Field names match index mapping | Bare names expected | Mapping defines types per field |
| Stdout/console | Human-readable | Bare names preferred | Display formatting |
| Null sink | Doesn't care | N/A | N/A |

### 4. Downstream systems (ultimate consumers)

| System | Schema expectation |
|---|---|
| Elasticsearch | Mapping is per-index. A field is one type. Mixed types → mapping conflict. |
| Grafana Loki | Labels are strings. No type enforcement. |
| ClickHouse | Column types fixed at table creation. |
| S3 / Parquet | Schema fixed per file. |
| Another logfwd | Accepts whatever schema it receives. |

## Constraints

### C1: Round-trip fidelity
A field that enters as int 200 must exit as int 200 (not string "200").
`SELECT * FROM logs` with no SQL modification must preserve types.
This means the internal representation must track per-row types.

### C2: Output uses bare field names
JSON output must produce `"status": 200`, not `"status_int": 200`.
OTLP must produce attribute name `status`, not `status_int`.
Output sinks must strip any internal naming convention.

### C3: Schema stability
The user's SQL is fixed. Column names must not change between batches.
A column that exists in batch N must exist in batch N+1 (possibly null).
Column names must not flip between bare and suffixed based on batch content.

### C4: SQL must be natural for clean-schema inputs
An OTLP pipeline where `status` is always Int64 should let the user write
`WHERE status > 400`. Forcing `WHERE status_int > 400` is unacceptable
when there are no type conflicts.

### C5: SQL must work for mixed-type inputs
A JSON pipeline where `status` is sometimes int, sometimes string must
let the user access both types. The user needs a way to say "give me the
integer version" or "give me the string version."

### C6: Mixed pipelines work
A single pipeline can have JSON + OTLP + CSV inputs. The internal
representation and SQL interface must handle all input types.

### C7: Downstream schema compatibility
The output field names and types must be stable enough for downstream
systems (Elasticsearch mappings, Grafana dashboards, Parquet schemas).
Changing field names based on batch content breaks downstream systems.

### C8: Configuration determines behavior, not data
The user should be able to predict the schema from the config, not from
the data content of a particular batch. "My field is called X and has
type Y" should be knowable at config time.

## Tensions

### C1 vs C4: Type fidelity vs clean SQL
Preserving per-row types requires multiple columns per field (int + str).
Clean SQL requires one column per field. These are in direct tension.

### C3 vs per-batch conflict detection
If we suffix only on conflict, column names change based on data content,
violating C3. If we always suffix, we violate C4.

### C4 vs C6: Clean SQL vs mixed pipelines
OTLP wants bare names. JSON with mixed types needs suffixed names. A
pipeline with both inputs can't have both.

### C8 vs data-driven schemas
Per-batch conflict detection means the config doesn't determine the
schema — the data does. This violates C8 and makes debugging hard.

## Questions resolved

**1. Typing strategy: per-input (reader-level), inferred from format.**
The scanner/reader decides. JSON gets conflict detection because it has no
schema. Protobuf/Avro/Parquet/OTLP never produce conflicts because their
schemas are fixed. This is the only place where it can be decided —
the reader sees the raw bytes and knows the format.

**2. Config declares type hints; reader infers by default.**
The reader infers by default (type conflicts produce suffixed columns).
Config can declare `schema: { status: int }` to pin a field type at scan
time, eliminating conflict columns. This satisfies C8 for teams with
known-type fields. Default behavior (no hint) is data-driven and
satisfies C1 (per-batch type fidelity) only. C3 (cross-batch schema
stability) is NOT guaranteed by default: a field that is always-int in one
batch gets a bare `status: Int64` column, but in a later batch where it
conflicts it becomes `status__int` + `status__str`. C3 is only satisfied
when type hints are pinned via config, or via the query-scoped Utf8 view
approach planned in #625.

The #625 design satisfies C3 without requiring config: for each column the
user's SQL references, the `TableProvider` advertises it as `Utf8` in the
schema it presents to DataFusion (stable name, stable type across all
batches). The `AnalyzerRule` then rewrites accesses at plan time —
`status` reads the Utf8 view; `CAST(status AS BIGINT)` is rewritten to
read `status__int` directly, bypassing the string round-trip. Columns the
query never references get no view synthesized at all. The current
`normalize_conflict_columns` is a batch-level approximation that covers
conflict batches but not clean ones; #625 replaces it with this
query-scoped approach.

**3. JSON offers detect-types mode only (current behavior).**
"Everything is string" mode is not worth the complexity — CSV/syslog
inputs already behave that way and use bare string columns natively.
A JSON user who wants strings can use `SELECT CAST(status AS VARCHAR)`.

**4. `SELECT *` emits bare names, correct types per row.**
The output layer uses `ConflictGroups` to skip typed variant columns
(`status__int`, `status__str`) and the synthesized SQL column (`status:
Utf8`), and instead emits one `status` field per row with the correct
type from `typed_value_for_row`. `SELECT *` round-trips correctly.

**5. Output sink is responsible for collapsing.**
The SQL transform synthesizes a bare `status: Utf8` column for SQL
resolution only. Output sinks use `ConflictGroups` to collapse typed
variants back into a single correctly-typed field per row. The SQL
transform must not be used to collapse — it would lose type fidelity.

**6. Elasticsearch output: emit per-row type, accept mapping conflict.**
Send `status: 200` (int) for integer rows and `status: "error"` (string)
for string rows. ES will reject the conflicting-type documents unless
`ignore_malformed: true` is set. This is an ES limitation that the user
must configure. Our job is fidelity; downstream schema enforcement is
the user's responsibility. Document in sink config: "ES mappings must
accommodate the field types your pipeline produces."
