# Column Type System — Constraint Analysis

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

## Questions to resolve

1. Should the typing strategy be per-input, per-pipeline, or global?

2. Should the user declare the typing mode in config, or should we
   infer it from the input type?

3. For JSON inputs specifically: should we offer both "everything is
   string" (CSV-like) and "detect types" (current behavior) modes?

4. When a field has multiple types and the user does `SELECT *`, what
   columns appear in the output? All typed variants? Just the string?

5. Should the output sink be responsible for collapsing multiple typed
   columns back into one field, or should the SQL transform do it?

6. For Elasticsearch output: if `status` is sometimes int and sometimes
   string, what do we send? The int (and drop the string rows)? The
   string (and lose type fidelity)? Both (and get a mapping conflict)?
