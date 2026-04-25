# Per-Input SQL: Design Analysis

> **Status:** Active  
> **Date:** 2026-04-06  
> **Context:** ffwd currently has one `SqlTransform` per pipeline. This analysis explores moving SQL to each input.  
> **Tracking:** Relates to [#1363](https://github.com/strawgate/fastforward/issues/1363)

---

## 1. Schema Unification Problem

### How it works today

All inputs in a pipeline share **one Scanner** and **one SqlTransform**. The flow:

```text
Input A ─bytes─┐
               ├─▶ BatchAccumulator ─▶ Scanner ─▶ SqlTransform ─▶ Outputs
Input B ─bytes─┘        (merge)        (shared)     (shared)
```

Raw bytes from all inputs are concatenated into a single `BytesMut` buffer in `BatchAccumulator`, then scanned together as **one RecordBatch**. The scanner unifies schemas automatically:

- **Missing fields** → null-padded (if Input A has `host` and Input B doesn't, B's rows get `host = NULL`)
- **Type conflicts** (same field, int in some rows, string in others) → emitted as `Struct { int: Int64, str: Utf8View }`, then normalized to `Utf8` by `normalize_conflict_columns()` before SQL execution
- **Schema evolution** → detected via `hash_schema()`; when the hash changes, the entire `SessionContext` is invalidated and rebuilt

This means today, if two inputs produce different JSON fields, they **already merge into a union schema** with nulls for missing columns. The SQL transform sees one wide table.

### What changes with per-input SQL

If each input has its own SQL, the transform runs **before** merging. Now there are two distinct output schemas:

| Input | SQL | Output Schema |
|-------|-----|---------------|
| A | `SELECT level, msg FROM logs` | `{level: Utf8, msg: Utf8}` |
| B | `SELECT timestamp, severity FROM logs` | `{timestamp: Utf8, severity: Utf8}` |

**What schema does the downstream step chain see?** Three options:

#### Option (a): All inputs must produce the same schema

Validated at config load time. Simple but restrictive — defeats the purpose of per-input SQL since inputs often have different fields. This is how **DataFusion MemTable** works internally: all RecordBatches in a MemTable must share one schema.

**Verdict:** Too restrictive. Reject.

#### Option (b): Union schema (missing columns = null)

The pipeline merges per-input RecordBatches into a union schema before passing to steps. This is exactly what the scanner does today for raw fields.

**Implementation:** After each input's SQL produces its batch, we'd need a `schema_union()` pass:
1. Collect all output schemas from all inputs
2. Build union schema (superset of all fields)
3. Pad each batch with null columns for fields it doesn't have
4. Downstream steps see the full union

**Arrow support:** `arrow::compute::concat_batches()` requires identical schemas, so we'd need to pad first. This is ~50 lines of code (iterate missing fields, append `NullArray` columns).

**Concern:** Schema discovery is now **lazy** — we don't know the union until all inputs have produced at least one batch. The first batch from Input A might flow through steps before Input B has started, meaning steps see a partial schema initially.

**Verdict:** Workable. This is the natural extension of what ffwd already does.

#### Option (c): Each input is an independent pipeline

Per-input SQL produces independent streams that are never merged. Steps run per-input.

**Verdict:** This is just multiple pipelines with a shared output. Already supported by config today (define two `pipelines:`). No new design needed — and it loses cross-input capabilities like dedup or aggregate.

### Recommendation

**Option (b)** — union schema with null padding. It's consistent with how the scanner already handles schema diversity, and it preserves the ability to do cross-input operations downstream.

---

## 2. Per-Input Scanner Instances

### Scanner architecture today

```rust
pub struct Scanner {
    builder: StreamingBuilder,  // Holds field index, per-field column state
    config: ScanConfig,         // wanted_fields, extract_all, line_field_name
}
```

The scanner is **stateful but reusable**: `StreamingBuilder` maintains a field index (`HashMap<Vec<u8>, usize>`) and per-field column builders. It's cleared between batches via `begin_batch()`. One scanner is reused across all batches in a pipeline.

### Can we have one Scanner per input?

**Yes, easily.** Scanner state is per-batch (cleared between scans), so N scanners is just N independent instances. The only shared state would be `ScanConfig`, which differs meaningfully per input when SQL differs.

### Memory cost of N scanners

Per-scanner baseline:
- `ScanConfig`: ~100 bytes (field specs + flags)
- `StreamingBuilder`: ~500 bytes base + per-field overhead
- Per-field: `HashMap` entry (~64 bytes) + column builder state (~256 bytes for `StringViewArray` builder)
- Typical 20-field JSON: ~6 KB per scanner

**For 10 inputs:** ~60 KB total. Negligible compared to batch buffers (4 MB default each).

### Why per-input scanners help

With per-input SQL, the `ScanConfig` **should differ per input**:

| Input | SQL | ScanConfig |
|-------|-----|-----------|
| App logs | `SELECT level, msg, trace_id FROM logs` | `wanted_fields: [level, msg, trace_id], extract_all: false` |
| Access logs | `SELECT * FROM logs WHERE status >= 400` | `extract_all: true` |

Today, `ScanConfig` is derived from the SQL transform's column analysis. If Input A references 3 columns and Input B uses `SELECT *`, the shared scanner must use `extract_all: true` for everything — **losing the pushdown optimization** for Input A.

Per-input scanners restore this: each scanner extracts only what its SQL needs.

### Recommendation

Per-input scanners are trivially cheap and unlock column pushdown. Do it.

---

## 3. How Other Collectors Handle Per-Input Transforms

### Fluent Bit

**Architecture:** Parsers are per-input. Filters are global (tag-matched). Processors are per-input.

```yaml
pipeline:
  inputs:
    - name: tail
      path: /var/log/app.log
      parser: json              # Per-input parsing
      processors:               # Per-input transforms
        logs:
          - name: sql
            query: "SELECT level, msg FROM STREAM;"
  filters:
    - name: grep               # Global filter, tag-matched
      match: '*'
      regex: level error
```

**Key insight:** Fluent Bit added processors (per-input) because global filters were a bottleneck — filters run on the main thread, while processors run on the input's thread. The SQL processor is explicitly per-input.

**Schema handling:** Fluent Bit uses MessagePack (schemaless), so there's no schema unification problem. Each record is an independent key-value map. Filters see heterogeneous records and handle missing keys gracefully (key-not-found → skip/null).

### Vector

**Architecture:** Full DAG. Sources, transforms, and sinks are named nodes connected by `inputs:` references.

```yaml
sources:
  app_logs:
    type: file
    include: [/var/log/app.log]

transforms:
  parse_app:
    type: remap
    inputs: [app_logs]          # Connected to specific source
    source: '. = parse_json!(.message)'

  filter_errors:
    type: filter
    inputs: [parse_app]         # Connected to parse_app output
    condition: '.level == "error"'

sinks:
  out:
    type: console
    inputs: [filter_errors]
```

**Key insight:** Vector's DAG model means each transform explicitly declares which sources (or other transforms) feed it. A transform receiving data from multiple sources sees a **union of their event schemas** — VRL handles this gracefully because events are dynamic JSON-like objects (no fixed schema).

**When a transform has multiple inputs:** VRL code must handle both schemas. Users typically use `exists()` checks or write transforms that only touch common fields.

### Filebeat

**Architecture:** Processors can be per-input or global.

```yaml
filebeat.inputs:
  - type: log
    paths: [/var/log/app.log]
    processors:                 # Per-input processors
      - decode_json_fields:
          fields: [message]
      - drop_fields:
          fields: [agent]

processors:                     # Global processors
  - add_host_metadata: ~
```

**Key insight:** Per-input processors run **before** global processors. The execution order is: input → per-input processors → global processors → output. This is exactly the model we're considering.

### Synthesis

All three collectors support per-input transforms:

| Collector | Per-Input | Global/Shared | Schema Model |
|-----------|-----------|---------------|-------------|
| Fluent Bit | Parsers + Processors | Filters (tag-matched) | Schemaless (MessagePack) |
| Vector | Any transform via DAG | N/A (DAG is explicit) | Schemaless (JSON-like events) |
| Filebeat | `processors:` on input | Top-level `processors:` | Schemaless (JSON events) |

**The common pattern:** Per-input transforms handle source-specific parsing/filtering. Global transforms handle cross-source operations. Schema is typically schemaless (JSON/MessagePack), avoiding the union problem entirely.

**ffwd's difference:** We use Arrow RecordBatches with fixed schemas. This makes per-input SQL more complex but also more powerful (columnar pushdown, vectorized execution).

---

## 4. Config Design Exploration

### Option A: `sql` field on input

```yaml
pipelines:
  default:
    inputs:
      - type: file
        path: /var/log/app/*.log
        format: json
        sql: SELECT level, msg, trace_id FROM logs WHERE level != 'DEBUG'

      - type: file
        path: /var/log/access/*.log
        format: json
        sql: |
          SELECT
            method, path, int(status) as status,
            float(duration) as duration_s
          FROM logs
          WHERE int(status) >= 400

    outputs:
      - type: otlp
        endpoint: http://collector:4317
```

| Criterion | Assessment |
|-----------|-----------|
| **Simplicity** | ✅ Excellent. One field, right where you'd expect it. |
| **Extensibility** | ⚠️ Limited. What if you need two SQL stages per input? |
| **Column pushdown** | ✅ Natural. Scanner for this input extracts only referenced columns. |
| **Enrichment scope** | ❓ Ambiguous. Does `sql` on Input A see the same enrichment tables as Input B? |
| **Reusability** | ❌ No reuse. If 5 inputs need the same SQL, it's duplicated 5 times. |

### Option B: Linked transform (named, reusable)

```yaml
pipelines:
  default:
    inputs:
      - type: file
        path: /var/log/app/*.log
        format: json
        transform: app_filter

      - type: file
        path: /var/log/access/*.log
        format: json
        transform: access_filter

    transforms:
      app_filter:
        sql: SELECT level, msg, trace_id FROM logs WHERE level != 'DEBUG'
      access_filter:
        sql: |
          SELECT method, path, int(status) as status
          FROM logs WHERE int(status) >= 400

    outputs:
      - type: otlp
        endpoint: http://collector:4317
```

| Criterion | Assessment |
|-----------|-----------|
| **Simplicity** | ⚠️ More config structure, but clear. |
| **Extensibility** | ✅ Transforms can grow (enrichment tables, UDFs per transform). |
| **Column pushdown** | ✅ Same — scanner derives config from transform's SQL. |
| **Enrichment scope** | ✅ Clear — enrichment can be per-transform. |
| **Reusability** | ✅ Multiple inputs can reference the same named transform. |

### Option C: Inline steps on input (OTel-style)

```yaml
pipelines:
  default:
    inputs:
      - type: file
        path: /var/log/app/*.log
        format: json
        steps:
          - type: sql
            query: SELECT level, msg FROM logs WHERE level != 'DEBUG'
          - type: add_field
            field: source
            value: app

      - type: file
        path: /var/log/access/*.log
        format: json
        steps:
          - type: sql
            query: SELECT * FROM logs WHERE int(status) >= 400

    outputs:
      - type: otlp
        endpoint: http://collector:4317
```

| Criterion | Assessment |
|-----------|-----------|
| **Simplicity** | ⚠️ More verbose but very explicit. |
| **Extensibility** | ✅✅ Best. Any step type can appear per-input. |
| **Column pushdown** | ⚠️ Harder — must analyze the full step chain to determine scanner config. |
| **Enrichment scope** | ✅ Enrichment could be a step type too. |
| **Reusability** | ❌ Same as Option A — no reuse without anchors/aliases. |

### Recommendation

**Option A** (`sql` field on input) for the common case, with the ability to also keep `transform` at the pipeline level for cross-input SQL.

Rationale:
1. **90% use case is simple filtering.** `sql: SELECT * FROM logs WHERE level != 'DEBUG'` doesn't need named transforms or step chains.
2. **Option B adds indirection** without proportional benefit. Named transforms are useful for reuse, but per-input SQL is rarely identical across inputs (that's the whole point of per-input).
3. **Option C is premature.** We don't have non-SQL step types today (everything is SQL). Adding a step chain for one step type is overengineering. If we later need per-input step chains, we can add `steps:` alongside `sql:`.

**Proposed config:**

```yaml
pipelines:
  default:
    inputs:
      - type: file
        path: /var/log/app/*.log
        sql: SELECT * FROM logs WHERE level != 'DEBUG'

      - type: file
        path: /var/log/access/*.log
        sql: SELECT method, path, int(status) as status FROM logs

    # Optional: pipeline-level SQL for cross-input operations
    transform: |
      SELECT *, geo_lookup(client_ip) as geo
      FROM logs

    enrichment:
      - type: geo_database
        format: mmdb
        path: /etc/ffwd/GeoIP2-City.mmdb

    outputs:
      - type: otlp
        endpoint: http://collector:4317
```

If an input has `sql:`, it runs **before** the pipeline-level `transform:`. If an input lacks `sql:`, it passes through raw (equivalent to `SELECT * FROM logs`).

---

## 5. What Changes in the Step Compiler

### Current state: there is no step compiler

Today's pipeline is a fixed linear chain (as of the original research; a
post-transform `Processor` chain was added in PR #1402):

```text
Inputs → Scanner → SqlTransform → [Processors...] → Outputs
```

The `SqlTransform` **is** the primary transform layer for user-visible SQL.
The post-SQL `Processor` chain is reserved for internal pipeline stages
(e.g. stateless sampling or enrichment steps) and is currently stateless-only
(stateful processors require deferred-ACK checkpointing; see #1404).

Historically (before PR #1402), there was no step chain, no step compiler, and
no step types; SQL was the only transform.

### If SQL moves to inputs, what does the pipeline-level transform contain?

**Cross-input operations** that can't be expressed per-input:

| Operation | Why pipeline-level |
|-----------|--------------------|
| Aggregate across inputs | `SELECT source, COUNT(*) FROM logs GROUP BY source` |
| Deduplicate across inputs | Same event from overlapping file paths |
| Enrich with global context | `JOIN host_info` that applies to all inputs |
| Rate limit | Global rate limiting across all sources |
| Schema normalization | Rename fields from different inputs to common names |

### Can we still have `sql` at the pipeline level?

**Yes, and we should.** The pipeline becomes:

```text
Input A ──Scanner A──SQL A──┐
                            ├──▶ union schema ──▶ Pipeline SQL ──▶ Outputs
Input B ──Scanner B──SQL B──┘
```

Per-input SQL handles: source-specific parsing, filtering, projection.  
Pipeline SQL handles: cross-source joins, global enrichment, aggregation.

### What about non-SQL step types?

Today, SQL covers everything. Future step types would only be needed for operations that SQL can't express efficiently:

- **`checkpoint`** — already handled by the pipeline state machine, not a transform
- **`tail_sampling`** — requires stateful, time-windowed decisions across traces; SQL is awkward for this
- **`rate_limit`** — requires a token bucket or similar; could be a UDF but a step is cleaner
- **`dedup`** — could be `SELECT DISTINCT` but needs cross-batch state that SQL doesn't have
- **`route`** — send different rows to different outputs; SQL can't express output routing

**Recommendation:** Keep the pipeline-level `transform:` as SQL. Add non-SQL step types only when there's a concrete need. Don't build a step compiler until there are at least 2-3 non-SQL steps that justify it.

---

## 6. The Enrichment Table Question

### Current design

Enrichment tables are registered on the single `SqlTransform`:

```rust
pub struct SqlTransform {
    enrichment_tables: Vec<Arc<dyn enrichment::EnrichmentTable>>,
    geo_database: Option<Arc<dyn enrichment::GeoDatabase>>,
    // ...
}
```

Each batch execution re-registers all enrichment tables as DataFusion MemTables:

```rust
for et in &self.enrichment_tables {
    let _ = ctx.deregister_table(et.name());
    if let Some(snapshot) = et.snapshot() {
        let table = MemTable::try_new(snapshot.schema(), vec![vec![snapshot]])?;
        ctx.register_table(et.name(), Arc::new(table))?;
    }
}
```

Enrichment tables are `Arc<dyn EnrichmentTable>` — reference-counted and cheap to share.

### With per-input SQL: three options

#### Option 1: Enrichment tables available to all SQL (per-input and pipeline)

Each per-input `SqlTransform` and the pipeline-level `SqlTransform` all register the same enrichment tables.

```yaml
pipelines:
  default:
    inputs:
      - type: file
        path: /var/log/app/*.log
        sql: SELECT *, env.dc FROM logs JOIN env  # Uses shared enrichment

    enrichment:
      - type: static
        table_name: env
        labels: { dc: us-east-1 }
```

**Pros:** Simple, consistent. Any SQL can reference any enrichment table.  
**Cons:** Per-input SqlTransforms each re-register all enrichment tables per batch (N inputs × M tables). But since `snapshot()` is just an `RwLock` read and `MemTable::try_new` is cheap, the overhead is minimal.

#### Option 2: Per-input enrichment tables

```yaml
pipelines:
  default:
    inputs:
      - type: file
        path: /var/log/app/*.log
        sql: SELECT * FROM logs JOIN app_meta
        enrichment:
          - type: csv_file
            table_name: app_meta
            path: /etc/ffwd/app_metadata.csv
```

**Pros:** Explicit scoping. Input A's enrichment tables don't pollute Input B's namespace.  
**Cons:** Config complexity. Most enrichment (host_info, k8s metadata, geo) is global.

#### Option 3: Scoped + shared (Filebeat model)

Per-input enrichment overrides/extends pipeline-level enrichment.

```yaml
pipelines:
  default:
    enrichment:  # Available to all SQL (per-input and pipeline)
      - type: static
        table_name: env
        labels: { dc: us-east-1 }

    inputs:
      - type: file
        path: /var/log/app/*.log
        sql: SELECT * FROM logs JOIN app_meta JOIN env
        enrichment:  # Additional, only for this input
          - type: csv_file
            table_name: app_meta
            path: /etc/ffwd/app_metadata.csv
```

### Recommendation

**Option 1** — shared enrichment, available everywhere. Reasons:

1. **Simplicity.** One `enrichment:` block at the pipeline level. No scoping rules to learn.
2. **Cost is negligible.** `Arc` sharing means each SqlTransform registers the same `Arc<dyn EnrichmentTable>` — no data duplication.
3. **Enrichment tables are inherently global.** Host info, K8s metadata, GeoIP — these apply to all inputs.
4. **Per-input enrichment is rare.** In the rare case where Input A needs a CSV lookup that Input B doesn't, registering it globally but only JOINing it in Input A's SQL achieves the same result with zero extra config.

If a user doesn't JOIN an enrichment table in their SQL, it's registered but never queried — zero runtime cost beyond the `MemTable::try_new` call (~1 μs).

---

## Summary: Recommended Design

### Architecture

```text
                    Per-Input Layer                    Pipeline Layer
              ┌─────────────────────────┐        ┌──────────────────┐
Input A ───▶  │ Scanner A → SQL A       │──┐     │                  │
              │ (own ScanConfig)        │  ├──▶  │ Union Schema     │──▶ Outputs
Input B ───▶  │ Scanner B → SQL B       │──┘     │ → Pipeline SQL   │
              │ (own ScanConfig)        │        │ → Enrichment JOINs│
              └─────────────────────────┘        └──────────────────┘
```

### Config shape

```yaml
pipelines:
  default:
    inputs:
      - type: file
        path: /var/log/app/*.log
        format: json
        sql: SELECT level, msg, trace_id FROM logs WHERE level != 'DEBUG'

      - type: file
        path: /var/log/access/*.log
        format: cri
        sql: SELECT method, path, int(status) as status FROM logs

    # Optional pipeline-level SQL for cross-input transforms
    transform: |
      SELECT *, geo_lookup(client_ip) as geo
      FROM logs
      JOIN host_info

    enrichment:
      - type: host_info
      - type: geo_database
        format: mmdb
        path: /etc/ffwd/GeoIP2-City.mmdb

    outputs:
      - type: otlp
        endpoint: http://collector:4317
```

### What works well

1. **Column pushdown per input.** Per-input ScanConfig means Scanner A only extracts `level`, `msg`, `trace_id` — massive win for wide JSON with 50+ fields.
2. **Early filtering.** `WHERE level != 'DEBUG'` runs before batches are merged, reducing data volume early.
3. **Source-specific transforms.** CRI inputs can parse CRI-specific fields; JSON inputs can do JSON-specific operations.
4. **Consistent with industry.** Fluent Bit, Vector, and Filebeat all support per-input transforms.
5. **Backward compatible.** If no `sql:` on input, behavior is identical to today (`SELECT * FROM logs`).

### What problems it introduces

1. **Two SQL execution points.** Users must understand when to use per-input SQL vs pipeline SQL. Rule of thumb: "per-input for filtering/projection, pipeline for enrichment/aggregation."
2. **Schema union complexity.** The pipeline must compute a union schema from heterogeneous per-input outputs. This is new code but straightforward (~100 lines).
3. **Per-input SqlTransform instances.** N inputs → N `SessionContext` instances → N DataFusion plan caches. Memory cost: ~10 KB per SqlTransform × N inputs. Negligible for typical N (2-10).
4. **Enrichment table re-registration.** Each per-input SqlTransform registers all enrichment tables per batch. Cost: N × M × ~1 μs. For 10 inputs × 5 tables = 50 μs per batch cycle. Negligible vs batch processing time (~1 ms).
5. **Testing surface.** Need tests for: per-input SQL + pipeline SQL interaction, schema union edge cases, enrichment visibility, config validation.

### Surprises and concerns

1. **No step compiler exists.** The codebase has no step chain — SQL is the only transform. This makes per-input SQL simpler to implement (no step ordering to worry about) but means we're designing the first "two-layer transform" architecture.
2. **BatchAccumulator needs rework.** Today, all input bytes are concatenated before scanning. Per-input SQL requires scanning per-input, then merging RecordBatches (not bytes). The accumulator must buffer per-input and trigger scanning independently.
3. **Checkpoint tracking changes.** Today, checkpoints are tracked in the shared accumulator. With per-input scanning, checkpoints must be tracked per-input (which is actually simpler and more correct).
4. **`SELECT *` interaction.** If per-input SQL uses `SELECT * FROM logs` and pipeline SQL also uses `SELECT * FROM logs`, the pipeline SQL sees the full scanner output (no pushdown benefit). Users should be guided to use explicit columns in per-input SQL.
5. **Error handling.** If Input A's SQL fails (bad query, type error), should the batch from Input A be dropped? Passed through raw? Today there's one error path; per-input SQL adds N error paths.

### Implementation order

1. **Add `sql: Option<String>` to `InputConfig`** — config change only
2. **Create per-input Scanner instances** — derive ScanConfig from per-input SQL
3. **Move scanning from shared accumulator to per-input** — each input scans its own bytes
4. **Add per-input SqlTransform** — execute per-input SQL after scanning
5. **Implement schema union** — merge per-input RecordBatches into union schema
6. **Wire pipeline-level SqlTransform** — runs on the union RecordBatch
7. **Register enrichment tables on all SqlTransforms** — shared Arc references
8. **Validation and testing** — schema mismatch tests, multi-input compliance tests
