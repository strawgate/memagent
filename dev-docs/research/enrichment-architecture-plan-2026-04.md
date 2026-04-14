# Enrichment Architecture Plan

> **Status:** Active
> **Date:** 2026-04-12
> **Context:** Evaluate enrichment architecture for mixed local and live lookups, prototype-friendly experiments, and a phased implementation plan.

## Summary

The current pipeline already supports two enrichment shapes:

- SQL-visible snapshot data via enrichment tables and UDFs
- post-transform processors with synchronous contracts and room for internal background tasks

That makes the architecture decision clearer than it first appears:

- **pure local, deterministic, snapshot-backed enrichment** belongs in SQL
- **live, remotely refreshed, rate-limited, or failure-prone enrichment** belongs in a dedicated enrichment processor stage, not in SQL

The near-term goal should not be "pick one enrichment system for everything." It should be to separate enrichment into two execution classes and run focused experiments against both.

## Existing Surfaces

### Snapshot enrichment already in SQL

Current pipeline build wires enrichment into every `SqlTransform` at pipeline construction time.

- Geo database is attached as a UDF backend: [crates/logfwd-runtime/src/pipeline/build.rs](../../crates/logfwd-runtime/src/pipeline/build.rs)
- Enrichment tables are registered as DataFusion MemTables: [crates/logfwd-transform/src/sql_transform.rs](../../crates/logfwd-transform/src/sql_transform.rs)
- Geo lookup UDF is synchronous and per-row: [crates/logfwd-transform/src/udf/geo_lookup.rs](../../crates/logfwd-transform/src/udf/geo_lookup.rs)
- Enrichment tables expose snapshots behind `Arc<RwLock<Option<RecordBatch>>>`: [crates/logfwd-transform/src/enrichment.rs](../../crates/logfwd-transform/src/enrichment.rs)

### A processor seam already exists for non-SQL work

The `Processor` contract is synchronous, but it explicitly allows internal async work via background tasks and channels.

- Processor contract: [crates/logfwd-runtime/src/processor/mod.rs](../../crates/logfwd-runtime/src/processor/mod.rs)

This is the most important design fact for future live enrichment. We do not need to force async into DataFusion. We already have a pipeline seam that can host an async-backed enrichment stage while preserving batch-level pipeline semantics.

## Enrichment Categories We Should Plan For

### 1. Pure local snapshot lookups

Properties:

- loaded from disk or config
- bounded lookup cost
- no network I/O
- deterministic for a given snapshot
- safe to use in `WHERE`, projection, and grouping

Examples:

- MMDB geo lookup
- DB-IP Lite or other CSV range-backed geo/ASN database
- static labels
- host info
- K8s path metadata
- CSV/JSONL reference tables
- local ASN or threat-list snapshots

**Recommended execution surface:** SQL UDF or SQL JOIN table

### 2. Reloadable local snapshot lookups

Properties:

- still local and deterministic per snapshot
- need periodic reload / swap semantics
- stale-vs-fresh semantics matter operationally
- should never block the hot path during reload

Examples:

- geo database with `refresh_interval`
- reloaded CSV/JSONL watchlists
- rotating asset inventory snapshot
- refreshed ASN mapping files

**Recommended execution surface:** still SQL-visible, but backed by atomic snapshot swap outside DataFusion planning

### 3. Live cached service lookups

Properties:

- require network I/O
- can fail, timeout, retry, throttle, and partially succeed
- usually need caching and batching
- should expose enrichment status and fallback behavior explicitly

Examples:

- customer/account metadata from a service
- IP reputation / threat intel API
- CMDB / asset inventory API
- user directory / org lookup service
- feature-flag or tenant policy service
- live geo provider API

**Recommended execution surface:** async enrichment processor stage before SQL

### 4. Stateful/derived enrichments

Properties:

- depend on prior batches, timers, or local state
- may need heartbeats and flush semantics
- not naturally modeled as SQL UDFs

Examples:

- trace/session correlation
- dedupe / suppression
- rolling counters / rate buckets
- delayed join against pending side-channel data

**Recommended execution surface:** processor stage only

## Core Decision

We should not design "enrichment" as a single mechanism.

We should define two first-class execution classes:

1. `snapshot enrichment`
2. `live enrichment`

### Snapshot enrichment

Characteristics:

- SQL-safe
- deterministic over a single snapshot
- may be exposed as UDFs or joinable tables
- can be replayed with clear stale/fresh semantics

### Live enrichment

Characteristics:

- SQL-unsafe
- async-backed
- cache-aware
- must surface operational policy explicitly
- materializes columns before SQL runs

This split matches current code reality better than forcing everything through either DataFusion or processors alone.

## Experiment Matrix

The right next step is to prototype a small number of enrichment archetypes, not to prematurely generalize every future use case.

| Experiment | Enrichment type | Execution surface | Main question | Success criteria |
|---|---|---|---|---|
| A | MMDB geo with reload | SQL UDF + atomic snapshot swap | Can we preserve current SQL ergonomics while implementing reload safely? | no blocking in lookup path, explicit reload metrics, deterministic tests |
| B | CSV range geo/ASN | SQL table or SQL-safe lookup backend | Do we want a non-MaxMind open backend without changing the model? | acceptable lookup cost, acceptable memory, simple config parity |
| C | Static/local metadata join | Enrichment table JOIN | Are table-based enrichments enough for most local metadata? | cheap registration, stable schemas, straightforward docs |
| D | Live HTTP enrichment with cache | Processor stage | What batch/concurrency/cache shape fits the pipeline contract? | bounded latency impact, explicit miss/error columns, no async leakage into SQL |
| E | Replay semantics comparison | SQL snapshot vs live processor | How do pre/post-transform queues interact with enrichment freshness? | documented guarantees and queue-mode-specific behavior |

## Recommended Experiments

### Experiment A: Reloadable MMDB without changing the UDF model

Goal:

- keep `geo_lookup()` in SQL
- move reload responsibility outside UDF execution
- atomically swap current database snapshot

Sketch:

- change `GeoDatabase` backing from fixed `Arc<MmdbDatabase>` to a reloadable shared handle
- background task watches file timestamp or timer
- load new DB off-path
- swap pointer only after successful load
- lookups always see a complete old or complete new snapshot

Questions to answer:

- what is the cheapest concurrency primitive for read-mostly lookups?
- how do we expose generation/version and reload failures to diagnostics?
- what should happen if the DB disappears or reload parse fails?

Why this experiment matters:

It solves the current contract drift around `refresh_interval` without dragging async behavior into SQL execution.

### Experiment B: CSV/IP-range backend for open GeoIP/ASN data

Goal:

- support a non-MaxMind local backend such as DB-IP Lite
- test whether a second local backend still fits the SQL-safe enrichment model

Sketch:

- extend `GeoDatabaseFormat`
- build a new local backend with preprocessed ranges or interval search
- present same `GeoDatabase` trait to UDF layer

Questions to answer:

- do we keep one logical `geo_lookup()` result schema across backends?
- do we split geo and ASN into separate backends or one composite config?
- is lookup cost acceptable at target throughput?

Why this experiment matters:

It tells us whether backend diversity is just an implementation detail or whether the current trait/schema is too tightly shaped around MMDB.

### Experiment C: Generic async enrichment processor

Goal:

- prove the processor seam can host live enrichers without forcing SQL or output layers to handle network semantics

Sketch:

- define a processor that reads configured source columns from each `RecordBatch`
- deduplicates keys within batch
- checks local cache
- batches misses to a background worker pool
- materializes enrichment columns plus status columns back into the batch

Minimum output columns:

- `<prefix>.*` enrichment result columns
- `<prefix>_status` with values like `hit`, `miss`, `timeout`, `error`, `partial`
- optional `<prefix>_generation` or `<prefix>_source`

Questions to answer:

- do we block batch progression until misses resolve, or emit partial columns with miss status?
- what retry model belongs here versus upstream service behavior?
- how do checkpoint semantics work if enrichment fails transiently?

Why this experiment matters:

If this shape works, we can safely support future remote enrichers without turning SQL into an I/O runtime.

## Proposed Architectural Rules

### Rule 1: No live I/O in SQL

SQL transforms may use:

- UDFs over local snapshots
- joins against local snapshot tables
- pure transforms over already-materialized columns

SQL transforms may not own:

- network calls
- retry logic
- service throttling
- request batching
- cache fill behavior

### Rule 2: Live enrichment must materialize explicit status

Every live enricher should produce both value columns and operational status columns.

This avoids silent NULLs that are ambiguous between:

- no match
- timeout
- upstream failure
- throttling
- temporary cache miss

### Rule 3: Reloadable local data must remain snapshot-safe

Reloaded local enrichment should never expose partial reload state. The hot path should only read complete snapshots.

### Rule 4: Queue semantics must document enrichment freshness

The current persistence docs already call out that enrichment can be query-time or ingest-time dependent in different queue modes.

- [book/src/content/docs/architecture/pipeline.md](../../book/src/content/docs/architecture/pipeline.md)

Future live enrichment must define whether it is:

- replay-time enrichment
- ingest-time baked enrichment
- or unsupported in certain queue modes

## Candidate Trait Split

A useful target model is:

```rust
trait SnapshotEnricher: Send + Sync {
    fn name(&self) -> &str;
}

trait SnapshotLookup: Send + Sync {
    type Output;
    fn lookup(&self, key: &str) -> Option<Self::Output>;
}

trait LiveEnricher: Send + Sync {
    fn name(&self) -> &str;
    fn enrich_batch(&mut self, batch: RecordBatch, meta: &BatchMetadata)
        -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError>;
}
```

This does not need to be implemented literally as-is, but it captures the key architectural split:

- snapshot enrichers compose into SQL
- live enrichers compose into processors

## Phased Plan

### Phase 1: Stabilize snapshot enrichment model

Scope:

- document snapshot-vs-live split
- fix `geo_database.refresh_interval` contract drift
- decide whether to reject unsupported reload or implement it
- prototype reloadable MMDB handle

Deliverables:

- design doc in `dev-docs/`
- issue tree for snapshot reload + open backend support
- tests for reload semantics
- docs updates for queue freshness guarantees

### Phase 2: Add second local backend

Scope:

- choose DB-IP Lite or equivalent CSV range backend
- preserve `geo_lookup()` result schema when possible
- benchmark lookup cost and memory

Deliverables:

- backend implementation
- config validation
- perf note
- fallback/unsupported feature documentation

### Phase 3: Prototype live enrichment processor

Scope:

- build one real live enrichment example
- add cache, batching, concurrency cap, metrics, and status columns
- decide checkpoint/error policy before broadening API

Recommended first live prototype:

- a simple HTTP-backed key lookup with small bounded response schema

Deliverables:

- one processor implementation
- one diagnostics panel/metric story
- one replay/queue semantics note

## What We Should Not Do Yet

- do not generalize all enrichment behind a single magical abstraction now
- do not move current MMDB geo lookup out of SQL prematurely
- do not add live network-backed UDFs to DataFusion
- do not support both SQL and processor implementations of the same enrichment until we have a proven need

## Recommended Near-Term Decision

1. Treat current geo/MMDB as **snapshot enrichment**.
2. Fix reload semantics as a snapshot-management problem, not a SQL problem.
3. Add one non-MaxMind local backend experiment.
4. Prototype one live enrichment processor separately.
5. Document the split as a first-class architecture rule.

That path keeps current value, avoids forcing async into SQL, and gives us a clean growth path for the larger enrichment roadmap.
