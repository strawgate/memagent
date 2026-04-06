# Source Metadata Prototype Options

**Status:** Active investigation  
**Date:** 2026-04-06  
**Owner:** Codex

## Goal

Prototype a few viable ways to expose source metadata to SQL so we can:

- join logs to Kubernetes path enrichment cleanly;
- validate benchmark and e2e scenarios by true source identity;
- let users answer "which file/pod/container did this row come from?" without sink-side reconstruction.

This note compares three concrete designs against the current `memagent` pipeline.

## Current State

Today the collector already knows a lot of source metadata before SQL:

- `FileTailer` tracks both a stable `SourceId` and a canonical `PathBuf` per tailed file in [tail.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-io/src/tail.rs).
- `TailEvent::Data` includes both `path` and `source_id` at emission time in [tail.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-io/src/tail.rs).
- `InputEvent::Data` keeps only `bytes` and `source_id`, dropping `path`, in [input.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-io/src/input.rs).
- `FramedInput` preserves `source_id` per emitted chunk, but still only returns bytes plus `source_id`, in [framed.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-io/src/framed.rs).
- `BatchAccumulator` then merges bytes from multiple sources into one buffer while only retaining latest checkpoint offsets per `SourceId`, in [batch_accumulator.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd/src/batch_accumulator.rs).
- The scanner sees only NDJSON bytes. SQL sees only columns that were already present in those rows, in [pipeline.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd/src/pipeline.rs) and [SCANNER_CONTRACT.md](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/dev-docs/SCANNER_CONTRACT.md).

So the actual gap is:

1. We drop `path` before SQL.
2. We preserve `source_id`, but not in row form.
3. Once chunks from multiple sources are coalesced, we no longer know which output row belongs to which source unless we injected metadata earlier.

## Requirements

A good design should:

- support file and CRI inputs first;
- preserve correctness across rotation and pod churn;
- work when a batch contains rows from multiple sources;
- be usable from SQL without magical hidden state;
- leave room for non-file inputs later;
- not require sink-side reconstruction to understand the source.

Nice-to-haves:

- low hot-path overhead;
- simple user experience for Kubernetes joins;
- stable identity plus human-readable path;
- minimal scanner contract churn.

## Existing Enrichment Surface

The current `k8s_path` enrichment already expects a path-like join key:

```sql
SELECT l.level, l.message, k.namespace, k.pod_name, k.container_name
FROM logs l
JOIN k8s k ON l._file_str = k.log_path_prefix
```

That example is currently aspirational in [reference.md](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/book/src/config/reference.md), because `_file_str` does not actually exist in rows today.

The enrichment implementation itself is real:

- `K8sPathTable` can build a lookup table from CRI log paths in [enrichment.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-transform/src/enrichment.rs).
- `SqlTransform` can register dynamic enrichment tables per batch in [lib.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-transform/src/lib.rs).

That makes enrichment-based prototypes realistic, not hypothetical.

## Prior Art

Other collectors tend to expose source metadata directly on the event, not only out-of-band:

- Vector's `file` source documents event context keys like `file`, `host`, `container_name`, and `pod_name`, and its file source configuration exposes `file_key` and `offset_key`. Source: [Vector file source docs](https://vector.dev/docs/reference/configuration/sources/file/).
- Fluent Bit's `tail` input exposes `path_key` and `offset_key`, and its docs describe those as keys appended to each record. Source: [Fluent Bit tail input docs](https://docs.fluentbit.io/manual/data-pipeline/inputs/tail).
- OpenTelemetry Collector's `filelog` receiver commonly uses `include_file_path: true` in Kubernetes log collection examples, which means file path is carried as record attributes before later parsing/enrichment. One example appears in the upstream Kubernetes logs discussion: [open-telemetry/opentelemetry-collector-contrib#25251](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/25251).

The common theme is:

- stable file identity for checkpointing lives in the collector internals;
- human-meaningful path/host/container metadata is attached to events early;
- downstream parsing and enrichment operate on that attached metadata.

That makes a pure "metadata only exists outside the row/event" design unusual from a user-experience standpoint.

## Prototype A: Direct Row Columns

### Idea

Inject source metadata directly into each emitted JSON row before the scanner sees it.

For file-backed rows, produce fields like:

- `_source_id`
- `_source_path`
- `_input`

Examples:

```json
{"_source_id":"1432949071","_source_path":"/var/log/pods/ns_pod_uid/c/0.log","_input":"pods","message":"..."}
```

or for line/raw-style inputs:

```json
{"_source_id":"1432949071","_source_path":"/var/log/nginx/access.log","_input":"nginx","_raw":"127.0.0.1 ..."}
```

### Where it fits

- keep `path` on `InputEvent::Data`
- pass source context through `FramedInput`
- inject metadata in the format layer before bytes are accumulated together

### Pros

- best SQL ergonomics: `SELECT _source_path, * FROM logs`
- easiest mental model for users
- `k8s_path` joins become straightforward
- no DataFusion join required for basic source visibility

### Cons

- metadata injection must happen before multi-source coalescing
- mutates every row in the hot path
- risks reserved-field collisions with user data
- path is human-friendly but not stable across rotation/rename
- for JSON inputs, injection means rewriting valid JSON objects rather than pure passthrough

### Assessment

This is the simplest user-facing model, but it is not the simplest implementation. It forces row rewriting on every file-backed event and makes the format layer responsible for a larger reserved-column contract.

## Prototype B: Hybrid `_source_id` Column + Source Enrichment Table

### Idea

Expose only stable identity directly in rows, then join the rest from a live enrichment table.

Rows get:

- `_source_id`
- maybe `_input`

A dynamic enrichment table, for example `sources`, provides:

- `source_id`
- `source_path`
- `input_name`
- maybe `source_kind`

SQL:

```sql
SELECT l.message, s.source_path, s.input_name
FROM logs l
LEFT JOIN sources s ON l._source_id = s.source_id
```

Kubernetes then has two sub-options:

1. keep `k8s_path` keyed by path:
   `JOIN k8s k ON s.source_path LIKE concat(k.log_path_prefix, '%')`
2. add a `k8s_sources` enrichment keyed by `source_id` directly:
   `JOIN k8s_sources k ON l._source_id = k.source_id`

### Where it fits

- inject `_source_id` in the format layer before coalescing
- keep a live source table from `FileTailer::file_paths()`
- refresh the enrichment snapshot when tailed files change

### Pros

- keeps direct row metadata minimal and stable
- stable identity survives path churn better than `_source_path`
- enrichment can carry more metadata without widening every row
- fits the existing dynamic enrichment model well
- opens the door to source metadata for TCP/UDP too, not just files

### Cons

- still requires one row-level injected field: `_source_id`
- users need a join for the path unless we later also expose `_source_path`
- string-vs-integer type choice for `source_id` needs to be pinned early
- DataFusion join cost becomes part of the story for enriched queries

### Assessment

This is the cleanest architectural fit. It respects that stable identity and descriptive metadata are different things. It also gives us a natural place to hang future source metadata without bloating all rows.

## Prototype C: Full Source Enrichment by Path Only

### Idea

Do not add new reserved row columns except maybe `_source_path`, then rely on enrichment for everything else.

SQL:

```sql
SELECT l.message, k.namespace, k.pod_name
FROM logs l
LEFT JOIN k8s k ON l._source_path LIKE concat(k.log_path_prefix, '%')
```

### Pros

- easiest for the current `k8s_path` table shape
- users see a path directly
- no new stable ID type contract needed in SQL

### Cons

- `_source_path` is not a stable identity
- path-only joins are brittle under rename/churn
- non-file inputs have no obvious equivalent
- still requires hot-path row injection, so it keeps the hardest part of Prototype A without getting a stable row identity

### Assessment

This is the weakest long-term option. If we are going to do row injection work at all, path-only does not buy enough.

## Prototype D: Source-Partitioned Batches + Batch Metadata

### Idea

Instead of injecting source metadata into each row, keep accumulation partitioned by source.

Then each flushed sub-batch has a single source, which means:

- `BatchMetadata.resource_attrs` could carry source-level attributes;
- output formats like OTLP could model source metadata as resource attributes naturally;
- a dynamic enrichment table could still exist for SQL if the transform executed per-source batch.

### Where it fits

- replace `BatchAccumulator`'s single merged buffer with per-source buffers;
- scan/transform/output one source-partitioned sub-batch at a time;
- optionally promote source metadata into `BatchMetadata` rather than row columns

### Pros

- aligns nicely with OTLP resource semantics
- no need to rewrite each row just to add metadata
- preserves a very clean notion of "this batch came from this source"
- avoids mixed-source ambiguity entirely

### Cons

- fights the current throughput strategy of cross-source coalescing into large batches
- can dramatically increase the number of scanner/DataFusion executions
- hurts fairness/load behavior when many low-volume sources are active
- makes cross-source SQL behavior more fragmented in time
- still does not help SQL joins unless we also expose source metadata inside the queryable row/table surface

### Assessment

This is the most architecturally elegant option, but probably the worst trade for the current performance model. It only becomes compelling if we decide source-partitioned batching is a goal in its own right.

## Comparison

| Criterion | A: Direct row columns | B: `_source_id` + enrichment | C: Path-only enrichment | D: Source-partitioned batches |
|---|---|---|---|---|
| User ergonomics | best | good | good | weak |
| Stable identity | good | best | weak | best |
| Hot-path overhead | highest | medium | high | medium |
| K8s join fit | good | good | good | medium |
| Rotation/churn behavior | medium | best | weak | best |
| Fit for TCP/UDP later | medium | best | weak | medium |
| Contract clarity | medium | best | medium | medium |
| Fit for current batching model | medium | best | medium | weak |

## Measured POCs

Research benches added in this investigation branch:

- [source_metadata_bench.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-io/tests/source_metadata_bench.rs)
- [source_metadata_table_bench.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-transform/tests/it/source_metadata_table_bench.rs)

### Ingest-side row injection

Command:

```bash
cargo test --release -p logfwd-io --test source_metadata_bench \
  source_metadata_injection_benchmark -- --ignored --nocapture
```

Observed results on this machine:

| Case | Median time | Approx throughput |
|---|---:|---:|
| baseline passthrough (1 MiB) | `0.022 ms` | `~45.0 GiB/s` |
| inject `_source_id` (1 MiB) | `0.167 ms` | `~5.8 GiB/s` |
| inject `id+path+input` (1 MiB) | `0.249 ms` | `~3.9 GiB/s` |
| baseline passthrough (4 MiB) | `0.778 ms` | `~5.0 GiB/s` |
| inject `_source_id` (4 MiB) | `0.876 ms` | `~4.5 GiB/s` |
| inject `id+path+input` (4 MiB) | `1.104 ms` | `~3.5 GiB/s` |

Interpretation:

- `_source_id`-only injection on a realistic 4 MiB batch was only about **13-15% slower** than pure passthrough in this narrow POC.
- injecting `_source_id + _source_path + _input` was more like **40% slower** on the same batch.
- the fixed-cost story is much worse on tiny chunks, which matters for low-volume or highly fragmented sources.

This makes the hybrid design look better than the direct full-path row design:

- a stable row key appears affordable;
- a full path-heavy row contract is materially more expensive on the hot path.

### Enrichment-side snapshot construction

Command:

```bash
cargo test --release -p logfwd-transform \
  source_metadata_snapshot_benchmark -- --ignored --nocapture
```

This bench was added but was still compiling during the current investigation pass, so the ingest-side measurements above are the only completed numbers so far. The important thing we will learn from this second bench is whether a live `sources` table with optional parsed Kubernetes columns is clearly cheap enough to treat as a cold-path refresh.

### Source-partitioned batch-size model

Prototype D looks weaker after a quick size model based on the measured 4 MiB POC payload shape above:

- measured line size was about `123.7 bytes/line`
- current default flush timeout is `100 ms`
- with `30` active pod sources:

| EPS per source | Source-partitioned batch size per 100 ms | Cross-source combined batch size per 100 ms |
|---|---:|---:|
| `100` | `~1.2 KiB` | `~36 KiB` |
| `300` | `~3.7 KiB` | `~109 KiB` |
| `600` | `~7.2 KiB` | `~217 KiB` |

So even before we measure actual scanner/DataFusion overhead, source-partitioned batching implies much smaller effective batches under the current timeout-driven pipeline. That reinforces the concern that Prototype D would trade away a lot of batching efficiency just to keep metadata out of rows.

## Recommendation

Prototype **B** first:

- add `_source_id` as a reserved row column for source-aware inputs;
- optionally add `_input` if we want input-name visibility without a join;
- build a dynamic `sources` enrichment table keyed by `source_id`;
- add a Kubernetes enrichment variant keyed by `source_id`, or derive it from the `sources` snapshot.

Why this is the best first prototype:

- it solves the stable identity problem directly;
- it avoids forcing `_source_path` into every row as the primary contract;
- it fits the current `SqlTransform` enrichment model;
- it preserves the current cross-source batching strategy;
- it leaves us free to add `_source_path` later if we want a convenience column for users.

## Suggested Prototype Plan

### Prototype B1

Minimal viable hybrid:

1. Extend `InputEvent::Data` to carry `source_id` plus enough source context to later build metadata rows.
2. Inject `_source_id` into file/CRI rows before batch coalescing.
3. Add a `SourceMetadataTable` enrichment with:
   - `source_id`
   - `source_path`
   - `input_name`
4. Prove the flow with SQL like:

```sql
SELECT l._source_id, s.source_path, l.message
FROM logs l
LEFT JOIN sources s ON l._source_id = s.source_id
```

### Prototype B2

Kubernetes-friendly follow-up:

1. Add `k8s_sources` keyed by `source_id`, or
2. extend `sources` to include parsed `namespace`, `pod_name`, `pod_uid`, `container_name`

That gives users:

```sql
SELECT l.message, s.namespace, s.pod_name, s.container_name
FROM logs l
LEFT JOIN sources s ON l._source_id = s.source_id
```

## Things To Avoid

- depending on sink-side reconstruction for correctness or enrichment
- using path alone as the only stable source key
- trying to attach source metadata after multi-source byte coalescing
- reviving `_file_str` as a docs-only promise without a concrete runtime contract

## Open Questions

1. Should `_source_id` be `Utf8` or `UInt64` in SQL?
   My preference: `Utf8` for initial SQL ergonomics and consistency across source kinds, unless there is a strong DataFusion/join reason to keep it numeric.

2. Should `_input` be a first-class row column?
   Probably yes if cheap, because it helps debugging and multi-input pipelines.

3. Should `_source_path` ever be a direct row column?
   Maybe later as a convenience field, but not as the primary stable contract.

4. Should `k8s_path` stay path-based?
   Long term, I would rather expose a `source_id`-keyed Kubernetes enrichment surface than keep teaching users to join on string prefixes.

5. Should source metadata live in row columns, resource attrs, or both?
   My current leaning is: row surface for queryability, resource attrs as an export mirror where appropriate. Resource attrs alone are not enough for SQL in the current architecture.

## Recommendation Summary

The best-looking prototype is:

- direct row column for stable identity: `_source_id`
- optional direct row column for operator visibility: `_input`
- live enrichment table for descriptive metadata: `sources`
- Kubernetes enrichment keyed by `source_id` rather than path prefix once the source table exists

That gives us a stable core contract now, while keeping the user experience clean enough to grow into.
