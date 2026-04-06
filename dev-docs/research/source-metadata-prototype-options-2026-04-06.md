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
- The OTLP HTTP receiver can see client peer addresses through `tiny_http::Request::remote_addr()`, but `memagent` currently drops that transport metadata instead of surfacing it to SQL, in [otlp_receiver.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-io/src/otlp_receiver.rs) and the upstream `tiny_http` crate.

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
- support non-JSON payload shapes such as CSV-ish text lines and OTLP request metadata;
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

## Prototype E: Row-Origin Sidecar + Post-Scan Column Attach

### Idea

Carry row-origin metadata alongside bytes instead of rewriting the payload.

At framing time, each emitted chunk would also record row spans like:

- `source_id`
- optional `input_name`
- optional `peer_addr`
- `row_count`

The batch accumulator would merge both:

- the existing byte buffer
- a parallel row-origin span list

Then, after the scanner produces a `RecordBatch`, the pipeline would append
Arrow columns such as `_source_id`, `_input`, and `_peer_addr` from that span
list before handing the batch to SQL.

### Where it fits

- extend `InputEvent::Data` or channel messages with row-origin spans;
- keep the scanner completely payload-focused;
- attach source columns to `RecordBatch` after scan and before `SqlTransform::execute()`

### Pros

- works for JSON, CRI, raw/text-line, CSV-ish text, and OTLP equally well
- no payload rewriting or JSON surgery in the hot path
- naturally supports transport-level metadata like OTLP peer IP
- keeps `source_id` numeric if we want a cheap join key
- lets us treat source metadata as metadata instead of pretending it is part of the payload

### Cons

- more invasive pipeline changes than direct injection
- requires row-count fidelity from framing through scanning
- mixed-source batches now need a real row-origin sidecar contract
- if a format processor changes row counts unexpectedly, the sidecar can drift and fail badly

### Assessment

This is now the most promising implementation strategy if we care about
non-JSON sources and OTLP transport metadata. It keeps the user-facing
contract of Prototype B, but avoids tying the implementation to byte rewriting.

### Current prototype status

As of this branch, the prototype now does three concrete things:

- carries row-origin spans beside the shared batch buffer through the accumulator;
- attaches `_source_id` and `_input` as Arrow columns after scan and before SQL;
- includes a standalone `sources` enrichment table prototype keyed by `source_id`,
  with nullable `source_path`, `input_name`, and CRI-derived Kubernetes fields.

The important missing piece is still **live refresh wiring** from active inputs
into the `sources` table. That is the next layer to prototype if this design
continues to feel right.

## Comparison

| Criterion | A: Direct row columns | B: `_source_id` + enrichment | C: Path-only enrichment | D: Source-partitioned batches | E: Row-origin sidecar + attach |
|---|---|---|---|---|---|
| User ergonomics | best | good | good | weak | good |
| Stable identity | good | best | weak | best | best |
| Hot-path overhead | highest | medium | high | medium | medium |
| K8s join fit | good | good | good | medium | good |
| Rotation/churn behavior | medium | best | weak | best | best |
| Fit for TCP/UDP/OTLP later | medium | best | weak | medium | best |
| Contract clarity | medium | best | medium | medium | good |
| Fit for current batching model | medium | best | medium | weak | medium-good |

## Measured POCs

Research benches added in this investigation branch:

- [source_metadata_bench.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-io/tests/source_metadata_bench.rs)
- [source_metadata_table_bench.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-transform/tests/it/source_metadata_table_bench.rs)
- [source_metadata_attach_bench.rs](/Users/billeaston/Documents/repos/memagent-source-metadata-investigation/crates/logfwd-arrow/tests/source_metadata_attach_bench.rs)

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

### Post-scan column attachment

Command:

```bash
cargo test --release -p logfwd-arrow \
  source_metadata_attach_benchmark -- --ignored --nocapture
```

This bench was added in the current pass and is meant to answer the specific
non-JSON concern:

- attach `_source_id` as `UInt64`
- attach `_source_id` as `UInt64` plus `_input`
- attach `_source_id` as `Utf8` plus `_input`

on top of an already-scanned `RecordBatch`, driven by a row-origin span list.

This is the most relevant implementation benchmark if we want the metadata
mechanism to work equally well for CSV-ish text and OTLP without payload
rewriting.

### Worst-case sidecar footprint

The current prototype sidecar shape is intentionally tiny:

```rust
struct RowOriginSpan {
    source_id: Option<SourceId>,
    rows: usize,
}
```

Measured on this machine:

- `size_of::<Option<SourceId>>() = 16`
- `size_of::<RowOriginSpan>() = 24`

So the worst-case memory shape, where every contributing source only adds one
row and no spans coalesce, is approximately:

| Contributing spans in one batch | Approx sidecar bytes |
|---|---:|
| `1,000` | `~24 KiB` |
| `10,000` | `~240 KiB` |
| `100,000` | `~2.4 MiB` |
| `1,000,000` | `~24 MiB` |

That is not free, but it is also not catastrophic at realistic batch sizes.
The more important design rule is:

- **coalesce adjacent rows from the same source aggressively**

The current prototype already does that via `push_row_origin_span()`. The
remaining risk is a highly interleaved multi-source batch where each source
contributes only a single row before another source takes over. That is the
true worst case to keep in mind as we iterate.

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

Prototype **B** first as the user-visible contract, but prefer **E** as the
implementation strategy:

- add `_source_id` as a reserved row column for source-aware inputs;
- optionally add `_input` if we want input-name visibility without a join;
- build a dynamic `sources` enrichment table keyed by `source_id`;
- add a Kubernetes enrichment variant keyed by `source_id`, or derive it from the `sources` snapshot;
- avoid payload rewriting where possible by carrying a row-origin sidecar and attaching columns after scan.

Why this is the best first prototype:

- it solves the stable identity problem directly;
- it avoids forcing `_source_path` into every row as the primary contract;
- it fits the current `SqlTransform` enrichment model;
- it preserves the current cross-source batching strategy;
- it leaves us free to add `_source_path` later if we want a convenience column for users.

## Suggested Prototype Plan

### Prototype B1/E1

Minimal viable hybrid:

1. Extend `InputEvent::Data` or the pipeline channel to carry `source_id` plus row-origin span information.
2. Append `_source_id` to the scanned batch before SQL, rather than rewriting bytes.
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

### Prototype B2/E2

Kubernetes-friendly follow-up:

1. Add `k8s_sources` keyed by `source_id`, or
2. extend `sources` to include parsed `namespace`, `pod_name`, `pod_uid`, `container_name`

Transport-friendly follow-up:

1. Add optional source transport columns such as `_peer_addr` for OTLP/UDP/TCP inputs
2. decide which of those should remain row columns versus enrichment-table-only

That gives users:

```sql
SELECT l.message, s.namespace, s.pod_name, s.container_name
FROM logs l
LEFT JOIN sources s ON l._source_id = s.source_id
```

## Things To Avoid

- depending on sink-side reconstruction for correctness or enrichment
- using path alone as the only stable source key
- trying to recover source metadata after multi-source byte coalescing without a row-origin sidecar
- reviving `_file_str` as a docs-only promise without a concrete runtime contract

## Open Questions

1. Should `_source_id` be `Utf8` or `UInt64` in SQL?
   This now looks more open. For a sidecar-based implementation, `UInt64` may be materially cheaper and a better join key. We can still expose a string form later if needed.

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
- implemented via a row-origin sidecar and post-scan column attach, not by default payload rewriting

What is already proven on this branch:

- `_source_id` and `_input` can be attached without payload rewriting
- SQL joins against a `sources` enrichment table keyed by `_source_id` work cleanly
- `#608`'s “owned chunks with attached metadata” direction is a better fit than direct injection

## Impact Review

### Design docs impacted

#### `dev-docs/ARCHITECTURE.md`

The architecture doc currently says:

- input flow is bytes-only after `InputEvent::Data`
- `FramedInput` converts raw bytes to scanner-ready bytes
- source metadata is described mostly as payload injection or `_resource_*` direction

If the sidecar design sticks, this doc should explicitly mention:

- the pipeline may carry **batch sidecars** alongside `Bytes`
- source identity can be attached **after scan and before SQL**
- scanner contract stays payload-focused, which is a feature of this design

#### `dev-docs/DESIGN.md`

The design doc's “RecordBatch in, RecordBatch out” framing still fits well.
This prototype actually strengthens that story:

- source metadata becomes a `RecordBatch` concern rather than a payload-munging concern
- OTLP/file/TCP/UDP can all converge on the same pre-SQL column-attach point

The doc sections around `_resource_*` should eventually explain the layering:

1. row-visible source identity (`_source_id`, maybe `_input`, maybe `_peer_addr`)
2. optional enrichment tables (`sources`, k8s)
3. optional output/resource mirroring (`_resource_*` or sink resource attrs)

#### `dev-docs/SCANNER_CONTRACT.md`

This is one of the best arguments for the sidecar design:

- **the scanner contract does not need to change**

If we inject metadata after scan, the scanner can remain a pure NDJSON scanner
without learning about file paths, source ids, peer addresses, or transport
metadata. That is architecturally cleaner than payload injection.

### Open issues impacted

#### `#608` — perf: eliminate per-poll allocations in input→scanner data path

This issue is strongly aligned with the sidecar design.

Its owner note already calls out:

- “owned chunks with attached metadata”
- “stream semantics (`poll_next() -> Option<InputChunk>`)”

That is very close to where this prototype naturally wants to go. So this
design likely **advances** `#608` rather than conflicting with it.

#### `#1346` — missing / inconsistent source metadata column

This issue absolutely still matters, but the likely implementation answer
changes:

- old likely answer: inject path-like metadata into payload before scan
- new likely answer: expose `_source_id` first, then join to `sources`

So the issue remains valid, but its implementation plan should probably be
updated to reflect the sidecar architecture.

#### `#1356` — source metadata work-unit

Same story as `#1346`: still relevant, but the right “work unit” probably
becomes:

- `_source_id` sidecar/column
- `sources` enrichment table
- docs/examples migrated away from `_source_path` as the primary contract

#### `#153` — `_resource_*` column injection for source metadata

This stays relevant as the longer-term output/resource model, but the sidecar
prototype suggests a more layered relationship:

- `_source_id` / source-side metadata for SQL and correctness
- `_resource_*` for output/resource grouping and sink semantics

That separation feels healthier than trying to make `_resource_*` do all jobs.

#### `#971` — resource attributes and receiver efficiency

This issue becomes more coherent if source metadata reaches rows through the
same sidecar path as future OTLP peer metadata. It reinforces that row/source
metadata and output resource grouping are related but not identical concerns.

### Docs/examples likely to change if this wins

- Kubernetes examples that currently join on `_source_path`
- any remaining `_file_str` / `_source_path` expectations
- architecture text that assumes source metadata must be injected in the payload

### Why this still looks like the right architecture

This design keeps the three important layers cleaner:

1. **framing stays source-local and payload-local**
2. **batching carries row-origin facts without mutating payload**
3. **SQL sees normal columns on a normal `RecordBatch`**

That is a better fit for:

- `raw` / text-line inputs
- CSV-ish future parsing
- OTLP peer address / transport metadata
- the existing scanner contract
- the “share the buffer when possible” performance direction

That gives us a stable core contract now, while keeping the user experience clean enough to grow into.
