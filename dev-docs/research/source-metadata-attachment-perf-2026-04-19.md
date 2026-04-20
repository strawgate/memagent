# Source Metadata Attachment Performance Plan

> **Status:** Active
> **Date:** 2026-04-19
> **Context:** Post-merge follow-up for source metadata sidecar requirements, benchmark coverage, and optimization gates.

## Goal

Source metadata is now a first-class pipeline boundary: source identity travels
beside payload bytes and is materialized as Arrow columns only after scan and
before SQL. The remaining work is to make this path boring under production
cardinality, prove it does not penalize default users, and define the source
metadata contract for every input type.

The default rule remains:

- do not inject source metadata into payload bytes;
- do not attach source metadata unless `source_metadata` is a non-`none` style;
- keep SQL unsurprising: SQL sees ordinary Arrow columns and `SELECT *`
  returns the table schema as-is;
- use `source_metadata: fastforward` for the cheap internal `__source_id`
  handle, and use public styles (`ecs`/`beats`, `otel`, `vector`) when metadata
  should be materialized as normal columns;
- drop known FastForward internal fields such as `__source_id` at user-facing
  output boundaries unless SQL aliases them to public names.

## Current Implementation Snapshot

PR #2317 landed the sidecar spine:

- `InputEvent::Data` and `InputEvent::Batch` carry `Option<SourceId>`.
- Runtime I/O workers coalesce scanner-ready row spans in
  `RowOriginSpan { source_id, input_name, rows }`.
- `IoChunk` carries `row_origins` and a filtered `SourceId -> path` map.
- The CPU worker scans unchanged bytes, attaches requested metadata columns,
  and then runs SQL.
- `source_metadata: fastforward` materializes `__source_id` as nullable
  `UInt64`.
- Public styles materialize source paths as normal columns: `file.path` for
  ECS/Beats, `log.file.path` for OTel, and `file` for Vector.
- Public source path styles are accepted only for inputs that expose path
  snapshots. File tailing exposes filesystem paths. S3 exposes object keys when
  a public style is configured. This guard avoids replacing a payload
  `file.path` column with null metadata for UDP/TCP/stdin/structured inputs
  that do not yet provide public source descriptors.
- SQL no longer prunes metadata after execution. Internal visibility is handled
  by known internal-column output filtering, not by SQL-stage magic.
- Output boundaries drop known internal columns such as `__source_id` across
  row-JSON/HTTP/TCP/UDP/ES/Loki, OTLP attributes, OTAP log attributes, and Arrow
  IPC serialization. Public style columns and user double-underscore fields are
  normal data and are emitted unless the user removes them in SQL.

The implementation is correct enough to build on, but benchmark coverage is
still thin. The hot path currently pays for:

- row counting while appending scanner-ready bytes;
- row-origin span storage and coalescing;
- optional `source_paths()` snapshots when a public path style is requested;
- path string conversion at flush time;
- Arrow array construction and schema replacement;
- DataFusion planning/execution against attached columns.

## Source Requirement Matrix

| Source | Current identity | Required metadata contract | Open work |
|---|---|---|---|
| File tail | Stable file `SourceId`; `source_paths()` returns active path map | `fastforward`: `__source_id`; public styles: `file.path`, `log.file.path`, or `file`; future `sources` rows with file path and parsed Kubernetes fields | Replace ad hoc `source_paths()` with a richer snapshot; avoid full path snapshots when only `__source_id` is requested |
| UDP | Sender-scoped `SourceId` from `recv_from()` | `fastforward`: `__source_id`; cleartext peer address only by explicit future public style/config | Add cold `sources` descriptor for peer endpoint if/when configured; benchmark high sender cardinality |
| TCP | Per-connection `SourceId` | `fastforward`: `__source_id`; peer/local address should be explicit metadata, not default payload fields | Decide descriptor retention after disconnect; avoid unbounded per-connection source tables |
| S3 | Object/key-derived `SourceId`; optional current-poll object-key snapshots for public styles | `fastforward`: `__source_id`; public styles: object key in the selected path column | Consider whether future URI-style descriptors should include bucket/endpoint; handle multipart sub-source IDs deterministically if introduced |
| Stdin | No stable external source; finite stream | No metadata by default; future public stream descriptor only by explicit style/config | No path work needed |
| Generator | No external source by default | Optional synthetic source descriptors only for benchmark/debug configs | Keep default cheap |
| OTLP receiver | Structured `RecordBatch`, currently no source identity | Future sender/resource source id only if a source contract is defined | Preserve incoming OTLP attributes as payload/resource data, not source metadata |
| Arrow IPC receiver | Structured `RecordBatch`, currently no source identity | Future stream/source id only if configured | Avoid rewriting user schema unless source metadata is requested |
| Host metrics | Structured host batch, no per-row source id | Future host source descriptor belongs in `sources`/resource attrs | Keep metrics resource semantics separate |
| Platform/eBPF sensor | Structured event batch, no source id today | Future process/container/host source descriptors must be explicit | Do not overload `__source_id` until lifecycle and cardinality are defined |
| HTTP receiver | Request-level input source, not row source today | Future remote endpoint descriptor only by explicit config | Privacy/cardinality decision needed |

## Benchmark Coverage Added

`crates/logfwd-bench/benches/source_metadata.rs` adds four groups:

- `source_metadata_attach`: isolated column attachment for 50k rows, 1/30/300
  sources, and contiguous/32-row/round-robin spans.
- `source_metadata_scan_sql`: scan + attach + DataFusion projection/filter.
- `source_metadata_raw_rewrite_baseline`: historical JSON `_source_path`
  rewrite cost for comparison only.
- `source_metadata_snapshot_filter`: path snapshot filtering at 30/300/10k
  active sources.

Run fast local samples with:

```bash
just bench-source-metadata-fast
```

Run full decision-grade samples with:

```bash
just bench-source-metadata
```

For quick CPU/allocation iteration without Criterion's full bench-package
linking cost, use:

```bash
cargo run -p logfwd-bench --release --bin source_metadata_profile -- \
  --rows 50000 --sources 300 --iterations 200
```

## Optimization Questions

## Initial Local Profile

Environment: local macOS worktree, release build,
`source_metadata_profile --rows 50000 --sources 300 --iterations 100`.
These are first-pass directional numbers, not final Criterion baselines.

32-row source runs:

| Mode | ns/row | allocs/iter | bytes/row |
|---|---:|---:|---:|
| `__source_id` UInt64 | 1.17 | 13 | 8.02 |
| `file.path` Utf8View, string-keyed block cache | 13.55 | 922 | 23.11 |
| `file.path` Utf8View, source-keyed block cache | 11.73 | 615 | 22.15 |
| `file.path` Utf8 | 7.77 | 16 | 100.02 |
| `file.path` dictionary Int32 | 10.49 | 28 | 6.38 |
| Historical raw JSON rewrite | 133.58 | 5 | 407.44 |

Round-robin one-row source runs:

| Mode | ns/row | allocs/iter | bytes/row |
|---|---:|---:|---:|
| `__source_id` UInt64 | 1.84 | 13 | 8.02 |
| `file.path` Utf8View, string-keyed block cache | 41.75 | 922 | 23.11 |
| `file.path` Utf8View, source-keyed block cache | 14.06 | 615 | 22.15 |
| `file.path` Utf8 | 11.94 | 16 | 100.02 |
| `file.path` dictionary Int32 | 15.35 | 28 | 6.38 |
| Historical raw JSON rewrite | 173.83 | 5 | 407.44 |

Early conclusions:

- Numeric `__source_id` is comfortably cheap enough for hot identity.
- Post-scan public path columns are roughly 10x faster than raw JSON rewrite in the
  normal 32-row-run case and roughly 12x faster in round-robin stress when the
  source-keyed block cache variant is used.
- String-keyed `Utf8View` block caching has a clear worst case:
  round-robin spans repeatedly hash long path strings. Caching by `SourceId`
  keeps the same Arrow representation but cuts the round-robin path attach cost
  from 41.75 ns/row to 14.06 ns/row.
- The runtime public path attachment path now uses the source-keyed block
  cache; the string-keyed numbers remain as a historical baseline in the
  benchmark/profile harness.
- Plain `Utf8` can be CPU-fast, but it materializes path bytes per row and is
  not acceptable as the default path representation.
- Dictionary paths are promising for memory, but need DataFusion/output sink
  compatibility checks before adoption.

### 1. Should string metadata be cached by string value or source id?

Current code caches `Utf8View` backing blocks with `HashMap<String, (block,
len)>`. That avoids row-wise path copies but allocates one key per distinct
string and hashes long paths by content.

Benchmark variant `path_utf8view_by_source` caches blocks by source index. If
that wins consistently, production should change to a
`HashMap<SourceId, (block, len)>` cache while still looking up the actual path
from `SourceId -> String`.

This optimization is now applied in runtime public source-path attachment.
Expected benefit: lower allocation and hashing cost when a public path style is
requested with many sources.

### 2. Is dictionary encoding worth exposing to SQL?

Dictionary arrays are an excellent shape for repeated paths, but adoption
depends on DataFusion and output sink behavior. The benchmark includes
`path_dictionary_i32` as an experimental comparison, not a default candidate.

Adoption gate:

- at least 20% faster or materially lower allocation than `Utf8View` under
  300+ source cardinality;
- no sink compatibility regressions for JSON, Loki, Elasticsearch, OTLP, and
  Arrow IPC;
- no DataFusion cast/projection penalty that erases the win.

### 3. Can `__source_id` attach stay below the noise floor?

`__source_id` is the preferred hot identity. It should be close to a simple
numeric builder pass plus schema replacement.

Gate: less than 10 ns/row at 50k+ rows on local release benchmarks, and no
visible change to no-metadata pipelines.

### 4. How expensive is path snapshotting?

Today runtime calls `source_paths()` only when a public path style is requested.
That is correct but still coarse: file input may return all active paths, then
runtime filters to paths present in the current chunk.

Gate: if 10k active sources with 30 touched sources costs more than direct
column attach, introduce a richer source snapshot API that can answer by
`SourceId` or produce changed descriptors incrementally.

### 5. What is the cold `sources` table shape?

Long-term joins should use:

```sql
SELECT l.message, s.source_path, s.namespace, s.pod_name
FROM logs l
LEFT JOIN sources s ON l.__source_id = s.source_id
```

This keeps row batches narrow while still supporting rich descriptors. The
first table should be an in-memory `source_id` keyed table refreshed from
input snapshots. Later versions can add event-driven updates from tailer
discovery and network source lifecycle events.

## Performance Gates

| Area | Gate |
|---|---|
| Default pipelines | `source_metadata: none` remains within +2% CPU and no material allocation increase |
| `__source_id` | < 10 ns/row at 50k+ rows |
| Public source path direct column | at least 5x faster than raw JSON rewrite at 100k rows |
| Path snapshot filter | 10k active/30 touched sources should not dominate attach cost |
| Span storage | memory is `O(spans)`, not `O(rows * source metadata width)` before attach |
| Dictionary path | only adopt if DataFusion and sinks preserve a real win |

## Implementation Backlog

1. Run and commit source metadata Criterion baselines on current `main`.
2. Extract the production attach helper from runtime into `logfwd-arrow` or a
   small shared module so benchmarks can call the real implementation instead
   of mirroring the algorithm.
3. Add `SourceMetadataEntry` and `InputSource::source_metadata_snapshot()`.
4. Add `sources` table and refresh it from runtime snapshots.
5. Add output-sink tests proving selected source metadata columns serialize
   normally and unselected metadata is absent.
6. Add no-metadata E2E benchmark to ensure machinery does not affect default
   users.
