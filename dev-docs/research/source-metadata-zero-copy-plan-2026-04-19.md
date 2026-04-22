# Source Metadata Zero-Copy Plan

> **Status:** Historical
> **Date:** 2026-04-19
> **Context:** Source metadata zero-copy migration plan after fan-out research and profiling.
> The active contract now lives in
> `source-metadata-attachment-perf-2026-04-19.md`: `source_metadata: none` is
> the default, `fastforward` attaches internal `__source_id`, public styles
> (`ecs`/`beats`, `otel`, `vector`) attach normal columns, and SQL does not
> prune or hide metadata columns.

## Summary

The sidecar replacement was started, but it was not merged. The migration now
has a hard rule: source metadata must never be injected into source bytes. File
paths, sender addresses, input names, and future source descriptors travel as
sidecar/source metadata and are materialized only when assembling the Arrow
table that SQL sees.

The active target architecture is:

1. carry row-origin metadata beside scanner-ready bytes;
2. scan the original payload bytes unchanged;
3. attach source metadata as Arrow columns after scan and before SQL;
4. keep descriptive source details in a cold-path `sources` enrichment table;
5. attach nothing by default; use `source_metadata: fastforward` for internal
   `__source_id`, or public styles (`ecs`/`beats`, `otel`, `vector`) for
   normal metadata columns;
6. keep SQL unsurprising: no hidden widening, pruning, or legacy metadata
   compatibility in the SQL stage;
7. drop known FastForward internal columns such as `__source_id` at user-facing
   output boundaries unless SQL aliases them to public names.

This keeps the scanner payload zero-copy, preserves multi-file batching, and
avoids paying for long path strings on rows that do not need source metadata.

The remaining sections are retained as historical notes from the superseded
proposal. Any `_source_path`, `_source_id`, `_input`, or `source_metadata: true`
references below describe the old design, not the current contract.

## Current Behavior Before This Migration

File input already has source identity:

- `InputEvent::Data` carries `source_id: Option<SourceId>`.
- `FileInput::source_paths()` exposes `SourceId -> PathBuf`.
- `FramedInput` preserves the event `source_id`.

The runtime loses the useful sidecar:

- `input_pipeline.rs` appends each framed event into `input.buf` with
  `extend_from_slice`.
- The event `source_id` is discarded at that append site.
- `IoChunk` carries bytes, checkpoints, timing, and input index, but no row
  origin spans.
- The CPU worker scans `chunk.bytes`, then immediately executes SQL, so any
  SQL-visible source column must be attached before `transform.execute(...)`.

Previous `_source_path` compatibility was implemented by mutating JSON-like rows
before scan:

- `Json`, `Cri`, and `Auto` formats support source path insertion.
- `Raw` does not.
- `inject_source_path_metadata` rewrites each eligible row and escapes the
  path.

That is expensive because it adds a hot-path copy and widens every downstream
stage. Scanner string values are otherwise mostly zero-copy through
`Bytes` + `StringViewArray`, so raw injection defeated the best part of the
current scanner design. That path must be deleted, not extended.

## What Happened To The Sidecar Work

The sidecar work exists on `origin/codex/source-metadata-investigation` and was
not accidentally reverted. It never became an ancestor of `origin/main`.

Unmerged commits on that branch:

- `8e09d13a` Investigate source metadata prototype options
- `d09302c7` Expand source metadata design investigation
- `d0b3b045` Add source metadata benchmark POCs
- `f5110f0f` Prototype source metadata injection
- `078dc676` Prototype row-origin sidecar metadata columns
- `4f2ea662` Prototype cold-path source metadata refresh

Useful pieces to resurrect:

- `RowOriginSpan { source_id, input_name, rows }`.
- Coalescing adjacent spans with the same source id and input name.
- Counting rows from scanner-ready bytes, not original raw input bytes.
- Carrying row origins beside accumulated bytes.
- Attaching `_source_id` and `_input` after scan and before SQL.
- Rejecting a batch if sidecar row counts do not match
  `RecordBatch::num_rows()`.
- `SourceMetadataEntry` / `source_metadata_snapshot()`.
- `SourcesTable` keyed by `source_id`, with source path, input name, and parsed
  Kubernetes fields.

Obsolete pieces:

- The old implementation lived before the current crate split.
- It used older `StringBuilder` loops; the current `StringViewBuilder`
  constant-block pattern is a better starting point for string metadata.
- It did not fully preserve scoped `_source_path` references for joins and
  explicit projections.

`origin/copilot/add-resource-column-injection` is separate prior art. Its
resource-column idea was later reimplemented in current `Scanner::with_resource_attrs`
and OTLP grouping, but it assumes one static resource identity per batch. That
does not solve per-row file provenance for globbed file inputs, rotations, or
mixed-source batches.

## Public Contract

Keep these user-visible behaviors:

- `_source_path` remains a SQL-visible source metadata column for file-backed
  inputs when explicitly referenced.
- Existing `_source_path` projections, filters, and joins work when the input
  explicitly enables `source_metadata: true`.
- `SELECT *` remains narrow. It does not expose `_source_path`, `_source_id`,
  `_input`, or future metadata columns unless they are explicitly projected.
  Metadata attached only for filters, joins, or expressions is pruned from the
  transform output.
- Outputs serialize selected metadata columns as normal log fields.
- OTLP keeps `_source_path` as a LogRecord attribute unless the user explicitly
  maps it elsewhere.

Add these preferred contracts:

- `_source_id: UInt64 nullable` as stable row-level source identity.
- `_input: Utf8 nullable` as optional configured input name.
- `sources` enrichment table keyed by `source_id`.

Recommended `sources` table columns:

| Column | Type | Notes |
|---|---|---|
| `source_id` | `UInt64` | Stable key from `SourceId` |
| `source_path` | `Utf8 nullable` | File path when available |
| `input_name` | `Utf8 nullable` | Configured input name |
| `source_kind` | `Utf8 nullable` | `file`, `tcp`, `s3`, etc. when available |
| `log_path_prefix` | `Utf8 nullable` | Kubernetes path join prefix |
| `namespace` | `Utf8 nullable` | Parsed from CRI path |
| `pod_name` | `Utf8 nullable` | Parsed from CRI path |
| `pod_uid` | `Utf8 nullable` | Parsed from CRI path |
| `container_name` | `Utf8 nullable` | Parsed from CRI path |

Keep `k8s_path` as compatibility/convenience, but make `sources` the preferred
source metadata table after the migration.

## Alternatives

| Rank | Design | Verdict |
|---:|---|---|
| 1 | Row-origin spans + post-scan `_source_id` + cold `sources` table | Primary design. Preserves batching and avoids raw payload mutation. |
| 2 | Row-origin spans + direct post-scan `_source_path` | Compatibility bridge. Use only when source metadata is enabled and SQL can observe `_source_path`. |
| 3 | `_source_id` + `sources` join only | Best long-term contract, but not enough alone until compatibility migration is complete. |
| 4 | Scanner constant resource attrs | Good for static batch-level resource metadata. Not sufficient for mixed-source rows. |
| 5 | Per-source scan chunks | Correct but likely harms throughput with many low-volume files. Benchmark as fallback only. |
| 6 | Dictionary path columns | Promising optimization for repeated paths; needs sink compatibility audit. |
| 7 | Run-end encoded path columns | Maps well to spans, but DataFusion and sink support are higher risk. |
| 8 | RecordBatch/schema metadata only | Not SQL-visible; insufficient. |
| 9 | Raw JSON `_source_path` insertion | Deleted/forbidden. Keep only as benchmark baseline from history. |

## Required Wiring

### I/O and framing

1. Add a source metadata snapshot API:

   ```rust
   pub struct SourceMetadataEntry {
       pub source_id: SourceId,
       pub source_path: Option<PathBuf>,
       pub input_name: Option<Arc<str>>,
       pub source_kind: SourceKind,
   }
   ```

2. Add `InputSource::source_metadata_snapshot()` with a default empty result.
   `FileInput` should populate it from current file path metadata.

3. Keep `source_paths()` temporarily for compatibility. Eventually replace
   per-poll path map creation in `FramedInput` with the snapshot path.

4. Raw metadata injection sites are forbidden. `FramedInput` may frame,
   validate, and format payload bytes, but it must not add `_source_*`,
   `_input`, peer address, resource, or source descriptor fields to those bytes.

### Runtime batching

1. Add runtime-side row spans:

   ```rust
   pub(crate) struct RowOriginSpan {
       pub source_id: Option<SourceId>,
       pub input_name: Arc<str>,
       pub rows: usize,
   }
   ```

2. Extend `InputState` with `row_origins: Vec<RowOriginSpan>`.

3. When `InputEvent::Data` arrives, count rows in the scanner-ready `bytes`
   and append/coalesce a span before `input.buf.extend_from_slice(&bytes)`.

4. Count emitted rows, not physical input lines. This keeps CRI partial/full
   aggregation, EOF remainder flushes, and future format wrappers aligned with
   the rows the scanner will produce.

5. Extend `IoChunk` with `row_origins`.

6. Move bytes and row origins together in `flush_buf`.

7. On shutdown drain, send the remaining `row_origins` with the remaining
   bytes.

8. Structured `InputEvent::Batch` needs either a single source span from its
   event `source_id`, or no source metadata until that receiver supplies a
   richer per-row sidecar.

### Post-scan attachment

1. Attach metadata in the CPU worker after `scanner.scan(...)` and before
   `transform.execute(...)`.

2. Add an Arrow helper, probably in `logfwd-arrow`, that accepts:

   - `RecordBatch`
   - `&[RowOriginSpan]`
   - metadata lookup by `SourceId`
   - an attachment plan from the analyzer

3. Validate `sum(span.rows) == batch.num_rows()`. If this fails, drop the
   batch and log the sidecar mismatch. Silent misattribution is worse than a
   dropped batch.

4. Attach `_source_id` using `UInt64Builder` when required.

5. Attach `_input` and `_source_path` using `StringViewBuilder` constant
   blocks first. Benchmark dictionary arrays separately before adopting them.

6. Define reserved-name collision behavior explicitly. The compatibility-safe
   policy is to let system metadata own `_source_path`, `_source_id`, and
   `_input`, while user fields with those names are renamed or rejected before
   SQL. Add tests before changing this path.

### Transform and tables

1. Use `QueryAnalyzer::source_metadata_plan()` to choose source metadata
   columns. Attach them only when the input has `source_metadata: true`:

   - `_source_path` for explicit references
   - `_source_id` for explicit references
   - `_input` for explicit references
   - no output metadata for wildcard projection or wildcard EXCEPT lists

2. Add `SourcesTable` to `logfwd-transform`.

3. Wire runtime source snapshots into `SourcesTable` refreshes.

4. Refresh `K8sPathTable` from the same source snapshots. Current table support
   exists, but runtime construction alone leaves it empty.

5. Document `sources` joins as preferred:

   ```sql
   SELECT l.message, s.namespace, s.pod_name, s.container_name
   FROM logs l
   LEFT JOIN sources s ON l._source_id = s.source_id
   ```

6. Keep existing `_source_path` joins working during migration:

   ```sql
   SELECT l.message, k.namespace, k.pod_name, k.container_name
   FROM logs l
   LEFT JOIN k8s k ON l._source_path = k.log_path_prefix
   ```

### Resource attributes and outputs

1. Do not make `_source_path` an OTLP Resource attribute by default. It is a
   LogRecord-level file/log attribute, and default resource promotion would add
   grouping/cardinality cost.

2. Keep `resource.attributes.*` as the canonical resource column namespace.
   Keep `_resource_*` as a legacy accepted prefix.

3. Only promote source metadata to `resource.attributes.*` through explicit
   SQL/config mapping.

4. Add output tests for JSON, Loki, Elasticsearch, OTLP, and Arrow IPC with
   selected source metadata columns.

### UDP sender metadata

Issue #1714 is the non-file proof point for this architecture. The first
implemented slice is the low-risk contract: UDP emits stable sender-derived
`_source_id` via the same row-origin sidecar path, without cleartext peer
address columns by default.

1. switch UDP receive to `recv_from()` so the sender address is available;
2. derive a stable sender-scoped `SourceId` with a domain-separated hash;
3. emit `InputEvent::Data { source_id: Some(...) }`;
4. attach `_source_id` and `_input` through the post-scan metadata path;
5. expose cleartext peer address only through an explicit config/metadata plan,
   because it has privacy and cardinality cost.

The important constraint from #1714 is that UDP must not introduce a second
source metadata model and must not write `_peer_addr`, `_sender_addr`, or
similar fields into datagram payloads.

## Benchmark Findings So Far

Local framed-input profiling shows raw `_source_path` insertion is measurable
even before full pipeline effects:

| Scenario | Lines | FramedInput | Total |
|---|---:|---:|---:|
| passthrough narrow JSON | 100k | 2.13 ms | 25.79 ms |
| raw `_source_path` insertion | 100k | 8.12 ms | 37.83 ms |
| SQL-gated insertion disabled | 100k | 2.08 ms | 25.81 ms |

Prior prototype numbers from the unmerged sidecar branch were stronger:

| Operation | Rows/Payload | Time |
|---|---:|---:|
| post-scan `_source_id UInt64` attach | 50k rows | about 0.088 ms |
| post-scan `_source_id + _input` attach | 50k rows | about 0.322 ms |
| `_source_id` as Utf8 | 50k rows | about 1.602 ms |
| baseline 4 MiB scan | 4 MiB | about 5.55 ms |
| raw id injection scan | 4 MiB | about 6.98 ms |
| raw id+path+input injection scan | 4 MiB | about 10.44 ms |

Conclusion: use numeric `_source_id` as the hot-path identity. Treat strings
as compatibility/convenience columns and attach them only when source metadata
is enabled and SQL needs them.

## Benchmark Plan

Add `crates/logfwd-bench/benches/source_metadata.rs` with these groups:

- `raw_rewrite`: historical raw `_source_path` insertion baseline.
- `post_scan_attach`: `_source_id`, `_input`, `_source_path` attachment.
- `source_table_snapshot`: cold `sources` and `k8s_path` refresh cost.
- `e2e_sql`: no metadata, selected metadata, `sources` join, `SELECT *`.
- `output_encode`: JSON, Loki, Elasticsearch, OTLP, Arrow IPC behavior.

Add a profile binary:

- `crates/logfwd-bench/src/bin/source_metadata_profile.rs`

Modes:

- `raw-path`
- `gated`
- `sidecar-id`
- `sidecar-id-input`
- `sidecar-path`
- `dictionary-path`
- `resource-cols`

Important dimensions:

- 1, 30, 300, and 10k sources.
- Contiguous, round-robin, and random-run interleaving.
- JSON, CRI full, and CRI partial/full workloads.
- 64 KiB aligned chunks and 97-byte remainder-heavy chunks.
- No metadata query, `_source_id`, `_source_path`, `sources` join,
  `k8s_path` join, `SELECT *`, and wildcard except.

Initial performance gates:

- No-metadata query with source machinery present: within +2% CPU and no
  meaningful allocation increase versus baseline.
- `_source_id UInt64` attach: less than 10 ns/row at 50k+ rows.
- `_source_id + _input`: less than 30 ns/row at 50k+ rows.
- Direct post-scan `_source_path`: at least 5x faster than raw rewrite for
  100k rows.
- Sidecar memory: `O(spans)`, not `O(rows * path_len)`, unless direct path
  materialization is explicitly required by SQL.
- Source-partitioned fallback must match or beat sidecar E2E throughput under
  30 low-EPS sources before it is considered as a default.

## Test Plan

Required correctness tests:

- `InputEvent::Data` source ids become row-origin spans.
- Adjacent spans coalesce.
- Remainder-heavy input keeps correct row counts.
- CRI partial/full aggregation counts emitted rows, not physical CRI lines.
- EOF remainder flushes create correct spans.
- Rotation/truncation preserves source identity for emitted rows.
- Post-scan attachment rejects mismatched span row counts.
- `_source_path`, `_source_id`, and `_input` are visible for explicit
  projection, filter, and join only when input source metadata is enabled.
- `SELECT *` remains narrow and does not implicitly include `_source_path`,
  `_source_id`, `_input`, or future source metadata, even when those columns
  were attached to evaluate filters or joins.
- Existing `_source_path` Kubernetes join still works.
- New `sources` join works.
- `k8s_path` refreshes from source snapshots.
- User payload collisions with `_source_path`, `_source_id`, and `_input` are
  deterministic.
- Output sinks only serialize metadata columns selected by SQL.
- OTLP keeps `_source_path` as a log attribute and only promotes
  `resource.attributes.*` / legacy `_resource_*` to Resource attributes.

Required verification:

- `cargo test -p logfwd-io source`
- `cargo test -p logfwd-runtime source`
- `cargo test -p logfwd-transform source`
- `cargo test -p logfwd-output source`
- `cargo check -p logfwd-runtime --no-default-features`
- `just bench-source-metadata-fast`
- `just bench-framed-input --lines 100000 --iterations 3`
- `just bench-framed-input-alloc --lines 200000`

Run `just ci` before pushing the migration. Run `just ci-all` once the
transform/output/doc surface is touched.

## Phased Implementation

### Phase 0: delete raw injection

Remove raw `_source_path` insertion from `FramedInput` and make the guardrail
fail on any source metadata raw-byte write.

### Phase 1: resurrect sidecar spine

- Add `RowOriginSpan`.
- Carry spans through `InputState`, `IoChunk`, and CPU worker.
- Count rows from scanner-ready framed bytes.
- Add mismatch rejection.
- Add focused runtime tests.

### Phase 2: post-scan metadata attach

- Add Arrow attach helper.
- Attach `_source_id` only when source metadata is enabled and SQL needs it.
- Attach `_source_path` only when source metadata is enabled and SQL needs it.
- Prove current `_source_path` examples still work.

### Phase 3: cold source registry

- Add `SourceMetadataEntry` / `source_metadata_snapshot()`.
- Add runtime snapshot hashing and refresh messages.
- Add `SourcesTable`.
- Refresh `K8sPathTable` from source snapshots.
- Document `sources` joins.

### Phase 4: benchmark and optimize

- Port prototype benches into `logfwd-bench`.
- Add profile binary and just targets.
- Compare historical raw rewrite, sidecar id, sidecar path, dictionary path,
  resource columns, and source-partitioned fallback.
- Keep dictionary/run-end encoded columns behind benchmark evidence.

### Phase 5: keep raw injection deleted

- Keep CI guardrail forbidding raw payload metadata insertion.
- Update adapter/config docs to say source metadata is attached post-scan.

## Open Decisions

1. Collision policy for user-provided `_source_path`, `_source_id`, and
   `_input` must be pinned before post-scan attach lands.

2. Should `sources` refresh be event-driven from file discovery/rotation or
   periodic hash-based? Initial plan: start hash-based from snapshots, then
   add event-driven refresh once file topology events expose enough detail.

3. Should `_source_path` be materialized as `Utf8View`, `Utf8`, or dictionary?
   Initial plan: `Utf8View` constant blocks, benchmark dictionary before
   adopting it.

## External References

- GitHub issue #1615: replace raw `_source_path` injection with post-scan
  column attachment.
- GitHub PR #1370: unmerged source metadata sidecar prototype.
- GitHub PR #1566: compatibility fix that preserved `_source_path` raw
  insertion.
- GitHub issue #1714: UDP sender attribution using the same post-scan metadata
  contract.
- Arrow columnar format: dictionary and run-end encoded layouts.
- DataFusion table providers and `MemTable` docs.
- OpenTelemetry semantic conventions for log file attributes.
