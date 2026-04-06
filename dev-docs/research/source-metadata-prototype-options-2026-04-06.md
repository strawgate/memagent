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

## Comparison

| Criterion | A: Direct row columns | B: `_source_id` + enrichment | C: Path-only enrichment |
|---|---|---|---|
| User ergonomics | best | good | good |
| Stable identity | good | best | weak |
| Hot-path overhead | highest | medium | high |
| K8s join fit | good | good | good |
| Rotation/churn behavior | medium | best | weak |
| Fit for TCP/UDP later | medium | best | weak |
| Contract clarity | medium | best | medium |

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

## Recommendation Summary

The best-looking prototype is:

- direct row column for stable identity: `_source_id`
- optional direct row column for operator visibility: `_input`
- live enrichment table for descriptive metadata: `sources`
- Kubernetes enrichment keyed by `source_id` rather than path prefix once the source table exists

That gives us a stable core contract now, while keeping the user experience clean enough to grow into.
