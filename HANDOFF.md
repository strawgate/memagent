# Handoff: Delete Sync Elasticsearch Sink + Rewrite Throughput Bench

## What Was Done

### CPU Profiling Investigation

We profiled the Elasticsearch output path against a real cloud cluster using a new
`es-throughput` bench binary (`crates/logfwd-bench/src/es_throughput.rs`).

**Results (GCP Serverless ES, 30s runs):**
- ~1,900 evt/s (0.2% of the 1M/s target)
- ~524 ms average batch latency (almost entirely network RTT)
- The bottleneck is 100% network — the cluster is in GCP us-central1

**Flamegraph findings:**
- ~15.9% of CPU in `lookup_host` — ureq re-resolves DNS on every `_bulk` call
  because it doesn't maintain a persistent connection pool by default
- The sync sink has no connection pooling, no gzip, single-worker only

### Root Cause: Wrong Sink in Production

During investigation we discovered that `ElasticsearchSink` (the sync/ureq
implementation) is **dead code** in production. The production path is
`ElasticsearchAsyncSink` / `ElasticsearchSinkFactory` (reqwest-based), which has:
- HTTP connection pooling (one `reqwest::Client` per worker)
- Gzip compression support
- Retry-After handling
- Up to 4 concurrent workers via `OutputWorkerPool`
- `@timestamp` injection

The bench binary was accidentally using the wrong sink, so it was measuring a
code path that never runs in production.

### What Is Committed Here

- `crates/logfwd-bench/src/es_throughput.rs` — new throughput bench binary  
  (reads ES credentials from `ES_ENDPOINT` / `ES_API_KEY` env vars)
- `crates/logfwd-bench/Cargo.toml` — adds `pprof`, `flate2`, `[[bin]] es-throughput`

**Known limitation:** `es_throughput.rs` still uses `ElasticsearchSink` (sync).
The async rewrite is the primary next step (see below).

---

## Next Steps

### 1. Delete `ElasticsearchSink` (sync) — **PRIORITY**

**File:** `crates/logfwd-output/src/elasticsearch.rs`

The `ElasticsearchSink` struct (lines ~14–314) and its `impl OutputSink` block are
dead code in production. Delete them entirely.

Things that reference `ElasticsearchSink` and need updating:

| Location | Action |
|----------|--------|
| `elasticsearch.rs:14–314` | Delete struct + both `impl` blocks |
| `elasticsearch.rs` imports | Remove `use super::is_transient_error` (only used by sync sink) |
| `lib.rs:19` | Remove `ElasticsearchSink` from `pub use elasticsearch::{...}` |
| `lib.rs:629` | `build_output_sink()` ES arm uses sync sink — replace with an error message telling callers to use `build_sink_factory()` instead |
| `tests/elasticsearch_integration.rs` | Port to async (see §2 below) |
| `tests/elasticsearch_arrow_ipc.rs` | Port to async (see §3 below) |
| `benches/elasticsearch_arrow.rs` | Port to async (see §4 below) |
| `src/es_throughput.rs` | Port to async (see §5 below) |

### 2. Port `elasticsearch_integration.rs` to Async Factory

**File:** `crates/logfwd-output/tests/elasticsearch_integration.rs`

These tests hit a `tiny_http` blackhole server and verify `send_batch` works.
They currently use `ElasticsearchSink::new(...)`. Port them to use
`ElasticsearchSinkFactory` + a `tokio::runtime::Runtime`.

Pattern:
```rust
use logfwd_output::{BatchMetadata, ElasticsearchSinkFactory};
use logfwd_output::sink::{Sink, SinkFactory, SendResult};
use tokio::runtime::Runtime;

let rt = Runtime::new().unwrap();
let factory = ElasticsearchSinkFactory::new(
    "test_es".to_string(),
    endpoint,
    "logs".to_string(),
    vec![],       // headers
    false,        // compress
    stats.clone(),
).unwrap();
let mut sink = factory.create().unwrap();

let result = rt.block_on(sink.send_batch(&batch, &metadata))
    .expect("send_batch failed");
assert!(matches!(result, SendResult::Ok));
```

Add `tokio = { version = "1", features = ["rt-multi-thread"] }` to
`crates/logfwd-output/Cargo.toml` dev-dependencies if not already present.

The blackhole server logic (`start_blackhole`, `handle_bulk_requests`) can remain
unchanged — the async sink sends the same `_bulk` ndjson wire format.

### 3. Port `elasticsearch_arrow_ipc.rs` to Async `query_arrow()`

**File:** `crates/logfwd-output/tests/elasticsearch_arrow_ipc.rs`

All tests in this file are `#[ignore]` (require a local ES instance). They call
`sink.query_arrow(...)` on the sync sink. Two steps:

**Step A — Add `query_arrow` to `ElasticsearchAsyncSink`** (in `elasticsearch.rs`):

```rust
pub async fn query_arrow(&self, query: &str) -> io::Result<Vec<RecordBatch>> {
    let query_body = serde_json::json!({ "query": query });
    let query_bytes = serde_json::to_vec(&query_body).map_err(io::Error::other)?;
    let url = format!("{}/_query", self.config.endpoint);
    let mut req = self.client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/vnd.apache.arrow.stream");
    for (k, v) in &self.config.headers {
        req = req.header(k.clone(), v.clone());
    }
    let response = req.body(query_bytes).send().await.map_err(io::Error::other)?;
    let body = response.bytes().await.map_err(io::Error::other)?;
    let cursor = io::Cursor::new(body);
    let reader = StreamReader::try_new(cursor, None).map_err(io::Error::other)?;
    reader.collect::<Result<Vec<_>, _>>().map_err(io::Error::other)
}
```

`StreamReader` is already imported at the top of `elasticsearch.rs`.

**Step B — Port the test file** to use `ElasticsearchSinkFactory` + tokio for
`send_batch`, and to call `sink.query_arrow(query).await` (or
`rt.block_on(sink.query_arrow(query))`). The `setup_test_data` helper needs the same
factory-based `send_batch` pattern from §2.

Remove the `ureq::get(...)` call in `check_elasticsearch_available()` — replace with
a plain `reqwest::blocking::get(...)` call or a `rt.block_on(reqwest::get(...))`.

### 4. Port `elasticsearch_arrow.rs` Bench to Async

**File:** `crates/logfwd-bench/benches/elasticsearch_arrow.rs`

This is a `[[bench]] harness = false` binary with a `main()`. It uses
`ElasticsearchSink` for indexing and `sink.query_arrow(...)` for querying.

Port `index_batch` and `bench_arrow_query` to use the factory and
`rt.block_on(...)`. The `bench_json_query` function uses `ureq` directly for the
JSON baseline — that can stay as-is (it's just a baseline comparison).

Add tokio to `crates/logfwd-bench/Cargo.toml`:
```toml
tokio = { version = "1", features = ["rt-multi-thread"] }
```

### 5. Rewrite `es_throughput.rs` Bench Binary to Use Async Factory

**File:** `crates/logfwd-bench/src/es_throughput.rs`

Current: uses `ElasticsearchSink` (sync, no pooling, no gzip at the sink level).  
Target: use `ElasticsearchSinkFactory::create()` → `Box<dyn Sink>` with tokio.

The `run_worker` function should become:
```rust
fn run_worker(...) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    
    let factory = ElasticsearchSinkFactory::new(
        format!("es-bench-{worker_id}"),
        es_endpoint(),
        es_index(),
        vec![("Authorization".to_string(), format!("ApiKey {}", es_api_key()))],
        compress,
        stats,
    ).unwrap();
    let mut sink = factory.create().unwrap();
    
    // ...loop...
    let result = rt.block_on(sink.send_batch(&result, &meta))?;
}
```

With `compress = true`, the factory handles gzip automatically — remove the manual
`GzEncoder` code from the bench. With `compress = false`, raw ndjson is sent.

This will measure the actual production code path and give meaningful throughput
numbers.

### 6. Port / Delete Internal Unit Tests in `elasticsearch.rs`

After deleting `ElasticsearchSink`, the `#[cfg(test)] mod tests` block has tests
that need porting:

- **`serialize_batch_basic`** — port to `ElasticsearchAsyncSink::serialize_batch`.
  The output will now include `"@timestamp"` (injected when `observed_time_ns` is set
  in `BatchMetadata`). Update assertions accordingly or suppress injection by adding a
  `@timestamp_str` column to the test schema.

- **`parse_bulk_response_*`** — call `ElasticsearchAsyncSink::parse_bulk_response(body)`
  directly. It's already a `fn(body: &[u8])` static method on the async sink.

- **`empty_batch_produces_empty_output`** — same pattern as `serialize_batch_basic`.

- **Snapshot tests** (`mod snapshot_tests`) — the existing `.snap` files under
  `crates/logfwd-output/src/snapshots/` were generated by the sync sink's
  `serialize_batch` which does not inject `@timestamp`. After porting to the async
  sink the snapshots will include `@timestamp`. Delete the old `.snap` files and
  regenerate with:
  ```
  cargo insta test -p logfwd-output --review
  ```

### 7. Verify Throughput With Real Numbers

After the async rewrite, re-run the bench against the cloud cluster:
```
ES_ENDPOINT=https://... ES_API_KEY=... \
  cargo run -p logfwd-bench --bin es-throughput --release -- 30 4 5000 1
```

Expected improvement vs the sync baseline:
- Connection pooling eliminates DNS re-resolution overhead (~15% CPU savings)
- Gzip reduces wire bytes by ~3–4x (less network time per batch)
- 4 workers pipeline batches in parallel (RTT is the bottleneck, so workers help a lot)
- Estimated: 5–20x improvement in evt/s vs the ~1,900 evt/s baseline

For a localhost ES instance the 1M evt/s target should be reachable.

---

## Architecture Reference

```
ElasticsearchSinkFactory          (keep — production path)
  └─ creates ElasticsearchAsyncSink  (keep — reqwest, gzip, @timestamp injection)
        └─ wrapped in OutputWorkerPool  (up to 4 concurrent workers)

ElasticsearchSink (sync/ureq)    (DELETE — dead code, was never in production)
  └─ wrapped in OnceFactory + SyncSinkAdapter  (max_workers=1)
```

`build_output_sink()` in `lib.rs` was the only production caller of
`ElasticsearchSink`, but `build_sink_factory()` bypasses it for ES and Loki —
so `build_output_sink()`'s ES arm has been unreachable since `build_sink_factory()`
was added.

## Files Changed In This PR

```
crates/logfwd-bench/Cargo.toml       — pprof + flate2 deps, [[bin]] es-throughput
crates/logfwd-bench/src/es_throughput.rs  — new throughput bench binary
HANDOFF.md                           — this file
```
