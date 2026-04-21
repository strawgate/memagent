# Research: Output Encode/Compress Worker Separation (Issue #1408)

## Context

The input side of logfwd has I/O/compute separation: `io_worker.rs` handles blocking file reads on an OS thread, `cpu_worker.rs` handles scanning/transforms on another. But the output side encodes and compresses inline within each output worker — there is no separate encode/compress worker pool.

Issue #1408 asks whether outputs should get the same treatment.

## What to investigate

1. Read current output encoding paths:
   - `crates/logfwd-output/src/otlp_sink/encode.rs` — OTLP protobuf encoding
   - `crates/logfwd-output/src/otlp_sink/send.rs` — compression + send
   - `crates/logfwd-output/src/elasticsearch/serialize.rs` — NDJSON serialization
   - `crates/logfwd-output/src/elasticsearch/transport.rs` — compression + HTTP
   - `crates/logfwd-output/src/loki.rs` — label grouping, snappy compression, HTTP
   - `crates/logfwd-output/src/file_sink.rs` — JSON line serialization

2. Read the worker pool model:
   - `crates/logfwd-runtime/src/worker_pool/pool.rs` — how workers are dispatched
   - `crates/logfwd-runtime/src/worker_pool/dispatch.rs` — dispatch helpers
   - How does the current worker model handle concurrency?

3. Profile the encode vs I/O split:
   - For OTLP: what fraction of `send_batch` time is encode vs HTTP?
   - For ES: what fraction is NDJSON serialize vs HTTP?
   - For Loki: what fraction is label grouping + snappy vs HTTP?
   - Use the existing benchmark infrastructure in `crates/logfwd-bench/`

4. Design encode worker separation:
   - Would a shared encode thread pool make sense?
   - Or per-output encode workers?
   - How does ack aggregation change?
   - What happens to the delivery contract?

## Deliverable

Write a research report to `dev-docs/research/fanout-2026-04-21/ws07-output-encode-workers-report.md` containing:

1. Current encode/send time breakdown per output type
2. Architecture proposal: encode worker pool design
3. Ack aggregation changes needed
4. Expected throughput improvement
5. Implementation plan with file paths
6. Risk assessment: what could go wrong?

Do NOT implement code changes. Research and design only.
