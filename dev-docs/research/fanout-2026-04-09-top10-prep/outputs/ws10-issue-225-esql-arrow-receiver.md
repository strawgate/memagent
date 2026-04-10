# WS10 #225 — Elasticsearch ES|QL Arrow Receiver MVP path

Date: 2026-04-09  
Scope: prototype + implementation planning, aligned to existing receiver/runtime architecture

## TL;DR

Primary recommendation: **ship `input.type: arrow_ipc` MVP as an HTTP Arrow IPC receiver with fixed `POST /v1/arrow` semantics now**, then layer ES|QL-specific transport ergonomics later (path/method/auth/query polling). This keeps receiver lifecycle/health/backpressure behavior consistent with current `http` and `otlp` input patterns while immediately unblocking issue `#225` from research-only to executable MVP.

This workstream includes a minimal implementation path and tests:
- `arrow_ipc` input is now accepted by config validation when `listen` is present.
- runtime input builder now instantiates the Arrow IPC receiver.
- receiver now implements `InputSource`, emitting structured `InputEvent::Batch` events.

## Required context inspection summary

I validated the MVP shape against the required files:

- `dev-docs/ARCHITECTURE.md`: confirms input threads push events to runtime pipeline and that structured batches are first-class via `InputEvent::Batch`.
- `crates/logfwd-io/src/http_input.rs` and `crates/logfwd-io/src/receiver_http.rs`: reused HTTP receiver conventions (bounded channel, body size limits, `declared_content_length`, `read_limited_body`, health transitions).
- `crates/logfwd-runtime/src/pipeline/`: verified `InputEvent::Batch` is already handled by both threaded and turmoil paths via `transform_direct_batch_for_send`.
- `crates/logfwd-config/src/`: verified `InputType::ArrowIpc` existed but was intentionally blocked by validation.
- `book/src/config/reference.md`: updated config surface/status for `arrow_ipc` input.

## Existing architecture fit (why MVP is feasible now)

The repo already has the key seams for Arrow ingress:

1. **Receiver exists**: `logfwd-io` already had `arrow_ipc_receiver.rs` with HTTP + Arrow stream decode + backpressure handling.
2. **Pipeline supports structured ingress**: `InputEvent::Batch` already bypasses scanner and enters SQL/output path directly.
3. **Health/lifecycle patterns exist**: receiver health reducer + background task lifecycle already implemented and used by other receivers.

So this is no longer a speculative greenfield effort; it is mostly integration and contract-hardening.

## MVP scope (implemented)

### In scope now

- Enable `input.type: arrow_ipc` in config validation.
- Require `listen` for `arrow_ipc` input.
- Build Arrow IPC receiver in runtime input construction.
- Emit structured `InputEvent::Batch` with source accounting metadata.
- Keep transport behavior aligned with current receiver contract:
  - `200` on full acceptance
  - `429` on channel backpressure (partial accept possible)
  - `503` on disconnected pipeline

### Explicitly out of scope (future expansion)

- ES|QL poller/client mode (calling Elasticsearch `_query?format=arrow`).
- Auth/TLS blocks on `arrow_ipc` input.
- Configurable route/path/method for Arrow IPC receiver.
- End-to-end dedupe/idempotency protocol for partial-accept retries.
- Schema normalization policy specific to ES|QL type drift.

## Config surface and validation requirements

### MVP config surface

```yaml
input:
  type: arrow_ipc
  listen: 0.0.0.0:4319
```

MVP validation rules:
- `listen` is required.
- reject input fields that do not apply to this receiver mode:
  - `path`, `max_open_files`, `glob_rescan_interval_ms`, `poll_interval_ms`,
    `read_buf_size`, `per_file_read_budget_bytes`, `adaptive_fast_polls_max`,
    `format`, `http`, `generator`, `sensor`, `tls`.

### Why this is the right minimum

This mirrors existing type-specific validation strategy and keeps `arrow_ipc` transport/lifecycle semantics narrow and operationally clear.

## Failure and observability behavior expectations (MVP)

### Request/result semantics

- **200 OK**: all non-empty decoded batches accepted into receiver channel.
- **429 Too Many Requests**: receiver channel became full; partial acceptance may have already happened; caller must retry with at-least-once expectations.
- **503 Service Unavailable**: downstream disconnected.
- **400 Bad Request**: invalid Arrow stream/zstd/decode or malformed headers.
- **413 Payload Too Large**: body exceeds configured hard limit (currently fixed in receiver).

### Health model

- healthy when accepting deliveries.
- degraded on backpressure events.
- failed on fatal runtime/server failures or disconnected downstream.
- stopping/stopped on shutdown path.

### Metrics/accounting

`InputEvent::Batch.accounted_bytes` should represent accepted request payload bytes. MVP approximates this by distributing request body bytes across non-empty decoded batches per request.

## Module touch points (implemented)

1. `crates/logfwd-io/src/arrow_ipc_receiver.rs`
   - Added `InputSource` implementation.
   - Receiver channel now carries decoded batch + accounted byte metadata.
   - Preserved existing HTTP/backpressure/health contract.

2. `crates/logfwd-runtime/src/pipeline/input_build.rs`
   - Added `InputType::ArrowIpc` runtime construction path.
   - Enforced supported-format policy parity (json-only config marker for structured ingress family).

3. `crates/logfwd-config/src/validate.rs`
   - Removed hard rejection for `arrow_ipc` input.
   - Added `listen` requirement and field compatibility checks for `arrow_ipc`.

4. `crates/logfwd-config/src/lib.rs`
   - Updated tests from “always rejected” to “requires listen / valid when listen provided”.

5. `book/src/config/reference.md`
   - Updated `arrow_ipc` input section from “not supported” to implemented MVP behavior.

## Alternative integration shape considered

## Alternative A: ES|QL pull input (`type: esql_arrow`)

Description:
- logfwd would actively call Elasticsearch ES|QL endpoints (e.g. `_query?format=arrow`) on a schedule, decode Arrow responses, then inject batches.

Pros:
- direct UX for ES users (single config with endpoint/auth/query).
- can align retries/checkpointing with query windows.

Cons:
- broader scope: auth/TLS/secrets, polling scheduler, cursor/window semantics, query failure policy, duplicate suppression strategy.
- less aligned with existing “receiver-style” input lifecycle in this MVP timeframe.

### What would change my mind toward Alternative A first

I would switch recommendation if one of these becomes true:
1. Product requirement is explicitly “memagent must independently query Elasticsearch” (no external forwarder allowed).
2. Operational owners require server-initiated pull due to network policy (no push allowed into memagent).
3. Team accepts larger MVP scope and can fund poller/checkpoint/dedupe work in the same milestone.

## Primary recommendation

Proceed with current implemented shape as the **issue #225 MVP**:
- `arrow_ipc` receiver enabled end-to-end with existing transport/lifecycle semantics.
- Document ES|QL as a producer-compatible Arrow stream source for this endpoint.
- Track next phase as separate work item for `esql_arrow` pull mode or configurable Arrow receiver route/auth.

This gives a low-risk bridge from research-needed → operational MVP while preserving architectural consistency.

## Suggested follow-up backlog (post-MVP)

1. Configurable Arrow receiver route/method (`arrow_ipc.path`, optional strict path).
2. Receiver auth/TLS support for production perimeter compatibility.
3. Optional idempotency key header + dedupe contract to mitigate partial-accept duplicates.
4. ES|QL poller input (`type: esql_arrow`) with explicit checkpoint window semantics.
5. End-to-end integration tests with Elasticsearch ES|QL Arrow responses (fixture or dockerized).
