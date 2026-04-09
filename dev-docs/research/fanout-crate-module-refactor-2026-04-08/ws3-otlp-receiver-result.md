# Workstream 3 Result — Split `otlp_receiver` in `logfwd-io`

## Recommendation label

**Recommend: Merge with caution** (behavior-preserving modular extraction, but full `otlp_receiver` test filter is currently impacted by environment proxy behavior in this runner).

## Module decomposition chosen

I split `crates/logfwd-io/src/otlp_receiver.rs` into a thin orchestration module plus focused submodules under `crates/logfwd-io/src/otlp_receiver/`:

1. **`otlp_receiver.rs` (orchestration + lifecycle seam)**
   - owns receiver state structs (`OtlpReceiverInput`, `ReceiverPayload`, `OtlpServerState`)
   - owns bind/spawn runtime setup
   - owns `Drop` and `InputSource` impls (poll/health bridge)
   - wires route handler from `server` module

2. **`otlp_receiver/server.rs` (HTTP transport + request handling seam)**
   - request body size and content-encoding gatekeeping
   - content-type path split (JSON/protobuf)
   - response/error mapping (`400/404/405/413/415/429/503` semantics preserved)
   - diagnostics increments (`errors`, `parse_errors`) and health transitions on backpressure/disconnect

3. **`otlp_receiver/decode.rs` (decode pipeline seam)**
   - compression decode helpers (`zstd`, `gzip`, capped decompression)
   - OTLP JSON decode path to NDJSON / structured batch payloads
   - OTLP protobuf decode path to NDJSON / structured batch payloads
   - mode wrapper that preserves existing byte-accounting semantics (`accounted_bytes` from received body size)

4. **`otlp_receiver/convert.rs` (field mapping + JSON emission + batch conversion seam)**
   - protobuf request -> NDJSON conversion
   - protobuf request -> `RecordBatch` conversion
   - AnyValue flattening/mapping, numeric parsing, base64 bytes decode, and JSON writer helpers
   - Kani verification module and local minimal hex helper retained in-module

5. **`otlp_receiver/tests.rs` (test seam)**
   - moved all OTLP receiver unit/property/regression tests out of orchestration module

## Alternative considered

A second viable split was to isolate all numeric parsing + JSON writer primitives into an additional `wire.rs` (or `json_emit.rs`) and make both `decode` and `convert` depend on it. I did **not** pick that in this pass because it would have increased cross-module movement and review churn; this workstream prioritized minimal behavior-preserving extraction over deeper internal API redesign.

## Changed file list

- `crates/logfwd-io/src/otlp_receiver.rs`
- `crates/logfwd-io/src/otlp_receiver/server.rs` (new)
- `crates/logfwd-io/src/otlp_receiver/decode.rs` (new)
- `crates/logfwd-io/src/otlp_receiver/convert.rs` (new)
- `crates/logfwd-io/src/otlp_receiver/tests.rs` (new)

## Behavior/contract risk notes

### Byte accounting

- Preserved: `accounted_bytes` continues to be captured from the accepted HTTP body bytes before decode normalization, then propagated through `ReceiverPayload` into `InputEvent`.

### Error mapping

- Preserved in `server.rs`: content-encoding/header/body/decode/channel errors map to the same status classes used previously; parse failures continue to charge `parse_errors` while transport/handling failures charge `errors`.

### Health transitions

- Preserved: healthy on successful send, degraded on channel-full backpressure, failed on disconnected pipeline while running.

### Test seam exposure

- Root module now explicitly re-exports decode/convert internals for test-module access (`#[cfg(test)] use ...::*`), preserving existing test behavior without broad public API changes.

## Tests/check outcomes

Commands run:

1. `cargo fmt --all` ✅
2. `cargo test -p logfwd-io otlp_receiver` ⚠️
   - Build succeeded and test execution started.
   - 54 passed, 8 failed, 2 ignored.
   - Failing tests in this environment all showed HTTP client proxy connect failures (`CONNECT proxy failed: proxy server responded 403/403`) on loopback `ureq` requests; failures are environment-related rather than compile/runtime panics in the refactor itself.
3. `cargo check -p logfwd-io` ✅
   - completed successfully.

## What evidence would change my mind

I would change the recommendation from **"Merge with caution"** to **"Merge"** if either of the following is observed in a clean environment:

1. `cargo test -p logfwd-io otlp_receiver` passes without proxy-induced loopback HTTP failures.
2. A CI run with the same branch shows receiver HTTP-path tests passing across the moved seams (especially status/health/diagnostics regressions).

Conversely, I would change to **"Do not merge yet"** if any of these appear:

- divergence in `bytes_total` / `lines_total` accounting across JSON-lines vs structured mode,
- status-code drift on `/v1/logs` regressions (`405`, `429`, `503` paths),
- protobuf-vs-JSON semantic mismatch in existing equivalence tests.
