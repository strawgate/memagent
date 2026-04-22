# Adapter Contract

This document freezes the correctness contract at the system edges.

It is the canonical contract doc for I/O and control-plane boundaries.

It complements [SCANNER_CONTRACT](SCANNER_CONTRACT.md). The scanner contract
defines what `Scanner` expects and produces. This document defines what the
OTLP, file, diagnostics, and pipeline adapters must preserve before data
reaches the scanner and after data leaves the `RecordBatch` pipeline.

## Scope

In-scope:

- OTLP HTTP input (`crates/logfwd-io/src/otlp_receiver.rs`)
- OTLP output (`crates/logfwd-output/src/otlp_sink.rs`)
- File input + framing + checkpointed delivery
- diagnostics/readiness/status surfaces
- pipeline checkpoint and output retry semantics

Out-of-scope for now:

- A true file sink. The repo does not yet have a first-class file output path,
  so the durability-critical file contract today is file input plus
  checkpointed delivery.

## Why this exists

Most recurring IO bugs in this repo have been boundary bugs:

- protocol-path drift between OTLP JSON, protobuf, and compression paths
- file lifecycle races around truncate, rotate, EOF, and restart
- checkpoint bugs where offsets move past bytes that are still buffered

The contract below exists so we can test invariant classes instead of adding
one regression test per incident.

## How To Read This Doc

Each guarantee should name:

- the boundary it applies to
- the current behavior or intended invariant
- the main enforcement mechanism
- important gaps or caveats

Enforcement labels:

| Label | Meaning |
|-------|---------|
| `Proof` | Enforced by Kani or model checking |
| `Proptest` | Enforced by property-based tests |
| `Integration test` | Enforced by deterministic integration or end-to-end tests |
| `Runtime check` | Enforced by code path checks or bounded behavior in production code |
| `Documented intent` | Desired contract called out in docs, but not yet strongly enforced |
| `TODO` | Not yet adequately enforced |

## OTLP Receiver Contract

The OTLP receiver accepts `POST /v1/logs` over HTTP and produces
scanner-ready JSON lines.

### Transport rules

- Only `POST /v1/logs` is accepted.
- Unsupported paths return `404`.
- Wrong methods return `405`.
- Bodies over the configured hard cap are rejected with `413`.
- Supported encodings are `identity`, `zstd`, and `gzip`.
- Malformed JSON, malformed protobuf, or malformed OTLP JSON field encodings
  are rejected with `400`.
- Backpressure returns `429`.
- A disconnected pipeline returns `503`.

### Diagnostics accounting rules

- Input diagnostics must charge OTLP request bytes from the accepted request
  body size at the receiver boundary as received on the wire, not from
  downstream Arrow memory estimates or post-decompression payload size.
- Legacy OTLP JSON-lines ingress and structured OTLP batch ingress must report
  the same `lines_total` and same `bytes_total` for the same accepted
  request body.
- Rejected OTLP payloads must not increment `lines_total` or `bytes_total`.
- Malformed OTLP payloads must increment input `parse_errors_total`.
- Transport and request-handling failures (read errors, unsupported encodings,
  disconnected downstream channel, oversized compressed bodies) must increment
  input `errors_total`.

### Semantic field rules

The receiver must preserve these semantic roles when they are present and
valid:

- OTLP HTTP/JSON uses `timeUnixNano`; generated/protobuf code may expose the
  same semantic field as `time_unix_nano`. Both map to `timestamp`, but
  `time_unix_nano` is not itself an accepted HTTP/JSON key.
- `severityText` -> `level`
- `body` -> `message`
- `traceId` -> `trace_id`
- `spanId` -> `span_id`
- resource attributes stay flattened as top-level JSON keys
- primitive log attributes stay primitive in emitted JSON:
  - string -> JSON string
  - int -> JSON number
  - double -> JSON number
  - bool -> JSON boolean
  - bytes -> lowercase hex string

### Equivalence rule

Semantically equivalent OTLP JSON and protobuf requests must decode to the
same emitted JSON-line semantics for the supported field set.

This is stronger than “both requests return 200”. It means the receiver must
not silently diverge by format path.

## Arrow IPC Receiver Contract

The Arrow IPC receiver accepts `POST /v1/arrow` and forwards decoded
`RecordBatch`es directly into the pipeline channel.

### Delivery rules

- If every non-empty batch in the request is accepted, return `200`.
- If the remaining channel capacity cannot fit every non-empty batch in the
  request, return `429` before enqueueing any request batch.
- A `429` on this path means zero batches from that request were accepted, so
  the client can retry the same request without duplicating an accepted prefix.
- A `500` means the receiver detected an internal channel-accounting invariant
  violation after reserving capacity. The request outcome is unknown and may be
  partially processed; clients must not automatically resend the same request
  unless they have application-level deduplication.
- A disconnected pipeline channel returns `503`.

## OTLP Sink Contract

The OTLP sink accepts a `RecordBatch` and emits a valid
`ExportLogsServiceRequest`.

### Role mapping rules

The sink resolves semantic roles by column meaning, not by suffix tricks:

- timestamp role: `timestamp | time | ts`
- severity role: `level | severity | log_level | loglevel | lvl`
- body role: `message | msg | _msg | body`
- trace role: `trace_id`
- span role: `span_id`
- flags role: `flags | trace_flags`

Other columns are attributes, dispatched by Arrow `DataType`.

### Preservation rules

- `Int64`, `Float64`, and `Boolean` attributes must stay typed OTLP attributes.
- `trace_id` and `span_id` must be encoded as OTLP binary fields, not as plain
  attributes.
- Conflict structs must be normalized before encoding so mixed-type columns are
  not silently dropped.

### Loopback rule

For the supported field set, a batch emitted by `OtlpSink` and immediately
ingested by `OtlpReceiverInput` must preserve the same semantics.

This is the cross-crate compatibility rule that catches sink/receiver drift.

## Input Source Metadata Contract

Source metadata belongs to the table assembly boundary, not to transport bytes.

### Source identity rules

- Inputs that can distinguish logical row sources must carry `SourceId` beside
  the data, not by writing identity fields into the payload.
- UDP inputs derive a stable sender-scoped `SourceId` from the `recv_from()`
  sender address and attach it to `InputEvent::Data.source_id`. They must not
  inject cleartext peer-address fields into the payload.
- `FramedInput` may reclaim per-source state once newline remainder and
  stateful format decoders are idle. It must retain state while a source has
  a pending partial line or CRI partial record.

### Source metadata rules

- Source metadata (`__source_id`, public style columns such as `file.path` or
  `log.file.path`, future fields) must be
  attached as Arrow columns **post-scan**, not injected into raw bytes pre-scan.
- There is no legacy exception. File paths, row source ids, UDP
  sender identity, and future source descriptors must travel as sidecar/source
  metadata and be materialized only when assembling the `RecordBatch` presented
  to SQL.
- Canonical pattern for row-native metadata: carry source-origin spans beside
  scanner-ready chunks, then attach `__source_id` for `source_metadata:
  fastforward` or public style columns for `source_metadata: ecs`, `otel`, or
  `vector` after scan and before SQL. Rich descriptors belong in cold-path
  enrichment tables such as `sources`.
- Runtime should not pay this cost by default. Config-driven inputs must opt in
  with a non-`none` `source_metadata` style.
- Inputs that support public source path styles must expose only bounded
  current-source snapshots through `InputSource::source_paths()`: file inputs
  use filesystem paths and S3 inputs use object keys. Inputs without such
  snapshots must reject public path styles instead of materializing null
  metadata columns.
- SQL has no hidden-source-column behavior. `SELECT *` returns the columns in
  the Arrow table. User-facing sinks drop known FastForward internal fields
  such as `__source_id` by default, and public styles such as ECS/Beats, OTel,
  and Vector are emitted as normal columns. User payload fields that happen to
  start with `__` are not treated as internal.
- Resource attributes are separate output semantics. Use
  `resource.attributes.*` for explicit OTLP resource columns, not as the only
  source metadata contract.
- Rationale: raw injection mutates user data, causes format-specific edge
  cases (invalid JSON for empty objects), and is 30x slower than post-scan
  column attachment (PR #1370 prototype measurements).

## File Input Contract

The file path is:

`FileTailer -> FileInput -> FramedInput -> scanner-ready lines`

### Source identity rules

- Each tracked file has its own `SourceId`.
- Remainders, CRI aggregation state, and checkpoint math are keyed by source.
- Bytes from one source must never complete or corrupt a partial line from a
  different source.
- UDP datagrams must preserve sender-scoped source identity at the input-event
  boundary (`recv_from` sender -> `InputEvent::Data.source_id`) so framing
  state is isolated by sender instead of co-mingling all datagrams under
  `None`.

### Newline-boundary checkpoint rules

- A checkpoint may advance only to the last complete newline boundary.
- Buffered remainder bytes are not checkpointable yet.
- When EOF flushes a remainder, or a remainder is explicitly discarded after a
  documented lifecycle event, the checkpoint may then advance.

### Lifecycle rules

- Rotate, truncate, delete/recreate, and EOF/stall notifications are explicit events.
- `Truncated` must be emitted before post-truncate data so framing state can be
  cleared safely.
- Every `InputSource` implementation must define its control-plane `health()`
  semantics explicitly; input lifecycle truth must not rely on a trait-level
  default.
- The current file tailer emits `EndOfFile` only after both:
  two consecutive no-data polls for a source, and
  an idle-duration gate (derived from `poll_interval_ms`) has elapsed.
  This avoids flushing mid-line fragments too aggressively on transient stalls
  while still providing a bounded flush path for trailing partial lines.
- Fresh data resets both the poll streak and idle timer, allowing a later
  `EndOfFile` signal after a new sustained no-data period.
- Graceful shutdown is a terminal lifecycle event, not a transient stall. During
  shutdown, file input may bypass the normal idle EOF gate for already tracked
  active files, perform one bounded read, and emit `EndOfFile` only for files
  whose tracked offset has caught up to the current file size so `FramedInput`
  can flush per-source remainders before the runtime drains input channels.
- Shutdown drain must not discover unrelated new glob matches; it drains active
  files and pending watcher transitions.
- Tailer watcher/file I/O error bursts that trigger poll backoff should surface
  as `degraded` control-plane health, and a later clean poll should recover the
  file input to `healthy`.
- TCP inputs should surface `degraded` when they are actively shedding load,
  such as dropping accepts past the configured `max_clients` limit or hitting the per-poll read
  budget, and recover to `healthy` after a clean poll.
- UDP inputs should surface `degraded` when the kernel reports receive-buffer
  pressure (`ENOBUFS`/`ENOMEM`) and recover to `healthy` after a clean poll.
- `FramedInput` should forward the wrapped input's health rather than inventing
  its own parallel lifecycle policy.
- Generator-style inputs with no independent bind/startup/shutdown lifecycle may
  report steady `healthy`, but that choice must still be explicit in code.

### Delivery semantics

- Goal: no silent corruption and no silent loss outside documented
  copytruncate windows.
- Duplicate delivery is acceptable when required for at-least-once recovery.
- Copytruncate is explicitly documented as a race window, not treated as an
  exactly-once path.
- Successful delivery advances checkpoints.
- Explicit permanent sink rejection may also advance checkpoints; this is the
  deliberate escape hatch for malformed or otherwise non-retriable data.
- Retry exhaustion, dispatch failure, timeout, and other control-plane failures
  must not advance checkpoints past undelivered data.
- The current worker/pipeline seam does not yet retain enough batch payload
  state to requeue held batches in-process, so non-advancing failures are
  replayed on restart if shutdown force-stops with unresolved tickets.

## Diagnostics And Control-Plane Contract

The control plane is part of the boundary contract now that `/live`, `/ready`,
and rich component health snapshots exist.

### Endpoint meanings

- `/live` reports process/control-plane liveness only.
- `/ready` means safe-to-accept-work.
- `/admin/v1/status` is the canonical rich status surface.

### Health vocabulary

- `starting` means a component exists but is not yet ready.
- `healthy` means the component is ready for work.
- `degraded` means the component is alive but under retry/pressure.
- `stopping` means shutdown or drain has started and readiness should fail.
- `stopped` means the component has finished shutting down and is not ready.
- `failed` means the component hit a fatal condition and is not ready.
- output startup success must not clear an already-`degraded` retrying state;
  only a later successful delivery may recover that output to `healthy`.
- output health should be aggregated from live worker-local slots rather than
  a single shared scalar; one worker becoming `healthy` must not clear a worse
  state still held by another live worker.
- once drain begins, the pool-level `stopping` or `stopped` phase must remain
  visible even if an in-flight worker later reports delivery success.
- per-worker sink shutdown errors during idle exit or scale-in should count as
  output errors, but must not by themselves make the shared output health
  permanently `failed`.

The stable ordering is:

`healthy < degraded < starting < stopping < stopped < failed`

Status roll-ups use that ordering and keep the less-ready state. `starting` is
therefore less ready than `healthy` or `degraded`, and aggregate status must
not hide startup behind an otherwise healthy component.

### Readiness rules

- `degraded` is visible in status and does not fail readiness by default.
- `starting`, `stopping`, `stopped`, and `failed` fail readiness.
- quiet sources must not fail readiness by themselves.
- readiness reflects the currently wired health sources, not speculative future ones.

### Status-snapshot rules

- top-level status snapshots should carry:
  - a state
  - a machine-stable reason
  - `observed_at_unix_ns`
- aggregated component-health snapshots should also carry `readiness_impact`
  so control-plane consumers do not have to infer whether a visible state is
  gating readiness.
- status roll-ups should be deterministic and local to the semantic seam that owns them.
- HTTP shell code should not own the policy for deciding what health means.
- input `bytes_total` is a source-accounting metric, not an Arrow-memory
  metric. Structured receivers must propagate source payload size explicitly
  rather than deriving bytes from `RecordBatch::get_array_memory_size()`.

## Verification Mapping

This architecture relies on multiple verification layers.
The canonical strategy lives in [VERIFICATION.md](VERIFICATION.md).
This section maps the I/O contracts to the expected enforcement style.

| Contract class | Preferred enforcement |
|----------------|-----------------------|
| Pure helper invariants and local state transitions | Kani |
| Stateful boundary sequences with many interleavings | proptest and deterministic regression tests |
| Protocol/lifecycle rules independent of implementation shape | TLA+ |
| Distributed, crash, timeout, and retry fault scenarios | Turmoil |
| Wire-level OTLP semantic equivalence | loopback tests and oracle checks |
| Performance goals | disciplined benchmark validation |

This table is normative for planning,
not a claim that every item is already complete on `main`.

## Update Rules

- Any PR that changes receiver, pipeline, sink, checkpoint, file lifecycle, or
  diagnostics/control-plane semantics should update this document in the same PR.
- If a stronger guarantee is not yet enforced, mark it as `Documented intent`
  or `TODO` rather than overstating certainty.
- If a more detailed subsystem contract already has a canonical document, link
  to it instead of duplicating it here.

## What still needs to grow

- gRPC OTLP contract coverage
- fixture corpora for valid/invalid OTLP payloads
- replay timelines for rotate/create, copytruncate, delete/recreate, and restart
- benchmark guardrails proving the contract checks stay within budget
