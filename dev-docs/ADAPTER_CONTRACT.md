# Adapter Contract

This document freezes the correctness contract at the system edges.

It complements [SCANNER_CONTRACT](SCANNER_CONTRACT.md). The scanner contract
defines what `Scanner` expects and produces. This document defines what the
OTLP and file adapters must preserve before data reaches the scanner and after
data leaves the `RecordBatch` pipeline.

## Scope

In-scope:

- OTLP HTTP input (`crates/logfwd-io/src/otlp_receiver.rs`)
- OTLP output (`crates/logfwd-output/src/otlp_sink.rs`)
- File input + framing + checkpointed delivery

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

## OTLP Receiver Contract

The OTLP receiver accepts `POST /v1/logs` over HTTP and produces
scanner-ready JSON lines.

### Transport rules

- Only `POST /v1/logs` is accepted.
- Unsupported paths return `404`.
- Wrong methods return `405`.
- Bodies over the configured hard cap are rejected with `413`.
- Supported encodings are `identity` and `zstd`.
- Malformed JSON, malformed protobuf, or malformed OTLP JSON field encodings
  are rejected with `400`.
- Backpressure returns `429`.
- A disconnected pipeline returns `503`.

### Semantic field rules

The receiver must preserve these semantic roles when they are present and
valid:

- OTLP HTTP/JSON uses `timeUnixNano`; generated/protobuf code may expose the
  same semantic field as `time_unix_nano`. Both map to `timestamp_int`, but
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

## File Input Contract

The file path is:

`FileTailer -> FileInput -> FramedInput -> scanner-ready lines`

### Source identity rules

- Each tracked file has its own `SourceId`.
- Remainders, CRI aggregation state, and checkpoint math are keyed by source.
- Bytes from one source must never complete or corrupt a partial line from a
  different source.

### Newline-boundary checkpoint rules

- A checkpoint may advance only to the last complete newline boundary.
- Buffered remainder bytes are not checkpointable yet.
- When EOF flushes a remainder, or a remainder is explicitly discarded after a
  documented lifecycle event, the checkpoint may then advance.

### Lifecycle rules

- Rotate, truncate, delete/recreate, and terminal EOF are explicit events.
- `Truncated` must be emitted before post-truncate data so framing state can be
  cleared safely.
- Only terminal EOF may flush a trailing partial line; transient “no new bytes
  right now” states must not flush buffered partial lines.

### Delivery semantics

- Goal: no silent corruption and no silent loss outside documented
  copytruncate windows.
- Duplicate delivery is acceptable when required for at-least-once recovery.
- Copytruncate is explicitly documented as a race window, not treated as an
  exactly-once path.

## Executable Enforcement

These contracts are enforced by a ladder, not by a single test type:

- Kani for pure OTLP helper logic and checkpoint arithmetic
- unit tests for per-source remainder and CRI isolation
- integration tests for:
  - OTLP JSON/protobuf/zstd semantic equivalence
  - sink-to-receiver loopback
  - newline-boundary checkpoint replay
  - multi-file remainder isolation with real `SourceId`s
- compliance and simulation tests for file lifecycle and checkpoint ordering

## Current Required Tests

The contract suite currently includes these anchor tests:

- `protobuf_and_json_inputs_match_semantics`
- `json_bytes_value_matches_protobuf_semantics`
- `otlp_receiver_preserves_semantics_across_json_protobuf_and_zstd`
- `otlp_output_to_input_loopback_preserves_semantics_under_zstd`
- `checkpoint_advances_only_at_real_newline_boundaries`
- `glob_sources_keep_partial_lines_and_checkpoints_isolated`

## What still needs to grow

- gRPC OTLP contract coverage
- fixture corpora for valid/invalid OTLP payloads
- replay timelines for rotate/create, copytruncate, delete/recreate, and restart
- benchmark guardrails proving the contract checks stay within budget
