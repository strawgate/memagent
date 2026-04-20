# Offset & Checkpoint Research

> **Status:** Historical
> **Date:** 2026-03-31
> **Context:** Research on offset semantics and checkpoint tracking for the pipeline state machine.

## Question

What does "offset" mean for each input source type? Should the pipeline
state machine enforce offset contiguity?

## Findings: How shippers track checkpoints

### Filebeat (Go)

- **Offset**: byte position (`int64`). Contiguous within a file.
- **File identity**: inode + device ID (default), fingerprint of first N bytes,
  or file path.
- **Rotation**: tracks by inode, so rename doesn't lose position. New file at
  old path = new entry (new inode).
- **Truncation**: detects `size < offset`, resets to 0.
- **Storage**: JSON registry file mapping file identity → state (offset, inode,
  timestamp, TTL).

### Vector (Rust)

- **File offset**: byte position (`u64`). Contiguous.
- **File identity**: CRC64 of first N bytes (default 256) or dev+inode.
- **Kafka offset**: partition offset (`i64`) per `(topic, partition)`. Has gaps
  (compacted topics, transactional aborts). Checkpointed via consumer group
  protocol or local `store_offset`.
- **Journald**: opaque cursor string from `__CURSOR` field. Has gaps (vacuum,
  rotation).
- **Windows Event Log**: opaque bookmark XML per channel. RecordIds can have
  gaps (purge, wrap).
- **HTTP/socket sources**: no checkpoint. Push-based, sender retries.

### Fluent Bit (C)

- **Offset**: byte position (`int64`) + separate `stream_offset` for
  compressed files.
- **File identity**: inode (SQLite lookup).
- **Rotation**: updates name, sets rotated flag, continues tracking by inode.
- **Truncation**: detects size decrease, seeks to 0, resets offset.
- **Storage**: SQLite database (`in_tail_files` table with name, offset, inode).

### OpenTelemetry Collector (Go)

- **Offset**: byte position (`int64`) + `record_num` (count of records).
- **File identity**: fingerprint (first N bytes content, default 1KB).
- **Rotation**: fingerprint survives rename. Copy/truncate detected via
  fingerprint mismatch.
- **Truncation**: `Validate()` checks `size < offset`, invalidates reader.
- **Storage**: protobuf checkpoint via storage extension.

## Input types and their natural checkpoints

| Input Type | Checkpoint Value | Type | Gaps? | Notes |
|-----------|-----------------|------|-------|-------|
| File tail | byte offset | `u64` | No | Contiguous within a file |
| Kafka | partition offset | `i64` | Yes | Compaction, txn aborts |
| OTLP receiver | none | — | N/A | Push: per-request ack |
| TCP/UDP syslog | none | — | N/A | Push: no resumption |
| Journald | cursor string | `String` | Yes | Opaque, survives vacuum |
| Windows Event Log | bookmark XML | `String` | Yes | Opaque, survives purge |

## Key insight

**The checkpoint is not a universal numeric offset.** Four different
types exist:

1. **Byte offset** (files) — contiguous u64, meaningful for sequential reads
2. **Partition offset** (Kafka) — i64 with gaps, per (topic, partition)
3. **Opaque cursor** (journald, Windows) — string, not comparable
4. **None** (push sources) — no checkpoint, sender retries

All four shippers converge on the same architecture: the checkpoint store
is a map from **source identity → opaque bytes**. Each input plugin owns
its checkpoint format. The pipeline doesn't interpret checkpoint values.

## Design decision

The pipeline state machine should:

1. **Track batch completion** via `BatchId` (sequential, no gaps, owned by
   the machine). This is what ordered ACK depends on.
2. **Store checkpoints as opaque data** — `Vec<u8>` or a trait object. The
   pipeline passes them through to the checkpoint store without interpreting
   them.
3. **Not enforce offset contiguity** — that's the input plugin's concern.
   Kafka has gaps. Journald has opaque cursors. Files are contiguous but
   that's enforced by the file reader, not the pipeline.
4. **Source identity** is also opaque to the pipeline. File inputs use
   fingerprint/inode. Kafka uses (topic, partition). Push sources may not
   have persistent identity at all.

The pipeline's job is: "when batch N for source S is durably delivered,
persist checkpoint C for source S." It doesn't need to know what C means.

## When does tracking begin?

Additional research into when shippers enter the "must be acked" state:

| System | Tracking entry point | Pre-tracking safe to drop? |
|--------|---------------------|---------------------------|
| Filebeat | `eventListener.AddEvent()` inside `client.Publish()` | Yes — harvester read to Publish is untracked |
| Vector | `BatchNotifier::apply_to()` attaches finalizers | Yes — event creation to notifier is untracked |
| OTel Collector | `ConsumeLogs()` call entry (synchronous) | Essentially no pre-tracking window |
| Fluentd | `chunk.commit` inside `buffer.write()` | Yes — emit to commit is untracked |

**Pattern:** Tracking begins at the point of ownership transfer to the
pipeline, not at creation. This directly informed our design: `create_batch`
returns a lightweight token (no tracking), `begin_send` registers in
the machine's in-flight set. Dropping a Queued ticket is safe.

## References

- Filebeat registry: `filebeat/input/file/state.go`
- Vector checkpointer: `lib/file-source-common/src/checkpointer.rs`
- Vector Kafka: `src/sources/kafka.rs`
- Fluent Bit tail: `plugins/in_tail/tail_file_internal.h`
- OTel filelog: `pkg/stanza/fileconsumer/internal/checkpoint/`
