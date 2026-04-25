# Zero-Copy Pipeline Design

> **Status:** Active
> **Date:** 2026-04-21
> **Context:** retain the deep-dive research behind the zero-copy framing and buffer-lifecycle direction outside the canonical architecture layer.

> **See `ARCHITECTURE.md` for the current high-level pipeline overview.**
> This document is the deep-dive research behind the zero-copy design.

Based on research of Vector, Fluent Bit, OTel Collector, simdjson,
and simd-json. See issue #302 for the original discussion.

## Current architecture (copies highlighted)

```
Tailer reads into read_buf: [u8; 8192]
  → COPY 1: extend_from_slice into result: Vec<u8> (new alloc per read)
  → FramedInput::process(&bytes, &mut json_buf)
    → COPY 2: extend_from_slice every line into json_buf: Vec<u8>
    → partial: Vec<u8> holds tail bytes (copy on every partial)
  → json_buf → Bytes (move, not copy)
  → Scanner receives Bytes
    → streaming structural classifier (zero-copy bitmasks)
    → StreamingBuilder (zero-copy offset/len views)
    → Arrow StringViewArray (references Bytes)
```

Two unnecessary copies before the scanner. The scanner and Arrow
layer are already zero-copy. The problem is upstream.

## Target architecture

```
Tailer reads into BytesMut (reusable, per-file)
  → freeze() → Bytes (refcounted, one alloc, reused capacity)
  → Framer yields line boundaries: Iterator<Bytes>
    → JSON framer: memchr(\n), yield Bytes::slice() per line
    → CRI framer: memchr(\n), parse CRI header, yield message Bytes
      → F lines: Bytes::slice_ref() of message field (ZERO COPY)
      → P+F merge: accumulate P messages in BytesMut, freeze on F
  → Scanner receives Bytes (the original buffer or reassembly buffer)
    → streaming structural classifier processes 64-byte blocks
    → StreamingBuilder stores views
    → Arrow StringViewArray references original Bytes
```

Zero copies for the 99% path (complete lines, no P/F reassembly).
One copy only for CRI partial line reassembly.

## Layer design (inspired by Vector)

### Layer 1: Reader

Reads bytes from a source into a `BytesMut`. Handles file watching,
rotation detection, offset tracking. Produces `Bytes` via `freeze()`.

The reader doesn't know about line boundaries or formats. It just
reads bytes and produces refcounted buffers.

```rust
trait Reader {
    fn poll(&mut self) -> io::Result<Option<ReadChunk>>;
}

struct ReadChunk {
    bytes: Bytes,        // refcounted buffer
    source_id: SourceId, // which file/source this came from
}
```

### Layer 2: Framer

Identifies boundaries within a `Bytes` buffer. Produces frames
(sub-slices of the input) without copying.

Inspired by Vector's Framer trait and OTel's SplitFunc.

```rust
trait Framer {
    fn frame<'a>(&mut self, input: &'a Bytes) -> FrameResult<'a>;
}

enum FrameResult<'a> {
    /// Complete frames found. Remainder is bytes after the last frame.
    Frames {
        lines: Vec<Bytes>,   // Bytes::slice() of input — zero copy
        remainder: usize,    // byte offset of unprocessed tail
    },
    /// No complete frame found (need more data).
    Incomplete,
}
```

Built-in framers:
- `NewlineFramer` — splits on `\n` using memchr
- `CriFramer` — splits on `\n`, extracts CRI message field via slice_ref

### Layer 3: Aggregator (optional, CRI P/F only)

Merges partial frames into complete messages. Only needed for CRI
format with P/F partial line splitting.

```rust
trait Aggregator {
    fn aggregate(&mut self, frame: Bytes, is_partial: bool) -> Option<Bytes>;
}
```

For CRI:
- F line with no pending partials → return frame directly (zero copy)
- P line → accumulate in BytesMut
- F line with pending partials → append to BytesMut, freeze, return

### Layer 4: Scanner

Receives `Bytes`, produces Arrow RecordBatch. Already zero-copy
via streaming structural classification + StreamingBuilder.

The scanner doesn't care where the Bytes came from — it could be
from the tailer's buffer, the CRI reassembly buffer, or an Arrow
IPC file reader.

## What changes

| Component | Current | Target |
|-----------|---------|--------|
| Tailer output | `Vec<u8>` per read | `Bytes` (reusable BytesMut) |
| SourceEvent | `Data { bytes: Vec<u8> }` | `Data { bytes: Bytes }` |
| FramedInput | `process(&[u8], &mut Vec<u8>)` | Replaced by Framer trait |
| json_buf accumulation | `Vec<u8>` copy of all lines | Eliminated — scanner operates on original Bytes |
| CRI reassembly | Copy every line | Zero-copy for F lines, copy only for P+F |
| Scanner input | `Vec<u8>` → `Bytes` conversion | Receives `Bytes` directly |

## Handling the partial-line problem

The framer's `remainder` output tells the reader how many bytes
at the end of the buffer were not consumed (no terminating newline).
The reader keeps those bytes in its BytesMut for the next read.

No separate partial buffer in the framer. The reader's BytesMut
IS the partial buffer. On the next read, new data is appended to
BytesMut (which still contains the remainder), and the framer
scans again.

This is exactly how Vector's FileWatcher works — `BytesMut buf`
persists across reads, accumulating until a delimiter is found.

## simdjson padding consideration

simdjson requires SIMDJSON_PADDING (32 bytes) after the input data
for SIMD overread safety. The scanner handles this by padding the last
64-byte block with spaces before classification. But if we wanted to
match simdjson's approach (pad the buffer itself), we'd need the reader
to allocate with extra capacity.

For now: keep our padding approach in the scanner's block loop. This
avoids requiring the reader to know about SIMD padding requirements.

## Batch accumulation

Currently, json_buf accumulates lines from multiple reads until
batch_target_bytes is reached, then flushes to the scanner. In the
new design, each read produces a Bytes buffer. The scanner can
process each buffer independently (one RecordBatch per read), or
we can accumulate multiple Bytes buffers and scan them together.

The simplest approach: each read → one Bytes → one scan → one
RecordBatch. Multiple RecordBatches become MemTable partitions
for DataFusion (already designed in ARCHITECTURE.md). No accumulation
buffer needed.

If batches are too small (few lines per read), we can accumulate
Bytes references and scan them in a batch. But this adds complexity.
Start simple, optimize if profiling shows small batches are a problem.

## Relationship to proven core (#262)

The Framer and Aggregator are pure logic — they identify ranges
within buffers without IO. They belong in ffwd-core (proven).

The Reader is IO — it belongs in ffwd-input.

The Scanner is already in ffwd-core.

This aligns perfectly with the proven core architecture: core
handles framing, aggregation, and scanning (provable pure logic).
IO crates handle reading and writing.
