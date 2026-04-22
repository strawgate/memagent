# File Tailing Design Notes

> **Status:** Active
> **Date:** 2026-04-02
> **Context:** file tailing design notes and recommendations informed by collector research and repo-local correctness gaps.

## Data Flow Diagram

```text
                        ┌─────────────────────────────────────────┐
                        │              FileTailer                  │
                        │                                         │
  ┌──────────┐    ①     │  ┌──────────┐  ②   ┌────────────────┐  │
  │ Disk     │─────────►│  │ File fd  │─────►│ read_new_data  │  │
  │ (files)  │          │  │ (per-file│      │ (shared 256KB  │  │
  └──────────┘          │  │  offset) │      │  read buffer)  │  │
       │                │  └──────────┘      └───────┬────────┘  │
       │                │                            │           │
       │  ⑥ Glob       │                     ③ TailEvent::Data  │
       │  rescan        │                            │           │
       │                └────────────────────────────┼───────────┘
       │                                             │
       │                ┌────────────────────────────▼───────────┐
       │                │           FramedInput                  │
       │         ④      │                                        │
       └───────────────►│  ┌────────────┐  ⑤  ┌──────────────┐  │
         identify_file  │  │ remainder  │────►│ split on \n   │  │
         (fingerprint)  │  │ (SHARED!)  │     │ + CRI parse   │  │
                        │  └────────────┘     └──────┬────────┘  │
                        └────────────────────────────┼───────────┘
                                                     │
                                              InputEvent::Data
                                              (complete lines)
                                                     │
                        ┌────────────────────────────▼───────────┐
                        │        input_poll_loop (OS thread)     │
                        │                                        │
                        │  ┌────────────┐  ⑦  ┌──────────────┐  │
                        │  │ input.buf  │────►│ batch by size │  │
                        │  │ (accumulate│     │ or timeout    │  │
                        │  │  lines)    │     └──────┬────────┘  │
                        │  └────────────┘            │           │
                        └────────────────────────────┼───────────┘
                                                     │
                                          ⑧ mpsc channel (cap 16)
                                                     │
                        ┌────────────────────────────▼───────────┐
                        │         run_async (tokio)              │
                        │                                        │
                        │  scan_buf → flush_batch → transform    │
                        │              → output sinks            │
                        └────────────────────────────────────────┘
```

Each numbered point is a decision point with options below.

---

## ① File Discovery + Identity

**Current**: xxh64 hash of first 1024 bytes. Glob dedup by `PathBuf` string.

**Problem**: Hash collisions cause checkpoint misattribution (#798).
Symlinks bypass path-string dedup (#799).

### Options

**A. Keep xxh64 hash, add (dev, inode) to SourceId**
- Pro: Minimal change, hash stays fast, inode breaks collisions
- Con: Inode changes on copy (not rename), so copytruncate needs care
- Industry: Fluent Bit uses dev+inode as primary identity

**B. Switch to raw-bytes fingerprint (no hash)**
- Pro: No collision possible, prefix-match supports growing files
- Con: Stores 1 KB per file in checkpoint, comparison is O(n) not O(1)
- Industry: OTel Collector uses raw first 1000 bytes

**C. CRC-64 of first line + dev+inode fallback**
- Pro: Survives renames (CRC stable), inode prevents collision
- Con: Requires at least one complete line to fingerprint
- Industry: Vector uses CRC-64 of first line, falls back to dev+inode

**Recommendation**: A — add `(dev, inode)` to the SourceId hash. Simplest,
most robust. Use `(dev, inode)` for glob dedup too.

---

## ② Read from Disk

**Current**: `read_new_data` reads ALL available bytes into unbounded
`Vec<u8>`. Shared 256 KB read buffer, but result Vec grows without limit.

**Problem**: 10 GB log burst → 10 GB allocation (#800). One chatty file
starves others (#801).

### Options

**A. Cap per-read to configurable max (e.g., 16 MiB)**
- Pro: Simple, prevents OOM, leftover data read next poll
- Con: Doesn't address fairness across files
- Industry: Fluent Bit caps at `Static_Batch_Size` (50 MB)

**B. Per-file read budget per poll cycle (e.g., 256 KiB)**
- Pro: Prevents OOM AND ensures fairness across files
- Con: May add latency for single-file inputs (extra poll cycles)
- Industry: Vector uses 2 KiB per file per cycle

**C. Read into fixed ring buffer, yield when full**
- Pro: Zero allocation, bounded memory
- Con: Complex, needs careful lifetime management
- Industry: None of the three do this

**Recommendation**: B — per-file read budget. Set default high enough
(256 KiB) that single-file inputs don't notice, but multi-file inputs
get fair scheduling. Also acts as an OOM cap.

---

## ③ Truncation + Rotation Events

**Current**: Truncation detected in `read_new_data` (offset > file size),
but `TailEvent::Truncated` is never emitted (#796). Rotation detected by
dev/inode change, old fd drained before `TailEvent::Rotated`.

**Problem**: On copytruncate, the remainder buffer is not cleared.
Stale partial-line bytes from old content corrupt new content.

### Options

**A. Emit TailEvent::Truncated when detected (one-line fix)**
- Pro: FramedInput already handles it (clears remainder + resets format)
- Con: None — this is a bug fix, not a design choice
- Industry: Fluent Bit clears buffer, OTel has configurable behavior

**B. Also add configurable truncation behavior (like OTel)**
- `ignore`: keep old offset (skip re-reading, risk missing data)
- `read_whole`: reset to 0, re-read everything (current behavior + event)
- `read_new`: set offset to current size, only read new data
- Pro: Flexibility for different deployment patterns
- Con: More config surface, `read_whole` (current) is the right default

**Recommendation**: A now, B later. The one-line fix is urgent. Configurable
behavior can come when we add a config schema for file inputs.

---

## ④ File Identity for Checkpoints

See ① — same issue. The checkpoint store maps fingerprint → offset.
A collision means one file's offset overwrites another's.

---

## ⑤ Remainder Buffer (Line Splitting)

**Current**: Single `FramedInput` remainder buffer shared across all
files in a glob input (#797).

**Problem**: Partial line from file A corrupts data from file B when
writes interleave.

### Options

**A. Per-file remainder in FramedInput**
- Store `HashMap<SourceId, Vec<u8>>` for remainders
- Requires `TailEvent::Data` to carry source identity (it already has `path`)
- Pro: Correct, matches every other collector
- Con: Needs `InputEvent::Data` to carry `SourceId` through the stack

**B. Per-file FramedInput (one per file)**
- Each file gets its own `FramedInput` with own remainder, format state
- Pro: Clean separation, each file has independent CRI aggregator too
- Con: Larger refactor — `FileInput` currently wraps one `FileTailer`

**C. Move framing into FileTailer (per-file)**
- Each `TailedFile` holds its own line buffer
- `poll()` returns complete lines only, never partial data
- Pro: Eliminates the FramedInput layer entirely
- Con: Moves framing logic into the I/O layer, mixes concerns

**Recommendation**: A for now, consider C long-term. A is the minimal fix —
add source identity to events and key the remainder by it. C is cleaner
architecturally but a bigger refactor.

---

## ⑥ Glob Rescan + Dedup

**Current**: Rescan every 5s, dedup by `PathBuf` string comparison.

**Problem**: Symlinks and non-canonical paths bypass dedup (#799).

### Options

**A. Dedup by (dev, inode)**
- Pro: Correct on all Unix systems, survives symlinks and relative paths
- Con: Doesn't work cross-filesystem (dev differs)
- Industry: Fluent Bit uses this

**B. Canonicalize paths before comparison**
- Pro: Resolves symlinks and relative paths
- Con: `canonicalize()` follows symlinks, so symlink and target are same
       path. But: `canonicalize()` can fail (permission, broken link).
       And: doesn't handle hard links.

**C. Dedup by fingerprint**
- Pro: Content-based, works regardless of path gymnastics
- Con: Requires reading file content for every glob match every rescan
- Industry: Vector and OTel use this

**Recommendation**: A — `(dev, inode)` dedup is cheapest (one `stat()` call)
and handles the common cases (symlinks, relative paths). Fingerprint dedup
(C) is overkill for glob matching.

---

## ⑦ Batch Accumulation

**Current**: `input.buf` accumulates lines until `batch_target_bytes`
(4 MiB) or `batch_timeout` (100ms).

**No issues found here.** The batching logic is correct and well-tested.
Shutdown properly drains the buffer.

---

## ⑧ Channel + Backpressure

**Current**: `tokio::sync::mpsc::channel(16)`. Input thread blocks on
`blocking_send` when full. Async side drains on shutdown.

**No major issues.** The shutdown drain sequence is correct (verified
in the audit). Channel capacity of 16 is reasonable.

**Minor**: Could reduce to 2-4 for tighter backpressure (Vector uses 2).
Lower capacity means the input thread blocks sooner, which means files
stop being read sooner, which means less memory buffered in the pipeline.

---

## Partial Line Flush Timer (not in diagram — cross-cutting)

**Current**: No timer. Partial lines flush only on EndOfFile event
(next poll cycle with no new data).

### Options

**A. Per-file flush timer (500ms like OTel)**
- After 500ms of no new data for a file, flush remainder as complete line
- Pro: Guarantees partial lines emitted promptly
- Con: Requires per-file timer tracking (need per-file remainder first)
- Industry: OTel persists flush state in checkpoint

**B. Flush on EndOfFile only (current)**
- Pro: Simple, no timer complexity
- Con: Partial data can sit for up to `poll_interval` (250ms) before
       EndOfFile fires. Worse if file keeps getting tiny writes.

**C. Flush all remainders on timeout (global timer)**
- Pro: Simpler than per-file timers
- Con: Flushes ALL files' partials at once, even ones actively receiving data

**Recommendation**: A after per-file remainders are implemented. The
per-file remainder (#797 fix) is a prerequisite.

---

## Partial Line on Shutdown (not in diagram — edge case)

**Current**: Silently dropped.

### Options

**A. Don't advance checkpoint past partial data (Vector approach)**
- The checkpoint offset stays at the last complete line
- On restart, the partial data is re-read from the file
- Pro: No data loss, at-least-once semantics
- Con: Requires checkpoint to track "last complete line offset"
       separately from "bytes read offset"

**B. Flush partial as complete line on shutdown (OTel approach)**
- Pro: Data is delivered, not lost
- Con: May produce malformed records if the partial was genuinely
       incomplete (e.g., mid-JSON-object)

**C. Drop partial on shutdown (current, same as Fluent Bit)**
- Pro: Simple, no malformed records
- Con: Data loss (usually just one line per file)

**Recommendation**: A is ideal but requires checkpoint redesign. For now,
C is acceptable — the data loss is minimal (one partial line per file
at shutdown time).

---

## Summary: Priority Order

| Priority | Issue | Fix | Effort |
|----------|-------|-----|--------|
| 1 | #796 Truncation event | Emit TailEvent::Truncated | 1 line |
| 2 | #797 Per-file remainder | HashMap keyed by SourceId | Medium |
| 3 | #800 Unbounded read | Per-file read budget | Small |
| 4 | #799 Glob dedup | Use (dev, inode) | Small |
| 5 | #798 Fingerprint collision | Add (dev, inode) to hash | Small |
| 6 | #801 Read fairness | Per-file budget (same as #800) | Small |
| 7 | Flush timer | Per-file 500ms timer | Medium (after #797) |
