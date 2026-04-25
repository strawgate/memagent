# Checkpoint Snapshot Design: #584 + #585

> **Status:** Active
> **Date:** 2026-04-01
> **Context:** Phase 5c design for flowing checkpoint metadata through the pipeline for at-least-once delivery.

## Problem

The pipeline channel carries bare `Vec<u8>` with no source identity. By the
time `flush_batch` runs, we don't know which files contributed to the batch.
We need to flow checkpoint metadata from input threads through the channel
to enable at-least-once delivery via PipelineMachine ordered ACK.

## Industry Context

| System | Checkpoint trigger | Granularity | Guarantee |
|--------|-------------------|-------------|-----------|
| Vector | Output ACK (OrderedFinalizer) | Per-event finalizer | At-least-once |
| Filebeat | Output ACK (event.Private) | Per-event in batch | At-least-once |
| Fluent Bit | On read (before output) | Per-read chunk | At-most-once |
| OTel filelog | On read (before output) | Per-poll interval | At-most-once |

We follow Vector/Filebeat: checkpoint advances only on output ACK.

**Key difference from Vector:** Vector uses per-event finalizers, so file A's
checkpoint can advance even if file B's events in the same batch fail. We use
per-batch granularity — the entire batch succeeds or fails atomically. This
is acceptable because HTTP-based outputs are batch-atomic (the POST either
succeeded or didn't). Per-event granularity adds complexity with no practical
benefit for our output types.

## Newtypes

All fingerprint and offset values use newtypes to prevent mixing up two `u64`s:

```rust
// In ffwd-io (I/O-layer identity)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileFingerprint(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteOffset(pub u64);
```

`SourceId(u64)` in ffwd-core is the pipeline-level source identity.
`FileFingerprint` converts to `SourceId` via `From` impl at the boundary.
They are NOT the same type — `SourceId` is generic (could be Kafka topic
hash), `FileFingerprint` is file-specific.

## Data Model

### Channel Message

```rust
/// Message from input thread to pipeline async loop.
enum ChannelMsg {
    /// Data with checkpoint metadata.
    Data {
        /// Accumulated scanner-ready bytes.
        bytes: Vec<u8>,
        /// Per-file checkpoints at time of send.
        /// Represents bytes consumed from each file that are in `bytes`.
        checkpoints: Vec<(FileFingerprint, ByteOffset)>,
    },
    /// Path mapping updates (sent on file open, rotation, periodically).
    /// Decoupled from data to keep the hot path allocation-free for paths.
    PathUpdate(Vec<(FileFingerprint, PathBuf)>),
}
```

**Why enum, not struct with optional fields:**
Non-file inputs send `Data { bytes, checkpoints: vec![] }` — clean.
PathUpdate is a separate concern sent infrequently. Mixing both into every
message wastes space and muddies the API.

### SourceRegistry (pipeline-side identity resolution)

```rust
/// Maps source identities to metadata needed for checkpoint persistence.
/// Generic: K=FileFingerprint for files, K=TopicPartition for Kafka, etc.
///
/// Lives in ffwd-core so it can be Kani-proven.
pub struct SourceRegistry<K: Eq + Hash, M> {
    entries: HashMap<K, SourceEntry<M>>,
}

pub struct SourceEntry<M> {
    pub metadata: M,        // PathBuf for files, String for Kafka topics
    pub state: SourceState,
    pub last_seen: Instant,
}

pub enum SourceState {
    Active,     // file being tailed, offsets flowing
    Draining,   // file gone, batches still in-flight
    Committed,  // all in-flight acked, checkpoint persisted
}
```

**Why generic, not FingerprintMap:**
The lifecycle (Active → Draining → Committed → pruned) is universal across
source types. Kani proofs written once apply to all instantiations.

## Critical Correctness Issues (from adversarial review)

### Issue 1: Offset Over-Reporting

**Problem:** Snapshotting ALL file offsets at send time captures bytes still
in `input.buf` from the current poll cycle that haven't been sent yet. If the
NEXT batch fails, those bytes' checkpoint was already committed via THIS
batch's ACK.

**Example:**
- Poll 1: read 100KB from A, 50KB from B → buf has 150KB
- Poll 2: read 200KB from A → buf has 350KB, exceeds threshold
- Send with swap. Snapshot: A=300K, B=50K.
- But `data` only contains the 350KB that was swapped out.
- A's offset (300K) is correct — all 300KB from A is in the send.
- HOWEVER: if the buffer was 4MB and we split mid-file...

**Analysis:** Actually, this is NOT a problem in our design. `input_poll_loop`
sends the ENTIRE `input.buf` via swap (line 473-474 of pipeline.rs). There is
no partial send. When `should_send` triggers, ALL accumulated bytes are sent.
The snapshot captures offsets for ALL bytes that were read into `input.buf`
since the last send. Since we swap the entire buffer, snapshot == sent data.

**Wait — what about the remainder buffer?**

### Issue 2: FramedInput Remainder Gap

**Problem:** `FramedInput` holds a `remainder` buffer (partial lines from the
last poll). FileTailer's `offset` includes bytes that are in the remainder.
Those bytes are NOT in the emitted `SourceEvent::Data` — they're held back
until a newline completes them.

**Impact:** The snapshotted offset is AHEAD of the bytes actually in
`input.buf`. If we commit this offset and crash, on restart we skip the
remainder bytes. Those bytes are re-read on restart anyway (we seek to the
committed offset, which is past them), so they're LOST.

**Resolution:** Track the remainder length and subtract it from the offset.

```rust
impl FramedInput {
    /// Bytes held in the remainder buffer (read but not yet emitted).
    pub fn remainder_len(&self) -> usize {
        self.remainder.len()
    }
}

// In file_offsets():
// For each file, the checkpoint offset should be:
//   tailer_offset - framed_input.remainder_len()
// But remainder is shared across all files, not per-file...
```

**Actually, this is more subtle.** The remainder buffer in FramedInput is a
single buffer across all files. We can't attribute it to a specific file.
But in practice, remainder is typically small (< one line, < 1KB) and only
exists for incomplete lines. The at-least-once guarantee means we'd re-read
and re-process at most one partial line per file on restart. This is acceptable
and matches Vector/Filebeat behavior (they also don't account for partial
lines in checkpoints).

**Decision:** Accept the remainder gap. Document it as a known property:
"On crash recovery, up to one partial line per file may be re-read." This
is inherent to line-based log shipping and all industry implementations
accept it.

### Issue 3: Empty File Fingerprint Collision

**Problem:** `compute_fingerprint()` returns sentinel `0` for empty files.
All empty files share fingerprint `0`. The `HashMap<FileFingerprint, ByteOffset>`
in the main loop collapses them.

**Resolution:** Use (device, inode) as the identity for empty files, or skip
them entirely.

Best approach: **skip empty files in `file_offsets()`**. An empty file has
offset 0 and no data to checkpoint. It contributes no bytes to the batch.
Including it in the checkpoint is meaningless. When content appears, the
fingerprint becomes non-zero and tracking begins.

```rust
pub fn file_offsets(&self) -> Vec<(FileFingerprint, ByteOffset)> {
    self.files.iter()
        .filter(|(_, tailed)| tailed.identity.fingerprint != 0)
        .map(|(_, tailed)| (
            FileFingerprint(tailed.identity.fingerprint),
            ByteOffset(tailed.offset),
        ))
        .collect()
}
```

### Issue 4: Copytruncate Fingerprint Mutation

**Problem:** On truncation detection (tail.rs lines 501-508), the offset
resets to 0 and fingerprint MAY change if new content starts differently.
The old fingerprint's final offset is never explicitly recorded.

**Resolution:** This is actually safe for at-least-once. When truncation is
detected:
1. The old fingerprint's bytes were already sent (previous InputMsgs)
2. Those InputMsgs' checkpoints were already accumulated
3. On the next send, the NEW fingerprint appears with offset starting from 0
4. The old fingerprint simply stops appearing in future `file_offsets()` calls
5. If the old fingerprint's in-flight batches are acked, checkpoint persists
6. If they fail, on restart we re-read from the old checkpoint (old fingerprint)
   — but the file has been truncated, so we start from 0 anyway

The only risk: if crash happens AFTER truncation detection but BEFORE the
new content is checkpointed, we restart, see a new fingerprint, and start
from 0 — which is correct (the file was truncated).

**Decision:** Copytruncate is safe by construction. Add a test.

### Issue 5: Per-Source vs Global Ordering in PipelineMachine

**Problem:** Does a stuck batch for source A block checkpoint advancement
for source B?

**Verified:** Reading lifecycle.rs — PipelineMachine uses
`in_flight: BTreeMap<SourceId, BTreeMap<BatchId, C>>` — ordering is
**per-source**. Source A and B have independent ordering. A stuck batch
on source A does NOT block source B. This is correct.

### Issue 6: Drain Loop Must Accumulate Checkpoints

**Problem:** The drain loop (pipeline.rs lines 300-305) currently just
extends scan_buf. When InputMsg is introduced, it must also accumulate
checkpoints, or the final batches during shutdown lose their checkpoint
metadata.

**Resolution:** The drain loop must process full ChannelMsg:

```rust
// Drain channel messages
while let Some(msg) = rx.recv().await {
    match msg {
        ChannelMsg::Data { bytes, checkpoints } => {
            scan_buf.extend_from_slice(&bytes);
            for (fp, offset) in checkpoints {
                batch_checkpoints.insert(fp, offset);
            }
        }
        ChannelMsg::PathUpdate(updates) => {
            for (fp, path) in updates {
                source_registry.update(fp, path);
            }
        }
    }
    if scan_buf.len() >= self.batch_target_bytes {
        self.flush_batch(&mut scan_buf, &mut batch_checkpoints).await;
    }
}
```

### Issue 7: Zero-Row Batch Must Still ACK

**Problem:** If scanner returns 0 rows (line 349-351), `flush_batch` returns
early. The bytes' checkpoints are never acknowledged. On restart, those bytes
are re-read and re-scanned, producing 0 rows again — infinite no-op loop.

**Resolution:** Create and ACK tickets even when scan/transform produces zero
rows. "Processed" means the pipeline dealt with the data, not that rows were
forwarded. SQL WHERE filtering all rows is a valid outcome, not a failure.

```rust
async fn flush_batch(&mut self, scan_buf: &mut Vec<u8>,
                     checkpoints: &mut HashMap<FileFingerprint, ByteOffset>) {
    // ... scan ...
    
    // Create tickets regardless of row count
    let cps = std::mem::take(checkpoints);
    let tickets: Vec<_> = cps.into_iter()
        .map(|(fp, offset)| machine.create_batch(fp.into(), offset.0))
        .collect();

    if batch.num_rows() == 0 {
        // No rows to output, but we still consumed the bytes.
        // ACK all tickets to advance checkpoints.
        for ticket in tickets {
            let sending = machine.begin_send(ticket);
            let receipt = sending.ack();
            let advance = machine.apply_ack(receipt);
            if advance.advanced {
                self.persist_checkpoint(advance);
            }
        }
        return;
    }
    
    // ... transform, output as before ...
}
```

## Buffer Reuse Strategy

The current code reuses Vec capacity via `std::mem::swap` (pipeline.rs line 474).
Moving data into `ChannelMsg::Data { bytes }` must preserve this pattern:

```rust
// In input_poll_loop, on send:
let mut data = Vec::with_capacity(batch_target_bytes);
std::mem::swap(&mut input.buf, &mut data);
// input.buf now has the pre-allocated capacity from `data`
// `data` has the accumulated bytes, moved into ChannelMsg

let file_offsets = input.file_offsets(); // hot path, no PathBuf
let msg = ChannelMsg::Data { bytes: data, checkpoints: file_offsets };
```

The swap trick is preserved because we swap before constructing the message.
The receiver side already discards msg.bytes after `extend_from_slice` — this
is unchanged.

## InputSource Trait and Type Access

`input_poll_loop` receives `InputState { source: Box<dyn InputSource> }`.
The `source` is a `FramedInput` wrapping the real source. We need
`file_offsets()` which is file-specific.

**Approach: add `checkpoint_data()` to InputSource trait with default no-op.**

```rust
pub trait InputSource: Send {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>>;
    fn name(&self) -> &str;
    fn apply_hints(&mut self, _hints: &FilterHints) {}
    
    /// Return checkpoint data for all active sources.
    /// Default: empty (push sources, generators).
    /// Uses FileFingerprint/ByteOffset newtypes from ffwd-io.
    fn checkpoint_data(&self) -> Vec<(FileFingerprint, ByteOffset)> {
        vec![]
    }
    
    /// Return identity-to-path mappings for persistence.
    /// Called on file open/rotation, not per-batch.
    /// Default: empty (push sources, generators).
    fn source_paths(&self) -> Vec<(FileFingerprint, PathBuf)> {
        vec![]
    }
    
    // NOTE: These use FileFingerprint (I/O-layer type), not SourceId
    // (pipeline-layer type). The conversion happens at the pipeline
    // boundary when creating BatchTickets. This keeps the InputSource
    // trait independent of ffwd-core.
}
```

FramedInput delegates to inner. FileInput delegates to FileTailer.
Generator/OTLP/TCP return empty (default).

**Why not downcast:** Requires `Any`, fragile, breaks if wrapping changes.
**Why not generic input_poll_loop:** The function is already not generic
(it takes InputState with Box<dyn InputSource>). Making it generic requires
changes throughout Pipeline construction. Not worth it for this.

## FileTailer API (#584)

```rust
impl FileTailer {
    /// Hot path: fingerprint + offset for all tailed files.
    /// Skips empty files (fingerprint 0) — they have no data to checkpoint.
    /// Called on every channel send (~100ms). No PathBuf allocation.
    pub fn file_offsets(&self) -> Vec<(FileFingerprint, ByteOffset)> {
        self.files.iter()
            .filter(|(_, tailed)| tailed.identity.fingerprint != 0)
            .map(|(_, tailed)| (
                FileFingerprint(tailed.identity.fingerprint),
                ByteOffset(tailed.offset),
            ))
            .collect()
    }

    /// Cold path: fingerprint + canonical path for all tailed files.
    /// Called on file open/close/rotate, not per-batch.
    pub fn file_paths(&self) -> Vec<(FileFingerprint, PathBuf)> {
        self.files.iter()
            .filter(|(_, tailed)| tailed.identity.fingerprint != 0)
            .map(|(path, tailed)| (
                FileFingerprint(tailed.identity.fingerprint),
                path.clone(),
            ))
            .collect()
    }
}
```

## SourceRegistry Design (ffwd-core)

```rust
/// Generic source identity registry with lifecycle tracking.
/// K = identity key (FileFingerprint, TopicPartition, etc.)
/// M = metadata for persistence (PathBuf, String, etc.)
pub struct SourceRegistry<K: Eq + Hash + Clone, M> {
    entries: HashMap<K, SourceEntry<M>>,
}

pub struct SourceEntry<M> {
    pub metadata: M,
    pub state: SourceState,
    pub last_seen: Instant,  // for TTL-based pruning of Committed entries
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceState {
    /// Source is actively producing data.
    Active,
    /// Source stopped producing but has in-flight batches.
    Draining,
    /// All in-flight batches resolved, checkpoint persisted.
    Committed,
}

impl<K: Eq + Hash + Clone, M> SourceRegistry<K, M> {
    pub fn new() -> Self { ... }
    
    /// Register or update a source. Returns previous state if existed.
    pub fn upsert(&mut self, key: K, metadata: M) -> Option<SourceState> { ... }
    
    /// Mark a source as draining (no more data expected).
    pub fn begin_drain(&mut self, key: &K) -> bool { ... }
    
    /// Mark a source as committed (all in-flight resolved).
    /// Fails if source has in-flight batches (caller must check).
    pub fn commit(&mut self, key: &K) -> bool { ... }
    
    /// Look up metadata for checkpoint persistence.
    pub fn metadata(&self, key: &K) -> Option<&M> { ... }
    
    /// Remove committed entries older than TTL.
    /// SAFETY: never removes Active or Draining entries.
    pub fn prune_committed(&mut self, max_age: Duration) { ... }
    
    /// All active source keys (for liveness checks).
    pub fn active_keys(&self) -> Vec<&K> { ... }
}
```

**Kani proofs:**
1. `upsert` on new key → state is Active
2. `begin_drain` only succeeds on Active entries
3. `commit` only succeeds on Draining entries
4. `prune_committed` never removes Active or Draining entries
5. State transitions are monotonic: Active → Draining → Committed (no backwards)

**Note on Instant and Kani:** Kani cannot model `std::time::Instant`. For
proofs, `last_seen` is replaced with a logical generation counter (`u64`).
`prune_committed` takes a minimum generation instead of a Duration. The
time-based interface is a thin wrapper tested via unit tests, not Kani.

**Interaction with PipelineMachine:**
- PipelineMachine handles batch ordering and ACK (generic over C)
- SourceRegistry handles identity→metadata mapping and lifecycle
- They share the SourceId key; SourceRegistry is consulted on ACK to get path

## Checkpoint Persistence

After `apply_ack` where `advance.advanced == true`:
1. Look up path: `source_registry.metadata(&fingerprint)`
2. Update in-memory: `checkpoint_store.update(SourceCheckpoint { ... })`
3. Flush to disk: throttled to once per 5 seconds (configurable)
4. On shutdown: final flush after `machine.stop()`

**Checkpoint store expiry:** When `source_registry.prune_committed()` removes
an entry, also remove it from the checkpoint store. Prevents unbounded growth.

## Verification Plan

| Property | Technique | Priority |
|----------|-----------|----------|
| SourceRegistry state transitions (monotonic, no invalid) | Kani proof | P0 |
| prune never removes Active/Draining | Kani proof | P0 |
| Ordered ACK per-source independence | Already proven (Phase 5b) | Done |
| Zero-row batch still advances checkpoint | Unit test | P0 |
| Drain loop accumulates checkpoints | Unit test | P0 |
| Empty file (fp=0) excluded from checkpoints | Unit test | P1 |
| Copytruncate produces correct new fingerprint | Integration test | P1 |
| Remainder gap bounded to < 1 line per file | Integration test | P1 |
| End-to-end: file → pipeline → shutdown → resume | Integration test | P1 |
| Random InputMsg sequences → correct checkpoints | Proptest | P1 |
| Checkpoint store pruning matches registry pruning | Unit test | P2 |

## Remaining Design Gaps

### FanoutSink Partial Success

If a batch is sent to multiple outputs via FanoutSink and only some succeed,
the current pipeline code (line 378-391) treats this as a full batch failure:
no checkpoint advance. Successful sinks already received the data. On restart,
ALL sinks re-receive from the old checkpoint — successful sinks get duplicates.

This is at-least-once correct per-sink but wasteful. Acceptable for now.
Future work (#318 retry) should consider per-sink ACK tracking.

### PathUpdate/Data Ordering

If `PathUpdate` arrives AFTER the `Data` message that first references a new
fingerprint, `source_registry.metadata()` returns `None` during ACK.

**Resolution:** Input thread MUST send `PathUpdate` before or with the first
`Data` containing a new fingerprint. Enforce in `input_poll_loop`: on file
open/rotation, queue a `PathUpdate` and send it BEFORE the next `Data`.
Alternatively, batch the path update into the first Data message (but we
chose the enum split to avoid this). Simplest: send PathUpdate immediately
on file change, don't wait for the next data send.

### Short File Fingerprint Instability

A file with <1024 bytes gets a fingerprint based on partial content. If the
file grows beyond 1024 bytes, the fingerprint computed on restart differs.
The old checkpoint won't match → file is re-read from 0.

This is safe for at-least-once (duplicates, not loss). Add to known
limitations. Mitigation: fingerprint computation always uses min(file_len,
fingerprint_bytes), which is deterministic given the same file content at
the same point in time. The instability only matters across process restarts
where the file grew between the original fingerprint and the restart.

## Known Accepted Limitations

1. **Remainder gap:** On crash recovery, up to one partial line per file may
   be re-read and re-sent. Inherent to line-based log shipping; all industry
   implementations accept this.

2. **Batch-level granularity:** If a batch fails, ALL files' checkpoints in
   that batch don't advance. Re-reads are bounded by batch size (~4MB).
   Acceptable because outputs are batch-atomic (HTTP POST succeeds or fails).

3. **5-second persistence window:** Up to 5 seconds of ACKed-but-unpersisted
   checkpoints lost on crash. Configurable. At 4MB/100ms that's ~40 batches
   = ~160MB of duplicates on restart.

4. **Fingerprint collision:** xxh64 of first 1024 bytes. Two files with
   identical first 1024 bytes collide. Mitigated by: (a) skipping empty
   files, (b) log files typically have unique timestamps in first few lines,
   (c) collision at practical file counts (<10K) is negligible.

5. **Short file fingerprint instability:** Files shorter than fingerprint_bytes
   at checkpoint time get a different fingerprint on restart if they grew.
   Results in re-read from offset 0 (duplicates, not loss). Uncommon in
   practice — log files quickly exceed 1024 bytes.

## Files to Modify

| File | Change |
|------|--------|
| `crates/ffwd-core/src/pipeline/registry.rs` | New: SourceRegistry generic type + Kani proofs |
| `crates/ffwd-io/src/tail.rs` | Add `file_offsets()`, `file_paths()`, FileFingerprint, ByteOffset |
| `crates/ffwd-io/src/input.rs` | Add `checkpoint_data()`, `source_paths()` to InputSource trait; impl on FileInput |
| `crates/ffwd-io/src/framed.rs` | Delegate `checkpoint_data()`, `source_paths()` to inner |
| `crates/ffwd/src/pipeline.rs` | ChannelMsg enum, SourceRegistry, main loop, flush_batch, drain loop |
| `crates/ffwd/Cargo.toml` | Add ffwd-core dependency |
