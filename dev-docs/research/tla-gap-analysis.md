# TLA+ Gap Analysis: FileCheckpoint.tla

> **Status:** Active
> **Date:** 2026-04-03
> **Context:** Gap analysis of FileCheckpoint TLA+ spec against actual code, with CRITICAL findings.
Auditor: Claude Opus 4.6 (1M context)
Scope: What the spec does NOT model but SHOULD, informed by the actual
code in `tail.rs`, `framed.rs`, and `pipeline.rs`.

TLC status at time of audit:
- Safety (MaxLines=4, MaxCrashes=1, MaxRotations=1, BatchSize=2): **PASS**
  19,141 states generated, 10,939 distinct, depth 29.
- Liveness (MaxLines=3, MaxCrashes=1, MaxRotations=0, BatchSize=1): **PASS**
  471 states generated, 288 distinct, depth 28.

---

## Gap 1: File Grows During Read (Concurrent Append + TailerRead)

**Real-world scenario**: The tailer calls `read()` which returns N bytes.
While the tailer was reading, a writer appended more bytes. The tailer's
`read_offset` advances by N, but the file now has N+M bytes. The next
`read_new_data` call sees more data. This is the normal steady-state
case, but it also means a single `TailerRead` does not atomically
capture all available data -- it captures a snapshot.

In `tail.rs:547-556`, the read loop drains until `read()` returns 0,
but the file can grow between the last read returning 0 and the metadata
check on the next poll. This is benign for correctness but creates a
subtle timing window.

**Why the spec misses it**: `TailerRead` atomically reads ALL unread
lines (`read_offset' = Len(file_content)`). There is no concept of a
partial read that captures a subset of available lines.

**Could it hide a bug?** Unlikely for checkpoint correctness. However,
it masks a scenario where `SendBatch` computes `in_flight_offset` based
on `read_offset` that has advanced beyond what was actually in the
batch. In the real system, the checkpoint metadata is captured at
channel-send time (`file_offsets()` in `tail.rs:589`), which reads the
tailer's offset -- but the tailer may have read more data since the
bytes were emitted. The channel message's `checkpoints` field may
reference a read offset that includes bytes not yet in the channel.

**Concrete failure mode**: If `file_offsets()` is called after
`read_new_data` puts bytes into the input buffer but before those bytes
reach the channel, the checkpoint offset could be ahead of the data
actually in the batch. On crash, restart skips lines.

In practice, `input_poll_loop` calls `source.checkpoint_data()` at the
same time it calls `poll()`, and both are on the same thread, so this
race does not actually happen. But the spec should model the possibility
of partial reads to confirm this reasoning.

**How to add it**:
```tla
\* TailerRead reads 1..K lines (nondeterministic), not all lines.
TailerReadPartial ==
    /\ alive
    /\ read_offset < Len(file_content)
    /\ \E k \in 1..(Len(file_content) - read_offset) :
       LET new_lines == SubSeqFromTo(file_content, read_offset + 1, read_offset + k)
       IN ...
       /\ read_offset' = read_offset + k
```

**Priority**: MEDIUM -- the code avoids this race by design (single
thread), but the spec should confirm that partial reads do not break
the checkpoint arithmetic.

---

## Gap 2: File Deleted While Being Read (nlink Drops to 0)

**Real-world scenario**: A log rotator deletes the old log file. On
Unix, the open fd keeps the inode alive, so `read()` still works. The
tailer detects `nlink == 0` via `metadata().nlink()` and drains any
remaining data before closing the fd (`tail.rs:472-498`).

**Why the spec misses it**: The spec has no concept of file deletion.
`RotateFile` renames the file but the old content persists in
`rotated_content`. There is no action for "file unlinked, inode still
readable via fd."

**Could it hide a bug?** Yes. The real code's deletion handling is
a race: the tailer reads remaining data, then removes the entry. But
what if the file grows between the drain read and the removal? The
fd is still open. That data is lost -- the entry is removed, so no
further reads happen.

More critically: after deletion, the framer may still have a partial
line in its remainder. The code does NOT emit a `Truncated` or `Rotated`
event on file deletion (only on rotation detection via inode change).
So the framer's remainder is NOT cleared. If a new file appears at the
same path, the stale remainder from the deleted file corrupts the new
file's first line.

**How to add it**:
```tla
VARIABLE file_deleted  \* BOOLEAN: inode unlinked but fd still open

DeleteFile ==
    /\ alive
    /\ ~file_deleted
    /\ Len(file_content) > 0
    /\ file_deleted' = TRUE
    /\ UNCHANGED <<...all other vars...>>

\* Tailer drains remaining data from deleted file
TailerDrainDeleted ==
    /\ alive
    /\ file_deleted
    /\ read_offset < Len(file_content)
    /\ ...read remaining lines...
    /\ UNCHANGED <<...>>

\* Tailer closes deleted file
TailerCloseDeleted ==
    /\ alive
    /\ file_deleted
    /\ read_offset = Len(file_content)
    /\ file_deleted' = FALSE
    \* BUG: framer_buf NOT cleared, framer_source NOT reset
    /\ UNCHANGED <<framer_buf, framer_source, ...>>
```

**Priority**: HIGH -- the framer remainder corruption on file deletion
is a real bug that the spec could surface if it modeled deletion.

---

## Gap 3: Multiple Rotations Before First Is Drained

**Real-world scenario**: File rotates (A -> A.1, new A created). Before
the tailer finishes draining A.1, another rotation happens (A -> A.2,
new A created). Now A.1 still has unread data, A.2 has unread data,
and the new A is empty.

In the real code (`tail.rs:396-425`), rotation is detected by
device/inode change. The old fd is drained, then the entry is replaced
by the new file. But only one old fd can be drained at a time -- the
`files` HashMap has one entry per path. If A.1 was not fully drained
when A.2 appears, the remaining A.1 data is lost because the fd is
replaced.

**Why the spec misses it**: `RotateFile` has a guard `rotated_active = FALSE`,
preventing a second rotation while the first is being drained. This is
a simplification -- the real system has no such guard.

The safety config uses `MaxRotations=1`, so this scenario is structurally
impossible to explore. Even with `MaxRotations=2`, the `rotated_active`
guard serializes rotations.

**Could it hide a bug?** Yes. Rapid rotation (e.g., logrotate running
while a burst of logs is being written) could cause data loss from the
intermediate file generation. The spec assumes this cannot happen.

**How to add it**: Replace the single `rotated_*` variables with a
sequence of pending rotated files:
```tla
VARIABLE rotated_queue  \* Seq of [content: Seq, identity: Nat, offset: Nat]

RotateFile ==
    /\ alive
    /\ rotation_count < MaxRotations
    /\ Len(file_content) > 0
    \* Queue current file for draining (even if previous rotation not done)
    /\ rotated_queue' = Append(rotated_queue,
         [content |-> file_content,
          identity |-> file_identity,
          offset |-> read_offset])
    /\ file_content' = <<>>
    /\ ...
```

**Priority**: CRITICAL -- this is a concrete data loss scenario in the
real system that the spec explicitly excludes via the `rotated_active`
guard. The fix is to either queue rotated files or document that rapid
rotation causes expected data loss.

---

## Gap 4: Inode Reuse After Rotation

**Real-world scenario**: File A is rotated to A.1. The OS deletes A.1
(or it ages out). A new log file is created at path B, which happens to
get the same inode number as the deleted A.1. The tailer's identity
check (`device == && inode ==`) matches, but the fingerprint differs.

In `tail.rs:390-393`, the code explicitly ignores fingerprint changes
for already-open files (because a small file growing past the
fingerprint window changes its fingerprint legitimately). This means
inode reuse with different content is NOT detected for files already in
the `files` HashMap.

For glob-discovered files, the check is path-based (`!existing.contains(p)`),
so a new path with a reused inode would be opened as a new file. The
checkpoint store maps `SourceId(fingerprint)` to offset, so the old
file's checkpoint would be for a different fingerprint -- no collision.

**Why the spec misses it**: Identity is modeled as a monotonically
increasing natural number (`next_identity`). Reuse never occurs.

**Could it hide a bug?** Only in the specific case where the SAME path
gets the same inode with different content. This is extremely rare in
practice (requires the filesystem to recycle inodes quickly) but the
spec should at least document that it assumes unique identities.

**How to add it**:
```tla
\* Allow identity reuse: new file after deletion can get same identity
RotateFileWithInodeReuse ==
    /\ RotateFile
    /\ file_identity' \in SourceIds  \* may reuse an old identity
```

**Priority**: LOW -- filesystem inode reuse with same path is extremely
rare and the fingerprint mismatch would be detected on the next poll
for new files. The current abstraction is reasonable.

---

## Gap 5: Partial Line Spanning Multiple Reads

**Real-world scenario**: A log line is split across two `read()` calls.
First read returns `{"msg": "hel`, second returns `lo"}\n`. The framer
accumulates the partial in `remainder`, then concatenates on the next
read.

In `framed.rs:71-99`, the remainder is prepended to the next chunk,
then `memrchr` finds the last newline. Everything before is complete
lines; everything after is the new remainder.

**Why the spec misses it**: The spec models lines as atomic units.
`TailerRead` moves complete lines from `file_content` to `framer_buf`.
There is no concept of a partial line.

**Could it hide a bug?** Yes. The critical question is: what is the
checkpoint offset for a partial line? In the spec, `in_flight_offset =
read_offset - Len(framer_buf)`, where `framer_buf` contains complete
lines. In the real system, the remainder is bytes, not lines. The
checkpoint should be `read_offset - remainder.len()` (Finding 002).

But the real code's `checkpoint_data()` (`tail.rs:589`) returns the
tailer's `read_offset`, which includes the partial line bytes. The
pipeline's `flush_batch` uses this offset directly. The remainder
subtraction must happen somewhere, but looking at the code, it does
not. The `FramedInput::checkpoint_data()` delegates to
`self.inner.checkpoint_data()` which is the tailer's raw offset.

This means the checkpoint offset can be ahead of the last complete
line boundary. On crash + restart, the tailer resumes from the
checkpoint, which is past the start of the partial line. That partial
line's first half is lost -- only the second half (after the checkpoint
offset) would be read, producing a corrupted line.

**Concrete failure mode**:
1. File contains `line1\nline2\npar` (15 bytes)
2. Tailer reads all 15 bytes, read_offset = 15
3. Framer splits: `line1\nline2\n` emitted, `par` in remainder
4. Channel message carries checkpoint offset = 15 (tailer's offset)
5. Batch with line1+line2 is sent and acked
6. Checkpoint advances to 15
7. Crash. Restart from offset 15.
8. File now has `line1\nline2\npartial_complete\n`
9. Tailer reads from offset 15: `tial_complete\n`
10. Framer emits `tial_complete` -- CORRUPTED LINE

**How to add it**: Model byte-level offsets alongside lines:
```tla
VARIABLE remainder_bytes  \* Nat: bytes in framer that aren't complete lines

\* TailerRead now also tracks bytes
TailerRead ==
    ...
    /\ remainder_bytes' = <bytes of partial line>

\* SendBatch subtracts remainder bytes
SendBatch ==
    ...
    /\ in_flight_offset' = read_offset_bytes - remainder_bytes
```

**Priority**: CRITICAL -- this is a real data corruption bug. The
checkpoint offset in the real system does not subtract the framer
remainder. The spec's line-level abstraction hides this because lines
are atomic. The Kani proofs on CheckpointTracker model the subtraction
correctly, but the pipeline code does not wire it up.

---

## Gap 6: Line at MAX_REMAINDER_BYTES Boundary (2 MiB Cap)

**Real-world scenario**: A single log line exceeds 2 MiB (e.g., a Java
stack trace, a serialized object). The framer discards the remainder
and calls `self.format.reset()` (`framed.rs:81-84`).

**Why the spec misses it**: No concept of line size limits or remainder
overflow.

**Could it hide a bug?** After the remainder is discarded:
1. `self.format.reset()` clears CRI aggregation state -- correct.
2. But the tailer's `read_offset` has already advanced past the
   discarded bytes.
3. On the next read, the framer starts fresh from wherever the next
   newline appears.
4. If checkpoint was written before the discard, it points to a valid
   offset. If after, it points past the discarded data -- correct.

The main risk: the discard happens silently. No event is emitted to
the pipeline. The checkpoint can advance past data that was never
delivered.

**How to add it**:
```tla
CONSTANT MaxLineLenBytes  \* e.g., 2 MiB

\* Framer discards if accumulated bytes exceed limit
FramerDiscard ==
    /\ alive
    /\ Len(framer_buf) > MaxLineLenBytes  \* in byte model
    /\ framer_buf' = <<>>
    /\ framer_source' = 0
    /\ UNCHANGED <<...read_offset...>>  \* offset NOT rolled back
```

**Priority**: MEDIUM -- the discard behavior is arguably correct
(protecting against OOM), but the checkpoint advancing past discarded
data means those bytes are permanently lost with no record. Should at
minimum be observable via a metric (which the code does: `inc_parse_errors`).

---

## Gap 7: Empty File (0 Bytes, Fingerprint = 0)

**Real-world scenario**: A new file is created but no data is written
yet. `compute_fingerprint` returns 0 (sentinel). The tailer's
`file_offsets()` (`tail.rs:593`) skips files with fingerprint 0.

**Why the spec misses it**: `Init` starts with an empty file
(`file_content = <<>>`) but gives it identity 1 immediately. The spec
does not model the "empty file has no identity" case.

**Could it hide a bug?** If a file is empty at startup but gets data
later, its fingerprint changes from 0 to a real hash. The tailer skips
it for checkpointing while it is empty. When data arrives, the
fingerprint changes, and the file gets a new SourceId. The old SourceId
(0) has no checkpoint. The new SourceId starts at offset 0.

Edge case: if the file was previously checkpointed (before it was
truncated to 0), the old checkpoint is under the old fingerprint. After
truncation, the empty file has fingerprint 0 (skipped). After new data
arrives, the new fingerprint has no checkpoint -- correct, starts from 0.

This seems correct, but the spec does not model the transient state
where the file exists but has no usable identity.

**How to add it**:
```tla
\* CopyTruncate produces a file with identity 0 (no fingerprint)
CopyTruncate ==
    ...
    /\ file_identity' = 0  \* empty file sentinel
    /\ ...

\* AppendLine to empty file triggers fingerprint computation
AppendLineToEmpty ==
    /\ file_identity = 0
    /\ next_line_id <= MaxLines
    /\ file_content' = Append(file_content, next_line_id)
    /\ file_identity' = next_identity
    /\ next_identity' = next_identity + 1
    /\ ...
```

**Priority**: LOW -- the real behavior appears correct. Modeling it
would increase confidence but is unlikely to find bugs.

---

## Gap 8: File With No Trailing Newline (Last Line Never Terminated)

**Real-world scenario**: A process writes `{"msg":"hello"}` to a log
file and exits without writing a newline. The file ends without `\n`.

In the real code, `tail.rs` emits `TailEvent::EndOfFile` when no new
data is available. `framed.rs:129-145` handles this by appending a
synthetic `\n` to the remainder and processing it as a complete line.

**Why the spec misses it**: Lines in the spec are atomic and always
complete. The `FlushPartialBatch` action flushes when the tailer is
caught up, but it operates on complete lines already in `framer_buf`.
There is no concept of a line stuck in the remainder waiting for a
newline that never comes.

**Could it hide a bug?** The EOF flush is a separate code path from
normal framing. If there is a bug in the synthetic-newline path, the
spec cannot find it because it does not model incomplete lines.

The interaction with checkpointing is the concern: after the EOF flush,
the remainder is cleared. The checkpoint offset should be at the end of
the file (all bytes consumed). This appears correct in the code.

**How to add it**: Would require byte-level modeling (see Gap 5). In a
line-level model, this cannot be meaningfully represented.

**Priority**: LOW -- the code handles this correctly and has test
coverage (`eof_flushes_remainder` test in `framed.rs`).

---

## Gap 9: Batch Permanently Rejected by Output

**Real-world scenario**: The output sink returns a permanent error (400
Bad Request, schema mismatch, etc.). The batch cannot be retried.

In `pipeline.rs:576-579`, transform errors cause tickets to be rejected
(`ack_all_tickets(sending, false)`). For output errors, the worker pool
handles retries. But permanent rejection (if ever implemented) would
need to advance the checkpoint to avoid infinite retry loops.

**Why the spec misses it**: `AckBatch` is the only action that resolves
an in-flight batch, and it always succeeds (appends to `emitted`). There
is no `RejectBatch` action.

**Could it hide a bug?** Yes. The spec proves that emitted lines are
never corrupted and checkpoints never exceed file length. But it does
not model what happens when a batch is dropped:

1. If checkpoint advances: data loss (skipped lines)
2. If checkpoint does NOT advance: infinite retry on restart (the batch
   is re-read and re-sent, hits the same permanent error forever)

The real system currently drops the batch and rejects the tickets
(`ack_all_tickets(sending, false)`), which means the checkpoint does
NOT advance. On restart, the data is re-read. If the error is permanent,
this creates an infinite loop.

**How to add it**:
```tla
\* Batch rejected: data not delivered, checkpoint NOT advanced
RejectBatch ==
    /\ alive
    /\ Len(in_flight_batch) > 0
    /\ in_flight_batch' = <<>>
    /\ in_flight_source' = 0
    /\ in_flight_offset' = 0
    \* No change to checkpoints — will re-read on restart
    /\ UNCHANGED <<checkpoints, emitted, ...>>

\* Batch dropped: data not delivered, checkpoint IS advanced (skip)
DropBatch ==
    /\ alive
    /\ Len(in_flight_batch) > 0
    /\ in_flight_batch' = <<>>
    \* Advance checkpoint despite non-delivery — data loss
    /\ checkpoints' = [checkpoints EXCEPT
         ![in_flight_source] = in_flight_offset]
    /\ in_flight_source' = 0
    /\ in_flight_offset' = 0
    /\ UNCHANGED <<emitted, ...>>
```

Adding `RejectBatch` would let us verify that rejected batches are
retried. Adding `DropBatch` would let us detect data loss (violation of
a completeness property).

**Priority**: HIGH -- the infinite-retry-on-permanent-error scenario
is a real operational concern. The spec should model both paths to
clarify the design intent.

---

## Gap 10: Transform Error Drops a Batch

**Real-world scenario**: The SQL transform fails (e.g., type error,
UDF exception). In `pipeline.rs:572-579`, the batch is dropped and
tickets are rejected.

**Why the spec misses it**: The spec has no transform step. Batching
goes directly from `pipeline_batch` to `in_flight_batch`.

**Could it hide a bug?** Same issue as Gap 9 -- rejected tickets mean
the checkpoint does not advance, so on restart the same data is re-read
and re-transformed. If the SQL error is deterministic (e.g., malformed
data), this creates an infinite loop.

**How to add it**: Same as Gap 9's `RejectBatch` action, but triggered
between `FormBatch` and `SendBatch`.

**Priority**: HIGH -- same reasoning as Gap 9. The design needs to
decide: should transform errors skip the data (advance checkpoint) or
retry (leave checkpoint)?

---

## Gap 11: Channel Full (Backpressure Blocks Input Thread)

**Real-world scenario**: The async pipeline is slow (transform or output
stalled). The bounded channel (capacity 16) fills up. The input thread's
`blocking_send` blocks. While blocked, no files are read, no poll cycles
run, no rotation is detected.

In the real code, `input_poll_loop` alternates between `source.poll()`
and `tx.blocking_send()`. If `blocking_send` blocks, the entire input
thread stalls.

**Why the spec misses it**: The spec has no concept of a bounded channel
or backpressure. `SendBatch` is always enabled when there is a
`pipeline_batch`. There is no state where the pipeline is "full."

**Could it hide a bug?** While the input thread is blocked:
1. File rotation may happen (external process renames the file)
2. The tailer does not detect it (no poll cycle running)
3. New data is written to the new file at the same path
4. When the input thread unblocks and polls, it detects the rotation
   and drains the old fd

This sequence is safe -- the old fd still works, the rotation is
detected on the next poll. But if the old file is DELETED during the
block (not just renamed), and the OS recycles the inode, the tailer
could miss the rotation entirely.

**How to add it**:
```tla
VARIABLE channel_full  \* BOOLEAN

\* SendBatch blocked when channel is full
SendBatch ==
    /\ alive
    /\ Len(pipeline_batch) > 0
    /\ ~channel_full
    /\ ...

\* Channel becomes full (environment action)
ChannelFull ==
    /\ ~channel_full
    /\ channel_full' = TRUE
    /\ UNCHANGED <<...all other vars...>>
```

**Priority**: MEDIUM -- backpressure-induced rotation blindness is a
real scenario in high-throughput deployments. The main risk is that the
spec proves liveness (EventualEmission) without considering that the
pipeline can stall indefinitely.

---

## Gap 12: Shutdown While Framer Has Remainder

**Real-world scenario**: Graceful shutdown is triggered. The framer has
a partial line in its remainder (bytes without a trailing newline). The
pipeline drains the channel and flushes remaining buffered data, but
the partial line in the framer's remainder was never sent to the channel
(it was waiting for a newline).

In `pipeline.rs:419-428`, the shutdown drain flushes `scan_buf` (bytes
already in the pipeline). But the input thread's framer remainder is
never flushed -- the input thread exits when the shutdown token is
cancelled, and its local framer state is dropped.

**Why the spec misses it**: `framer_buf` in the spec contains complete
lines (line-level model). The only "remainder" concept is incomplete
batches (fewer than BatchSize lines), which `FlushPartialBatch` handles.
There is no concept of bytes in the framer that cannot form a complete
line.

**Could it hide a bug?** Yes -- partial data loss on shutdown. However,
this is documented as acceptable in the file-tailing-design.md (option C:
"Drop partial on shutdown, data loss is minimal -- one partial line per
file at shutdown time").

The checkpoint concern: if the checkpoint was advanced past the partial
line's starting offset (because the tailer's `read_offset` includes
those bytes), then on restart the partial line's bytes are skipped. This
is the same issue as Gap 5.

**How to add it**: Requires byte-level modeling. In the current line
model, `Crash` already handles this -- it clears `framer_buf` and on
restart, re-reads from checkpoint. The issue is that the checkpoint may
be too far ahead (Gap 5).

**Priority**: MEDIUM -- documented acceptable loss, but interacts with
the checkpoint offset bug (Gap 5).

---

## Gap 13: Checkpoint Flush Fails (Disk Full, Permission Error)

**Real-world scenario**: The checkpoint file cannot be written (disk full,
read-only filesystem, permission denied). The process continues running
with stale checkpoints. On crash, it restarts from the stale checkpoint,
re-processing already-delivered data.

**Why the spec misses it**: `AckBatch` atomically advances
`checkpoints[in_flight_source]`. There is no concept of checkpoint
persistence failure.

**Could it hide a bug?** This is a durability gap, not a correctness
gap. At-least-once delivery is preserved (stale checkpoint causes
re-delivery, not loss). The risk is operational: excessive re-delivery
after a crash following a long period of checkpoint write failures.

**How to add it**:
```tla
VARIABLE persisted_checkpoints  \* [SourceIds -> Nat]

\* AckBatch advances in-memory checkpoint (always succeeds)
AckBatch ==
    /\ checkpoints' = [checkpoints EXCEPT ...]  \* in-memory

\* Checkpoint flush may succeed or fail (nondeterministic)
FlushCheckpoint ==
    /\ alive
    /\ \/ persisted_checkpoints' = checkpoints  \* success
       \/ persisted_checkpoints' = persisted_checkpoints  \* failure

\* Restart uses persisted, not in-memory
Restart ==
    /\ read_offset' = persisted_checkpoints[file_identity]
```

**Priority**: MEDIUM -- the at-least-once guarantee is preserved, but
the spec implicitly assumes checkpoint persistence always succeeds.
Making this explicit would improve the model's realism.

---

## Gap 14: Checkpoint File Corrupted (Torn Write)

**Real-world scenario**: The process crashes while writing the checkpoint
file. The file is half-written and unreadable.

In the real system, this should be prevented by atomic writes (write to
temp file, then rename). If the code uses `write()` directly, a crash
mid-write produces a torn file.

**Why the spec misses it**: Checkpoints are a function (mathematical
map) that is always in a consistent state. There is no concept of partial
updates.

**Could it hide a bug?** If the checkpoint file is corrupted, the
process either fails to start (good -- operator intervention needed) or
falls back to offset 0 (re-reads everything). The spec models neither.

**How to add it**:
```tla
\* On restart, checkpoint may be corrupted (falls back to 0)
Restart ==
    /\ \/ read_offset' = checkpoints[file_identity]  \* normal
       \/ read_offset' = 0  \* corrupted checkpoint, start over
```

**Priority**: LOW -- atomic writes (rename) prevent this in practice.
The spec's abstraction is reasonable.

---

## Gap 15: Glob Rescan Discovers Files During Processing

**Real-world scenario**: Glob rescan (`tail.rs:267-298`) discovers new
files. These files get `start_from_end = false` (read from beginning).
But existing files are still being processed. The new files' data enters
the pipeline mixed with existing files' data, all through the same
shared `FramedInput`.

**Why the spec misses it**: The spec models a single file path. There
is no multi-file model.

**Could it hide a bug?** Yes -- this is the shared remainder bug
(file-tailing-design.md issue #797). Data from file A's partial line is
concatenated with data from file B, producing a corrupted line. The spec
cannot find this because it only models one file.

**How to add it**: Extend the spec to model 2+ files:
```tla
CONSTANTS MaxFiles  \* e.g., 2

VARIABLES
    files,          \* [1..MaxFiles -> Seq(LineValues)]
    file_ids,       \* [1..MaxFiles -> SourceIds]
    file_offsets,   \* [1..MaxFiles -> Nat]
    \* Single shared framer (the bug)
    framer_buf,
    framer_source
```

**Priority**: HIGH -- the shared remainder bug is a known critical issue
(file-tailing-design.md priority 2). The spec should model multi-file to
verify the fix (per-source remainder) once implemented.

---

## Gap 16: LRU File Eviction While Data Is in Pipeline

**Real-world scenario**: The tailer has more open files than
`max_open_files` (1024). It evicts the least-recently-read files
(`tail.rs:501-517`), saving their offsets in `evicted_offsets`. If
evicted data is still in the pipeline (in the channel, in `scan_buf`,
or in an in-flight batch), the checkpoint for that source may not be
written before eviction.

When the file is re-opened, it resumes from the saved offset. But if
the pipeline has already advanced the checkpoint past the saved offset
(from an acked batch), the re-open uses the stale eviction offset
instead of the checkpoint, causing duplicate reads. Conversely, if the
checkpoint was never advanced (batch still in flight), the re-open from
the eviction offset is correct.

**Why the spec misses it**: No concept of file eviction or re-opening.

**Could it hide a bug?** The interaction between eviction offsets and
checkpoint offsets is subtle. They are maintained independently -- the
eviction offset is in `evicted_offsets` (HashMap in FileTailer), while
the checkpoint is in the pipeline's `PipelineMachine`. On restart, the
pipeline loads from the checkpoint file, not from `evicted_offsets`
(which is in-memory only). So on crash, eviction offsets are lost and
the checkpoint is used correctly.

The risk is during normal operation (no crash): an evicted file is
re-opened at an old offset while the pipeline has already checkpointed
further. This causes duplicate reads but not data loss.

**How to add it**:
```tla
VARIABLE evicted_offset  \* 0..MaxLines (0 = not evicted)

EvictFile ==
    /\ alive
    /\ evicted_offset = 0
    /\ evicted_offset' = read_offset
    /\ read_offset' = 0  \* file closed
    /\ UNCHANGED <<...>>

ReopenFile ==
    /\ alive
    /\ evicted_offset > 0
    /\ read_offset' = evicted_offset
    /\ evicted_offset' = 0
    /\ UNCHANGED <<...>>
```

**Priority**: LOW -- the behavior is at-least-once (duplicates, not
loss), which is the documented delivery guarantee.

---

## Gap 17: Two Pipelines Sharing Same Checkpoint Directory

**Real-world scenario**: Misconfiguration: two pipeline instances watch
different files but write checkpoints to the same directory with the
same format. Checkpoint entries collide.

**Why the spec misses it**: Single-process, single-pipeline model.

**Could it hide a bug?** Yes -- checkpoint corruption. Pipeline A's
checkpoint for source X overwrites pipeline B's checkpoint for source Y
if they happen to have the same fingerprint (SourceId collision).

**How to add it**: Out of scope for the current spec, which models a
single pipeline. The fix is operational (namespaced checkpoint files).

**Priority**: LOW -- configuration error, not a design bug.

---

## Gap 18: SendBatch Uses Stale framer_source After FormBatch

**Real-world scenario**: This is a bug already in the spec, found
during this audit.

In the spec, `SendBatch` reads `framer_source` to determine which
source's offset to use for `in_flight_offset`. But `FormBatch` may have
changed `framer_source` to 0 (if all lines were consumed from the
buffer). The sequence:

1. `TailerRead` on current file: framer_buf = <<1,2>>, framer_source = 1
2. `FormBatch` (BatchSize=2): pipeline_batch = <<1,2>>, framer_buf = <<>>,
   framer_source = 0 (cleared because buffer is empty)
3. `SendBatch`: in_flight_source = framer_source = 0 (BUG!)
4. `AckBatch`: in_flight_source = 0, which is not in SourceIds,
   so checkpoint is NOT advanced.

The batch is delivered (appended to `emitted`) but the checkpoint never
advances. On crash + restart, the same lines are re-read and
re-delivered (duplicates but no loss).

Wait -- looking more carefully at the spec:

```tla
SendBatch ==
    ...
    /\ in_flight_source' = framer_source
```

If `framer_source = 0` at SendBatch time, then `in_flight_source = 0`.
In `AckBatch`:
```tla
    /\ IF in_flight_source \in SourceIds
          /\ in_flight_offset > checkpoints[in_flight_source]
       THEN checkpoints' = ...
       ELSE checkpoints' = checkpoints
```

Since `0 \notin SourceIds` (SourceIds = 1..MaxSourceIds), the ELSE
branch fires and checkpoints are NOT advanced. This means every batch
where the framer is emptied by FormBatch causes a checkpoint stall.

Actually, re-reading `FormBatch`:
```tla
    /\ IF Len(rest) = 0
       THEN framer_source' = 0
       ELSE framer_source' = framer_source
```

So if `BatchSize = Len(framer_buf)`, framer_source is set to 0, and
the subsequent SendBatch will fail to advance the checkpoint.

Let me check if TLC actually finds this... It passes safety, which
means the invariants are not violated. But `CheckpointMonotonicity`
only checks that checkpoints never decrease -- it does not check that
they eventually advance. And `EventualEmission` only checks liveness,
not checkpoint progress.

The consequence: checkpoints fall behind actual emission. This is
at-least-once (safe), but it means every restart causes more re-delivery
than necessary.

**How to fix**: `SendBatch` should capture the source from the batch
itself (the source that was active when the lines were read), not from
the current framer state. The simplest fix:

```tla
\* FormBatch also records the batch's source
VARIABLE pipeline_source  \* Nat: source identity of pipeline_batch

FormBatch ==
    ...
    /\ pipeline_source' = framer_source  \* capture BEFORE clearing

SendBatch ==
    ...
    /\ in_flight_source' = pipeline_source  \* use batch's source
```

**Priority**: HIGH -- this is a spec bug that causes unnecessary
re-delivery after every crash. The real code may or may not have the
same issue (the pipeline carries per-source checkpoints in the channel
message, not via framer state).

---

## Gap 19: Shutdown While Batch Is In-Flight (Drain Sequence)

**Real-world scenario**: Shutdown signal arrives. A batch is in-flight
(sent to output, awaiting ack). The pipeline must wait for the ack or
time out.

In `pipeline.rs:430-457`, the drain sequence is:
1. Pool drain with 60s timeout
2. Drain remaining acks
3. Transition machine Running -> Draining -> Stopped

**Why the spec misses it**: There is no shutdown action. `Crash` models
a hard crash (all in-memory state lost). There is no graceful shutdown
that waits for in-flight batches.

**Could it hide a bug?** The spec proves properties about crash
recovery, but not about graceful shutdown. Specifically, the code at
`pipeline.rs:447-454` prints a warning if in-flight batches remain at
shutdown. Those batches' data was sent to the output (may or may not have
been delivered) but the checkpoint was not advanced. On restart, the
data is re-read.

This is the at-least-once guarantee working correctly. But the spec does
not model this path, so we rely on code review rather than formal
verification.

**How to add it**:
```tla
GracefulShutdown ==
    /\ alive
    /\ alive' = FALSE
    \* In-flight batch may or may not have been acked externally
    /\ \/ (Len(in_flight_batch) = 0 /\ ...)  \* clean shutdown
       \/ (Len(in_flight_batch) > 0 /\ ...)  \* unclean, checkpoint stale
    /\ UNCHANGED <<checkpoints, ...>>  \* checkpoint NOT updated
```

**Priority**: MEDIUM -- the behavior is correct (at-least-once) but
unverified by the spec.

---

## Gap 20: Multiple Batches From Different Sources Interleaved

**Real-world scenario**: Two files are being tailed. Batches from file A
and file B arrive interleaved in the channel. The pipeline's `scan_buf`
accumulates bytes from both. On flush, a single RecordBatch contains
lines from both sources.

The checkpoint metadata (`batch_checkpoints` HashMap in pipeline.rs:316)
merges offsets from both sources. Each source gets its own checkpoint
entry. On ack, both sources' checkpoints advance.

**Why the spec misses it**: Single-file model. One source identity at a
time.

**Could it hide a bug?** The HashMap merge is correct for last-writer-wins
semantics -- each source's latest offset overwrites the previous. But
there is a subtle issue: if source A's batch is in the channel (with
checkpoint offset 100) and source A emits another batch (offset 200),
the second batch's offset 200 overwrites 100 in the HashMap. If the
flush only includes bytes from the first batch, the checkpoint advances
to 200 instead of 100.

Looking at the code more carefully: `batch_checkpoints.insert(sid, offset)`
overwrites unconditionally. If two channel messages from the same source
arrive before a flush, the second offset wins. But the second message's
bytes ARE in `scan_buf` (they were appended). So the flush includes both
messages' bytes. The offset 200 is correct.

Unless the flush happens between the two messages (size threshold hit
after the first message). Then the first flush has bytes for offset 100
but the HashMap has offset 200 (from a message that arrived after the
flush started but before the HashMap was drained... no, the HashMap is
drained at flush time: `std::mem::take(batch_checkpoints)`).

Actually, looking at `pipeline.rs:340-343`: if the size threshold is hit
after the first message, the flush drains the HashMap. The second message
arrives after the flush, so its offset goes into a fresh HashMap. This
is correct.

**How to add it**: Multi-source model (same as Gap 15).

**Priority**: MEDIUM -- the real code appears correct, but the spec
cannot verify this because it only models one source.

---

## Summary Table

| # | Gap | Priority | Category | Could hide bug? |
|---|-----|----------|----------|-----------------|
| 1 | Partial read (file grows during read) | MEDIUM | File system | Unlikely (single-thread design) |
| 2 | File deleted while being read | HIGH | File system | Yes: framer remainder corruption |
| 3 | Multiple rotations before drain | CRITICAL | File system | Yes: data loss from intermediate generation |
| 4 | Inode reuse after rotation | LOW | File system | Unlikely (fingerprint mismatch detects) |
| 5 | Partial line spanning reads / checkpoint offset | CRITICAL | Framing | Yes: checkpoint ahead of complete lines |
| 6 | MAX_REMAINDER_BYTES discard | MEDIUM | Framing | Checkpoint past discarded data |
| 7 | Empty file (fingerprint = 0) | LOW | Framing | Unlikely (correct by inspection) |
| 8 | No trailing newline | LOW | Framing | Unlikely (tested, EOF flush handles) |
| 9 | Batch permanently rejected | HIGH | Pipeline | Infinite retry loop |
| 10 | Transform error drops batch | HIGH | Pipeline | Infinite retry loop |
| 11 | Channel full (backpressure) | MEDIUM | Pipeline | Rotation blindness during stall |
| 12 | Shutdown with framer remainder | MEDIUM | Pipeline | Interacts with Gap 5 |
| 13 | Checkpoint flush fails | MEDIUM | Checkpoint | Excessive re-delivery |
| 14 | Checkpoint file corrupted | LOW | Checkpoint | Atomic rename prevents |
| 15 | Multi-file shared remainder | HIGH | Multi-file | Known critical bug (#797) |
| 16 | LRU eviction with in-flight data | LOW | Multi-file | At-least-once preserved |
| 17 | Two pipelines, same checkpoint dir | LOW | Checkpoint | Config error, not design |
| 18 | SendBatch uses stale framer_source | HIGH | Spec bug | Checkpoint stall on full-buffer batches |
| 19 | Graceful shutdown drain | MEDIUM | Pipeline | At-least-once, unverified |
| 20 | Multi-source batch interleaving | MEDIUM | Multi-file | Appears correct, unverified |

## Recommended Actions (Priority Order)

### Immediate (spec bugs)

1. **Gap 18**: Fix `SendBatch` to capture source identity at `FormBatch`
   time, not at send time. Add a `pipeline_source` variable. Re-run TLC.

2. **Gap 5**: Verify that the real code subtracts framer remainder from
   the checkpoint offset. The spec models this correctly
   (`read_offset - Len(framer_buf)`), but the actual `checkpoint_data()`
   in `tail.rs` returns raw `read_offset` without subtraction. **This
   may be a real data corruption bug in the implementation.**

### Short-term (model improvements)

3. **Gap 3**: Remove the `rotated_active = FALSE` guard from `RotateFile`,
   or add a queue of rotated files. Run with `MaxRotations=2` to explore
   rapid rotation.

4. **Gap 9/10**: Add `RejectBatch` and `DropBatch` actions. Add a
   property: "after MaxCrashes restarts, no batch is permanently stuck."

5. **Gap 2**: Add file deletion with framer remainder corruption
   scenario.

### Medium-term (spec extensions)

6. **Gap 15**: Extend to a 2-file model to verify per-source remainder
   design.

7. **Gap 13**: Split checkpoints into in-memory and persisted.

8. **Gap 11**: Model bounded channel capacity.

### Deferred (nice to have)

9. **Gap 1**: Nondeterministic partial reads.
10. **Gap 19**: Graceful shutdown action.
11. **Gaps 4, 7, 8, 14, 16, 17**: Low priority, current abstractions
    are reasonable.
