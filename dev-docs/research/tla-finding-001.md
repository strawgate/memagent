# TLA+ Finding 001: Checkpoint must be per-file-identity, not per-path

Date: 2026-04-03
Found by: TLC model checker on FileCheckpoint.tla

## The counterexample

```
State 1: Init (empty file)
State 2: AppendLine (file = <<1>>)
State 3: TailerRead (framer_buf = <<1>>, read_offset = 1)
State 4: FlushPartialBatch (pipeline_batch = <<1>>)
State 5: SendBatch (in_flight_batch = <<1>>, in_flight_offset = 1)
State 6: CopyTruncate (file = <<>>, checkpoint_offset = 0)
State 7: AckBatch → checkpoint_offset = 1, but file is empty!
```

On restart, we'd seek to offset 1 in a file with 0 bytes. We'd miss
any new data written to the file after truncation.

## Root cause

The spec has one `checkpoint_offset` for "the file at the watched path."
But after rotation or truncation, the file at that path is a DIFFERENT
file (different content, possibly different inode). The in-flight batch's
offset belongs to the OLD file, not the new one.

## How production collectors solve this

Every production collector keys checkpoints by **file identity** (fingerprint,
inode, or content hash), not by file path:

- **Checkpoint store**: `Map<FileIdentity, Offset>`
- **Each batch carries**: the FileIdentity of the file it was read from
- **On ack**: advance the checkpoint for THAT identity
- **On rotation**: new file → new identity → new checkpoint entry at 0
- **Old identity**: its checkpoint advancing is correct (reflects progress
  on the old file, useful if the old file is still being drained)

## Impact on our design

Our `PipelineMachine` already has `SourceId` (per-file identity).
The checkpoint store already maps `SourceId → offset`. The design
is correct at the pipeline level.

The TLA+ spec needs revision: instead of one global `checkpoint_offset`,
model a function from source identity to offset. This also correctly
models the multi-file case (glob tailing).

## Impact on the spec

The `checkpoint_gen` variable was a workaround for this. The correct
model has:

```
checkpoint == [source_id \in SourceIds -> Nat]
```

Where rotation creates a new source_id and truncation resets the
existing source_id's checkpoint to 0 (same inode, same identity).

Wait — copytruncate keeps the same inode. So the identity doesn't
change. The offset SHOULD reset to 0. But the in-flight batch from
the pre-truncation read has the old offset. When it acks, it should
NOT advance the checkpoint because the file was truncated.

This means we need either:
1. A generation counter per source_id (increment on truncation)
2. The batch carries a snapshot of the source's state at send time,
   and ack checks if the state is still valid

Option 1 is simpler. The batch carries `(source_id, generation)`.
On ack, if the source's current generation matches, advance checkpoint.
If not (truncation happened), discard the offset — the data was
delivered but the checkpoint can't advance because the file changed.

## Revised spec variables (sketch)

```tla
\* Replace single checkpoint_offset with per-source map
VARIABLES
    source_gen,         \* [SourceId -> Nat]: generation counter per source
    source_checkpoint,  \* [SourceId -> Nat]: checkpoint offset per source
    in_flight_gen,      \* Nat: generation at time of SendBatch

\* AckBatch only advances if generation matches
AckBatch ==
    /\ in_flight_gen = source_gen[current_source]
    /\ source_checkpoint' = [source_checkpoint EXCEPT
         ![current_source] = in_flight_offset]
```

## Action items

1. Revise FileCheckpoint.tla to use per-source checkpoint with
   generation counter
2. Re-run TLC to verify safety properties pass
3. Update the implementation design (per-source-remainder-design.md)
   to include generation tracking
4. Ensure PipelineMachine's BatchTicket carries source generation
