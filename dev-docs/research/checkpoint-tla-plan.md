# FileCheckpoint TLA+ Specification: Design Document

Date: 2026-03-31
Context: Phase 7b — file tailing + checkpoint formal model

## Motivation

PipelineMachine.tla (Phase 7a, PR #651) proves the batch lifecycle is correct:
drain completes, checkpoints advance monotonically, ordered ack works. But it
does not model the DATA path: how bytes flow from files through framing and
batching, and how checkpoints interact with crash recovery, file rotation, and
copytruncate.

The file tailing audit (`dev-docs/research/file-tailing-audit.md`) found six
bugs (#796-#801). Several of these interact with checkpoint correctness in subtle
ways:

- **#796 (truncation event not emitted)**: after copytruncate, stale remainder
  corrupts new data. If a checkpoint persists an offset into the old content,
  restart re-reads stale data.
- **#797 (shared remainder)**: interleaved files corrupt each other's framing.
  Checkpoint offset for file A may account for bytes that actually came from
  file B.
- **#800 (unbounded read)**: an OOM during a large read can crash the process,
  and the checkpoint may not reflect how far data was actually processed.

A TLA+ model of the file-to-emission data path lets us verify that these fixes
compose correctly and that the fundamental safety properties hold under crash,
rotation, and truncation.

## Scope

### What FileCheckpoint.tla models

1. **File content**: append-only sequence of lines, with truncation and rotation
   as non-deterministic environment actions
2. **Tailer**: reads unread lines from the current file, advances `read_offset`
3. **Framer**: splits data on newlines (implicit in the line-level model),
   manages `framer_buf` (per-source remainder equivalent)
4. **Pipeline**: batches lines into fixed-size groups, sends to output
5. **Checkpoint**: persists `checkpoint_offset` when output acks a batch
6. **Crash + Restart**: destroys all in-memory state; restart resumes from
   `checkpoint_offset` (at-least-once semantics)
7. **File rotation**: old file content moves to a "rotated" slot; tailer drains
   it before closing
8. **Copytruncate**: file content zeroed, same identity; tailer resets offset
   and clears framer state

### What it does NOT model

- Multiple output sinks (single abstract output suffices)
- SQL transforms (pass-through for correctness)
- CRI format parsing (orthogonal)
- Network message loss (PipelineMachine handles ack ordering)
- Multiple concurrent files (modeled as single file; per-file remainder is
  structurally the same problem scaled out)

### Abstraction level

Files are modeled as **sequences of abstract line values** (natural numbers),
not raw bytes. Newlines are implicit between sequence elements. This eliminates
byte-level complexity while preserving the essential properties:

- Lines are the atomic unit of emission (complete lines only)
- Offsets count lines (equivalent to byte offsets at newline boundaries)
- Remainder = partial line = not modeled (lines are always complete in the model)

The framer buffer models the real system's `framer_buf` / `remainder` as a
sequence of complete lines waiting to be batched. The key correctness property
(remainder cleared on truncation) is modeled by `CopyTruncate` zeroing
`framer_buf`.

## Safety Properties

| Property | Type | Description |
|----------|------|-------------|
| `NoCorruption` | Invariant | Every emitted line was written to a file |
| `CheckpointAtBoundary` | Invariant | Checkpoint offset is at a line boundary |
| `CheckpointMonotonicity` | Temporal | Checkpoint never decreases (except truncation reset to 0) |
| `ReadOffsetBounded` | Invariant | Read offset never exceeds file length |
| `RotatedReadBounded` | Invariant | Rotated file read offset never exceeds its length |
| `InFlightInvariant` | Invariant | At most one batch in flight |
| `PipelineConsistency` | Invariant | All in-pipeline lines were written to files |
| `NoEmitBeforeWrite` | Invariant | No line emitted before it was written |

### NoDataLoss (design-level argument)

True no-data-loss is a liveness property that depends on the system eventually
processing all data. In TLA+ this is expressed as `EventualEmission`: if a
line exists in the file and the system is alive and no more writes occur, the
line eventually appears in `emitted`.

The safety complement is `NoCorruption`: nothing fabricated. Together they give
the at-least-once guarantee: every line is emitted at least once and every
emitted line is real.

### Copytruncate data loss caveat

All four production shippers (Filebeat, Vector, Fluent Bit, OTel) document the
same limitation: with copytruncate, lines written between the last checkpoint
and the truncation event are lost if a crash occurs in that window. This is
inherent to copytruncate (the old content is destroyed). The spec models this
faithfully: `CopyTruncate` resets `checkpoint_offset` to 0 and clears
`framer_buf`, so uncheckpointed lines are gone.

## Liveness Properties

| Property | Type | Description |
|----------|------|-------------|
| `Progress` | Leads-to | Unread lines in a live system are eventually emitted |
| `EventualEmission` | Leads-to | After writes stop, every written line is emitted |
| `CheckpointProgress` | Leads-to | In-flight batch ack eventually advances checkpoint |

### Fairness assumptions

| Action | Fairness | Justification |
|--------|----------|---------------|
| `TailerRead` | WF | Tailer poll loop runs continuously |
| `TailerReadRotated` | WF | Same poll loop reads rotated file |
| `TailerFinishRotated` | WF | Cleanup fires when drain completes |
| `FormBatch` | WF | Batching runs on every poll cycle |
| `FlushPartialBatch` | WF | Timeout/EndOfFile flush is reliable |
| `SendBatch` | WF | Output send is always attempted |
| `AckBatch` | WF | Output eventually responds (retry budget) |
| `Restart` | WF | Crashed system eventually restarts |
| `Crash` | NONE | Adversarial — crashes are not guaranteed |
| `AppendLine` | NONE | Environment — file may stop growing |
| `RotateFile` | NONE | Environment — rotation is not guaranteed |
| `CopyTruncate` | NONE | Environment — truncation is not guaranteed |

## Model Checking Parameters

### Safety model (FileCheckpoint.cfg)

| Constant | Value | Rationale |
|----------|-------|-----------|
| `MaxLines` | 4 | Enough for 2 full batches + partial |
| `MaxBatches` | 2 | Two batch cycles exercises pipeline |
| `MaxCrashes` | 1 | One crash + restart covers the recovery path |
| `MaxRotations` | 1 | One rotation exercises old-file drain |
| `BatchSize` | 2 | Small enough for tractable state space |

Estimated state space: ~200K states. Expected TLC runtime: < 2 minutes on a
modern laptop.

### Liveness model (FileCheckpoint.liveness.cfg)

| Constant | Value | Rationale |
|----------|-------|-----------|
| `MaxLines` | 3 | Minimal for liveness |
| `MaxBatches` | 2 | One full cycle + one partial |
| `MaxCrashes` | 1 | Recovery liveness |
| `MaxRotations` | 0 | Rotation adds state; test separately |
| `BatchSize` | 1 | Minimal batching for fast convergence |

Estimated state space: ~50K states. Expected TLC runtime: < 5 minutes.
Liveness checking is quadratic in state space (SCC detection), so small
constants are critical.

### Thorough model (not a separate .cfg; use MCFileCheckpoint constants)

| Constant | Value |
|----------|-------|
| `MaxLines` | 6 |
| `MaxBatches` | 3 |
| `MaxCrashes` | 2 |
| `MaxRotations` | 1 |
| `BatchSize` | 2 |

Estimated: ~2M states, ~30 minutes. Run pre-release, not in CI.

## Relationship to PipelineMachine.tla

### Two specs, not one

PipelineMachine.tla and FileCheckpoint.tla model **different abstraction levels**
of the same system:

| Aspect | PipelineMachine.tla | FileCheckpoint.tla |
|--------|--------------------|--------------------|
| Focus | Batch lifecycle (create/send/ack/drain) | Data flow (file to emission) |
| Granularity | Batch IDs (abstract) | Lines (abstract content) |
| Sources | Multiple sources, per-source ordering | Single file (representative) |
| Checkpoint | Monotonic sequence number | Byte offset with truncation reset |
| Crash | Not modeled (no persistent state) | Core feature |
| Rotation | Not modeled | Core feature |
| Drain | Core feature | Not modeled (orthogonal) |

They are **complementary**, not redundant:

- PipelineMachine proves: drain completes, checkpoint ordering is correct,
  no batch left behind.
- FileCheckpoint proves: every written line is emitted, checkpoints survive
  crashes, rotation and truncation are handled correctly.

### Where they connect

The `AckBatch` action in FileCheckpoint corresponds to the state where
PipelineMachine's `AckBatch` fires AND `NewCommitted` advances the checkpoint.
In FileCheckpoint this is simplified to a single atomic action (ack advances
checkpoint). The multi-source ordered-ack complexity is in PipelineMachine.

### Phase 7b: PipelineBatch.tla

PHASES.md describes Phase 7b as "PipelineBatch.tla -- channel + batching
protocol." FileCheckpoint.tla covers the batching and channel aspects from
the file-centric perspective. PipelineBatch.tla (if still needed) would model
the mpsc channel between the input thread and the async pipeline, focusing on
backpressure and message ordering. These are separable concerns:

- **FileCheckpoint.tla**: file to emission correctness (this spec)
- **PipelineBatch.tla**: channel protocol, backpressure, multi-source
  interleaving (future, after Phase 5c wires PipelineMachine into pipeline.rs)

If the channel protocol turns out to be simple enough (bounded mpsc with
blocking send), PipelineBatch.tla may not be needed at all. The critical
properties (no data loss, checkpoint correctness) are already covered by
FileCheckpoint.tla + PipelineMachine.tla together.

## File Structure

```text
dev-docs/tla/
  FileCheckpoint.tla           -- clean spec, no TLC overrides
  MCFileCheckpoint.tla         -- TLC config: model constants for each model size
  FileCheckpoint.cfg           -- safety model (~200K states, < 2 min)
  FileCheckpoint.liveness.cfg  -- liveness model (~50K states, < 5 min)
  FileCheckpoint.coverage.cfg  -- reachability / vacuity guards
```

These live in `dev-docs/tla/` rather than `tla/` to separate them from the
existing Phase 7a specs. Once both are stable, they can be consolidated under
`tla/` with a shared README.

## Open Questions

### 1. Per-file remainder in the model

The current model uses a single file. The per-file remainder bug (#797) is
about interleaving across multiple files. A multi-file extension of this spec
could model two files with separate `framer_buf` variables to verify that the
per-file remainder fix is correct. This would roughly double the state space.

### 2. Checkpoint persistence atomicity

The real checkpoint is persisted via atomic rename (write to temp file, rename
over old checkpoint). The model treats checkpoint writes as atomic. If we want
to verify torn-write recovery, we would need to model a two-phase checkpoint
write. This is probably not worth the state space cost — atomic rename is well-
understood and used by all four shippers.

### 3. Duplicate emission

At-least-once semantics means duplicates are possible after crash recovery
(lines between checkpoint and read_offset are re-read). The spec does not
currently verify a bound on duplicates. A `MaxDuplicates` invariant could
be added: no line appears in `emitted` more than K times (K = MaxCrashes + 1).

### 4. Shutdown/drain interaction

FileCheckpoint.tla does not model graceful shutdown (drain). The drain
guarantee is in PipelineMachine.tla. A composition spec that combines both
would prove: "on graceful shutdown, all lines up to the last checkpoint are
emitted, and the checkpoint is durably persisted." This is the most complete
end-to-end property but requires a significantly larger model.

## Next Steps

1. Run TLC on the safety model to validate the spec is well-formed
2. Run the coverage model to verify all reachability assertions fire
3. Run the liveness model to check Progress and EventualEmission
4. If liveness fails, examine the counterexample — likely an issue with
   fairness assumptions or the interaction between Crash and progress
5. Add the spec to CI (Phase 7 tracking issue #272)
6. Consider multi-file extension for #797 verification
