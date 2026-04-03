------------------------- MODULE FileCheckpoint -------------------------
(*
 * Formal model of logfwd's file tailing + framing + checkpoint + crash
 * recovery system.
 *
 * KEY DESIGN INSIGHT: Checkpoints are keyed by file identity (fingerprint),
 * not by file path. When a file is rotated or truncated, its identity
 * changes (new fingerprint). In-flight batches carry the identity of the
 * file they were read from. On ack, only THAT identity's checkpoint
 * advances. This cleanly handles rotation and truncation during in-flight
 * batches — a counterexample found by TLC on the single-checkpoint model.
 *
 * Models:
 *   - File content (append-only except for truncation)
 *   - File identity (changes on rotation, truncation, or content change)
 *   - Tailer: read from file, advance read_offset
 *   - Framer: per-source remainder, split on newlines
 *   - Pipeline: batch complete lines, send to output, receive acks
 *   - Checkpoint: per-source-identity offset persistence
 *   - Crash + Restart: lose in-memory state, resume from checkpoint
 *   - File rotation: rename old file, new file at same path (new identity)
 *   - Copytruncate: truncate to 0, recompute fingerprint (new identity)
 *
 * What this spec does NOT model:
 *   - Multiple output sinks (single abstract output)
 *   - SQL transforms (pass-through)
 *   - CRI parsing (orthogonal to checkpoint correctness)
 *   - Byte-level framing (lines are atomic units)
 *   - Multiple concurrent files (single file path, but identity changes)
 *)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    MaxLines,        \* Maximum total lines written across all generations
    MaxCrashes,      \* Bound on crash events
    MaxRotations,    \* Bound on rotation/truncation events
    BatchSize        \* Lines per batch (fixed for simplicity)

ASSUME MaxLines \in Nat /\ MaxLines >= 1
ASSUME MaxCrashes \in Nat /\ MaxCrashes >= 0
ASSUME MaxRotations \in Nat /\ MaxRotations >= 0
ASSUME BatchSize \in Nat /\ BatchSize >= 1

(*------------------------------------------------------------------------
 * Abstract line values and source identities.
 *
 * Lines are natural numbers 1..MaxLines (unique, monotonic).
 * Source identities are natural numbers representing fingerprints.
 * Each file generation gets a new identity.
 *------------------------------------------------------------------------*)

LineValues == 1..MaxLines

\* Source identities: 0 = no source, 1..N = file generations.
\* Identity changes on rotation or truncation (fingerprint changes).
MaxSourceIds == MaxRotations + 1
SourceIds == 1..MaxSourceIds

(*------------------------------------------------------------------------
 * State variables
 *------------------------------------------------------------------------*)

VARIABLES
    \* === File state (on disk) ===
    file_content,       \* Seq(LineValues): lines in the current file
    file_identity,      \* SourceIds: current file's fingerprint/identity
    next_line_id,       \* Nat: next unique line to write
    next_identity,      \* Nat: next identity to assign

    \* === Rotated file (on disk, may be drained) ===
    rotated_content,    \* Seq(LineValues): old file content (<<>> if none)
    rotated_identity,   \* Nat: old file's identity (0 if none)

    \* === Tailer state (in-memory, lost on crash) ===
    read_offset,        \* Nat: lines read from current file
    rotated_offset,     \* Nat: lines read from rotated file
    rotated_active,     \* BOOLEAN: draining rotated file?

    \* === Framer state (in-memory, lost on crash) ===
    \* Per-source remainder. In line-level model, lines are complete,
    \* so this is a buffer of lines awaiting batching.
    framer_buf,         \* Seq(LineValues): lines waiting to be batched
    framer_source,      \* SourceIds \cup {0}: which source the buffer came from

    \* === Pipeline state (in-memory, lost on crash) ===
    pipeline_batch,     \* Seq(LineValues): batch being formed
    pipeline_source,    \* SourceIds \cup {0}: source identity when batch was formed
    pipeline_offset,    \* Nat: checkpoint offset to use when batch is acked
    in_flight_batch,    \* Seq(LineValues): batch sent, awaiting ack
    in_flight_source,   \* Nat: source identity of the in-flight batch
    in_flight_offset,   \* Nat: offset to checkpoint on ack

    \* === Checkpoint state (persistent, survives crash) ===
    \* Map from source identity to last checkpointed offset.
    \* checkpoint[id] = N means "lines 1..N of source id are committed."
    checkpoints,        \* [SourceIds -> Nat]: per-source checkpoint offsets

    \* === Ghost variable (verification only) ===
    emitted,            \* Seq(LineValues): all lines successfully delivered

    \* === System state ===
    alive,              \* BOOLEAN: process running?
    crash_count,        \* Nat: crashes so far
    rotation_count      \* Nat: rotations/truncations so far

vars == <<file_content, file_identity, next_line_id, next_identity,
          rotated_content, rotated_identity,
          read_offset, rotated_offset, rotated_active,
          framer_buf, framer_source,
          pipeline_batch, pipeline_source, pipeline_offset,
          in_flight_batch, in_flight_source, in_flight_offset,
          checkpoints, emitted, alive, crash_count, rotation_count>>

(*------------------------------------------------------------------------
 * Helpers
 *------------------------------------------------------------------------*)

SubSeqFromTo(seq, from, to) ==
    IF from > to \/ from > Len(seq) THEN <<>>
    ELSE SubSeq(seq, from, IF to > Len(seq) THEN Len(seq) ELSE to)

UnreadLines ==
    SubSeqFromTo(file_content, read_offset + 1, Len(file_content))

UnreadRotatedLines ==
    SubSeqFromTo(rotated_content, rotated_offset + 1, Len(rotated_content))

SeqToSet(seq) == {seq[i] : i \in 1..Len(seq)}

(*------------------------------------------------------------------------
 * Type invariant
 *------------------------------------------------------------------------*)

TypeOK ==
    /\ file_content \in Seq(LineValues)
    /\ Len(file_content) <= MaxLines
    /\ file_identity \in SourceIds
    /\ next_line_id \in 1..(MaxLines + 1)
    /\ next_identity \in 1..(MaxSourceIds + 1)
    /\ rotated_content \in Seq(LineValues)
    /\ rotated_identity \in 0..MaxSourceIds
    /\ read_offset \in 0..MaxLines
    /\ rotated_offset \in 0..MaxLines
    /\ rotated_active \in BOOLEAN
    /\ framer_buf \in Seq(LineValues)
    /\ framer_source \in 0..MaxSourceIds
    /\ pipeline_batch \in Seq(LineValues)
    /\ pipeline_source \in 0..MaxSourceIds
    /\ pipeline_offset \in 0..MaxLines
    /\ in_flight_batch \in Seq(LineValues)
    /\ in_flight_source \in 0..MaxSourceIds
    /\ in_flight_offset \in 0..MaxLines
    /\ checkpoints \in [SourceIds -> 0..MaxLines]
    /\ emitted \in Seq(LineValues)
    /\ alive \in BOOLEAN
    /\ crash_count \in 0..MaxCrashes
    /\ rotation_count \in 0..MaxRotations

(*------------------------------------------------------------------------
 * Initial state
 *------------------------------------------------------------------------*)

Init ==
    /\ file_content     = <<>>
    /\ file_identity    = 1
    /\ next_line_id     = 1
    /\ next_identity    = 2        \* Identity 1 is the initial file
    /\ rotated_content  = <<>>
    /\ rotated_identity = 0
    /\ read_offset      = 0
    /\ rotated_offset   = 0
    /\ rotated_active   = FALSE
    /\ framer_buf       = <<>>
    /\ framer_source    = 0
    /\ pipeline_batch   = <<>>
    /\ pipeline_source  = 0
    /\ pipeline_offset  = 0
    /\ in_flight_batch  = <<>>
    /\ in_flight_source = 0
    /\ in_flight_offset = 0
    /\ checkpoints      = [s \in SourceIds |-> 0]
    /\ emitted          = <<>>
    /\ alive            = TRUE
    /\ crash_count      = 0
    /\ rotation_count   = 0

(*========================================================================
 * ENVIRONMENT ACTIONS
 *========================================================================*)

\* Append a new line to the current file.
AppendLine ==
    /\ next_line_id <= MaxLines
    /\ file_content' = Append(file_content, next_line_id)
    /\ next_line_id' = next_line_id + 1
    /\ UNCHANGED <<file_identity, next_identity,
                   rotated_content, rotated_identity,
                   read_offset, rotated_offset, rotated_active,
                   framer_buf, framer_source,
                   pipeline_batch, pipeline_source, pipeline_offset,
                   in_flight_batch, in_flight_source,
                   in_flight_offset, checkpoints, emitted,
                   alive, crash_count, rotation_count>>

\* File rotation: current file renamed away, new empty file created.
\* New file gets a new identity (different fingerprint).
\*
\* Known limitation (Gap 3): The guard `rotated_active = FALSE` prevents
\* a second rotation while the first rotated file is still being drained.
\* The real system can handle multiple rapid rotations (it holds an fd to
\* each rotated file), but this spec models only a single rotated-file
\* slot for state-space tractability. If multiple rotations occur before
\* the first rotated file is fully drained, intermediate data could be
\* lost in the real system — the spec intentionally does not explore this.
RotateFile ==
    /\ alive
    /\ rotation_count < MaxRotations
    /\ next_identity <= MaxSourceIds
    /\ Len(file_content) > 0
    /\ rotated_active = FALSE
    \* Old file moves to rotated slot, keeps its identity
    /\ rotated_content'  = file_content
    /\ rotated_identity' = file_identity
    /\ rotated_offset'   = read_offset
    /\ rotated_active'   = TRUE
    \* New empty file with new identity
    /\ file_content'  = <<>>
    /\ file_identity'  = next_identity
    /\ next_identity'  = next_identity + 1
    /\ read_offset'    = 0
    /\ rotation_count' = rotation_count + 1
    \* Clear framer if it was buffering data from the old file — the
    \* tailer will re-read the rotated file's unread portion separately.
    \* In reality, the framer keeps its buffer (data already read is not
    \* re-read). But the framer_source must update if we're switching files.
    /\ UNCHANGED <<next_line_id, framer_buf, framer_source,
                   pipeline_batch, pipeline_source, pipeline_offset,
                   in_flight_batch, in_flight_source,
                   in_flight_offset, checkpoints, emitted,
                   alive, crash_count>>

\* Copytruncate: file truncated to 0 bytes, same path.
\* Fingerprint changes (empty file ≠ original content), so new identity.
CopyTruncate ==
    /\ alive
    /\ rotation_count < MaxRotations
    /\ next_identity <= MaxSourceIds
    /\ Len(file_content) > 0
    \* File becomes empty with new identity
    /\ file_content'  = <<>>
    /\ file_identity'  = next_identity
    /\ next_identity'  = next_identity + 1
    /\ read_offset'    = 0
    /\ rotation_count' = rotation_count + 1
    \* Clear framer buffer — stale data from old content
    /\ framer_buf'    = <<>>
    /\ framer_source' = 0
    \* No rotated file to drain (copytruncate doesn't preserve the old file
    \* as a separate fd — the content is gone after truncation)
    /\ UNCHANGED <<next_line_id, rotated_content, rotated_identity,
                   rotated_offset, rotated_active,
                   pipeline_batch, pipeline_source, pipeline_offset,
                   in_flight_batch, in_flight_source,
                   in_flight_offset, checkpoints, emitted,
                   alive, crash_count>>

(*========================================================================
 * TAILER ACTIONS
 *========================================================================*)

\* Read unread lines from the current file.
TailerRead ==
    /\ alive
    /\ read_offset < Len(file_content)
    /\ LET new_lines == UnreadLines
       IN
       \* If framer is empty or same source, append
       /\ IF framer_source = 0 \/ framer_source = file_identity
          THEN /\ framer_buf' = framer_buf \o new_lines
               /\ framer_source' = file_identity
          \* Different source — in real system, per-source remainder
          \* handles this. In our single-file model, this shouldn't happen.
          \* Model it as overwriting (the per-source remainder handles
          \* the old source's data separately).
          ELSE /\ framer_buf' = new_lines
               /\ framer_source' = file_identity
       /\ read_offset' = Len(file_content)
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   rotated_content, rotated_identity,
                   rotated_offset, rotated_active,
                   pipeline_batch, pipeline_source, pipeline_offset,
                   in_flight_batch, in_flight_source,
                   in_flight_offset, checkpoints, emitted,
                   alive, crash_count, rotation_count>>

\* Read remaining lines from the rotated file.
TailerReadRotated ==
    /\ alive
    /\ rotated_active
    /\ rotated_offset < Len(rotated_content)
    /\ LET new_lines == UnreadRotatedLines
       IN
       /\ IF framer_source = 0 \/ framer_source = rotated_identity
          THEN /\ framer_buf' = framer_buf \o new_lines
               /\ framer_source' = rotated_identity
          ELSE /\ framer_buf' = new_lines
               /\ framer_source' = rotated_identity
       /\ rotated_offset' = Len(rotated_content)
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   rotated_content, rotated_identity,
                   read_offset, rotated_active,
                   pipeline_batch, pipeline_source, pipeline_offset,
                   in_flight_batch, in_flight_source,
                   in_flight_offset, checkpoints, emitted,
                   alive, crash_count, rotation_count>>

\* Finish draining the rotated file.
TailerFinishRotated ==
    /\ alive
    /\ rotated_active
    /\ rotated_offset = Len(rotated_content)
    /\ framer_buf = <<>>  \* All rotated data has been batched/sent
    /\ pipeline_batch = <<>>
    /\ in_flight_batch = <<>>
    /\ rotated_active'   = FALSE
    /\ rotated_content'  = <<>>
    /\ rotated_identity' = 0
    /\ rotated_offset'   = 0
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   read_offset, framer_buf, framer_source,
                   pipeline_batch, pipeline_source, pipeline_offset,
                   in_flight_batch, in_flight_source,
                   in_flight_offset, checkpoints, emitted,
                   alive, crash_count, rotation_count>>

(*========================================================================
 * FRAMER → PIPELINE ACTIONS
 *========================================================================*)

\* Form a full batch from the framer buffer.
FormBatch ==
    /\ alive
    /\ Len(framer_buf) >= BatchSize
    /\ pipeline_batch = <<>>
    /\ in_flight_batch = <<>>
    /\ LET batch == SubSeq(framer_buf, 1, BatchSize)
           rest  == SubSeqFromTo(framer_buf, BatchSize + 1, Len(framer_buf))
       IN
       /\ pipeline_batch' = batch
       /\ pipeline_source' = framer_source  \* Capture source BEFORE clearing framer_source
       \* Compute the checkpoint offset at batch-formation time, when we still
       \* know which source the framer_buf belongs to and how many lines remain.
       \* offset = (lines read from source) - (lines remaining in framer after batch)
       /\ pipeline_offset' = IF framer_source = file_identity
                             THEN read_offset - Len(rest)
                             ELSE IF framer_source = rotated_identity
                                  THEN rotated_offset - Len(rest)
                                  ELSE 0
       /\ framer_buf' = rest
       /\ IF Len(rest) = 0
          THEN framer_source' = 0
          ELSE framer_source' = framer_source
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   rotated_content, rotated_identity,
                   read_offset, rotated_offset, rotated_active,
                   in_flight_batch, in_flight_source, in_flight_offset,
                   checkpoints, emitted, alive, crash_count, rotation_count>>

\* Flush a partial batch (fewer than BatchSize) when file is caught up.
FlushPartialBatch ==
    /\ alive
    /\ Len(framer_buf) > 0
    /\ Len(framer_buf) < BatchSize
    /\ pipeline_batch = <<>>
    /\ in_flight_batch = <<>>
    \* Only flush when caught up (EndOfFile condition)
    /\ read_offset = Len(file_content)
    /\ pipeline_batch' = framer_buf
    /\ pipeline_source' = framer_source  \* Capture source BEFORE clearing framer_source
    \* Flush drains the entire framer_buf, so remaining = 0.
    /\ pipeline_offset' = IF framer_source = file_identity
                          THEN read_offset
                          ELSE IF framer_source = rotated_identity
                               THEN rotated_offset
                               ELSE 0
    /\ framer_buf' = <<>>
    /\ framer_source' = 0
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   rotated_content, rotated_identity,
                   read_offset, rotated_offset, rotated_active,
                   in_flight_batch, in_flight_source, in_flight_offset,
                   checkpoints, emitted, alive, crash_count, rotation_count>>

(*========================================================================
 * PIPELINE → OUTPUT ACTIONS
 *========================================================================*)

\* Send the batch to output. Record the source identity and offset
\* so that on ack, we advance the correct source's checkpoint.
SendBatch ==
    /\ alive
    /\ Len(pipeline_batch) > 0
    /\ in_flight_batch = <<>>
    /\ in_flight_batch'  = pipeline_batch
    /\ in_flight_source' = pipeline_source
    \* Gap 18 fix: pipeline_offset was computed at batch-formation time
    \* (FormBatch / FlushPartialBatch), when the framer still knew the
    \* correct source and remaining-line count. SendBatch just passes it
    \* through — reading framer_source here would be stale (cleared to 0
    \* when the framer buffer was fully drained).
    /\ in_flight_offset' = pipeline_offset
    /\ pipeline_batch'   = <<>>
    /\ pipeline_source'  = 0
    /\ pipeline_offset'  = 0
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   rotated_content, rotated_identity,
                   read_offset, rotated_offset, rotated_active,
                   framer_buf, framer_source,
                   checkpoints, emitted, alive, crash_count, rotation_count>>

\* Output acknowledges the batch. Advance the checkpoint for the
\* batch's source identity. The key insight: we advance the checkpoint
\* for in_flight_source, which may be different from the current
\* file_identity if rotation/truncation happened while in flight.
AckBatch ==
    /\ alive
    /\ Len(in_flight_batch) > 0
    /\ emitted' = emitted \o in_flight_batch
    /\ in_flight_batch' = <<>>
    \* Advance checkpoint for THIS source (not necessarily current file)
    /\ IF in_flight_source \in SourceIds
          /\ in_flight_offset > checkpoints[in_flight_source]
       THEN checkpoints' = [checkpoints EXCEPT
                ![in_flight_source] = in_flight_offset]
       ELSE checkpoints' = checkpoints
    /\ in_flight_source' = 0
    /\ in_flight_offset' = 0
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   rotated_content, rotated_identity,
                   read_offset, rotated_offset, rotated_active,
                   framer_buf, framer_source,
                   pipeline_batch, pipeline_source, pipeline_offset,
                   alive, crash_count, rotation_count>>

(*========================================================================
 * CRASH + RESTART
 *========================================================================*)

Crash ==
    /\ alive
    /\ crash_count < MaxCrashes
    /\ alive' = FALSE
    /\ crash_count' = crash_count + 1
    \* All in-memory state destroyed
    /\ read_offset'      = 0
    /\ rotated_offset'   = 0
    /\ rotated_active'   = FALSE
    /\ framer_buf'       = <<>>
    /\ framer_source'    = 0
    /\ pipeline_batch'   = <<>>
    /\ pipeline_source'  = 0
    /\ pipeline_offset'  = 0
    /\ in_flight_batch'  = <<>>
    /\ in_flight_source' = 0
    /\ in_flight_offset' = 0
    \* Persistent state survives
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   rotated_content, rotated_identity,
                   checkpoints, emitted, rotation_count>>

Restart ==
    /\ ~alive
    /\ alive' = TRUE
    \* Resume from checkpoint for the current file identity.
    \* If file was truncated (new identity, checkpoint = 0), start at 0.
    /\ LET cp == checkpoints[file_identity]
       IN read_offset' = IF cp <= Len(file_content) THEN cp ELSE 0
    \* Rotated file's fd is closed on crash; can't reopen by inode.
    \* Lines in the rotated file that weren't checkpointed are lost.
    /\ rotated_active'   = FALSE
    /\ rotated_content'  = <<>>
    /\ rotated_identity' = 0
    /\ rotated_offset'   = 0
    /\ UNCHANGED <<file_content, file_identity, next_line_id, next_identity,
                   framer_buf, framer_source,
                   pipeline_batch, pipeline_source, pipeline_offset,
                   in_flight_batch, in_flight_source,
                   in_flight_offset, checkpoints, emitted,
                   crash_count, rotation_count>>

(*========================================================================
 * Next-state relation
 *========================================================================*)

Next ==
    \/ AppendLine
    \/ RotateFile
    \/ CopyTruncate
    \/ TailerRead
    \/ TailerReadRotated
    \/ TailerFinishRotated
    \/ FormBatch
    \/ FlushPartialBatch
    \/ SendBatch
    \/ AckBatch
    \/ Crash
    \/ Restart

(*========================================================================
 * Fairness
 *========================================================================*)

Fairness ==
    /\ WF_vars(TailerRead)
    /\ WF_vars(TailerReadRotated)
    /\ WF_vars(TailerFinishRotated)
    /\ WF_vars(FormBatch)
    /\ WF_vars(FlushPartialBatch)
    /\ WF_vars(SendBatch)
    /\ WF_vars(AckBatch)
    /\ WF_vars(Restart)

Spec == Init /\ [][Next]_vars /\ Fairness

(*========================================================================
 * SAFETY INVARIANTS
 *========================================================================*)

\* Every emitted line was actually written.
NoCorruption ==
    \A i \in 1..Len(emitted) :
        emitted[i] \in LineValues /\ emitted[i] < next_line_id

\* Checkpoint for any source never exceeds lines available for that source.
\* For the current file: checkpoints[file_identity] <= Len(file_content).
\* For rotated file (if active): checkpoints[rotated_identity] <= Len(rotated_content).
\* For any other source: checkpoint stays at whatever it was.
CheckpointBounded ==
    /\ alive => checkpoints[file_identity] <= Len(file_content)
    /\ (alive /\ rotated_active /\ rotated_identity \in SourceIds)
           => checkpoints[rotated_identity] <= Len(rotated_content)

\* Read offset never exceeds file length.
ReadOffsetBounded ==
    alive => read_offset <= Len(file_content)

\* Rotated read offset never exceeds rotated file length.
RotatedReadBounded ==
    (alive /\ rotated_active) => rotated_offset <= Len(rotated_content)

\* At most one batch in flight.
InFlightInvariant ==
    (Len(in_flight_batch) > 0) => (Len(pipeline_batch) = 0)

\* All in-pipeline lines were actually written.
PipelineConsistency ==
    alive =>
        LET all_lines == SeqToSet(framer_buf) \cup
                         SeqToSet(pipeline_batch) \cup
                         SeqToSet(in_flight_batch)
        IN \A v \in all_lines : v \in LineValues /\ v < next_line_id

\* No line emitted before it was written.
NoEmitBeforeWrite ==
    \A i \in 1..Len(emitted) : emitted[i] < next_line_id

\* Checkpoint monotonicity per source: each source's checkpoint never
\* decreases. (New sources start at 0; existing sources only increase.)
CheckpointMonotonicity ==
    [][\A s \in SourceIds : checkpoints'[s] >= checkpoints[s]]_vars

(*========================================================================
 * LIVENESS PROPERTIES
 *========================================================================*)

\* If a line is in the current file, system is alive, and file isn't about
\* to be truncated, the line eventually gets emitted or system crashes.
Progress ==
    \A v \in LineValues :
        (alive /\ v \in SeqToSet(file_content) /\ read_offset < Len(file_content))
            ~> (v \in SeqToSet(emitted) \/ ~alive)

\* After all writes stop, every line in the current file is eventually emitted.
EventualEmission ==
    \A v \in LineValues :
        (v \in SeqToSet(file_content) /\ alive /\ next_line_id > MaxLines)
            ~> (v \in SeqToSet(emitted))

(*========================================================================
 * REACHABILITY (vacuity guards)
 * As INVARIANTS: TLC violation = state IS reachable.
 *========================================================================*)

EmitOccurs == ~(Len(emitted) > 0)
CheckpointAdvances == ~(\E s \in SourceIds : checkpoints[s] > 0)
CrashOccurs == ~(crash_count > 0)
RotationOccurs == ~(rotation_count > 0)
RestartOccurs == ~(alive /\ crash_count > 0)

=============================================================================
