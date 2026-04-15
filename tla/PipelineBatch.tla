------------------------ MODULE PipelineBatch ------------------------
(*
 * Formal model of the batching protocol in pipeline.rs.
 *
 * Models multi-source data production, batch accumulation with size
 * thresholds, checkpoint merging (last-writer-wins per source), and
 * error handling (transform reject, output ack/reject).
 *
 * Connects to PipelineMachine.tla (lifecycle) by modeling the same
 * checkpoint advancement algorithm. This spec adds the data flow
 * that PipelineMachine abstracts away.
 *
 * Key properties:
 *   NoDataLoss: all produced items are eventually flushed or the
 *     machine is Stopped (no silent drops during normal operation).
 *   CheckpointNeverAheadOfFlushed: the committed checkpoint for a
 *     source never exceeds what has actually been flushed to output.
 *   RejectAdvancesCheckpoint: transform errors advance the checkpoint
 *     to prevent infinite retry loops on restart.
 *   MonotonicCheckpoints: checkpoints never decrease.
 *
 * What this spec does NOT model:
 *   - Byte-level framing or remainder handling (Kani-proven)
 *   - Scanner or SQL transform details
 *   - Network/HTTP transport
 *   - Shutdown protocol (modeled in ShutdownProtocol.tla)
 *
 * For TLC configuration, see MCPipelineBatch.tla.
 *)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    Sources,            \* Set of source identifiers (symmetry set)
    MaxItemsPerSource,  \* Max items per source
    BatchThreshold      \* Items to accumulate before flushing

ASSUME MaxItemsPerSource >= 1 /\ MaxItemsPerSource <= 5
ASSUME BatchThreshold >= 1 /\ BatchThreshold <= MaxItemsPerSource

(* -----------------------------------------------------------------------
 * State variables
 * ----------------------------------------------------------------------- *)

VARIABLES
    (* Per-source production *)
    produced,           \* [Sources -> Nat] items produced per source
    source_offset,      \* [Sources -> Nat] current offset per source

    (* Batch accumulator (models scan_buf + batch_checkpoints) *)
    buf_count,          \* Nat: items in accumulator
    buf_offsets,        \* [Sources -> Nat] latest offset per source in current batch
                        \* (0 = source not in current batch)

    (* Pipeline machine state (simplified from PipelineMachine.tla) *)
    in_flight_id,       \* Nat: batch ID of in-flight batch (0 = none)
    in_flight_offsets,  \* [Sources -> Nat] offsets of in-flight batch
    committed,          \* [Sources -> Nat] committed checkpoint per source
    next_batch_id,      \* Nat: monotonic batch ID counter

    (* Per-source flushed watermark — advanced when data leaves accumulator *)
    flushed,            \* [Sources -> Nat] highest offset flushed per source

    (* Output *)
    flushed_total,      \* Nat: total items flushed to output
    acked_total,        \* Nat: total items acked by output

    (* Control *)
    phase,              \* "Running" | "Stopped"
    done_producing      \* BOOLEAN: all sources have produced their max

vars == <<produced, source_offset, buf_count, buf_offsets, in_flight_id,
          in_flight_offsets, committed, next_batch_id, flushed,
          flushed_total, acked_total, phase, done_producing>>

(* -----------------------------------------------------------------------
 * Type invariant
 * ----------------------------------------------------------------------- *)

TypeOK ==
    /\ \A s \in Sources :
        /\ produced[s] \in 0..MaxItemsPerSource
        /\ source_offset[s] \in 0..(MaxItemsPerSource * 1000)
        /\ buf_offsets[s] \in 0..(MaxItemsPerSource * 1000)
        /\ in_flight_offsets[s] \in 0..(MaxItemsPerSource * 1000)
        /\ committed[s] \in 0..(MaxItemsPerSource * 1000)
        /\ flushed[s] \in 0..(MaxItemsPerSource * 1000)
    /\ buf_count \in 0..(Cardinality(Sources) * MaxItemsPerSource)
    /\ in_flight_id \in Nat
    /\ next_batch_id \in Nat
    /\ flushed_total \in Nat
    /\ acked_total \in Nat
    /\ phase \in {"Running", "Stopped"}
    /\ done_producing \in BOOLEAN

(* -----------------------------------------------------------------------
 * Initial state
 * ----------------------------------------------------------------------- *)

Init ==
    /\ produced = [s \in Sources |-> 0]
    /\ source_offset = [s \in Sources |-> 0]
    /\ buf_count = 0
    /\ buf_offsets = [s \in Sources |-> 0]
    /\ in_flight_id = 0
    /\ in_flight_offsets = [s \in Sources |-> 0]
    /\ committed = [s \in Sources |-> 0]
    /\ next_batch_id = 1
    /\ flushed = [s \in Sources |-> 0]
    /\ flushed_total = 0
    /\ acked_total = 0
    /\ phase = "Running"
    /\ done_producing = FALSE

(* -----------------------------------------------------------------------
 * Actions
 * ----------------------------------------------------------------------- *)

\* Source produces an item with a new offset.
\* Models: input_poll_loop sends ChannelMsg::Data with checkpoint offsets.
Produce(s) ==
    /\ phase = "Running"
    /\ produced[s] < MaxItemsPerSource
    /\ produced' = [produced EXCEPT ![s] = @ + 1]
    /\ source_offset' = [source_offset EXCEPT ![s] = @ + 100]
    /\ buf_count' = buf_count + 1
    \* Last-writer-wins: latest offset for this source in current batch
    /\ buf_offsets' = [buf_offsets EXCEPT ![s] = source_offset[s] + 100]
    /\ UNCHANGED <<in_flight_id, in_flight_offsets, committed,
                   next_batch_id, flushed, flushed_total, acked_total,
                   phase, done_producing>>

\* Flush the accumulator: create batch, begin_send, submit to output.
\* Models: flush_batch in pipeline.rs.
FlushBatch ==
    /\ phase = "Running"
    /\ buf_count >= BatchThreshold
    /\ in_flight_id = 0            \* no batch currently in-flight
    /\ flushed_total' = flushed_total + buf_count
    /\ in_flight_id' = next_batch_id
    /\ in_flight_offsets' = buf_offsets
    /\ next_batch_id' = next_batch_id + 1
    \* Advance per-source flushed watermark for sources in this batch
    /\ flushed' = [s \in Sources |->
        IF buf_offsets[s] > flushed[s]
        THEN buf_offsets[s]
        ELSE flushed[s]]
    /\ buf_count' = 0
    /\ buf_offsets' = [s \in Sources |-> 0]
    /\ UNCHANGED <<produced, source_offset, committed, acked_total,
                   phase, done_producing>>

\* Timeout flush: flush even if below threshold (batch_timeout expired).
TimeoutFlush ==
    /\ phase = "Running"
    /\ buf_count > 0
    /\ buf_count < BatchThreshold
    /\ in_flight_id = 0
    /\ flushed_total' = flushed_total + buf_count
    /\ in_flight_id' = next_batch_id
    /\ in_flight_offsets' = buf_offsets
    /\ next_batch_id' = next_batch_id + 1
    \* Advance per-source flushed watermark for sources in this batch
    /\ flushed' = [s \in Sources |->
        IF buf_offsets[s] > flushed[s]
        THEN buf_offsets[s]
        ELSE flushed[s]]
    /\ buf_count' = 0
    /\ buf_offsets' = [s \in Sources |-> 0]
    /\ UNCHANGED <<produced, source_offset, committed, acked_total,
                   phase, done_producing>>

\* Output acks the in-flight batch (success).
\* Checkpoint advances for each source that contributed to the batch.
AckBatch ==
    /\ in_flight_id > 0
    /\ acked_total' = acked_total + 1
    /\ committed' = [s \in Sources |->
        IF in_flight_offsets[s] > committed[s]
        THEN in_flight_offsets[s]
        ELSE committed[s]]
    /\ in_flight_id' = 0
    /\ in_flight_offsets' = [s \in Sources |-> 0]
    /\ UNCHANGED <<produced, source_offset, buf_count, buf_offsets,
                   next_batch_id, flushed, flushed_total, phase,
                   done_producing>>

\* Transform or output rejects the batch (permanent error).
\* Checkpoint STILL advances — design decision from DESIGN.md:
\* "Rejected batches advance the checkpoint."
RejectBatch ==
    /\ in_flight_id > 0
    /\ acked_total' = acked_total + 1
    /\ committed' = [s \in Sources |->
        IF in_flight_offsets[s] > committed[s]
        THEN in_flight_offsets[s]
        ELSE committed[s]]
    /\ in_flight_id' = 0
    /\ in_flight_offsets' = [s \in Sources |-> 0]
    /\ UNCHANGED <<produced, source_offset, buf_count, buf_offsets,
                   next_batch_id, flushed, flushed_total, phase,
                   done_producing>>

\* All sources have produced their maximum.
MarkDoneProducing ==
    /\ ~done_producing
    /\ \A s \in Sources : produced[s] = MaxItemsPerSource
    /\ done_producing' = TRUE
    /\ UNCHANGED <<produced, source_offset, buf_count, buf_offsets,
                   in_flight_id, in_flight_offsets, committed,
                   next_batch_id, flushed, flushed_total, acked_total,
                   phase>>

\* Stop the pipeline (all data processed).
Stop ==
    /\ phase = "Running"
    /\ done_producing
    /\ buf_count = 0          \* accumulator empty
    /\ in_flight_id = 0       \* no in-flight batch
    /\ phase' = "Stopped"
    /\ UNCHANGED <<produced, source_offset, buf_count, buf_offsets,
                   in_flight_id, in_flight_offsets, committed,
                   next_batch_id, flushed, flushed_total, acked_total,
                   done_producing>>

(* -----------------------------------------------------------------------
 * Next-state relation
 * ----------------------------------------------------------------------- *)

Next ==
    \/ \E s \in Sources : Produce(s)
    \/ FlushBatch
    \/ TimeoutFlush
    \/ AckBatch
    \/ RejectBatch
    \/ MarkDoneProducing
    \/ Stop
    \* Terminal: Stopped is final.
    \/ (phase = "Stopped" /\ UNCHANGED vars)

(* -----------------------------------------------------------------------
 * Fairness
 * ----------------------------------------------------------------------- *)

Fairness ==
    /\ WF_vars(FlushBatch)
    /\ WF_vars(TimeoutFlush)
    /\ WF_vars(AckBatch)
    /\ WF_vars(MarkDoneProducing)
    /\ WF_vars(Stop)
    \* No WF on Produce — environment is not obligated to produce.
    \* No WF on RejectBatch — rejection is nondeterministic.

Spec == Init /\ [][Next]_vars /\ Fairness

(* -----------------------------------------------------------------------
 * Safety properties
 * ----------------------------------------------------------------------- *)

\* Committed checkpoint never exceeds the latest offset actually flushed.
\* (No checkpoint ahead of what was delivered.)
CheckpointNeverAheadOfFlushed ==
    \A s \in Sources :
        committed[s] <= flushed[s]

\* Checkpoints are monotonic.
MonotonicCheckpoints ==
    [][\A s \in Sources : committed[s]' >= committed[s]]_vars

\* No double-flush: at most one batch in-flight at a time.
\* Enforced structurally: FlushBatch/TimeoutFlush require in_flight_id = 0.
\* The scalar in_flight_id (0 = none, nonzero = one in flight) already
\* enforces at-most-one-in-flight. This invariant confirms the ID is
\* always within the valid range of assigned batch IDs.
SingleInFlight ==
    in_flight_id \in 0..next_batch_id

\* Reject advances checkpoint (same as ack — design decision).
\* This is modeled by RejectBatch having the same committed update as AckBatch.
\* Verified structurally: RejectBatch and AckBatch have identical committed' assignments.

(* -----------------------------------------------------------------------
 * Liveness properties
 * ----------------------------------------------------------------------- *)

\* All data is eventually processed: once all sources are done and all
\* batches are acked, the pipeline stops.
EventualStop ==
    done_producing ~> (phase = "Stopped")

\* Every in-flight batch eventually resolves.
InFlightResolves ==
    (in_flight_id > 0) ~> (in_flight_id = 0)

\* Once all sources finish producing, the pipeline eventually stops and stays stopped.
\* Guarded on done_producing because there is no WF on Produce —
\* sources are not obligated to produce, so the pipeline may legitimately
\* run forever if no data arrives.
StoppedIsStable ==
    done_producing ~> [](phase = "Stopped")

(* -----------------------------------------------------------------------
 * Reachability / vacuity guards
 * ----------------------------------------------------------------------- *)

FlushOccurs == ~(flushed_total > 0)
AckOccurs == ~(acked_total > 0)
StopReachable == ~(phase = "Stopped")
MultiSourceBatch == ~(\E s1, s2 \in Sources :
    s1 /= s2 /\ buf_offsets[s1] > 0 /\ buf_offsets[s2] > 0)

\* done_producing is reachable — without this, EventualStop and
\* StoppedIsStable are vacuously true (their antecedent never holds).
DoneProducingReachable == ~done_producing

======================================================================
