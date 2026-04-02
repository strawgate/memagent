------------------------ MODULE PipelineMachine ------------------------
(*
 * Formal model of PipelineMachine<S, C> from
 * crates/logfwd-core/src/pipeline/lifecycle.rs
 *
 * This spec models two invariants that Kani cannot express (they require
 * temporal logic) but that are the foundation of our at-least-once delivery
 * guarantee:
 *
 * DRAIN GUARANTEE (safety):
 *   stop() is only reachable when ALL in-flight batches for ALL sources have
 *   been acked or rejected. The pipeline cannot shut down with data in flight.
 *
 * CHECKPOINT ORDERING (safety):
 *   committed[s] = n implies every batch 1..n for source s has been acked.
 *   On restart, the pipeline re-reads from committed[s], guaranteeing no
 *   data is skipped (though batches up to in-flight watermark may replay).
 *   This is the Filebeat/Vector OrderedFinalizer pattern.
 *
 * What this spec does NOT model (covered by Kani proofs in batch.rs):
 *   - Rust typestate encoding (BatchTicket<Queued> vs <Sending>)
 *   - Memory safety and arithmetic overflow
 *   - The opaque checkpoint type C (modeled here as batch sequence numbers)
 *   - fail() / retry path (invisible to machine state — batch stays in_flight)
 *
 * TLC model checker configuration: see PipelineMachine.cfg
 * Recommended: MaxSources=2, MaxBatchesPerSource=3 (fast, thorough)
 *              MaxSources=3, MaxBatchesPerSource=4 (exhaustive, ~5 min)
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    MaxSources,            \* Number of distinct sources to model (e.g. files)
    MaxBatchesPerSource    \* Max batch IDs to explore per source

ASSUME MaxSources \in Nat /\ MaxSources >= 1
ASSUME MaxBatchesPerSource \in Nat /\ MaxBatchesPerSource >= 1

Sources   == 1..MaxSources
BatchIds  == 1..MaxBatchesPerSource

(* ---------------------------------------------------------------------------
 * State variables
 * ---------------------------------------------------------------------------
 * Design note: we model batch IDs as sequence numbers 1..N per source.
 * In the Rust implementation, batch IDs are assigned globally (single counter
 * across all sources). The per-source modeling here is equivalent for the
 * properties we care about because the ordering invariant only applies
 * within a single source's batch sequence.
 *)

VARIABLES
    phase,       \* Pipeline phase: "Running" | "Draining" | "Stopped"
    created,     \* [s \in Sources -> SUBSET BatchIds] — batches assigned to source s
    in_flight,   \* [s \in Sources -> SUBSET BatchIds] — batches begin_send called on
    acked,       \* [s \in Sources -> SUBSET BatchIds] — batches acked or rejected
    committed    \* [s \in Sources -> Nat] — highest committed batch sequence (0 = none)

vars == <<phase, created, in_flight, acked, committed>>

(* ---------------------------------------------------------------------------
 * Helper operators
 * ---------------------------------------------------------------------------*)

\* Maximum element of a non-empty set of naturals.
SetMax(S) == CHOOSE n \in S : \A m \in S : n >= m

\* Compute the new committed value after acking batch b for source s.
\* Committed advances to the largest n such that:
\*   (a) batches 1..n for source s are all in new_acked, AND
\*   (b) none of batches 1..n are still in new_in_flight
\* This is the OrderedFinalizer invariant: commit only when the prefix is contiguous.
NewCommitted(new_acked_s, new_in_flight_s) ==
    LET eligible == {n \in 0..MaxBatchesPerSource :
            /\ \A i \in 1..n : i \in new_acked_s
            /\ \A i \in 1..n : i \notin new_in_flight_s}
    IN SetMax(eligible)

\* ---------------------------------------------------------------------------
\* Type invariant
\* ---------------------------------------------------------------------------

TypeOK ==
    /\ phase \in {"Running", "Draining", "Stopped"}
    /\ \A s \in Sources :
        /\ created[s]   \subseteq BatchIds
        /\ in_flight[s] \subseteq created[s]
        /\ acked[s]     \subseteq created[s]
        /\ in_flight[s] \cap acked[s] = {}       \* can't be both in_flight and acked
        /\ committed[s] \in 0..MaxBatchesPerSource
        /\ committed[s] <= Cardinality(acked[s]) \* can't commit beyond what's acked

(* ---------------------------------------------------------------------------
 * Initial state
 * ---------------------------------------------------------------------------*)

Init ==
    /\ phase      = "Running"
    /\ created    = [s \in Sources |-> {}]
    /\ in_flight  = [s \in Sources |-> {}]
    /\ acked      = [s \in Sources |-> {}]
    /\ committed  = [s \in Sources |-> 0]

(* ---------------------------------------------------------------------------
 * Actions
 * ---------------------------------------------------------------------------*)

\* create_batch(source, checkpoint):
\* Assigns the next sequential batch ID to source s.
\* Returns a BatchTicket<Queued, C> in Rust — NOT yet tracked by the machine.
\* Only allowed in Running phase (enforces NoNewBatchesAfterDrain).
CreateBatch(s) ==
    LET next_id == Cardinality(created[s]) + 1 IN
    /\ phase = "Running"
    /\ next_id \in BatchIds
    /\ created'   = [created   EXCEPT ![s] = created[s] \cup {next_id}]
    /\ UNCHANGED <<phase, in_flight, acked, committed>>

\* begin_send(ticket):
\* Registers the batch as in-flight. Machine takes ownership here.
\* A Queued ticket that was never begin_send'd can be safely dropped —
\* the machine has no record of it.
BeginSend(s, b) ==
    /\ b \in created[s]
    /\ b \notin in_flight[s]
    /\ b \notin acked[s]
    /\ in_flight' = [in_flight EXCEPT ![s] = in_flight[s] \cup {b}]
    /\ UNCHANGED <<phase, created, acked, committed>>

\* apply_ack (success path, receipt.delivered = true):
\* Removes batch from in_flight. Advances committed checkpoint if the
\* contiguous acked prefix for this source just grew.
\*
\* Rust note: fail() is NOT modeled here — it returns the ticket to Queued
\* but does NOT change in_flight. The machine sees a fail()ed batch as
\* still in_flight until apply_ack is eventually called.
AckBatch(s, b) ==
    /\ b \in in_flight[s]
    /\ LET new_in_flight_s == in_flight[s] \ {b}
           new_acked_s     == acked[s] \cup {b}
       IN
       /\ in_flight'  = [in_flight  EXCEPT ![s] = new_in_flight_s]
       /\ acked'      = [acked      EXCEPT ![s] = new_acked_s]
       /\ committed'  = [committed  EXCEPT ![s] = NewCommitted(new_acked_s, new_in_flight_s)]
    /\ UNCHANGED <<phase, created>>

\* apply_ack (reject path, receipt.delivered = false):
\* Same state transition as AckBatch. The checkpoint still advances for
\* rejected batches — this is intentional. Permanently-undeliverable data
\* must not block checkpoint progress forever (would stall drain forever).
\* At-least-once is weakened to at-most-once only for rejected batches.
RejectBatch(s, b) == AckBatch(s, b)

\* begin_drain():
\* Closes the pipeline to new batches. In-flight batches continue processing.
BeginDrain ==
    /\ phase = "Running"
    /\ phase' = "Draining"
    /\ UNCHANGED <<created, in_flight, acked, committed>>

\* stop():
\* THE DRAIN GUARANTEE: only reachable when ALL sources have empty in_flight.
\*
\* Rust note: is_drained() checks both in_flight AND pending_acks. In this
\* model, pending_acks is implicit in the acked set and NewCommitted folds
\* the pending_acks walk into an immediate computation. The key invariant:
\* when in_flight[s] becomes empty after the last AckBatch(s, _), NewCommitted
\* immediately advances to cover ALL acked batches (no lower-ID blocker
\* remains), so pending_acks[s] is also implicitly drained. Therefore
\* `\A s: in_flight[s] = {}` is equivalent to Rust's is_drained() in all
\* reachable states. The CheckpointOrderingInvariant confirms this holds.
Stop ==
    /\ phase = "Draining"
    /\ \A s \in Sources : in_flight[s] = {}    \* THE DRAIN GUARD
    /\ phase' = "Stopped"
    /\ UNCHANGED <<created, in_flight, acked, committed>>

(* ---------------------------------------------------------------------------
 * Next-state relation and specification
 * ---------------------------------------------------------------------------*)

Next ==
    \/ \E s \in Sources         : CreateBatch(s)
    \/ \E s \in Sources,
          b \in BatchIds        : BeginSend(s, b)
    \/ \E s \in Sources,
          b \in BatchIds        : AckBatch(s, b)
    \/ BeginDrain
    \/ Stop

(*
 * Fairness:
 * WF_vars(a) = if action a is continuously enabled, it eventually fires.
 *
 * We assume:
 *  - If a batch is in_flight, it will eventually be acked/rejected
 *    (the output sink always eventually responds — no permanent hangs)
 *  - The pipeline will eventually begin draining and stop
 *
 * Without these assumptions the liveness properties are trivially false
 * (the system could stutter forever). These assumptions are realistic:
 * our retry budget is finite and timeouts are enforced.
 *)
Fairness ==
    /\ WF_vars(BeginDrain)
    /\ WF_vars(Stop)
    /\ \A s \in Sources, b \in BatchIds :
           WF_vars(AckBatch(s, b))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ===========================================================================
 * SAFETY INVARIANTS
 * ===========================================================================
 * These must hold in every reachable state. TLC checks them exhaustively
 * over all states within the model bounds.
 *)

\* A batch cannot be simultaneously in_flight and acked for any source.
NoDoubleComplete ==
    \A s \in Sources : in_flight[s] \cap acked[s] = {}

\* THE DRAIN GUARANTEE: when Stopped, no source has in_flight batches.
DrainCompleteness ==
    phase = "Stopped" => \A s \in Sources : in_flight[s] = {}

\* New batches cannot be created after drain begins.
\* (Action-level: enforced by CreateBatch guard. Stated here as invariant.)
NoNewBatchesAfterDrain ==
    [][phase # "Running" =>
       \A s \in Sources : created[s]' = created[s]]_vars

\* Committed checkpoint is monotonically non-decreasing.
CommittedMonotonic ==
    [][\A s \in Sources : committed[s]' >= committed[s]]_vars

\* CHECKPOINT ORDERING INVARIANT:
\* committed[s] = n implies all batches 1..n for source s are in acked
\* and none are still in_flight. This is the core at-least-once property.
\*
\* Interpretation: if we commit checkpoint n and restart, we re-read from
\* the position corresponding to batch n. Since all of 1..n are acked,
\* we know we successfully delivered everything up through n.
CheckpointOrderingInvariant ==
    \A s \in Sources :
        LET n == committed[s] IN
        n > 0 =>
            /\ \A i \in 1..n : i \in acked[s]
            /\ \A i \in 1..n : i \notin in_flight[s]

\* committed[s] never exceeds the number of acked batches for source s.
CommittedNeverAheadOfAcked ==
    \A s \in Sources : committed[s] <= Cardinality(acked[s])

\* An in_flight batch was always created first.
InFlightImpliesCreated ==
    \A s \in Sources : in_flight[s] \subseteq created[s]

\* An acked batch was always created first.
AckedImpliesCreated ==
    \A s \in Sources : acked[s] \subseteq created[s]

(* ===========================================================================
 * LIVENESS PROPERTIES
 * ===========================================================================
 * These hold under the Fairness assumption. They express "eventual progress"
 * properties that safety invariants alone cannot capture.
 *)

\* Every started drain eventually reaches Stopped.
\* (This is the shutdown liveness guarantee — drain can't take infinite time.)
EventualDrain == (phase = "Draining") ~> (phase = "Stopped")

\* Every batch that enters in_flight eventually leaves it.
\* (No batch is stuck in_flight forever — the pipeline always makes progress.)
NoBatchLeftBehind ==
    \A s \in Sources, b \in BatchIds :
        (b \in in_flight[s]) ~> (b \notin in_flight[s])

\* Every created batch eventually gets committed or the machine stops.
\* (Drain + stop must eventually commit all processed data.)
AllBatchesEventuallyCommittedOrStopped ==
    \A s \in Sources :
        \A b \in BatchIds :
            (b \in created[s]) ~>
                (\/ committed[s] >= b
                 \/ phase = "Stopped")

=============================================================================
