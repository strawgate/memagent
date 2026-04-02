------------------------ MODULE PipelineMachine ------------------------
(*
 * Formal model of PipelineMachine<S, C> from
 * crates/logfwd-core/src/pipeline/lifecycle.rs
 *
 * Two invariants that Kani (bounded model checker) cannot express:
 *
 * DRAIN GUARANTEE (safety):
 *   stop() is only reachable when ALL in-flight batches for ALL sources
 *   have been acked or rejected. Maps to is_drained() guard in Rust.
 *
 * CHECKPOINT ORDERING (safety):
 *   committed[s] = n implies every batch 1..n for source s has been
 *   acknowledged and none remain in_flight. At-least-once delivery
 *   invariant: safe to restart from committed[s].
 *   This is the Filebeat/Vector OrderedFinalizer pattern.
 *
 * What this spec does NOT model (verified separately):
 *   - Rust typestate encoding (BatchTicket<Queued> vs <Sending>)
 *   - Memory safety and arithmetic overflow (Kani proofs in batch.rs)
 *   - The opaque checkpoint type C (modeled as sequence numbers)
 *   - fail() / retry path (invisible to machine state)
 *   - Network message loss (modeled at a higher level)
 *
 * Comparison with production systems (Vector, Filebeat, Fluent Bit, OTel):
 *   - Our design is structurally closest to Vector's OrderedFinalizer +
 *     Filebeat's cursor model, but differs in:
 *     (1) Explicit typestate enforcement (no production system does this)
 *     (2) Explicit BTreeMap ordered-ack vs async executor ordering
 *     (3) stop() returning Err(self) as an unbypassable guard
 *   - ForceStop is modeled here; every production system has an equivalent
 *     (Vector: shutdown_force_trigger, Fluent Bit: grace timer, OTel: ctx timeout)
 *
 * For TLC model checker configuration (symmetry, bounds, model constants)
 * see MCPipelineMachine.tla — keep this file clean of TLC-specific overrides.
 *)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Sources,               \* Set of source identifiers (use symmetry in MC file)
    MaxBatchesPerSource    \* Max batch IDs to explore per source

ASSUME MaxBatchesPerSource \in Nat /\ MaxBatchesPerSource >= 1

BatchIds == 1..MaxBatchesPerSource

(* ---------------------------------------------------------------------------
 * State variables
 *
 * Design note: batch IDs are modeled as sequence numbers 1..N per source.
 * In Rust, IDs are assigned globally (single counter across all sources).
 * Per-source modeling is equivalent for the properties we care about
 * because ordering invariants only apply within a single source's sequence.
 * ---------------------------------------------------------------------------*)

VARIABLES
    phase,       \* Pipeline phase: "Running" | "Draining" | "Stopped"
    created,     \* [Sources -> SUBSET BatchIds] — assigned via create_batch
    in_flight,   \* [Sources -> SUBSET BatchIds] — after begin_send, before apply_ack
    acked,       \* [Sources -> SUBSET BatchIds] — after apply_ack (ack or reject)
    committed,   \* [Sources -> Nat]  — highest committed batch sequence (0 = none)
    forced       \* BOOLEAN — TRUE if ForceStop was used (data loss accepted)

vars == <<phase, created, in_flight, acked, committed, forced>>

(* ---------------------------------------------------------------------------
 * Helper operators
 * ---------------------------------------------------------------------------*)

\* Maximum element of a finite set of naturals (set must be non-empty).
SetMax(S) == CHOOSE n \in S : \A m \in S : n >= m

\* Compute committed checkpoint after acking a batch.
\* Advances to the largest n s.t. all batches 1..n are acked AND none are
\* still in_flight. This is the ordered-ack / contiguous-prefix invariant.
\*
\* Rust note: record_ack_and_advance() walks pending_acks in BTreeMap order,
\* stopping when a lower-ID in_flight batch exists. This operator is the
\* direct functional equivalent. When in_flight[s] = {}, NewCommitted returns
\* Cardinality(acked[s]) — draining all pending_acks in one step, matching
\* the Rust behavior (pending_acks is fully consumed when in_flight is empty).
NewCommitted(new_acked_s, new_in_flight_s) ==
    LET eligible == {n \in 0..MaxBatchesPerSource :
            /\ \A i \in 1..n : i \in new_acked_s
            /\ \A i \in 1..n : i \notin new_in_flight_s}
    IN SetMax(eligible)  \* 0 is always eligible (vacuous), so set is non-empty

(* ---------------------------------------------------------------------------
 * Type invariant
 * ---------------------------------------------------------------------------*)

TypeOK ==
    /\ phase \in {"Running", "Draining", "Stopped"}
    /\ forced \in BOOLEAN
    /\ \A s \in Sources :
        /\ created[s]   \subseteq BatchIds
        /\ in_flight[s] \subseteq created[s]
        /\ acked[s]     \subseteq created[s]
        /\ in_flight[s] \cap acked[s] = {}
        /\ committed[s] \in 0..MaxBatchesPerSource
        /\ committed[s] <= Cardinality(acked[s])

(* ---------------------------------------------------------------------------
 * Initial state
 * ---------------------------------------------------------------------------*)

Init ==
    /\ phase      = "Running"
    /\ created    = [s \in Sources |-> {}]
    /\ in_flight  = [s \in Sources |-> {}]
    /\ acked      = [s \in Sources |-> {}]
    /\ committed  = [s \in Sources |-> 0]
    /\ forced     = FALSE

(* ---------------------------------------------------------------------------
 * Actions
 * ---------------------------------------------------------------------------*)

\* create_batch: assigns next sequential batch ID to source s.
\* Only allowed in Running phase (NoNewBatchesAfterDrain is verified as invariant).
CreateBatch(s) ==
    LET next_id == Cardinality(created[s]) + 1 IN
    /\ phase = "Running"
    /\ next_id \in BatchIds
    /\ created'   = [created   EXCEPT ![s] = created[s] \cup {next_id}]
    /\ UNCHANGED <<phase, in_flight, acked, committed, forced>>

\* begin_send: machine takes ownership of batch b for source s.
\* A Queued ticket dropped before begin_send has no machine state — safe.
\* Phase guard: matches Rust typestate — PipelineMachine<Draining,C> has no
\* begin_send method. Without this guard the trace CreateBatch → BeginDrain →
\* BeginSend is valid in TLA+, letting in_flight grow after drain begins and
\* violating DrainMeansNoNewSending.
BeginSend(s, b) ==
    /\ phase = "Running"
    /\ b \in created[s]
    /\ b \notin in_flight[s]
    /\ b \notin acked[s]
    /\ in_flight' = [in_flight EXCEPT ![s] = in_flight[s] \cup {b}]
    /\ UNCHANGED <<phase, created, acked, committed, forced>>

\* apply_ack (ack OR reject): batch leaves in_flight, checkpoint may advance.
\*
\* Reject note: RejectBatch has the same state transition as AckBatch.
\* Permanently-undeliverable data must not block checkpoint progress
\* (would stall drain indefinitely). At-least-once is weakened to
\* at-most-once only for rejected batches. This matches Filebeat's behavior
\* (advance past malformed records) and differs from Fluent Bit (which drops
\* the route but re-tries via a separate backlog).
\*
\* For metrics/observability, the Rust receipt.delivered flag distinguishes
\* ack from reject. That distinction is not modeled here (orthogonal to
\* the safety/liveness properties this spec targets).
AckBatch(s, b) ==
    /\ b \in in_flight[s]
    /\ LET new_in_flight_s == in_flight[s] \ {b}
           new_acked_s     == acked[s] \cup {b}
       IN
       /\ in_flight' = [in_flight EXCEPT ![s] = new_in_flight_s]
       /\ acked'     = [acked     EXCEPT ![s] = new_acked_s]
       /\ committed' = [committed EXCEPT ![s] = NewCommitted(new_acked_s, new_in_flight_s)]
    /\ UNCHANGED <<phase, created, forced>>

RejectBatch(s, b) == AckBatch(s, b)

\* begin_drain: closes pipeline to new batches.
BeginDrain ==
    /\ phase = "Running"
    /\ phase' = "Draining"
    /\ UNCHANGED <<created, in_flight, acked, committed, forced>>

\* stop: THE DRAIN GUARANTEE.
\* Rust: PipelineMachine<Draining, C>::stop() returns Err(self) if not drained.
\* Rust's is_drained() checks BOTH in_flight.all_empty() AND pending_acks.all_empty().
\*
\* TLA+ models both conditions explicitly:
\*   (1) in_flight[s] = {} — no batch is actively being sent
\*   (2) acked[s] ⊆ 1..committed[s] — no acked batch is awaiting commit (≡ pending_acks empty)
\*
\* Why (2) is necessary: consider batch 1 created-but-never-sent, batch 2 sent+acked.
\*   in_flight = {}, acked = {2}, committed = 0.
\*   NewCommitted({2}, {}) = 0 because batch 1 was never acked (gap at position 1).
\*   In Rust: pending_acks = {2: ...}, is_drained() = false — stop() returns Err(self).
\*   Without guard (2), TLA+ Stop would fire here, diverging from the Rust behavior.
\*
\* Why (2) is sufficient: if acked[s] ⊆ 1..committed[s] AND in_flight[s] = {},
\*   then committed[s] = Cardinality(acked[s]) and all acked batches are committed,
\*   which exactly corresponds to pending_acks[s] being empty in Rust.
Stop ==
    /\ phase = "Draining"
    /\ \A s \in Sources : in_flight[s] = {}                    \* no active sends
    /\ \A s \in Sources : acked[s] \subseteq 1..committed[s]  \* no pending_acks (≡ is_drained())
    /\ phase' = "Stopped"
    /\ UNCHANGED <<created, in_flight, acked, committed, forced>>

\* force_stop: emergency shutdown when grace period expires.
\* Discards in-flight state. Every production system has an equivalent:
\*   Vector: shutdown_force_trigger.cancel() after deadline
\*   Fluent Bit: grace timer expiry → flb_engine_shutdown()
\*   OTel: context.WithTimeout cancellation
\*
\* ForceStop is always in Next (production reality: the kill switch always exists).
\* The `forced` flag records that it fired so DrainCompleteness can be conditioned on
\* ~forced. All other invariants hold regardless.
\*
\* NOTE: WF(ForceStop) is intentionally NOT in Fairness. ForceStop is an escape
\* hatch, not a scheduled event. Without WF, liveness checks (EventualDrain,
\* NoBatchLeftBehind) still require the normal path to work — TLC cannot satisfy
\* them by always choosing the ForceStop shortcut.
ForceStop ==
    /\ phase = "Draining"
    /\ phase' = "Stopped"
    /\ forced' = TRUE
    /\ UNCHANGED <<created, in_flight, acked, committed>>

(* ---------------------------------------------------------------------------
 * Next-state relation
 * ---------------------------------------------------------------------------*)

Next ==
    \/ \E s \in Sources         : CreateBatch(s)
    \/ \E s \in Sources,
          b \in BatchIds        : BeginSend(s, b)
    \/ \E s \in Sources,
          b \in BatchIds        : AckBatch(s, b)
    \/ BeginDrain
    \/ Stop
    \/ ForceStop

(*
 * Fairness:
 * WF_vars(a) = if action a is continuously enabled, it eventually fires.
 *
 * WF(AckBatch): once a batch is Sending, it stays there until acked.
 *   Correct because the Sending state is stable until apply_ack is called.
 *   The downstream sink always eventually responds (retry budget is finite).
 *
 * WF(BeginDrain): the drain signal is sticky (CancellationToken semantics).
 *   Correct because once signaled, the drain state is not un-signaled.
 *
 * WF(Stop): correct IFF in_flight = {} is a STABLE condition once reached
 *   during Draining. This is verified by DrainMeansNoNewSending: no
 *   new BeginSend is possible during Draining, so in_flight cannot grow.
 *   WF suffices; SF is not needed.
 *
 * Note: WF(ForceStop) would make EventualDrain trivially true. Do NOT
 * add it here — ForceStop is an escape hatch, not a scheduled event.
 *)
Fairness ==
    /\ WF_vars(BeginDrain)
    /\ WF_vars(Stop)
    /\ \A s \in Sources, b \in BatchIds : WF_vars(AckBatch(s, b))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ===========================================================================
 * SAFETY INVARIANTS
 * Checked exhaustively over all reachable states. No fairness needed.
 * ===========================================================================*)

\* Basic disjointness: a batch cannot be both in_flight and acked.
NoDoubleComplete ==
    \A s \in Sources : in_flight[s] \cap acked[s] = {}

\* THE DRAIN GUARANTEE: normal-path Stop implies in_flight is empty for all sources.
\* Conditioned on ~forced: ForceStop intentionally bypasses this guarantee (data
\* loss accepted in exchange for liveness). The `forced` flag records which path
\* was taken so this invariant can be checked unconditionally in all configs.
DrainCompleteness ==
    (phase = "Stopped" /\ ~forced) => \A s \in Sources : in_flight[s] = {}

\* in_flight cannot grow once drain begins.
\* This is what makes WF(Stop) sufficient — once in_flight[s] = {} is reached
\* during Draining it stays empty, so Stop's enabledness is stable (doesn't
\* oscillate), meaning WF (not SF) suffices.
\*
\* Enforced structurally: BeginSend requires phase = "Running", so it cannot
\* fire during Draining or Stopped. This property is therefore a consequence
\* of the action guards and is verified here as an explicit temporal check.
DrainMeansNoNewSending ==
    [][(phase # "Running") =>
        (\A s \in Sources : in_flight[s]' \subseteq in_flight[s])]_vars

\* New CreateBatch is disabled outside Running phase (action-level enforcement).
\* Stated as a temporal invariant here for explicit documentation.
NoCreateAfterDrain ==
    [][phase # "Running" =>
       \A s \in Sources : created[s]' = created[s]]_vars

\* committed[s] is monotonically non-decreasing.
CommittedMonotonic ==
    [][\A s \in Sources : committed[s]' >= committed[s]]_vars

\* THE CHECKPOINT ORDERING INVARIANT:
\* committed[s] = n implies all batches 1..n for source s are acked AND
\* none are still in_flight.
\*
\* This is the at-least-once delivery invariant. On restart, the pipeline
\* re-reads from position corresponding to batch n. Since all of 1..n are
\* acked, no data before n was skipped.
\*
\* Equivalent to the "consistent prefix" invariant from the distributed
\* database literature and Kafka Streams' checkpointable-offsets invariant.
CheckpointOrderingInvariant ==
    \A s \in Sources :
        LET n == committed[s] IN
        n > 0 =>
            /\ \A i \in 1..n : i \in acked[s]
            /\ \A i \in 1..n : i \notin in_flight[s]

\* committed[s] never exceeds the count of acked batches.
CommittedNeverAheadOfAcked ==
    \A s \in Sources : committed[s] <= Cardinality(acked[s])

\* Structural invariants (catch model misconfiguration).
InFlightImpliesCreated ==
    \A s \in Sources : in_flight[s] \subseteq created[s]

AckedImpliesCreated ==
    \A s \in Sources : acked[s] \subseteq created[s]

(* ===========================================================================
 * LIVENESS PROPERTIES (hold under Fairness; require no ForceStop)
 * ===========================================================================
 *
 * IMPORTANT: use `<>[]P` (eventually-stable) for convergence properties,
 * NOT `<>P` (eventually). `<>P` is satisfied vacuously if P holds even
 * transiently. `<>[]P` requires P to hold from some point forever.
 *
 * Use leads-to `P ~> Q` for causal properties: P eventually causes Q.
 * ===========================================================================*)

\* Every started drain eventually reaches Stopped.
\* <>[](phase = "Stopped") would also work but EventualDrain is more readable.
EventualDrain == (phase = "Draining") ~> (phase = "Stopped")

\* Once Stopped, it stays Stopped (convergence).
StoppedIsStable == <>[](phase = "Stopped")

\* Every batch that enters in_flight eventually leaves it.
NoBatchLeftBehind ==
    \A s \in Sources, b \in BatchIds :
        (b \in in_flight[s]) ~> (b \notin in_flight[s])

\* Every created batch is eventually committed or the machine is Stopped.
\* (Drain + stop must eventually account for all data.)
AllCreatedBatchesEventuallyAccountedFor ==
    \A s \in Sources :
        \A b \in BatchIds :
            (b \in created[s]) ~>
                (\/ committed[s] >= b
                 \/ phase = "Stopped")

(* ===========================================================================
 * REACHABILITY ASSERTIONS  (vacuity guards)
 *
 * These are the TLA+ equivalent of kani::cover!() — they prove that the
 * interesting states are actually reachable. A spec with vacuous invariants
 * (impossible preconditions) would pass all INVARIANTS and PROPERTIES
 * trivially. These PROPERTIES catch that.
 *
 * Run in PipelineMachine.coverage.cfg. TLC reports a PROPERTY VIOLATION
 * when a reachability assertion is FALSE — which here means the state was
 * never reached. A "violation" is the desired outcome: it is TLC confirming
 * coverage.
 *
 * Sabotage test: to verify an invariant P is non-vacuous, temporarily replace
 * P's consequent with FALSE. TLC should find a counterexample. If it does
 * not, P was vacuously true — the precondition was unreachable.
 * ===========================================================================*)

\* The Draining phase is reachable (BeginDrain fires at least once).
BeginDrainReachable == <>(phase = "Draining")

\* The Stopped phase is reachable (the full lifecycle completes).
StopReachable == <>(phase = "Stopped")

\* At least one batch is eventually acked (AckBatch fires at least once).
AckOccurs == \E s \in Sources : \E b \in BatchIds : <>(b \in acked[s])

\* The committed checkpoint advances at least once (ordering logic is exercised).
CommitAdvances == \E s \in Sources : <>(committed[s] > 0)

=============================================================================
