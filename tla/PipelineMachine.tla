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
\*
\* Mirrors record_ack_and_advance() in Rust exactly: advance each pending ack
\* while no lower-ID batch is still in_flight. Gaps from never-sent batch IDs
\* (created Queued tickets that were dropped without begin_send) are invisible —
\* the machine never tracked them, so they cannot block checkpoint advancement.
\*
\* ever_sent = acked ∪ in_flight — the set of batches that entered the machine
\* (via begin_send). Only these participate in ordering. A batch n is eligible
\* for commitment when all ever-sent batches with ID ≤ n have been acked.
\*
\* SetMax safety: eligible always contains 0 (vacuously: no ever_sent batch
\* has ID ≤ 0), so the set is never empty.
NewCommitted(new_acked_s, new_in_flight_s) ==
    LET ever_sent == new_acked_s \cup new_in_flight_s
        eligible  == {n \in (ever_sent \cup {0}) :
                        \A b \in ever_sent : b <= n => b \in new_acked_s}
    IN SetMax(eligible)

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
    \* In Rust, Queued tickets are consumed by begin_send or dropped.
    \* Once dropped, the ticket is gone — you cannot send a batch whose
    \* ticket was dropped. This means sends are monotonic: you cannot send
    \* batch b if any batch with a higher ID was already sent or acked.
    /\ \A other \in (in_flight[s] \cup acked[s]) : b >= other
    /\ in_flight' = [in_flight EXCEPT ![s] = in_flight[s] \cup {b}]
    /\ UNCHANGED <<phase, created, acked, committed, forced>>

\* apply_ack (ack OR reject): batch leaves in_flight, checkpoint may advance.
\*
\* Reject note: RejectBatch has the same state transition as AckBatch.
\* This is intentionally reserved for explicit permanent rejects only.
\* Permanently-undeliverable data must not block checkpoint progress
\* (would stall drain indefinitely). At-least-once is weakened to
\* at-most-once only for rejected batches.
\*
\* Retry/control-plane failures are a different category and must not be
\* modeled as RejectBatch. The current Rust rollout keeps those batches
\* unresolved so checkpoints do not advance past undelivered data.
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
\* Rust's is_drained() = in_flight.all_empty() && pending_acks.all_empty().
\*
\* With the corrected NewCommitted formula (ever_sent-based), when in_flight[s] = {}
\* the last ack drains all of pending_acks atomically: the loop finds no lower-ID
\* in_flight batch, so every remaining pending ack is consumed in sequence.
\* Therefore in_flight[s] = {} ≡ is_drained() — the single guard is sufficient.
\*
\* Previously a second guard `acked[s] ⊆ 1..committed[s]` was added to handle a
\* spurious liveness hole caused by the old NewCommitted requiring all IDs 1..n to
\* be acked (including gaps from never-sent tickets). The corrected formula makes
\* that guard redundant: when in_flight = {}, NewCommitted advances past all gaps.
Stop ==
    /\ phase = "Draining"
    /\ \A s \in Sources : in_flight[s] = {}    \* THE DRAIN GUARD (≡ is_drained())
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
    \* Terminal state: Stopped is final, nothing more can happen.
    \* Explicit stuttering prevents TLC from reporting a false deadlock.
    \/ (phase = "Stopped" /\ UNCHANGED vars)

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
\* committed[s] = n implies every SENT batch with ID ≤ n is acked and none
\* are still in_flight. "Sent" = ever passed through begin_send (ever_sent).
\*
\* Scoped to ever-sent batches — not all IDs 1..n — because gaps from
\* Queued tickets that were dropped without begin_send are invisible to the
\* machine and do not affect checkpoint ordering. A created-but-never-sent
\* ticket has no checkpoint and no machine state; it cannot be committed or
\* skipped. Only begin_send'd batches participate in ordered-ack.
\*
\* Equivalent to: "no sent batch below the committed watermark is in_flight."
\* This is the at-least-once delivery invariant: on restart from committed[s],
\* no sent data is re-read before the checkpoint.
CheckpointOrderingInvariant ==
    \A s \in Sources :
        LET n        == committed[s]
            ever_sent_s == in_flight[s] \cup acked[s]
        IN n > 0 =>
            \A i \in ever_sent_s :
                i <= n =>
                    /\ i \in acked[s]
                    /\ i \notin in_flight[s]

\* committed[s] never exceeds the highest created batch ID.
\* (It can exceed Cardinality(acked[s]) when some batch IDs were created
\* but never sent — skipped batches don't block checkpoint advancement.)
CommittedNeverAheadOfCreated ==
    \A s \in Sources : committed[s] <= Cardinality(created[s])

\* Structural invariants (catch model misconfiguration).
InFlightImpliesCreated ==
    \A s \in Sources : in_flight[s] \subseteq created[s]

AckedImpliesCreated ==
    \A s \in Sources : acked[s] \subseteq created[s]

(* ===========================================================================
 * LIVENESS PROPERTIES (hold under Fairness; ForceStop always in Next but
 * WF(ForceStop) intentionally absent — normal path must work without it)
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
 * REACHABILITY ASSERTIONS  (vacuity guards — kani::cover!() equivalent)
 *
 * Defined as state-predicate NEGATIONS. Used as INVARIANTS in
 * PipelineMachine.coverage.cfg. When TLC violates an invariant I == ~P,
 * it found a reachable state where P holds — the violation trace IS the
 * witness. No violation = P unreachable = spec or model bug.
 *
 * Why INVARIANTS, not PROPERTIES: <>(P) as a PROPERTY produces a
 * counterexample when P is never reached, which is the wrong signal —
 * no-violation means "P is always eventually reached," not "P is reachable."
 * INVARIANTS with ~P give the correct semantics: violation = witness.
 *
 * Sabotage test: to verify an invariant I is non-vacuous, temporarily
 * replace I's consequent with FALSE. TLC must find a counterexample.
 * If it reports "No error found," the precondition is unreachable and
 * the invariant was trivially satisfied.
 * ===========================================================================*)

\* The Draining phase is reachable (BeginDrain fires at least once).
BeginDrainReachable == ~(phase = "Draining")

\* The Stopped phase is reachable (the full lifecycle completes).
StopReachable == ~(phase = "Stopped")

\* At least one batch is acked (AckBatch fires at least once).
AckOccurs == ~(\E s \in Sources : acked[s] /= {})

\* The committed checkpoint advances at least once (ordering logic is exercised).
CheckpointAdvances == ~(\E s \in Sources : committed[s] > 0)

\* ForceStop is reachable (the kill-switch path is exercised).
ForcedReachable == ~(forced = TRUE)

\* At least one batch is created (CreateBatch fires).
\* Covers NoBatchLeftBehind and AllCreatedBatchesEventuallyAccountedFor
\* antecedents — without this, those properties are vacuously true.
CreateOccurs == ~(\E s \in Sources : created[s] /= {})

=============================================================================
