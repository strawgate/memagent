------------------------ MODULE PipelineMachine ------------------------
(*
 * Formal model of PipelineMachine<S, C> from
 * crates/ffwd-core/src/pipeline/lifecycle.rs
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
 *   - Backoff timing between non-terminal hold/retry attempts
 *   - Batch payload retention across retries (only lifecycle state is modeled)
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
    MaxBatchesPerSource,   \* Max batch IDs to explore per source
    MaxNonTerminalHolds    \* Max non-terminal hold/fail/panic events per batch

ASSUME MaxBatchesPerSource \in Nat /\ MaxBatchesPerSource >= 1
ASSUME MaxNonTerminalHolds \in Nat /\ MaxNonTerminalHolds >= 1

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
    sent,        \* [Sources -> SUBSET BatchIds] — entered machine via begin_send
    in_flight,   \* [Sources -> SUBSET BatchIds] — after begin_send, before apply_ack
    held,        \* [Sources -> SUBSET BatchIds] — non-terminal hold/fail/panic, still in_flight
    retried,     \* [Sources -> SUBSET BatchIds] — observed RetryHeldBatch release
    panic_held,  \* [Sources -> SUBSET BatchIds] — observed panic-driven hold
    hold_count,  \* [Sources -> [BatchIds -> Nat]] — bounded non-terminal attempts
    acked,       \* [Sources -> SUBSET BatchIds] — terminal success outcome
    rejected,    \* [Sources -> SUBSET BatchIds] — terminal permanent-reject outcome
    abandoned,   \* [Sources -> SUBSET BatchIds] — terminal crash/force-stop outcome
    committed,   \* [Sources -> Nat]  — highest committed batch sequence (0 = none)
    forced,      \* BOOLEAN — TRUE if ForceStop was used (data loss accepted)
    stop_reason  \* "none" | "graceful" | "force" | "crash"

vars == <<phase, created, sent, in_flight, held, retried, panic_held, hold_count, acked, rejected, abandoned, committed, forced, stop_reason>>

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
NewCommitted(new_acked_s, new_rejected_s, new_in_flight_s) ==
    LET terminal_for_commit == new_acked_s \cup new_rejected_s
        ever_sent == terminal_for_commit \cup new_in_flight_s
        eligible  == {n \in (ever_sent \cup {0}) :
                        \A b \in ever_sent : b <= n => b \in terminal_for_commit}
    IN SetMax(eligible)

\* Highest safe checkpoint prefix based only on terminal commit outcomes.
\* Held, active in-flight, and abandoned batches are not commit-terminal.
TerminalizedPrefix(s) ==
    LET terminal_for_commit == acked[s] \cup rejected[s]
        ever_sent == sent[s]
        eligible  == {n \in (ever_sent \cup {0}) :
                        \A b \in ever_sent : b <= n => b \in terminal_for_commit}
    IN SetMax(eligible)

(* ---------------------------------------------------------------------------
 * Type invariant
 * ---------------------------------------------------------------------------*)

TypeOK ==
    /\ phase \in {"Running", "Draining", "Stopped"}
    /\ forced \in BOOLEAN
    /\ stop_reason \in {"none", "graceful", "force", "crash"}
    /\ \A s \in Sources :
        /\ created[s]   \subseteq BatchIds
        /\ sent[s]      \subseteq created[s]
        /\ in_flight[s] \subseteq sent[s]
        /\ held[s]      \subseteq in_flight[s]
        /\ retried[s]   \subseteq sent[s]
        /\ panic_held[s] \subseteq sent[s]
        /\ acked[s]     \subseteq sent[s]
        /\ rejected[s]  \subseteq sent[s]
        /\ abandoned[s] \subseteq sent[s]
        /\ in_flight[s] \subseteq created[s]
        /\ held[s]      \subseteq created[s]
        /\ retried[s]   \subseteq created[s]
        /\ panic_held[s] \subseteq created[s]
        /\ acked[s]     \subseteq created[s]
        /\ rejected[s]  \subseteq created[s]
        /\ abandoned[s] \subseteq created[s]
        /\ hold_count[s] \in [BatchIds -> 0..MaxNonTerminalHolds]
        /\ \A b \in BatchIds : hold_count[s][b] > 0 => b \in sent[s]
        /\ in_flight[s] \cap (acked[s] \cup rejected[s] \cup abandoned[s]) = {}
        /\ acked[s] \cap rejected[s] = {}
        /\ acked[s] \cap abandoned[s] = {}
        /\ rejected[s] \cap abandoned[s] = {}
        /\ committed[s] \in 0..MaxBatchesPerSource

StopMetadataConsistent ==
    /\ (forced <=> stop_reason = "force")
    /\ ((phase = "Stopped") <=> (stop_reason # "none"))

(* ---------------------------------------------------------------------------
 * Initial state
 * ---------------------------------------------------------------------------*)

Init ==
    /\ phase      = "Running"
    /\ created    = [s \in Sources |-> {}]
    /\ sent       = [s \in Sources |-> {}]
    /\ in_flight  = [s \in Sources |-> {}]
    /\ held       = [s \in Sources |-> {}]
    /\ retried    = [s \in Sources |-> {}]
    /\ panic_held = [s \in Sources |-> {}]
    /\ hold_count = [s \in Sources |-> [b \in BatchIds |-> 0]]
    /\ acked      = [s \in Sources |-> {}]
    /\ rejected   = [s \in Sources |-> {}]
    /\ abandoned  = [s \in Sources |-> {}]
    /\ committed  = [s \in Sources |-> 0]
    /\ forced     = FALSE
    /\ stop_reason = "none"

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
    /\ UNCHANGED <<phase, sent, in_flight, held, retried, panic_held, hold_count, acked, rejected, abandoned, committed, forced, stop_reason>>

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
    /\ b \notin held[s]
    /\ b \notin acked[s]
    /\ b \notin rejected[s]
    /\ b \notin abandoned[s]
    /\ b \notin sent[s]
    \* In Rust, Queued tickets are consumed by begin_send or dropped.
    \* Once dropped, the ticket is gone — you cannot send a batch whose
    \* ticket was dropped. This means sends are monotonic: you cannot send
    \* batch b if any batch with a higher ID was already sent or acked.
    /\ \A other \in (in_flight[s] \cup acked[s] \cup rejected[s] \cup abandoned[s]) : b >= other
    /\ sent'      = [sent      EXCEPT ![s] = sent[s] \cup {b}]
    /\ in_flight' = [in_flight EXCEPT ![s] = in_flight[s] \cup {b}]
    /\ UNCHANGED <<phase, created, held, retried, panic_held, hold_count, acked, rejected, abandoned, committed, forced, stop_reason>>

\* Non-terminal control-plane failures hold a batch without advancing the
\* checkpoint. This models fail(), retry exhaustion, dispatch failure, timeout,
\* and other "retry later" outcomes that must stay unresolved.
\*
\* A held batch remains in_flight, so it blocks normal Stop and cannot be
\* committed past. MaxNonTerminalHolds bounds retry/failure churn for TLC.
HoldBatch(s, b) ==
    /\ b \in in_flight[s]
    /\ b \notin held[s]
    /\ hold_count[s][b] < MaxNonTerminalHolds
    /\ held'       = [held       EXCEPT ![s] = held[s] \cup {b}]
    /\ hold_count' = [hold_count EXCEPT ![s][b] = hold_count[s][b] + 1]
    /\ UNCHANGED <<phase, created, sent, in_flight, retried, panic_held, acked, rejected, abandoned, committed, forced, stop_reason>>

\* Panic is modeled as an explicit non-terminal hold with an audit marker.
\* The machine does not commit through panic-held work; it must be retried and
\* terminalized, or explicitly abandoned by ForceStop.
PanicHoldBatch(s, b) ==
    /\ b \in in_flight[s]
    /\ b \notin held[s]
    /\ hold_count[s][b] < MaxNonTerminalHolds
    /\ held'       = [held       EXCEPT ![s] = held[s] \cup {b}]
    /\ panic_held' = [panic_held EXCEPT ![s] = panic_held[s] \cup {b}]
    /\ hold_count' = [hold_count EXCEPT ![s][b] = hold_count[s][b] + 1]
    /\ UNCHANGED <<phase, created, sent, in_flight, retried, acked, rejected, abandoned, committed, forced, stop_reason>>

\* Retry releases a non-terminal hold. It does not itself commit or terminalize.
RetryHeldBatch(s, b) ==
    /\ b \in held[s]
    /\ held'    = [held    EXCEPT ![s] = held[s] \ {b}]
    /\ retried' = [retried EXCEPT ![s] = retried[s] \cup {b}]
    /\ UNCHANGED <<phase, created, sent, in_flight, panic_held, hold_count, acked, rejected, abandoned, committed, forced, stop_reason>>

\* apply_ack / apply_reject: batch leaves in_flight, checkpoint may advance.
\*
\* Reject note: RejectBatch is intentionally reserved for explicit permanent rejects only.
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
    /\ b \notin held[s]
    /\ LET new_in_flight_s == in_flight[s] \ {b}
           new_acked_s     == acked[s] \cup {b}
           new_rejected_s  == rejected[s]
       IN
       /\ in_flight' = [in_flight EXCEPT ![s] = new_in_flight_s]
       /\ acked'     = [acked     EXCEPT ![s] = new_acked_s]
       /\ committed' = [committed EXCEPT ![s] = NewCommitted(new_acked_s, new_rejected_s, new_in_flight_s)]
    /\ UNCHANGED <<phase, created, sent, held, retried, panic_held, hold_count, rejected, abandoned, forced, stop_reason>>

RejectBatch(s, b) ==
    /\ b \in in_flight[s]
    /\ b \notin held[s]
    /\ LET new_in_flight_s == in_flight[s] \ {b}
           new_acked_s     == acked[s]
           new_rejected_s  == rejected[s] \cup {b}
       IN
       /\ in_flight' = [in_flight EXCEPT ![s] = new_in_flight_s]
       /\ rejected'  = [rejected  EXCEPT ![s] = new_rejected_s]
       /\ committed' = [committed EXCEPT ![s] = NewCommitted(new_acked_s, new_rejected_s, new_in_flight_s)]
    /\ UNCHANGED <<phase, created, sent, held, retried, panic_held, hold_count, acked, abandoned, forced, stop_reason>>

\* begin_drain: closes pipeline to new batches.
BeginDrain ==
    /\ phase = "Running"
    /\ phase' = "Draining"
    /\ UNCHANGED <<created, sent, in_flight, held, retried, panic_held, hold_count, acked, rejected, abandoned, committed, forced, stop_reason>>

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
    /\ stop_reason' = "graceful"
    /\ UNCHANGED <<created, sent, in_flight, held, retried, panic_held, hold_count, acked, rejected, abandoned, committed, forced>>

\* force_stop: emergency shutdown when grace period expires.
\* Discards in-flight state. Every production system has an equivalent:
\*   Vector: shutdown_force_trigger.cancel() after deadline
\*   Fluent Bit: grace timer expiry → flb_engine_shutdown()
\*   OTel: context.WithTimeout cancellation
\*
\* ForceStop is always in Next (production reality: the kill switch always exists).
\* The `forced` flag records that the hard-failure policy path was taken.
\* ForceStop explicitly terminalizes each in-flight batch as abandoned.
\*
\* NOTE: WF(ForceStop) is intentionally NOT in Fairness. ForceStop is an escape
\* hatch, not a scheduled event. Without WF, liveness checks (EventualDrain,
\* NoBatchLeftBehind) still require the normal path to work — TLC cannot satisfy
\* them by always choosing the ForceStop shortcut.
ForceStop ==
    /\ phase = "Draining"
    /\ phase' = "Stopped"
    /\ forced' = TRUE
    /\ stop_reason' = "force"
    /\ abandoned' = [s \in Sources |-> abandoned[s] \cup in_flight[s]]
    /\ in_flight' = [s \in Sources |-> {}]
    /\ held' = [s \in Sources |-> {}]
    /\ UNCHANGED <<created, sent, retried, panic_held, hold_count, acked, rejected, committed>>

\* crash_stop: panic/unwind-equivalent terminalization.
\* Models runtime abort/failure completion obligations without encoding Rust stack unwind.
\* Any sent-but-not-terminalized batch must become abandoned before terminal state.
CrashStop ==
    /\ phase \in {"Running", "Draining"}
    /\ phase' = "Stopped"
    /\ forced' = FALSE
    /\ stop_reason' = "crash"
    /\ abandoned' =
        [s \in Sources |->
            LET unresolved_s == sent[s] \ (acked[s] \cup rejected[s] \cup abandoned[s])
            IN abandoned[s] \cup unresolved_s]
    /\ in_flight' = [s \in Sources |-> {}]
    /\ held' = [s \in Sources |-> {}]
    /\ UNCHANGED <<created, sent, retried, panic_held, hold_count, acked, rejected, committed>>

(* ---------------------------------------------------------------------------
 * Next-state relation
 * ---------------------------------------------------------------------------*)

Next ==
    \/ \E s \in Sources         : CreateBatch(s)
    \/ \E s \in Sources,
          b \in BatchIds        : BeginSend(s, b)
    \/ \E s \in Sources,
          b \in BatchIds        : HoldBatch(s, b)
    \/ \E s \in Sources,
          b \in BatchIds        : PanicHoldBatch(s, b)
    \/ \E s \in Sources,
          b \in BatchIds        : RetryHeldBatch(s, b)
    \/ \E s \in Sources,
          b \in BatchIds        : AckBatch(s, b)
    \/ \E s \in Sources,
          b \in BatchIds        : RejectBatch(s, b)
    \/ BeginDrain
    \/ Stop
    \/ ForceStop
    \/ CrashStop
    \* Terminal state: Stopped is final, nothing more can happen.
    \* Explicit stuttering prevents TLC from reporting a false deadlock.
    \/ (phase = "Stopped" /\ UNCHANGED vars)

(*
 * Fairness:
 * WF_vars(a) = if action a is continuously enabled, it eventually fires.
 *
 * WF(AckBatch/RejectBatch): once a batch is Sending, it stays there until
 *   it receives an explicit terminal outcome (ack/reject/abandon), unless a
 *   bounded non-terminal hold temporarily disables terminalization.
 *   The downstream sink always eventually responds (retry budget is finite).
 *
 * WF(RetryHeldBatch): non-terminal hold state is stable until retry releases it
 *   or ForceStop abandons it. This models finite retry/backoff windows without
 *   modeling wall-clock delay.
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
    /\ \A s \in Sources, b \in BatchIds :
        /\ WF_vars(AckBatch(s, b))
        /\ WF_vars(RejectBatch(s, b))
        /\ WF_vars(RetryHeldBatch(s, b))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ===========================================================================
 * SAFETY INVARIANTS
 * Checked exhaustively over all reachable states. No fairness needed.
 * ===========================================================================*)

\* Basic disjointness across all terminal outcomes.
NoDoubleComplete ==
    \A s \in Sources :
        /\ in_flight[s] \cap acked[s] = {}
        /\ in_flight[s] \cap rejected[s] = {}
        /\ in_flight[s] \cap abandoned[s] = {}
        /\ acked[s] \cap rejected[s] = {}
        /\ acked[s] \cap abandoned[s] = {}
        /\ rejected[s] \cap abandoned[s] = {}

\* THE DRAIN GUARANTEE: Stopped implies no remaining in-flight work.
\* ForceStop preserves this by explicitly moving in-flight work to abandoned.
DrainCompleteness ==
    phase = "Stopped" => \A s \in Sources : in_flight[s] = {}

\* Stopped means no non-terminal hold remains. Normal Stop requires the
\* in_flight set to drain first; ForceStop clears held while abandoning it.
NoHeldWorkAfterStop ==
    phase = "Stopped" => \A s \in Sources : held[s] = {}

\* Crash-aware terminalization: every sent batch is explicit at quiescence.
\* This prevents silent "stranded in limbo" work after either normal Stop or ForceStop.
QuiescenceHasNoSilentStrandedWork ==
    phase = "Stopped" =>
        \A s \in Sources :
            LET terminal_s == acked[s] \cup rejected[s] \cup abandoned[s]
            IN sent[s] = terminal_s

\* No unresolved sent work may remain once Stopped, regardless of terminal path.
NoUnresolvedSentAtQuiescence ==
    phase = "Stopped" =>
        \A s \in Sources :
            sent[s] \ (acked[s] \cup rejected[s] \cup abandoned[s]) = {}

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

\* Failure-terminalization steps must not advance checkpoints.
\* Checkpoint progression remains tied to ack/reject ordering only.
FailureTerminalizationPreservesCheckpoint ==
    [][
        ((phase = "Draining" /\ phase' = "Stopped" /\ stop_reason' = "force")
          \/ (phase \in {"Running", "Draining"} /\ phase' = "Stopped" /\ stop_reason' = "crash"))
        => (\A s \in Sources : committed'[s] = committed[s])
    ]_vars

\* Non-terminal hold/retry transitions cannot advance checkpoints. Ack and
\* permanent reject are the only commit-moving outcomes.
HeldTransitionsDoNotCommit ==
    [][(\E s \in Sources : held[s]' /= held[s]) => committed' = committed]_vars

\* ForceStop must explicitly account for every unresolved batch it cuts off.
ForceStopAbandonsAllInFlight ==
    [][(phase = "Draining" /\ phase' = "Stopped" /\ forced' = TRUE /\ forced = FALSE) =>
        /\ \A s \in Sources : abandoned'[s] = abandoned[s] \cup in_flight[s]
        /\ \A s \in Sources : held'[s] = {}]_vars

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
            committed_terminal_s == acked[s] \cup rejected[s]
            ever_sent_s == sent[s]
        IN n > 0 =>
            \A i \in ever_sent_s :
                i <= n =>
                    /\ i \in committed_terminal_s
                    /\ i \notin in_flight[s]

\* A held/active in-flight batch is unresolved and must not be silently
\* committed past. This is intentionally explicit even though it follows from
\* CheckpointOrderingInvariant; it documents the no-advance-on-hold contract.
UnresolvedWorkNotCommittedPast ==
    \A s \in Sources :
        \A b \in in_flight[s] :
            committed[s] < b

\* The checkpoint is never ahead of the ack/reject terminalized prefix.
\* Abandoned work is explicitly accounted for at ForceStop, but it is not a
\* commit-terminal outcome and cannot justify checkpoint advancement.
CheckpointNeverAheadOfTerminalizedPrefix ==
    \A s \in Sources : committed[s] <= TerminalizedPrefix(s)

\* committed[s] never exceeds the highest created batch ID.
\* (It can exceed Cardinality(acked[s] \cup rejected[s]) when some batch IDs were created
\* but never sent — skipped batches don't block checkpoint advancement.)
CommittedNeverAheadOfCreated ==
    \A s \in Sources : committed[s] <= Cardinality(created[s])

\* Structural invariants (catch model misconfiguration).
InFlightImpliesCreated ==
    \A s \in Sources : in_flight[s] \subseteq created[s]

SentImpliesCreated ==
    \A s \in Sources : sent[s] \subseteq created[s]

InFlightImpliesSent ==
    \A s \in Sources : in_flight[s] \subseteq sent[s]

HeldImpliesInFlight ==
    \A s \in Sources : held[s] \subseteq in_flight[s]

RetriedImpliesSent ==
    \A s \in Sources : retried[s] \subseteq sent[s]

PanickedImpliesSent ==
    \A s \in Sources : panic_held[s] \subseteq sent[s]

HoldCountOnlyForSent ==
    \A s \in Sources :
        \A b \in BatchIds :
            hold_count[s][b] > 0 => b \in sent[s]

AckedImpliesCreated ==
    \A s \in Sources : acked[s] \subseteq created[s]

AckedImpliesSent ==
    \A s \in Sources : acked[s] \subseteq sent[s]

RejectedImpliesCreated ==
    \A s \in Sources : rejected[s] \subseteq created[s]

RejectedImpliesSent ==
    \A s \in Sources : rejected[s] \subseteq sent[s]

AbandonedImpliesCreated ==
    \A s \in Sources : abandoned[s] \subseteq created[s]

AbandonedImpliesSent ==
    \A s \in Sources : abandoned[s] \subseteq sent[s]

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
        (b \in in_flight[s]) ~>
            (\/ b \in acked[s]
             \/ b \in rejected[s]
             \/ b \in abandoned[s])

\* Non-terminal hold cannot become a silent permanent limbo state.
HeldBatchEventuallyReleased ==
    \A s \in Sources, b \in BatchIds :
        (b \in held[s]) ~> (b \notin held[s])

\* Panic-held work must still reach a terminal outcome eventually.
PanickedBatchEventuallyAccountedFor ==
    \A s \in Sources, b \in BatchIds :
        (b \in panic_held[s]) ~>
            (\/ b \in acked[s]
             \/ b \in rejected[s]
             \/ b \in abandoned[s])

\* Every created batch is eventually committed or the machine is Stopped.
\* (Drain + stop must eventually account for all data.)
AllCreatedBatchesEventuallyAccountedFor ==
    \A s \in Sources :
        \A b \in BatchIds :
            (b \in created[s]) ~>
                (\/ b \in acked[s]
                 \/ b \in rejected[s]
                 \/ b \in abandoned[s]
                 \/ committed[s] >= b
                 \/ phase = "Stopped")

(* ---------------------------------------------------------------------------
 * Approach B prototype (minimal/runnable only):
 * transition-class decomposition for failure transitions.
 * ---------------------------------------------------------------------------*)
FailureTransitionClass ==
    \/ ForceStop
    \/ CrashStop

FailureClassMustTerminalizePrototype ==
    [][
        FailureTransitionClass =>
        \A s \in Sources :
            sent'[s] \ (acked'[s] \cup rejected'[s] \cup abandoned'[s]) = {}
    ]_vars

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

\* At least one permanent reject occurs (explicit ack vs reject distinction).
RejectOccurs == ~(\E s \in Sources : rejected[s] /= {})

\* At least one non-terminal hold/failure occurs.
HoldOccurs == ~(\E s \in Sources : held[s] /= {})

\* At least one held batch is retried.
RetryOccurs == ~(\E s \in Sources : retried[s] /= {})

\* At least one panic-driven hold occurs.
PanicHoldOccurs == ~(\E s \in Sources : panic_held[s] /= {})

\* The committed checkpoint advances at least once (ordering logic is exercised).
CheckpointAdvances == ~(\E s \in Sources : committed[s] > 0)

\* ForceStop is reachable (the kill-switch path is exercised).
ForcedReachable == ~(forced = TRUE)

\* At least one in-flight batch is explicitly abandoned by ForceStop.
AbandonOccurs == ~(\E s \in Sources : abandoned[s] /= {})

\* Crash-stop path is reachable.
CrashReachable == ~(stop_reason = "crash")

\* At least one held batch is explicitly abandoned by ForceStop.
HeldAbandonOccurs ==
    ~(\E s \in Sources :
        \E b \in abandoned[s] :
            hold_count[s][b] > 0)

\* At least one batch is created (CreateBatch fires).
\* Covers NoBatchLeftBehind and AllCreatedBatchesEventuallyAccountedFor
\* antecedents — without this, those properties are vacuously true.
CreateOccurs == ~(\E s \in Sources : created[s] /= {})

=============================================================================
