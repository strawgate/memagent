------------------------ MODULE DeliveryRetry ------------------------
(*
 * Formal model of the worker delivery retry loop from
 * crates/logfwd-runtime/src/worker_pool/worker.rs (process_item).
 *
 * This spec proves the LIVENESS ASSUMPTION that PipelineMachine.tla
 * depends on: "every batch eventually terminalizes (reaches Ok or
 * Rejected)." PipelineMachine.tla assumes WF(AckBatch) — meaning a
 * batch in Sending state eventually receives an explicit terminal
 * outcome — but nothing formally verified that assumption until now.
 *
 * The retry loop:
 *   1. Send batch to sink
 *   2. On Ok: terminal success (Delivered)
 *   3. On Rejected: terminal permanent failure (Rejected)
 *   4. On IoError/RetryAfter/Timeout: exponential backoff, retry forever
 *   5. On cancel (shutdown): terminal (PoolClosed)
 *
 * Key insight: the code deliberately retries FOREVER for transient
 * errors (Filebeat-style). Liveness depends on sink recovery OR
 * shutdown cancellation. Without either, the loop runs indefinitely —
 * and that is correct by design (backpressure, not data loss).
 *
 * What this spec does NOT model (verified separately):
 *   - Jitter in backoff delays (orthogonal to liveness)
 *   - Per-batch timeout (60s) — modeled as a transient error
 *   - Batch payload / RecordBatch contents (opaque)
 *   - Output health tracking (OutputHealthEvent transitions)
 *   - Fanout/split-retry semantics (AsyncFanoutSink, send_split_halves)
 *   - The distinction between server-directed RetryAfter and IoError
 *     (both are modeled as transient; RetryAfter simply uses the
 *     server's delay instead of the backoff iterator)
 *   - Worker lifecycle (spawn, idle timeout, shutdown) — see
 *     ShutdownProtocol.tla
 *
 * For TLC model checker configuration, see MCDeliveryRetry.tla.
 *)

EXTENDS Naturals, TLC

CONSTANTS
    InitialBackoffMs,   \* Initial backoff delay in ms (production: 100)
    MaxBackoffMs,       \* Maximum backoff delay in ms (production: max_retry_delay)
    MaxRetries          \* Bounded retry count for model checking; real system is unbounded

ASSUME InitialBackoffMs \in Nat /\ InitialBackoffMs >= 1
ASSUME MaxBackoffMs \in Nat /\ MaxBackoffMs >= InitialBackoffMs
ASSUME MaxRetries \in Nat /\ MaxRetries >= 1

(* ---------------------------------------------------------------------------
 * State variables
 *
 * Design note: this models a single batch's delivery attempt. The worker
 * picks up a WorkItem, enters the retry loop, and eventually returns a
 * DeliveryOutcome. Multiple batches are independent (no shared state
 * between process_item calls), so modeling one suffices for the
 * per-batch liveness guarantee.
 * ---------------------------------------------------------------------------*)

VARIABLES
    state,       \* Worker state: "Idle" | "Sending" | "WaitingBackoff" | "Terminal"
    outcome,     \* Terminal outcome: "None" | "Ok" | "Rejected" | "Cancelled"
    retryCount,  \* Number of retry attempts so far
    backoffMs,   \* Current backoff delay in milliseconds
    cancelled,   \* Whether shutdown cancellation was observed
    sinkState    \* Remote sink condition: "Healthy" | "Transient" | "Permanent"

vars == <<state, outcome, retryCount, backoffMs, cancelled, sinkState>>

(* ---------------------------------------------------------------------------
 * Backoff computation
 *
 * Models the exponential backoff from backon::ExponentialBuilder:
 *   min_delay = InitialBackoffMs
 *   factor = 2
 *   max_delay = MaxBackoffMs
 *
 * After the backoff iterator is exhausted (max_times=10 in production),
 * the code stays at MaxBackoffMs. The Min(current * 2, max) formula
 * captures the essential doubling-with-cap behavior.
 * ---------------------------------------------------------------------------*)

Min(a, b) == IF a <= b THEN a ELSE b

NextBackoff(current) == Min(current * 2, MaxBackoffMs)

(* ---------------------------------------------------------------------------
 * Type invariant
 * ---------------------------------------------------------------------------*)

TypeOK ==
    /\ state \in {"Idle", "Sending", "WaitingBackoff", "Terminal"}
    /\ outcome \in {"None", "Ok", "Rejected", "Cancelled"}
    /\ retryCount \in 0..MaxRetries
    /\ backoffMs \in 0..MaxBackoffMs
    /\ cancelled \in BOOLEAN
    /\ sinkState \in {"Healthy", "Transient", "Permanent"}

(* ---------------------------------------------------------------------------
 * Initial state
 * ---------------------------------------------------------------------------*)

Init ==
    /\ state      = "Idle"
    /\ outcome    = "None"
    /\ retryCount = 0
    /\ backoffMs  = 0
    /\ cancelled  = FALSE
    /\ sinkState  = "Healthy"

(* ---------------------------------------------------------------------------
 * Environment actions (sink state changes)
 *
 * The sink can transition between states non-deterministically. This
 * models network flaps, server restarts, and permanent failures.
 * ---------------------------------------------------------------------------*)

\* Sink recovers from transient failure.
SinkRecover ==
    /\ sinkState = "Transient"
    /\ sinkState' = "Healthy"
    /\ UNCHANGED <<state, outcome, retryCount, backoffMs, cancelled>>

\* Sink enters transient failure (network error, timeout, rate limit).
SinkFail ==
    /\ sinkState = "Healthy"
    /\ sinkState' = "Transient"
    /\ UNCHANGED <<state, outcome, retryCount, backoffMs, cancelled>>

\* Sink enters permanent failure mode (schema rejection, 4xx).
SinkPermanentFail ==
    /\ sinkState \in {"Healthy", "Transient"}
    /\ sinkState' = "Permanent"
    /\ UNCHANGED <<state, outcome, retryCount, backoffMs, cancelled>>

(* ---------------------------------------------------------------------------
 * Worker actions
 * ---------------------------------------------------------------------------*)

\* Send: transition from Idle or WaitingBackoff to Sending.
\* Models the start of sink.send_batch().
Send ==
    /\ state \in {"Idle", "WaitingBackoff"}
    /\ state' = "Sending"
    /\ UNCHANGED <<outcome, retryCount, backoffMs, cancelled, sinkState>>

\* ReceiveOk: sink accepted the batch. Terminal success.
\* Maps to Ok(SendResult::Ok) in process_item.
ReceiveOk ==
    /\ state = "Sending"
    /\ sinkState = "Healthy"
    /\ state'   = "Terminal"
    /\ outcome' = "Ok"
    /\ UNCHANGED <<retryCount, backoffMs, cancelled, sinkState>>

\* ReceiveRejected: sink permanently rejected the batch. Terminal failure.
\* Maps to Ok(SendResult::Rejected) in process_item.
ReceiveRejected ==
    /\ state = "Sending"
    /\ sinkState = "Permanent"
    /\ state'   = "Terminal"
    /\ outcome' = "Rejected"
    /\ UNCHANGED <<retryCount, backoffMs, cancelled, sinkState>>

\* ReceiveTransient: transient failure, compute next backoff and wait.
\* Maps to Ok(SendResult::IoError), Ok(SendResult::RetryAfter), and
\* Err(_elapsed) (timeout) in process_item. All three follow the same
\* pattern: increment retry count, compute backoff, sleep, loop.
ReceiveTransient ==
    /\ state = "Sending"
    /\ sinkState = "Transient"
    /\ retryCount < MaxRetries
    /\ state'      = "WaitingBackoff"
    /\ retryCount' = retryCount + 1
    /\ backoffMs'  = IF retryCount = 0
                     THEN InitialBackoffMs
                     ELSE NextBackoff(backoffMs)
    /\ UNCHANGED <<outcome, cancelled, sinkState>>

\* Cancel: shutdown signal observed. Can happen in any non-terminal state.
\* Models cancel.cancelled() in the tokio::select! arms of process_item.
\* Cancellation can be observed:
\*   - Before send (during recv_with_idle_timeout / select)
\*   - During send (tokio::select! biased cancel check)
\*   - During backoff sleep (tokio::select! biased cancel check)
Cancel ==
    /\ state # "Terminal"
    /\ cancelled' = TRUE
    /\ state'     = "Terminal"
    /\ outcome'   = "Cancelled"
    /\ UNCHANGED <<retryCount, backoffMs, sinkState>>

(* ---------------------------------------------------------------------------
 * Next-state relation
 * ---------------------------------------------------------------------------*)

Next ==
    \/ Send
    \/ ReceiveOk
    \/ ReceiveRejected
    \/ ReceiveTransient
    \/ Cancel
    \/ SinkRecover
    \/ SinkFail
    \/ SinkPermanentFail
    \* Terminal is absorbing — explicit stuttering prevents false deadlock.
    \/ (state = "Terminal" /\ UNCHANGED vars)

(*
 * Fairness:
 *
 * WF(Send): once a batch is ready to send (Idle or WaitingBackoff),
 *   it eventually sends. This models the retry loop always iterating.
 *
 * WF(ReceiveOk): if the sink is healthy and we are sending, the send
 *   eventually completes successfully. Models sink.send_batch() returning.
 *
 * WF(ReceiveRejected): if the sink is in permanent failure and we are
 *   sending, the rejection eventually arrives.
 *
 * WF(ReceiveTransient): if the sink is transiently failing and we are
 *   sending, the transient error eventually arrives.
 *
 * WF(Cancel): shutdown cancellation is a sticky token (CancellationToken
 *   semantics). Once `cancelled` is true externally, the cancel check
 *   in tokio::select! will eventually fire.
 *
 * No WF on SinkRecover/SinkFail/SinkPermanentFail — the environment
 * is not obligated to recover. Liveness of TerminalReachable relies on
 * sink recovery OR cancel, not environment cooperation alone.
 *)
Fairness ==
    /\ WF_vars(Send)
    /\ WF_vars(ReceiveOk)
    /\ WF_vars(ReceiveRejected)
    /\ WF_vars(ReceiveTransient)
    /\ WF_vars(Cancel)

Spec == Init /\ [][Next]_vars /\ Fairness

(* ===========================================================================
 * SAFETY INVARIANTS
 * ===========================================================================*)

\* Backoff delay never exceeds the configured maximum.
BackoffCapRespected == backoffMs <= MaxBackoffMs

\* Once in WaitingBackoff, the backoff delay is at least the initial value.
\* This captures the monotonic-from-initial property of exponential backoff.
BackoffMonotonic ==
    state = "WaitingBackoff" => backoffMs >= InitialBackoffMs

\* Terminal state implies a definite outcome.
TerminalImpliesOutcome ==
    state = "Terminal" => outcome # "None"

\* Non-terminal state has no outcome yet.
NonTerminalImpliesNoOutcome ==
    state # "Terminal" => outcome = "None"

\* Retry count only increases when transitioning through WaitingBackoff.
\* At state Idle (initial), retryCount is always 0.
IdleImpliesNoRetries ==
    state = "Idle" => retryCount = 0

\* If cancelled, the outcome must be Cancelled (not Ok or Rejected).
CancelledImpliesCancelledOutcome ==
    (state = "Terminal" /\ cancelled) => outcome = "Cancelled"

\* Backoff delay is 0 only before the first transient failure.
BackoffZeroOnlyInitially ==
    (backoffMs = 0) => (retryCount = 0)

(* ===========================================================================
 * TEMPORAL SAFETY PROPERTIES
 * ===========================================================================*)

\* Retry count is monotonically non-decreasing.
RetryCountMonotonic ==
    [][retryCount' >= retryCount]_vars

\* Backoff delay is monotonically non-decreasing (once set, it only grows
\* up to the cap).
BackoffDelayMonotonic ==
    [][backoffMs' >= backoffMs]_vars

\* Terminal state is absorbing — once Terminal, always Terminal.
TerminalIsAbsorbing ==
    [][state = "Terminal" => state' = "Terminal"]_vars

\* Outcome is stable — once set, it never changes.
OutcomeIsStable ==
    [][outcome # "None" => outcome' = outcome]_vars

(* ===========================================================================
 * LIVENESS PROPERTIES (under Fairness)
 *
 * The key property this spec exists to prove: TerminalReachable.
 * PipelineMachine.tla's WF(AckBatch) assumption requires that every
 * batch delivery attempt eventually reaches a terminal outcome.
 * ===========================================================================*)

\* THE CRITICAL PROPERTY: every delivery eventually reaches Terminal.
\*
\* Under WF(Cancel), even if the sink never recovers, cancellation
\* eventually fires and terminates the loop. This is the formal
\* justification for PipelineMachine.tla's WF(AckBatch).
\*
\* Without WF(Cancel), TerminalReachable would NOT hold when the sink
\* stays Transient forever and MaxRetries is exhausted — the loop would
\* be stuck in WaitingBackoff with no Send available. The real system
\* has the same property: if shutdown never fires AND the sink never
\* recovers, the worker blocks forever (by design: backpressure).
TerminalReachable == <>(state = "Terminal")

\* If the sink is permanently healthy, delivery eventually succeeds.
\* This is the happy-path liveness guarantee.
HealthySinkDelivers ==
    ([](sinkState = "Healthy") /\ []~cancelled) => <>(outcome = "Ok")

\* Cancel always reaches terminal state.
\* This proves that shutdown is never stuck — the cancellation token
\* always eventually terminates the retry loop.
CancelTerminates == cancelled ~> (state = "Terminal")

\* Terminal state is eventually stable (convergence).
TerminalIsStable == <>[](state = "Terminal")

\* Every WaitingBackoff state eventually resolves — the worker does not
\* stay stuck in backoff forever.
BackoffEventuallyResolves ==
    (state = "WaitingBackoff") ~> (state # "WaitingBackoff")

(* ===========================================================================
 * REACHABILITY ASSERTIONS (vacuity guards — kani::cover!() equivalent)
 *
 * Defined as NEGATIONS of interesting states. Used as INVARIANTS in
 * DeliveryRetry.coverage.cfg. When TLC finds a violation, the trace IS
 * the witness that the state is reachable.
 * ===========================================================================*)

\* The Sending state is reachable.
SendingReachable == ~(state = "Sending")

\* Terminal with Ok outcome is reachable.
OkReachable == ~(state = "Terminal" /\ outcome = "Ok")

\* Terminal with Rejected outcome is reachable.
RejectedReachable == ~(state = "Terminal" /\ outcome = "Rejected")

\* Terminal with Cancelled outcome is reachable.
CancelledReachable == ~(state = "Terminal" /\ outcome = "Cancelled")

\* WaitingBackoff is reachable (retry path exercised).
BackoffReachable == ~(state = "WaitingBackoff")

\* At least one retry occurs.
RetryOccurs == ~(retryCount > 0)

\* Backoff reaches the cap.
BackoffCapReached == ~(backoffMs = MaxBackoffMs)

\* Multiple retries occur before terminal.
MultipleRetriesOccur == ~(retryCount > 1)

\* Sink recovery after transient failure is reachable.
SinkRecoveryReachable == ~(sinkState = "Healthy" /\ retryCount > 0)

=============================================================================
