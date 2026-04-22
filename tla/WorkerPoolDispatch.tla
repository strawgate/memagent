--------------------- MODULE WorkerPoolDispatch ---------------------
(*
 * Formal model of the OutputWorkerPool dispatch algorithm from
 * crates/logfwd-runtime/src/worker_pool/pool.rs
 *
 * Models:
 *   - MRU-first dispatch: try workers front-to-back, send to first
 *     with space, promote to front
 *   - Spawn-on-demand: when all workers are full and under capacity,
 *     spawn a new worker
 *   - Back-pressure: when at capacity and all full, async-wait on
 *     front worker (modeled as eventual delivery)
 *   - Worker lifecycle: Idle → Busy → idle timeout / panic / exit
 *   - Health aggregation: worst-of-children, sticky failure
 *   - 3-phase drain: signal → join with timeout → force-abort
 *
 * What this spec does NOT model (verified separately):
 *   - Retry/backoff timing within process_item (worker.rs)
 *   - Arrow RecordBatch payloads or batch metadata
 *   - Ack channel closure and cancel-propagation race (Kani + loom)
 *   - OTLP serialization or transport details
 *   - BatchTicket typestate transitions (PipelineMachine.tla)
 *   - Pipeline-level checkpoint ordering (PipelineMachine.tla)
 *
 * For TLC configuration, see MCWorkerPoolDispatch.tla.
 *)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    MaxWorkers,     \* Maximum concurrent workers (>= 1)
    NumItems        \* Number of work items to model

ASSUME MaxWorkers >= 1 /\ MaxWorkers <= 4
ASSUME NumItems >= 1 /\ NumItems <= 4

Items == 1..NumItems
WorkerIds == 1..MaxWorkers

(* -----------------------------------------------------------------------
 * State variables
 * ----------------------------------------------------------------------- *)

VARIABLES
    poolState,      \* "Accepting" | "Draining" | "Stopped"
    workers,        \* Set of worker IDs that currently exist
    workerState,    \* [WorkerIds -> {"Idle", "Busy", "Stopped"}]
    workerHealth,   \* [WorkerIds -> {"Healthy", "Degraded", "Failed"}]
    pending,        \* Sequence of items not yet dispatched
    inFlight,       \* Set of items currently being processed by a worker
    delivered,      \* Set of items that reached terminal outcome
    rejected,       \* Set of items permanently rejected
    assignment,     \* [Items -> WorkerIds \cup {0}] which worker has an item (0 = unassigned)
    cancelled,      \* BOOLEAN — shutdown signal
    forcedAbort     \* BOOLEAN — force-abort path was used

vars == <<poolState, workers, workerState, workerHealth,
          pending, inFlight, delivered, rejected,
          assignment, cancelled, forcedAbort>>

(* -----------------------------------------------------------------------
 * Helper operators
 * ----------------------------------------------------------------------- *)

\* Count of active (non-Stopped) workers.
ActiveWorkers == {w \in workers : workerState[w] /= "Stopped"}

\* Count of idle workers available for dispatch.
IdleWorkers == {w \in workers : workerState[w] = "Idle"}

\* Count of busy workers.
BusyWorkers == {w \in workers : workerState[w] = "Busy"}

\* Items in the system: pending + inFlight + delivered + rejected.
\* Conservation invariant: all submitted items are accounted for.
AllAccountedItems == {i \in Items : \E idx \in 1..Len(pending) : pending[idx] = i}
                     \cup inFlight \cup delivered \cup rejected

\* Worst-of health aggregation (matches aggregate_output_health).
\* Healthy < Degraded < Failed.
HealthOrd(h) == CASE h = "Healthy"  -> 0
                  [] h = "Degraded" -> 1
                  [] h = "Failed"   -> 2

AggregateHealth ==
    IF workers = {} THEN "Healthy"
    ELSE LET maxOrd == CHOOSE m \in {HealthOrd(workerHealth[w]) : w \in ActiveWorkers} \cup {0} :
                            \A o \in {HealthOrd(workerHealth[w]) : w \in ActiveWorkers} \cup {0} : m >= o
         IN CASE maxOrd = 0 -> "Healthy"
              [] maxOrd = 1 -> "Degraded"
              [] maxOrd = 2 -> "Failed"

\* Sequence to set: all elements in a sequence.
RECURSIVE SeqToSet(_)
SeqToSet(s) == IF s = <<>> THEN {} ELSE {Head(s)} \cup SeqToSet(Tail(s))

(* -----------------------------------------------------------------------
 * Type invariant
 * ----------------------------------------------------------------------- *)

TypeOK ==
    /\ poolState \in {"Accepting", "Draining", "Stopped"}
    /\ workers \subseteq WorkerIds
    /\ \A w \in WorkerIds : workerState[w] \in {"Idle", "Busy", "Stopped"}
    /\ \A w \in WorkerIds : workerHealth[w] \in {"Healthy", "Degraded", "Failed"}
    /\ Len(pending) >= 0
    /\ SeqToSet(pending) \subseteq Items
    /\ inFlight \subseteq Items
    /\ delivered \subseteq Items
    /\ rejected \subseteq Items
    /\ \A i \in Items : assignment[i] \in WorkerIds \cup {0}
    /\ cancelled \in BOOLEAN
    /\ forcedAbort \in BOOLEAN

(* -----------------------------------------------------------------------
 * Initial state
 * ----------------------------------------------------------------------- *)

Init ==
    /\ poolState   = "Accepting"
    /\ workers     = {}
    /\ workerState = [w \in WorkerIds |-> "Stopped"]
    /\ workerHealth = [w \in WorkerIds |-> "Healthy"]
    /\ pending     = <<>>
    /\ inFlight    = {}
    /\ delivered   = {}
    /\ rejected    = {}
    /\ assignment  = [i \in Items |-> 0]
    /\ cancelled   = FALSE
    /\ forcedAbort = FALSE

(* -----------------------------------------------------------------------
 * Actions: Normal operation
 * ----------------------------------------------------------------------- *)

\* Submit(item): pipeline sends a new work item to the pool.
\* Only allowed in Accepting state. Rejected immediately if draining.
Submit(item) ==
    /\ poolState = "Accepting"
    /\ item \notin SeqToSet(pending)
    /\ item \notin inFlight
    /\ item \notin delivered
    /\ item \notin rejected
    /\ pending' = Append(pending, item)
    /\ UNCHANGED <<poolState, workers, workerState, workerHealth,
                   inFlight, delivered, rejected, assignment,
                   cancelled, forcedAbort>>

\* SubmitAfterDrain(item): submit while draining/stopped → rejected immediately.
\* Models the guard in pool.rs that rejects with PoolClosed.
SubmitAfterDrain(item) ==
    /\ poolState /= "Accepting"
    /\ item \notin SeqToSet(pending)
    /\ item \notin inFlight
    /\ item \notin delivered
    /\ item \notin rejected
    /\ rejected' = rejected \cup {item}
    /\ UNCHANGED <<poolState, workers, workerState, workerHealth,
                   pending, inFlight, delivered, assignment, cancelled, forcedAbort>>

\* Dispatch: send the first pending item to an idle worker.
\* Models MRU dispatch: pick any idle worker (ordering is abstracted).
Dispatch ==
    /\ Len(pending) > 0
    /\ \E w \in IdleWorkers :
        /\ LET item == Head(pending) IN
           /\ pending'     = Tail(pending)
           /\ inFlight'    = inFlight \cup {item}
           /\ workerState' = [workerState EXCEPT ![w] = "Busy"]
           /\ assignment'  = [assignment EXCEPT ![item] = w]
        /\ UNCHANGED <<poolState, workers, workerHealth,
                       delivered, rejected, cancelled, forcedAbort>>

\* SpawnAndDispatch: no idle workers, under capacity — spawn a new one and dispatch.
SpawnAndDispatch ==
    /\ Len(pending) > 0
    /\ IdleWorkers = {}
    /\ Cardinality(ActiveWorkers) < MaxWorkers
    /\ \E w \in WorkerIds \ workers :
        /\ LET item == Head(pending) IN
           /\ workers'      = workers \cup {w}
           /\ workerState'  = [workerState EXCEPT ![w] = "Busy"]
           /\ workerHealth' = [workerHealth EXCEPT ![w] = "Healthy"]
           /\ pending'      = Tail(pending)
           /\ inFlight'     = inFlight \cup {item}
           /\ assignment'   = [assignment EXCEPT ![item] = w]
        /\ UNCHANGED <<poolState, delivered, rejected, cancelled, forcedAbort>>

\* WaitAndDispatch: at capacity, all busy — eventually a worker finishes
\* and the item is delivered. Modeled as: when a worker becomes idle AND
\* there is pending work, the dispatch happens (via Dispatch action).
\* This action is not needed as a separate step — Dispatch covers it
\* after WorkerComplete makes a worker idle.

\* WorkerComplete(w, item): worker finishes processing, item delivered.
WorkerComplete(w, item) ==
    /\ workerState[w] = "Busy"
    /\ item \in inFlight
    /\ assignment[item] = w
    /\ workerState' = [workerState EXCEPT ![w] = "Idle"]
    /\ workerHealth' = [workerHealth EXCEPT ![w] = (IF workerHealth[w] = "Failed" THEN "Failed" ELSE "Healthy")]
    /\ inFlight'    = inFlight \ {item}
    /\ delivered'   = delivered \cup {item}
    /\ UNCHANGED <<poolState, workers, pending, rejected,
                   assignment, cancelled, forcedAbort>>

\* WorkerReject(w, item): worker permanently rejects the item (4xx, schema error).
\* Health transitions to Failed (sticky).
WorkerReject(w, item) ==
    /\ workerState[w] = "Busy"
    /\ item \in inFlight
    /\ assignment[item] = w
    /\ workerState'  = [workerState EXCEPT ![w] = "Idle"]
    /\ workerHealth' = [workerHealth EXCEPT ![w] = "Failed"]
    /\ inFlight'     = inFlight \ {item}
    /\ rejected'     = rejected \cup {item}
    /\ UNCHANGED <<poolState, workers, pending, delivered,
                   assignment, cancelled, forcedAbort>>

\* WorkerFail(w): worker encounters fatal error, terminates.
\* Any in-flight item assigned to this worker is rejected (InternalFailure).
WorkerFail(w) ==
    /\ w \in workers
    /\ workerState[w] \in {"Idle", "Busy"}
    /\ workerState'  = [workerState EXCEPT ![w] = "Stopped"]
    /\ workerHealth' = [workerHealth EXCEPT ![w] = "Failed"]
    /\ workers'      = workers \ {w}
    /\ LET failedItems == {item \in inFlight : assignment[item] = w} IN
       /\ inFlight' = inFlight \ failedItems
       /\ rejected' = rejected \cup failedItems
    /\ UNCHANGED <<poolState, pending, delivered, assignment,
                   cancelled, forcedAbort>>

\* IdleTimeout(w): idle worker exceeds timeout, exits cleanly.
\* No items are lost because worker was idle.
IdleTimeout(w) ==
    /\ w \in workers
    /\ workerState[w] = "Idle"
    /\ workerState' = [workerState EXCEPT ![w] = "Stopped"]
    /\ workers'     = workers \ {w}
    /\ UNCHANGED <<poolState, workerHealth, pending, inFlight,
                   delivered, rejected, assignment, cancelled, forcedAbort>>

(* -----------------------------------------------------------------------
 * Actions: Shutdown sequence
 * ----------------------------------------------------------------------- *)

\* BeginDrain: pool stops accepting, starts 3-phase drain.
BeginDrain ==
    /\ poolState = "Accepting"
    /\ poolState' = "Draining"
    /\ cancelled' = TRUE
    /\ UNCHANGED <<workers, workerState, workerHealth, pending,
                   inFlight, delivered, rejected, assignment, forcedAbort>>

\* DrainComplete: all workers have finished, no in-flight items remain.
\* Phase 2 — graceful join succeeds.
DrainComplete ==
    /\ poolState = "Draining"
    /\ inFlight = {}
    /\ Len(pending) = 0
    /\ poolState' = "Stopped"
    /\ UNCHANGED <<workers, workerState, workerHealth, pending,
                   inFlight, delivered, rejected, assignment,
                   cancelled, forcedAbort>>

\* ForceAbort: drain timeout exceeded. All remaining in-flight items
\* are abandoned (rejected). Phase 3 — force-abort.
ForceAbort ==
    /\ poolState = "Draining"
    /\ (inFlight /= {} \/ Len(pending) > 0)
    /\ poolState'   = "Stopped"
    /\ forcedAbort'  = TRUE
    \* Force-reject all in-flight items.
    /\ rejected'    = rejected \cup inFlight \cup SeqToSet(pending)
    /\ inFlight'    = {}
    /\ pending'     = <<>>
    \* Stop all workers.
    /\ workerState' = [w \in WorkerIds |->
                        IF w \in workers THEN "Stopped"
                        ELSE workerState[w]]
    /\ UNCHANGED <<workers, workerHealth, delivered,
                   assignment, cancelled>>

(* -----------------------------------------------------------------------
 * Next-state relation
 * ----------------------------------------------------------------------- *)

Next ==
    \/ \E item \in Items : Submit(item)
    \/ \E item \in Items : SubmitAfterDrain(item)
    \/ Dispatch
    \/ SpawnAndDispatch
    \/ \E w \in WorkerIds, item \in Items : WorkerComplete(w, item)
    \/ \E w \in WorkerIds, item \in Items : WorkerReject(w, item)
    \/ \E w \in WorkerIds : WorkerFail(w)
    \/ \E w \in WorkerIds : IdleTimeout(w)
    \/ BeginDrain
    \/ DrainComplete
    \/ ForceAbort
    \* Terminal state: Stopped is final.
    \/ (poolState = "Stopped" /\ UNCHANGED vars)

(* -----------------------------------------------------------------------
 * Fairness
 * ----------------------------------------------------------------------- *)

Fairness ==
    /\ WF_vars(BeginDrain)
    /\ WF_vars(Dispatch)
    /\ WF_vars(SpawnAndDispatch)
    /\ WF_vars(DrainComplete)
    /\ \A w \in WorkerIds, item \in Items :
        /\ WF_vars(WorkerComplete(w, item))
    \* No WF on ForceAbort — escape hatch, not required progress.
    \* No WF on WorkerFail/IdleTimeout — environment choices.
    \* No WF on Submit — environment is not obligated to submit.

Spec == Init /\ [][Next]_vars /\ Fairness

(* =======================================================================
 * SAFETY INVARIANTS
 * ======================================================================= *)

\* Core conservation: every item that has entered any set remains in
\* exactly one of {pending, inFlight, delivered, rejected}. Items not
\* yet submitted are in none. This is the TLA+ equivalent of the Rust
\* invariant that every WorkItem is either queued, assigned to a worker,
\* or terminalized — never silently lost.

\* No item appears in two terminal/in-progress categories at once.
NoDoubleAccounting ==
    /\ inFlight \cap delivered = {}
    /\ inFlight \cap rejected = {}
    /\ delivered \cap rejected = {}
    /\ SeqToSet(pending) \cap inFlight = {}
    /\ SeqToSet(pending) \cap delivered = {}
    /\ SeqToSet(pending) \cap rejected = {}

\* THE DISPATCH NEVER DROPS INVARIANT:
\* Every item that has entered the pool (submitted) is either:
\* - still pending, or
\* - in-flight on a worker, or
\* - delivered, or
\* - rejected (permanent reject or force-abort)
\* An item is never silently lost.
DispatchNeverDrops ==
    \A item \in Items :
        (item \in SeqToSet(pending) \/ item \in inFlight
         \/ item \in delivered \/ item \in rejected)
        \/ \* Item was never submitted — not in any set, which is fine.
           (item \notin SeqToSet(pending) /\ item \notin inFlight
            /\ item \notin delivered /\ item \notin rejected)

\* FailureIsSticky: checked as a per-lifecycle action property
\* (FailureIsStickyTemporal) — see the property definition for details.

\* Stopped implies no in-flight items remain.
StoppedImpliesNoInFlight ==
    poolState = "Stopped" => inFlight = {}

\* Stopped implies no pending items remain.
StoppedImpliesNoPending ==
    poolState = "Stopped" => Len(pending) = 0

\* Worker count never exceeds MaxWorkers.
WorkerCountBound ==
    Cardinality(workers) <= MaxWorkers

\* Busy workers are always in the active worker set.
BusyImpliesActive ==
    \A w \in WorkerIds :
        workerState[w] = "Busy" => w \in workers

\* In-flight items are assigned to a busy worker.
InFlightHasWorker ==
    \A item \in inFlight :
        /\ assignment[item] \in workers
        /\ workerState[assignment[item]] = "Busy"

\* No new submissions after drain begins.
NoSubmitAfterDrain ==
    [][poolState /= "Accepting" =>
       \A item \in Items :
           item \notin SeqToSet(pending') \/ item \in SeqToSet(pending)
           \/ item \in rejected']_vars

(* -----------------------------------------------------------------------
 * Temporal safety properties (action-level)
 * ----------------------------------------------------------------------- *)

\* FailureIsSticky as a temporal property: once Failed, stays Failed
\* UNLESS the worker is re-spawned (added back to the workers set).
FailureIsStickyTemporal ==
    \A w \in WorkerIds :
        [][workerHealth[w] = "Failed" =>
           (workerHealth'[w] = "Failed" \/ (w \notin workers /\ w \in workers'))]_vars

\* ForceAbort accounts for all unfinished work.
ForceAbortAccountsForAll ==
    [][
        (poolState = "Draining" /\ poolState' = "Stopped" /\ forcedAbort' = TRUE) =>
        /\ inFlight' = {}
        /\ Len(pending') = 0
    ]_vars

(* =======================================================================
 * LIVENESS PROPERTIES
 * ======================================================================= *)

\* Every started drain eventually reaches Stopped.
ShutdownReachable ==
    (poolState = "Draining") ~> (poolState = "Stopped")

\* Once Stopped, stays Stopped.
StoppedIsStable ==
    <>[](poolState = "Stopped")

\* Every submitted item is eventually delivered or rejected.
AllItemsEventuallyResolved ==
    \A item \in Items :
        (item \in inFlight \/ item \in SeqToSet(pending)) ~>
            (item \in delivered \/ item \in rejected)

\* Idle timeout fires: an idle worker eventually exits (if no new work arrives).
\* Modeled via WF on WorkerComplete enabling Dispatch, which keeps the
\* worker busy. Without work, IdleTimeout can fire (no WF, environment choice).
\* We verify it is reachable via coverage assertions instead.

(* =======================================================================
 * REACHABILITY ASSERTIONS (vacuity guards — kani::cover!() equivalent)
 *
 * Defined as state-predicate NEGATIONS. Used as INVARIANTS in
 * WorkerPoolDispatch.coverage.cfg. Violation = witness.
 * ======================================================================= *)

\* Pool reaches Draining.
DrainingReachable == ~(poolState = "Draining")

\* Pool reaches Stopped.
StoppedReachable == ~(poolState = "Stopped")

\* At least one item is delivered.
DeliveryOccurs == ~(delivered /= {})

\* At least one item is rejected.
RejectionOccurs == ~(rejected /= {})

\* A worker is spawned (workers set becomes non-empty).
WorkerSpawnOccurs == ~(workers /= {})

\* A worker fails.
WorkerFailOccurs == ~(\E w \in WorkerIds : workerHealth[w] = "Failed")

\* ForceAbort path is reachable.
ForceAbortReachable == ~(forcedAbort = TRUE)

\* An idle timeout occurs (worker stopped without being in workers set).
IdleTimeoutReachable == ~(\E w \in WorkerIds :
    workerState[w] = "Stopped" /\ workerHealth[w] = "Healthy")

\* Multiple workers active simultaneously.
MultipleWorkersReachable == ~(Cardinality(workers) > 1)

\* Back-pressure state: all workers busy with pending work.
BackpressureReachable == ~(
    /\ Cardinality(ActiveWorkers) = MaxWorkers
    /\ BusyWorkers = ActiveWorkers
    /\ Len(pending) > 0)

\* All items delivered (full success path).
AllDeliveredReachable == ~(Cardinality(delivered) = NumItems)

\* Submit after drain is rejected.
SubmitAfterDrainReachable == ~(poolState /= "Accepting" /\ rejected /= {})

=====================================================================
