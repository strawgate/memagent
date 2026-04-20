----------------------- MODULE ShutdownProtocol -----------------------
(*
 * Formal model of the split input shutdown protocol used by
 * feat/io-compute-separation (PR #1512).
 *
 * Per input, runtime has two workers:
 *   I/O worker -> io_cpu bounded channel -> CPU worker -> shared pipeline channel
 *
 * Production capacities:
 *   io_cpu channel      = 4
 *   shared pipeline rx  = 16
 *
 * Model-checking capacities are intentionally small (IoChannelCapacity=2,
 * PipelineChannelCapacity=3) to keep the state space TLC-feasible.
 * The protocol is capacity-independent so small values are sufficient.
 *
 * Shutdown cascade:
 *   shutdown signal
 *     -> I/O workers stop + drop io senders
 *     -> per-input: io_cpu channel drains, CPU worker stops independently
 *     -> (all CPU workers stopped) -> pipeline channel drains
 *     -> join workers
 *     -> drain pool
 *     -> stop / force_stop
 *
 * This spec does NOT model:
 *   - Byte-level data or framing (modeled as abstract "items")
 *   - Scanner, transform, or output sink details
 *   - Checkpoint arithmetic (proven by Kani on CheckpointTracker)
 *   - The PipelineMachine lifecycle (modeled in PipelineMachine.tla)
 *   - The exact distinction between explicit permanent reject and held
 *     retry/control-plane failures at the worker/checkpoint seam. The
 *     current runtime may force-stop with unresolved held batches so they
 *     replay on restart.
 *
 * For TLC configuration, see MCShutdownProtocol.tla.
 *)

EXTENDS Naturals, Sequences, TLC

CONSTANTS
    NumInputs,                \* Number of input pairs (>= 1)
    IoChannelCapacity,        \* io_worker -> cpu_worker channel capacity
    PipelineChannelCapacity,  \* shared cpu_worker -> pipeline channel capacity
    MaxItems                  \* Max items any single input can produce

ASSUME NumInputs >= 1 /\ NumInputs <= 4
ASSUME IoChannelCapacity >= 1 /\ IoChannelCapacity <= 4
ASSUME PipelineChannelCapacity >= 1 /\ PipelineChannelCapacity <= 16
ASSUME MaxItems >= 1 /\ MaxItems <= 4

(* -----------------------------------------------------------------------
 * State variables
 * ----------------------------------------------------------------------- *)

VARIABLES
    (* Per-input I/O worker state *)
    input_produced,          \* [1..NumInputs -> Nat]
    io_alive,                \* [1..NumInputs -> BOOLEAN]

    (* Per-input io -> cpu channels *)
    io_channels,             \* [1..NumInputs -> Seq(Inputs)] (bounded)

    (* Per-input CPU worker state *)
    cpu_forwarded,           \* [1..NumInputs -> Nat]
    cpu_alive,               \* [1..NumInputs -> BOOLEAN]

    (* Shared pipeline channel consumed by run_async *)
    pipeline_channel,        \* Seq(Inputs), bounded

    (* Pipeline + output accounting *)
    consumed,                \* Nat: items received from pipeline_channel
    pool_pending,            \* Nat: items submitted to output pool
    pool_acked,              \* Nat: items acked by pool

    (* Shutdown coordination stages *)
    shutdown_signaled,       \* cancellation token fired
    io_channels_drained,     \* derived: all io workers exited and io channels empty
    cpu_workers_stopped,     \* all cpu workers exited
    pipeline_channel_drained,\* shared pipeline channel empty after cpu stop
    workers_joined,          \* manager.join() completed
    pool_drained,            \* pool drain completed
    machine_stopped,         \* PipelineMachine transitioned to Stopped
    forced,                  \* force_stop path was used
    output_health            \* "Healthy" | "Stopping" | "Stopped" | "Failed"

vars == <<input_produced, io_alive, io_channels, cpu_forwarded, cpu_alive,
          pipeline_channel, consumed, pool_pending, pool_acked,
          shutdown_signaled, io_channels_drained, cpu_workers_stopped,
          pipeline_channel_drained, workers_joined, pool_drained,
          machine_stopped, forced, output_health>>

Inputs == 1..NumInputs

(* -----------------------------------------------------------------------
 * Helper: sum a function over Inputs
 * ----------------------------------------------------------------------- *)

RECURSIVE SumInputs(_, _)
SumInputs(f, S) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE x \in S : TRUE
         IN  f[x] + SumInputs(f, S \ {x})

SumAll(f) == SumInputs(f, Inputs)

(* -----------------------------------------------------------------------
 * Type invariant
 * ----------------------------------------------------------------------- *)

TypeOK ==
    /\ \A i \in Inputs :
        /\ input_produced[i] \in 0..MaxItems
        /\ io_alive[i] \in BOOLEAN
        /\ cpu_forwarded[i] \in Nat
        /\ cpu_alive[i] \in BOOLEAN
        /\ io_channels[i] \in Seq(Inputs)
        /\ Len(io_channels[i]) \in 0..IoChannelCapacity
    /\ pipeline_channel \in Seq(Inputs)
    /\ Len(pipeline_channel) \in 0..PipelineChannelCapacity
    /\ consumed \in Nat
    /\ pool_pending \in Nat
    /\ pool_acked \in Nat
    /\ shutdown_signaled \in BOOLEAN
    /\ io_channels_drained \in BOOLEAN
    /\ cpu_workers_stopped \in BOOLEAN
    /\ pipeline_channel_drained \in BOOLEAN
    /\ workers_joined \in BOOLEAN
    /\ pool_drained \in BOOLEAN
    /\ machine_stopped \in BOOLEAN
    /\ forced \in BOOLEAN
    /\ output_health \in {"Healthy", "Stopping", "Stopped", "Failed"}

(* -----------------------------------------------------------------------
 * Initial state
 * ----------------------------------------------------------------------- *)

Init ==
    /\ input_produced = [i \in Inputs |-> 0]
    /\ io_alive = [i \in Inputs |-> TRUE]
    /\ io_channels = [i \in Inputs |-> <<>>]
    /\ cpu_forwarded = [i \in Inputs |-> 0]
    /\ cpu_alive = [i \in Inputs |-> TRUE]
    /\ pipeline_channel = <<>>
    /\ consumed = 0
    /\ pool_pending = 0
    /\ pool_acked = 0
    /\ shutdown_signaled = FALSE
    /\ io_channels_drained = FALSE
    /\ cpu_workers_stopped = FALSE
    /\ pipeline_channel_drained = FALSE
    /\ workers_joined = FALSE
    /\ pool_drained = FALSE
    /\ machine_stopped = FALSE
    /\ forced = FALSE
    /\ output_health = "Healthy"

(* -----------------------------------------------------------------------
 * Actions: Normal operation
 * ----------------------------------------------------------------------- *)

\* I/O worker polls input and sends one chunk to its io_cpu channel.
IoProduce(i) ==
    /\ io_alive[i]
    /\ input_produced[i] < MaxItems
    /\ Len(io_channels[i]) < IoChannelCapacity
    /\ input_produced' = [input_produced EXCEPT ![i] = @ + 1]
    /\ io_channels' = [io_channels EXCEPT ![i] = Append(@, i)]
    /\ UNCHANGED <<io_alive, cpu_forwarded, cpu_alive, pipeline_channel,
                   consumed, pool_pending, pool_acked, shutdown_signaled,
                   io_channels_drained, cpu_workers_stopped,
                   pipeline_channel_drained, workers_joined, pool_drained,
                   machine_stopped, forced, output_health>>

\* CPU worker drains one chunk from its io_cpu channel and forwards to pipeline channel.
CpuForward(i) ==
    /\ cpu_alive[i]
    /\ Len(io_channels[i]) > 0
    /\ Len(pipeline_channel) < PipelineChannelCapacity
    /\ io_channels' = [io_channels EXCEPT ![i] = Tail(@)]
    /\ cpu_forwarded' = [cpu_forwarded EXCEPT ![i] = @ + 1]
    /\ pipeline_channel' = Append(pipeline_channel, i)
    /\ UNCHANGED <<input_produced, io_alive, cpu_alive, consumed,
                   pool_pending, pool_acked, shutdown_signaled,
                   io_channels_drained, cpu_workers_stopped,
                   pipeline_channel_drained, workers_joined, pool_drained,
                   machine_stopped, forced, output_health>>

\* Pipeline main loop receives one batch from the shared channel and submits to pool.
ConsumePipeline ==
    /\ Len(pipeline_channel) > 0
    /\ pipeline_channel' = Tail(pipeline_channel)
    /\ consumed' = consumed + 1
    /\ pool_pending' = pool_pending + 1
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pool_acked, shutdown_signaled,
                   io_channels_drained, cpu_workers_stopped,
                   pipeline_channel_drained, workers_joined, pool_drained,
                   machine_stopped, forced, output_health>>

\* Output pool acks one submitted batch.
PoolAck ==
    /\ pool_pending > pool_acked
    /\ pool_acked' = pool_acked + 1
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained, machine_stopped, forced, output_health>>

\* Worker panic during output processing marks terminal output failure.
WorkerPanic ==
    /\ ~machine_stopped
    /\ pool_pending > pool_acked
    /\ output_health # "Failed"
    /\ output_health' = "Failed"
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained, machine_stopped, forced>>

(* -----------------------------------------------------------------------
 * Actions: Shutdown sequence
 * ----------------------------------------------------------------------- *)

\* 1) Cancellation token fires.
SignalShutdown ==
    /\ ~shutdown_signaled
    /\ shutdown_signaled' = TRUE
    /\ output_health' =
        IF output_health = "Healthy" THEN "Stopping" ELSE output_health
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, io_channels_drained, cpu_workers_stopped,
                   pipeline_channel_drained, workers_joined, pool_drained,
                   machine_stopped, forced>>

\* 2) I/O worker exits (after any final local drain, abstracted here).
IoWorkerStop(i) ==
    /\ shutdown_signaled
    /\ io_alive[i]
    /\ io_alive' = [io_alive EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<input_produced, io_channels, cpu_forwarded, cpu_alive,
                   pipeline_channel, consumed, pool_pending, pool_acked,
                   shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained, machine_stopped, forced, output_health>>

\* 3) Observe that all io workers are down and all io_cpu channels are empty.
\*    This is a derived observation -- not a precondition for CpuWorkerStop.
\*    Each CPU worker independently decides based on per-input state (step 4).
\*    Can fire before or after individual CpuWorkerStop(i) actions.
MarkIoChannelsDrained ==
    /\ shutdown_signaled
    /\ ~io_channels_drained
    /\ \A i \in Inputs : ~io_alive[i]
    /\ \A i \in Inputs : Len(io_channels[i]) = 0
    /\ io_channels_drained' = TRUE
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, cpu_workers_stopped,
                   pipeline_channel_drained, workers_joined, pool_drained,
                   machine_stopped, forced, output_health>>

\* 4) CPU worker exits once its own I/O worker is dead and its io channel is empty.
\*    This is per-input: each CPU worker independently observes that its own
\*    io_rx returns None (io_alive[i]=FALSE and io_channels[i] empty).
\*    No global barrier required -- matches the implementation.
CpuWorkerStop(i) ==
    /\ cpu_alive[i]
    /\ ~io_alive[i]
    /\ Len(io_channels[i]) = 0
    /\ cpu_alive' = [cpu_alive EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   pipeline_channel, consumed, pool_pending, pool_acked,
                   shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained, machine_stopped, forced, output_health>>

\* 5) Record that all CPU workers have stopped.
\*    Gates MarkPipelineChannelDrained -- no more pipeline sends are possible.
\*    Also derives io_channels_drained as a consistency observation: if all
\*    CPU workers stopped via per-input CpuWorkerStop, then each io channel
\*    was empty when its CPU worker exited and no io worker is alive to
\*    refill it, so all io channels are drained.
MarkCpuWorkersStopped ==
    /\ ~cpu_workers_stopped
    /\ \A i \in Inputs : ~cpu_alive[i]
    /\ cpu_workers_stopped' = TRUE
    /\ io_channels_drained' = TRUE
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled,
                   pipeline_channel_drained, workers_joined, pool_drained,
                   machine_stopped, forced, output_health>>

\* 6) Shared pipeline channel is drained after CPU workers have exited.
MarkPipelineChannelDrained ==
    /\ cpu_workers_stopped
    /\ ~pipeline_channel_drained
    /\ Len(pipeline_channel) = 0
    /\ pipeline_channel_drained' = TRUE
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, workers_joined, pool_drained,
                   machine_stopped, forced, output_health>>

\* 7) Join all input workers (CPU first, then I/O in implementation).
JoinWorkers ==
    /\ pipeline_channel_drained
    /\ ~workers_joined
    /\ \A i \in Inputs : ~io_alive[i]
    /\ \A i \in Inputs : ~cpu_alive[i]
    /\ workers_joined' = TRUE
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   pool_drained, machine_stopped, forced, output_health>>

\* 8) Drain pool after no more channel traffic is possible.
DrainPool ==
    /\ workers_joined
    /\ ~pool_drained
    /\ pool_acked = pool_pending
    /\ pool_drained' = TRUE
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, machine_stopped, forced, output_health>>

\* 9a) Normal stop path.
NormalStop ==
    /\ pool_drained
    /\ ~machine_stopped
    /\ machine_stopped' = TRUE
    /\ output_health' = IF output_health = "Failed" THEN "Failed" ELSE "Stopped"
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained, forced>>

\* 9b) Timeout escape hatch.
\*    Guards on actual outstanding backlog (pool_acked < pool_pending) rather
\*    than the latched ~pool_drained flag, so ForceStop only fires when there
\*    is genuinely unacked work pending.
ForceStop ==
    /\ workers_joined
    /\ pool_acked < pool_pending
    /\ ~machine_stopped
    /\ machine_stopped' = TRUE
    /\ forced' = TRUE
    /\ output_health' = "Failed"
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained>>

(* -----------------------------------------------------------------------
 * Next-state relation
 * ----------------------------------------------------------------------- *)

Next ==
    \/ \E i \in Inputs : IoProduce(i)
    \/ \E i \in Inputs : CpuForward(i)
    \/ ConsumePipeline
    \/ PoolAck
    \/ WorkerPanic
    \/ SignalShutdown
    \/ \E i \in Inputs : IoWorkerStop(i)
    \/ MarkIoChannelsDrained
    \/ \E i \in Inputs : CpuWorkerStop(i)
    \/ MarkCpuWorkersStopped
    \/ MarkPipelineChannelDrained
    \/ JoinWorkers
    \/ DrainPool
    \/ NormalStop
    \/ ForceStop
    \/ (machine_stopped /\ UNCHANGED vars)

(* -----------------------------------------------------------------------
 * Fairness
 * ----------------------------------------------------------------------- *)

Fairness ==
    /\ WF_vars(SignalShutdown)
    /\ \A i \in Inputs : WF_vars(IoWorkerStop(i))
    /\ \A i \in Inputs : WF_vars(CpuForward(i))
    /\ WF_vars(ConsumePipeline)
    /\ WF_vars(MarkIoChannelsDrained)
    /\ \A i \in Inputs : WF_vars(CpuWorkerStop(i))
    /\ WF_vars(MarkCpuWorkersStopped)
    /\ WF_vars(MarkPipelineChannelDrained)
    /\ WF_vars(JoinWorkers)
    /\ WF_vars(PoolAck)
    /\ WF_vars(DrainPool)
    /\ WF_vars(NormalStop)
    \* No WF on ForceStop -- timeout escape, not required progress.
    \* No WF on IoProduce -- environment is not obligated to keep producing.

Spec == Init /\ [][Next]_vars /\ Fairness

(* -----------------------------------------------------------------------
 * Safety properties
 * ----------------------------------------------------------------------- *)

NoCpuStopBeforeIoDrain ==
    cpu_workers_stopped => io_channels_drained

NoJoinBeforePipelineDrain ==
    workers_joined => pipeline_channel_drained

NoStopBeforeJoin ==
    machine_stopped => workers_joined

MachineStoppedImpliesOutputTerminal ==
    machine_stopped => output_health \in {"Stopped", "Failed"}

NormalStopImpliesPoolDrained ==
    (machine_stopped /\ ~forced) => pool_drained

ForcedStopImpliesOutputFailed ==
    (machine_stopped /\ forced) => output_health = "Failed"

\* Shutdown stage flags must reflect underlying worker/channel state.
DrainFlagsConsistent ==
    /\ workers_joined =>
        (cpu_workers_stopped /\ io_channels_drained /\ pipeline_channel_drained)
    /\ pool_drained =>
        (workers_joined /\ pool_acked = pool_pending)

\* Conservation: items produced by each I/O worker = items in its io channel
\* + items its CPU worker has forwarded.  Non-tautological: catches duplication
\* or loss in the io -> cpu path.
IoConservation ==
    \A i \in Inputs :
        input_produced[i] = Len(io_channels[i]) + cpu_forwarded[i]

\* Conservation: total items forwarded by all CPU workers = items currently
\* in the pipeline channel + items consumed from it.  Non-tautological: catches
\* duplication or loss in the cpu -> pipeline path.
PipelineConservation ==
    SumAll(cpu_forwarded) = Len(pipeline_channel) + consumed

(* -----------------------------------------------------------------------
 * Liveness properties
 * ----------------------------------------------------------------------- *)

ShutdownCompletes ==
    shutdown_signaled ~> machine_stopped

\* Required by issue #1529:
\* CPU workers must not deadlock waiting on pipeline backpressure forever.
NoCpuWorkerDeadlock ==
    io_channels_drained ~> cpu_workers_stopped

\* Required by issue #1529:
\* After I/O workers stop, io_cpu channels eventually drain.
IoCpuChannelEventuallyDrained ==
    (\A i \in Inputs : ~io_alive[i]) ~> io_channels_drained

\* Required by issue #1529:
\* Once I/O workers stop, CPU workers eventually stop.
CpuWorkersEventuallyStop ==
    (\A i \in Inputs : ~io_alive[i]) ~> (\A i \in Inputs : ~cpu_alive[i])

EventualStop ==
    <>[](machine_stopped)

OutputFailureSticky ==
    [](output_health = "Failed" => [](output_health = "Failed"))

(* -----------------------------------------------------------------------
 * Reachability / vacuity guards
 * ----------------------------------------------------------------------- *)

ShutdownReachable == ~shutdown_signaled
IoChannelsDrainedReachable == ~io_channels_drained
CpuWorkersStoppedReachable == ~cpu_workers_stopped
PipelineChannelDrainedReachable == ~pipeline_channel_drained
WorkersJoinedReachable == ~workers_joined
PoolDrainedReachable == ~pool_drained
NormalStopReachable == ~(machine_stopped /\ ~forced)
ForceStopReachable == ~(machine_stopped /\ forced)
OutputFailedReachable == ~(output_health = "Failed")

\* Backpressure reachability: witness that bounded channels can become full.
\* Important for shutdown deadlock analysis -- TLC must explore these states.
IoChannelFullReachable == ~(\E i \in Inputs : Len(io_channels[i]) = IoChannelCapacity)
PipelineChannelFullReachable == ~(Len(pipeline_channel) = PipelineChannelCapacity)

======================================================================
