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
 *   shared pipeline rx  = 3 (reduced from 16 to keep TLC state space tractable)
 *
 * Shutdown cascade:
 *   shutdown signal
 *     -> I/O workers stop + drop io senders
 *     -> io_cpu channels drain
 *     -> CPU workers stop + drop pipeline senders
 *     -> pipeline channel drains
 *     -> join workers
 *     -> drain pool
 *     -> stop / force_stop
 *)

EXTENDS Naturals, Sequences, TLC

CONSTANTS
    NumInputs,                \* Number of input pairs (>= 1)
    IoChannelCapacity,        \* io_worker -> cpu_worker channel capacity
    PipelineChannelCapacity,  \* shared cpu_worker -> pipeline channel capacity
    MaxItems                  \* Max items any single input can produce

ASSUME NumInputs >= 1 /\ NumInputs <= 4
ASSUME IoChannelCapacity >= 1 /\ IoChannelCapacity <= 4
ASSUME PipelineChannelCapacity >= 1 /\ PipelineChannelCapacity <= 4
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
    io_channels_drained,     \* all io workers exited and io channels empty
    cpu_workers_stopped,     \* all cpu workers exited
    pipeline_channel_drained,\* shared pipeline channel empty after cpu stop
    workers_joined,          \* manager.join() completed
    pool_drained,            \* pool drain completed
    machine_stopped,         \* PipelineMachine transitioned to Stopped
    forced                   \* force_stop path was used

vars == <<input_produced, io_alive, io_channels, cpu_forwarded, cpu_alive,
          pipeline_channel, consumed, pool_pending, pool_acked,
          shutdown_signaled, io_channels_drained, cpu_workers_stopped,
          pipeline_channel_drained, workers_joined, pool_drained,
          machine_stopped, forced>>

Inputs == 1..NumInputs

(* -----------------------------------------------------------------------
 * Type invariant
 * ----------------------------------------------------------------------- *)

TypeOK ==
    /\ \A i \in Inputs :
        /\ input_produced[i] \in 0..MaxItems
        /\ io_alive[i] \in BOOLEAN
        /\ cpu_forwarded[i] \in Nat
        /\ cpu_alive[i] \in BOOLEAN
        /\ Len(io_channels[i]) \in 0..IoChannelCapacity
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
                   machine_stopped, forced>>

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
                   machine_stopped, forced>>

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
                   machine_stopped, forced>>

\* Output pool acks one submitted batch.
PoolAck ==
    /\ pool_pending > pool_acked
    /\ pool_acked' = pool_acked + 1
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained, machine_stopped, forced>>

(* -----------------------------------------------------------------------
 * Actions: Shutdown sequence
 * ----------------------------------------------------------------------- *)

\* 1) Cancellation token fires.
SignalShutdown ==
    /\ ~shutdown_signaled
    /\ shutdown_signaled' = TRUE
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
                   workers_joined, pool_drained, machine_stopped, forced>>

\* 3) All io workers are down and all io_cpu channels are empty.
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
                   machine_stopped, forced>>

\* 4) CPU worker i exits once its own io_alive is down and its own io_channel is empty.
\* Each CPU worker exits independently — it does not wait for other workers.
CpuWorkerStop(i) ==
    /\ cpu_alive[i]
    /\ ~io_alive[i]
    /\ Len(io_channels[i]) = 0
    /\ cpu_alive' = [cpu_alive EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   pipeline_channel, consumed, pool_pending, pool_acked,
                   shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained, machine_stopped, forced>>

\* 5) Record that all CPU workers have stopped.
MarkCpuWorkersStopped ==
    /\ io_channels_drained
    /\ ~cpu_workers_stopped
    /\ \A i \in Inputs : ~cpu_alive[i]
    /\ cpu_workers_stopped' = TRUE
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, io_channels_drained,
                   pipeline_channel_drained, workers_joined, pool_drained,
                   machine_stopped, forced>>

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
                   machine_stopped, forced>>

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
                   pool_drained, machine_stopped, forced>>

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
                   workers_joined, machine_stopped, forced>>

\* 9a) Normal stop path.
NormalStop ==
    /\ pool_drained
    /\ ~machine_stopped
    /\ machine_stopped' = TRUE
    /\ UNCHANGED <<input_produced, io_alive, io_channels, cpu_forwarded,
                   cpu_alive, pipeline_channel, consumed, pool_pending,
                   pool_acked, shutdown_signaled, io_channels_drained,
                   cpu_workers_stopped, pipeline_channel_drained,
                   workers_joined, pool_drained, forced>>

\* 9b) Timeout escape hatch.
ForceStop ==
    /\ workers_joined
    /\ ~pool_drained
    /\ ~machine_stopped
    /\ machine_stopped' = TRUE
    /\ forced' = TRUE
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
    \* No WF on ForceStop — timeout escape, not required progress.
    \* No WF on IoProduce — environment is not obligated to keep producing.

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

NormalStopImpliesPoolDrained ==
    (machine_stopped /\ ~forced) => pool_drained

\* Helper: sum a function f over a set S.
RECURSIVE SumFn(_, _)
SumFn(S, f) ==
    IF S = {} THEN 0
    ELSE LET i == CHOOSE x \in S : TRUE
         IN  f[i] + SumFn(S \ {i}, f)

\* Conservation: items are never created or destroyed, only moved between buffers.
\*   per-input: produced by I/O = still in io_channel + forwarded to pipeline by CPU
\*   global:    sum of cpu_forwarded = items in pipeline_channel + items consumed
\*              consumed = pool_pending (each consumed item is submitted to pool)
\*              pool_acked <= pool_pending (acked <= submitted)
\* This invariant is independent of action preconditions and catches model errors.
MassConservation ==
    /\ \A i \in Inputs :
        input_produced[i] = Len(io_channels[i]) + cpu_forwarded[i]
    /\ SumFn(Inputs, cpu_forwarded) = Len(pipeline_channel) + consumed
    /\ consumed = pool_pending
    /\ pool_acked <= pool_pending

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

(* -----------------------------------------------------------------------
 * Reachability / vacuity guards
 * ----------------------------------------------------------------------- *)

ShutdownReachable == ~shutdown_signaled
IoChannelsDrainedReachable == ~io_channels_drained
CpuWorkersStoppedReachable == ~cpu_workers_stopped
PipelineChannelDrainedReachable == ~pipeline_channel_drained
NormalStopReachable == ~(machine_stopped /\ ~forced)
ForceStopReachable == ~(machine_stopped /\ forced)

======================================================================
