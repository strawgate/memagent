----------------------- MODULE ShutdownProtocol -----------------------
(*
 * Formal model of the pipeline shutdown coordination protocol from
 * crates/logfwd/src/pipeline.rs run_async().
 *
 * Models N input threads feeding a bounded channel consumed by a single
 * async consumer. Shutdown must:
 *   (1) Drain the channel before joining input threads (prevents deadlock)
 *   (2) Join input threads before final flush (no more data after join)
 *   (3) Drain the output pool before machine transition
 *   (4) All of the above must eventually complete (liveness)
 *
 * The protocol follows the ordering in pipeline.rs:412-518:
 *   shutdown signal → drain channel → join inputs → flush remaining →
 *   drain pool → drain acks → begin_drain → stop/force_stop
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

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    NumInputs,          \* Number of input threads (>= 1)
    ChannelCapacity,    \* Bounded channel size (>= 1)
    MaxItems            \* Max items any single input can produce

ASSUME NumInputs >= 1 /\ NumInputs <= 4
ASSUME ChannelCapacity >= 1 /\ ChannelCapacity <= 8
ASSUME MaxItems >= 1 /\ MaxItems <= 4

(* -----------------------------------------------------------------------
 * State variables
 * ----------------------------------------------------------------------- *)

VARIABLES
    (* Input thread state *)
    input_produced,     \* [1..NumInputs -> Nat] items produced per input
    input_alive,        \* [1..NumInputs -> BOOLEAN] thread still running

    (* Bounded channel *)
    channel,            \* Seq of items (FIFO, bounded by ChannelCapacity)

    (* Consumer state *)
    consumed,           \* Nat: total items consumed from channel
    consumer_buf,       \* Nat: items in scan_buf not yet flushed
    flushed,            \* Nat: total items flushed to output

    (* Output pool *)
    pool_pending,       \* Nat: items submitted to pool, not yet acked
    pool_acked,         \* Nat: items acked by pool

    (* Shutdown coordination *)
    shutdown_signaled,  \* BOOLEAN: cancellation token fired
    channel_drained,    \* BOOLEAN: channel fully drained post-shutdown
    inputs_joined,      \* BOOLEAN: all input threads joined
    pool_drained,       \* BOOLEAN: output pool drained
    final_flushed,      \* BOOLEAN: remaining scan_buf flushed
    machine_stopped,    \* BOOLEAN: PipelineMachine transitioned to Stopped
    forced              \* BOOLEAN: force_stop was used (timeout expired)

vars == <<input_produced, input_alive, channel, consumed, consumer_buf,
          flushed, pool_pending, pool_acked, shutdown_signaled,
          channel_drained, inputs_joined, pool_drained, final_flushed,
          machine_stopped, forced>>

Inputs == 1..NumInputs

(* -----------------------------------------------------------------------
 * Type invariant
 * ----------------------------------------------------------------------- *)

TypeOK ==
    /\ \A i \in Inputs :
        /\ input_produced[i] \in 0..MaxItems
        /\ input_alive[i] \in BOOLEAN
    /\ Len(channel) \in 0..ChannelCapacity
    /\ consumed \in Nat
    /\ consumer_buf \in Nat
    /\ flushed \in Nat
    /\ pool_pending \in Nat
    /\ pool_acked \in Nat
    /\ shutdown_signaled \in BOOLEAN
    /\ channel_drained \in BOOLEAN
    /\ inputs_joined \in BOOLEAN
    /\ pool_drained \in BOOLEAN
    /\ final_flushed \in BOOLEAN
    /\ machine_stopped \in BOOLEAN
    /\ forced \in BOOLEAN

(* -----------------------------------------------------------------------
 * Initial state
 * ----------------------------------------------------------------------- *)

Init ==
    /\ input_produced = [i \in Inputs |-> 0]
    /\ input_alive = [i \in Inputs |-> TRUE]
    /\ channel = <<>>
    /\ consumed = 0
    /\ consumer_buf = 0
    /\ flushed = 0
    /\ pool_pending = 0
    /\ pool_acked = 0
    /\ shutdown_signaled = FALSE
    /\ channel_drained = FALSE
    /\ inputs_joined = FALSE
    /\ pool_drained = FALSE
    /\ final_flushed = FALSE
    /\ machine_stopped = FALSE
    /\ forced = FALSE

(* -----------------------------------------------------------------------
 * Actions: Normal operation (before shutdown)
 * ----------------------------------------------------------------------- *)

\* Input thread produces an item and sends to channel (if not full).
\* Models input_poll_loop: poll() -> buf.extend -> blocking_send.
InputProduce(i) ==
    /\ input_alive[i]
    /\ input_produced[i] < MaxItems
    /\ Len(channel) < ChannelCapacity     \* not full (would block)
    /\ input_produced' = [input_produced EXCEPT ![i] = @ + 1]
    /\ channel' = Append(channel, i)      \* item identity = producing input
    /\ UNCHANGED <<input_alive, consumed, consumer_buf, flushed,
                   pool_pending, pool_acked, shutdown_signaled,
                   channel_drained, inputs_joined, pool_drained,
                   final_flushed, machine_stopped, forced>>

\* Consumer receives from channel and buffers.
\* Models: msg = rx.recv() -> scan_buf.extend.
ConsumerReceive ==
    /\ ~shutdown_signaled
    /\ Len(channel) > 0
    /\ consumed' = consumed + 1
    /\ consumer_buf' = consumer_buf + 1
    /\ channel' = Tail(channel)
    /\ UNCHANGED <<input_produced, input_alive, flushed, pool_pending,
                   pool_acked, shutdown_signaled, channel_drained,
                   inputs_joined, pool_drained, final_flushed,
                   machine_stopped, forced>>

\* Consumer flushes buffer to output pool.
\* Models: flush_batch (size threshold or timeout).
\* Allowed both in normal operation AND during the drain phase
\* (after shutdown but before inputs are joined), matching the real
\* code's channel-drain loop (pipeline.rs:449-471).
ConsumerFlush ==
    /\ (~shutdown_signaled \/ (~channel_drained /\ ~inputs_joined))
    /\ consumer_buf > 0
    /\ flushed' = flushed + consumer_buf
    /\ pool_pending' = pool_pending + 1
    /\ consumer_buf' = 0
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   pool_acked, shutdown_signaled, channel_drained,
                   inputs_joined, pool_drained, final_flushed,
                   machine_stopped, forced>>

\* Output pool worker acks a completed batch.
\* Models: pool.ack_rx.recv() -> apply_pool_ack.
PoolAck ==
    /\ pool_pending > pool_acked
    /\ pool_acked' = pool_acked + 1
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   consumer_buf, flushed, pool_pending, shutdown_signaled,
                   channel_drained, inputs_joined, pool_drained,
                   final_flushed, machine_stopped, forced>>

(* -----------------------------------------------------------------------
 * Actions: Shutdown sequence (ordered phases)
 * ----------------------------------------------------------------------- *)

\* 1. Shutdown signal fires (cancellation token).
SignalShutdown ==
    /\ ~shutdown_signaled
    /\ shutdown_signaled' = TRUE
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   consumer_buf, flushed, pool_pending, pool_acked,
                   channel_drained, inputs_joined, pool_drained,
                   final_flushed, machine_stopped, forced>>

\* 2. Input threads notice shutdown and stop producing.
\* In the real code, inputs drain their local buffer before exiting.
\* Modeled as: input can still produce via InputProduce (now enabled
\* during shutdown), then exits via InputShutdown.
InputShutdown(i) ==
    /\ shutdown_signaled
    /\ input_alive[i]
    /\ input_alive' = [input_alive EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<input_produced, channel, consumed, consumer_buf, flushed,
                   pool_pending, pool_acked, shutdown_signaled,
                   channel_drained, inputs_joined, pool_drained,
                   final_flushed, machine_stopped, forced>>

\* 3. Drain channel: consumer reads remaining messages after shutdown.
\* CRITICAL: must happen BEFORE joining input threads to prevent deadlock.
\* If channel is full and an input is blocked in blocking_send, joining
\* the input thread would deadlock because the send can't complete.
DrainChannel ==
    /\ shutdown_signaled
    /\ ~channel_drained
    /\ Len(channel) > 0
    /\ consumed' = consumed + 1
    /\ consumer_buf' = consumer_buf + 1
    /\ channel' = Tail(channel)
    /\ UNCHANGED <<input_produced, input_alive, flushed, pool_pending,
                   pool_acked, shutdown_signaled, channel_drained,
                   inputs_joined, pool_drained, final_flushed,
                   machine_stopped, forced>>

\* Channel is fully drained when empty AND all inputs have exited.
\* (Inputs exiting drops their Sender clones; when all are gone,
\* rx.recv() returns None.)
MarkChannelDrained ==
    /\ shutdown_signaled
    /\ ~channel_drained
    /\ Len(channel) = 0
    /\ \A i \in Inputs : ~input_alive[i]
    /\ channel_drained' = TRUE
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   consumer_buf, flushed, pool_pending, pool_acked,
                   shutdown_signaled, inputs_joined, pool_drained,
                   final_flushed, machine_stopped, forced>>

\* 4. Join input threads.
\* Guard: channel must be drained first (deadlock prevention).
JoinInputs ==
    /\ channel_drained
    /\ ~inputs_joined
    /\ \A i \in Inputs : ~input_alive[i]
    /\ inputs_joined' = TRUE
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   consumer_buf, flushed, pool_pending, pool_acked,
                   shutdown_signaled, channel_drained, pool_drained,
                   final_flushed, machine_stopped, forced>>

\* 5. Final flush of remaining scan_buf.
\* Guard: inputs must be joined first (no more data can arrive).
FinalFlush ==
    /\ inputs_joined
    /\ ~final_flushed
    /\ consumer_buf > 0
    /\ flushed' = flushed + consumer_buf
    /\ pool_pending' = pool_pending + 1
    /\ consumer_buf' = 0
    /\ final_flushed' = TRUE
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   pool_acked, shutdown_signaled, channel_drained,
                   inputs_joined, pool_drained, machine_stopped, forced>>

\* Final flush with empty buffer (nothing to flush).
FinalFlushEmpty ==
    /\ inputs_joined
    /\ ~final_flushed
    /\ consumer_buf = 0
    /\ final_flushed' = TRUE
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   consumer_buf, flushed, pool_pending, pool_acked,
                   shutdown_signaled, channel_drained, inputs_joined,
                   pool_drained, machine_stopped, forced>>

\* 6. Drain output pool.
\* Guard: final flush must be done (no more work to submit).
DrainPool ==
    /\ final_flushed
    /\ ~pool_drained
    /\ pool_acked = pool_pending   \* all submitted work is acked
    /\ pool_drained' = TRUE
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   consumer_buf, flushed, pool_pending, pool_acked,
                   shutdown_signaled, channel_drained, inputs_joined,
                   final_flushed, machine_stopped, forced>>

\* 7. Machine transition: begin_drain → stop (normal).
NormalStop ==
    /\ pool_drained
    /\ ~machine_stopped
    /\ pool_acked = pool_pending   \* all batches resolved
    /\ machine_stopped' = TRUE
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   consumer_buf, flushed, pool_pending, pool_acked,
                   shutdown_signaled, channel_drained, inputs_joined,
                   pool_drained, final_flushed, forced>>

\* 7b. Machine transition: force_stop (timeout expired).
ForceStop ==
    /\ final_flushed
    /\ ~machine_stopped
    /\ ~pool_drained             \* pool didn't drain in time
    /\ machine_stopped' = TRUE
    /\ forced' = TRUE
    /\ UNCHANGED <<input_produced, input_alive, channel, consumed,
                   consumer_buf, flushed, pool_pending, pool_acked,
                   shutdown_signaled, channel_drained, inputs_joined,
                   pool_drained, final_flushed>>

(* -----------------------------------------------------------------------
 * Next-state relation
 * ----------------------------------------------------------------------- *)

Next ==
    \* Normal operation
    \/ \E i \in Inputs : InputProduce(i)
    \/ ConsumerReceive
    \/ ConsumerFlush
    \/ PoolAck
    \* Shutdown sequence
    \/ SignalShutdown
    \/ \E i \in Inputs : InputShutdown(i)
    \/ DrainChannel
    \/ MarkChannelDrained
    \/ JoinInputs
    \/ FinalFlush
    \/ FinalFlushEmpty
    \/ DrainPool
    \/ NormalStop
    \/ ForceStop
    \* Terminal: machine_stopped is final.
    \/ (machine_stopped /\ UNCHANGED vars)

(* -----------------------------------------------------------------------
 * Fairness
 * ----------------------------------------------------------------------- *)

Fairness ==
    /\ WF_vars(SignalShutdown)
    /\ \A i \in Inputs : WF_vars(InputShutdown(i))
    /\ WF_vars(DrainChannel)
    /\ WF_vars(MarkChannelDrained)
    /\ WF_vars(JoinInputs)
    /\ WF_vars(FinalFlush)
    /\ WF_vars(FinalFlushEmpty)
    /\ WF_vars(DrainPool)
    /\ WF_vars(NormalStop)
    /\ WF_vars(PoolAck)
    \* No WF on ForceStop — it fires only as a timeout escape hatch,
    \* not as a guaranteed step.
    \* No WF on InputProduce, ConsumerReceive, ConsumerFlush —
    \* the environment is not obligated to produce or consume.

Spec == Init /\ [][Next]_vars /\ Fairness

(* -----------------------------------------------------------------------
 * Safety properties
 * ----------------------------------------------------------------------- *)

\* The shutdown ordering is maintained.
\* Inputs are joined only after channel is drained.
NoDeadlock ==
    inputs_joined => channel_drained

\* Final flush happens only after inputs are joined.
FlushAfterJoin ==
    final_flushed => inputs_joined

\* Machine stops only after final flush.
StopAfterFlush ==
    machine_stopped => final_flushed

\* Normal stop implies pool fully drained.
\* DrainCompleteness for the shutdown protocol.
NormalStopImpliesPoolDrained ==
    (machine_stopped /\ ~forced) => pool_drained

\* Helper: sum of a function over its domain
RECURSIVE SumProdHelper(_, _, _)
SumProdHelper(f, remaining, acc) ==
    IF remaining = {} THEN acc
    ELSE LET x == CHOOSE x \in remaining : TRUE
         IN SumProdHelper(f, remaining \ {x}, acc + f[x])
SumProd(f) == SumProdHelper(f, DOMAIN f, 0)

(* -----------------------------------------------------------------------
 * Liveness properties
 * ----------------------------------------------------------------------- *)

\* Shutdown eventually completes.
ShutdownCompletes ==
    shutdown_signaled ~> machine_stopped

\* Once shutdown is signaled, all inputs eventually exit.
InputsEventuallyStop ==
    \A i \in Inputs :
        shutdown_signaled ~> ~input_alive[i]

\* Channel is eventually drained after shutdown.
ChannelEventuallyDrained ==
    shutdown_signaled ~> channel_drained

\* Machine eventually stops (by normal or force path).
EventualStop ==
    <>[](machine_stopped)

(* -----------------------------------------------------------------------
 * Reachability / vacuity guards
 * ----------------------------------------------------------------------- *)

ShutdownReachable == ~shutdown_signaled  \* violation = shutdown fires
NormalStopReachable == ~(machine_stopped /\ ~forced)
ForceStopReachable == ~(machine_stopped /\ forced)
ChannelFullReachable == ~(Len(channel) = ChannelCapacity)

======================================================================
