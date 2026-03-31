# Pipeline Verification Research

Research conducted 2026-03-31 for formally verifying logfwd's pipeline
state machine using Kani and TLA+.

## Key Finding: Kani Cannot Verify Async Code

Kani explicitly does not support async/await, tokio::select!, mpsc
channels, or CancellationToken. Concurrent features are "out of scope"
— Kani compiles async code as if sequential.

**The approach**: Extract a pure state machine into logfwd-core (no_std).
The async driver in the logfwd binary translates IO events into state
machine events. Kani proves the state machine. TLA+ proves the
multi-process interaction.

## Architecture: State Machine Drives Async Code

Following the rustls unbuffered API pattern and s2n-quic's Kani
approach: the state machine IS the runtime decision logic, not a
separate verification artifact.

```
logfwd-core (no_std, Kani-verified)
├── pipeline/state.rs    — FlushMachine: pure state machine
│   step(state, event) → (state, action)
│   No IO, no allocation, no panics
│
logfwd (async, NOT Kani-verified)
├── pipeline.rs          — async driver
│   Translates IO events → FlushEvent
│   Calls machine.step()
│   Performs the requested FlushAction
│   Cannot make incorrect decisions — only does what machine says

TLA+ spec (design-level)
└── PipelineBatch.tla    — models N inputs, bounded channel, batching
    Proves: NoDataAbandoned, ShutdownCompletes, InputProgress
```

## What Kani Proves vs What TLA+ Proves

| Property | Tool | Why |
|----------|------|-----|
| step() never panics | Kani | Exhaustive over finite enums |
| Buffer cleared after flush | Kani | State machine invariant |
| Done is terminal | Kani | Exhaustive transition check |
| No double-flush | Kani | State invariant |
| No data abandoned after shutdown | TLA+ | Requires liveness (eventually) |
| Shutdown completes | TLA+ | Requires fairness (WF) |
| No deadlock | TLA+ | Multi-process interaction |
| No input starvation | TLA+ | Fairness property |

## The Real Pipeline is Three Interacting State Machines

Our initial 5-state model was too simple. The actual pipeline has:

### 1. Input Thread State Machine (one per input)
```
POLLING → ACCUMULATING → SENDING → (back to POLLING)
                                  ↘ DRAINING → DONE
```
- POLLING: waiting for source events
- ACCUMULATING: data in buf, tracking buffered_since
- SENDING: blocking_send to channel (blocks on backpressure)
- DRAINING: shutdown received, flushing remaining buf
- DONE: thread exits

### 2. Channel (bounded buffer between threads)
```
EMPTY ↔ PARTIAL ↔ FULL (backpressure)
```
- Capacity: 16 messages
- Full → input thread blocks in blocking_send
- Empty → consumer blocks in rx.recv()

### 3. Consumer/Flush State Machine
```
IDLE → BUFFERING → FLUSHING → (back to IDLE)
                             ↘ DRAINING → DONE
```
- IDLE: scan_buf empty, waiting for data
- BUFFERING: scan_buf has data, below threshold
- FLUSHING: scan + transform + output in progress
- DRAINING: shutdown received, draining channel
- DONE: all flushed, output.flush() called

### 4. Shutdown Protocol (multi-phase)
```
Phase 1: CancellationToken fires → consumer breaks select!
Phase 2: Consumer drains channel (inputs may still send)
Phase 3: Senders drop → rx.recv() returns None
Phase 4: Join input threads (AFTER drain to avoid deadlock)
Phase 5: Final flush of scan_buf
Phase 6: output.flush()
```

## Potential Bug Found: Remainder Lost on Shutdown

On shutdown, `input.buf` is drained (line 518-521) but
`input.remainder` (partial line without trailing newline) is silently
dropped. This is probably acceptable (partial lines are incomplete)
but should be an explicit design decision verified in the TLA+ spec.

## Production Patterns to Research

Need to compare against Vector, Fluent Bit, OTel Collector to ensure
our state machine isn't missing critical states (retry, circuit
breaker, dead letter queue, WAL). Research in progress.

## Sources

- s2n-quic Kani: model-checking.github.io/kani-verifier-blog/2023/05/30/
- rustls unbuffered API: docs.rs/rustls/latest/rustls/unbuffered/
- Datadog TLA+: datadoghq.com/blog/engineering/formal-modeling-and-simulation/
- AWS formal methods: queue.acm.org/detail.cfm?id=3712057
- Kani feature support: model-checking.github.io/kani/rust-feature-support.html

## Shipper State Machine Research (2026-03-31)

Analyzed Vector, Fluent Bit, OTel Collector, Filebeat, and Fluentd.

### Critical Finding: Two-Layer State Model

All five production systems use a **two-layer** model, not a single
pipeline state machine:

**Layer A: Pipeline lifecycle**
```
STARTING → RUNNING → DRAINING → STOPPED
             ↕
           DEGRADED (secondary output active)
           PAUSED (backpressure from downstream)
```

**Layer B: Per-batch lifecycle**
```
STAGED → QUEUED → DISPATCHED → DELIVERED
                    ↕
                  RETRYING (attempt count + backoff timer)
                    ↓
                  REJECTED (permanent error → dead letter / drop)
```

FLUSHING is a per-batch state (DISPATCHED), not a pipeline state.
The pipeline can be RUNNING while batches are independently in
DISPATCHED, RETRYING, or REJECTED states.

### States We Were Missing

| State | Who uses it | Why it matters |
|-------|-------------|---------------|
| RETRYING | All 5 | Retry with backoff, attempt counting |
| DEGRADED | Fluentd, OTel | Secondary output after threshold |
| PAUSED | Fluent Bit | Input paused when memory limit hit |
| STARTING | OTel | Startup order differs from shutdown |
| Per-batch states | All 5 | Batches have independent lifecycles |

### Error Recovery We Were Missing

| Pattern | Who uses it |
|---------|-------------|
| Permanent vs transient errors | Vector, OTel, Fluentd |
| Secondary/fallback output | Fluentd |
| Throttle-aware retry (429) | OTel Collector |
| Partial batch success | OTel Collector |
| Chunk takeback (failed → back to queue) | Fluentd |

### Startup/Shutdown Ordering (OTel pattern)

- **Start**: reverse topological (exporters before receivers)
- **Shutdown**: topological (receivers before exporters)
Ensures downstream is ready before upstream sends, and upstream
stops before downstream drains.

### Backpressure Strategies

| Strategy | Who |
|----------|-----|
| Channel block (default) | Vector, OTel, Filebeat |
| Drop newest | Vector |
| Drop oldest | Fluentd |
| Memory → disk overflow | Vector, Fluent Bit |
| Input pause/resume | Fluent Bit |

### Impact on FlushMachine Design

The FlushMachine should NOT model per-batch lifecycle — that's a
separate `BatchTracker`. The FlushMachine models pipeline lifecycle
only:

```rust
// Pipeline lifecycle (Kani-verifiable)
pub enum PipelineState { Starting, Running, Draining, Stopped }

// Per-batch lifecycle (separate tracker)
pub enum BatchState { Queued, Dispatched, Retrying(u32), Delivered, Rejected }

// Backpressure state (input-level)
pub enum InputState { Active, Paused, Shutdown, Done }
```

This matches how Vector (EventStatus per event), OTel Collector
(dispatched items list), and Fluentd (stage/queue/dequeued pools)
all separate batch tracking from pipeline lifecycle.

## Final Design Decision (2026-03-31)

### Per-Batch Tracking, Not Per-Event

Per-event tracking (Vector's approach) costs ~65ns/event = 6.5% CPU
at 1M/sec. Per-batch costs ~60ns/batch (negligible). Per-event is
only needed for fan-out + partial batch success + event-merging
transforms — none of which logfwd does.

Even with future Kafka/OTLP inputs, per-batch is sufficient:
- Kafka: commit offset = batch granularity
- OTLP: HTTP 200 = batch granularity
- File: advance offset = batch granularity

### Typestate Pattern for Correct-by-Construction

Instead of verifying state transitions after the fact, make illegal
transitions unrepresentable:

```rust
struct Queued<B>(B);
struct Sending<B>(B);
struct Acked;

impl<B> Queued<B> {
    fn begin_send(self) -> Sending<B> { ... }  // consumes self
}
impl<B> Sending<B> {
    fn ack(self) -> Acked { ... }              // consumes self
    fn fail(self) -> Queued<B> { ... }         // back to queue
}
```

Cannot ACK twice (self consumed). Cannot drop without transitioning
(#[must_use]). Cannot send a Queued batch without consuming it.
The Rust compiler proves state transition correctness.

### Ordered Batch Acknowledgement

For a given file, offsets committed in order:
  committed_offset[f] = max(end_offset for ACKED batch
    where all prior batches are also ACKED)

This matches Filebeat's registrar pattern and OTel's persistent queue.

### Retry Opaque to Pipeline

Retry logic lives inside the sink. The pipeline sees only:
- BatchAction::Send(batch_id)
- SinkResult::Success
- SinkResult::TransientFailure { retry_after }
- SinkResult::PermanentFailure

The sink handles backoff internally (OTel retrySender pattern).
This keeps the pipeline state machine small enough for Kani/TLA+.

### What to Verify Where

| Property | Tool | How |
|----------|------|-----|
| State transitions valid | Rust compiler | Typestate pattern |
| No double-ACK | Rust compiler | Self consumed on transition |
| Offsets monotonically increase | Kani | Bounded model check |
| No batch silently dropped | Kani | All paths reach Acked or requeue |
| All data eventually delivered | TLA+ | Liveness with fairness |
| Shutdown drains all batches | TLA+ | Liveness |
| No deadlock under backpressure | TLA+ | Safety |
| Retry eventually succeeds or rejects | TLA+ | Liveness |
| Network failures handled | Turmoil | Deterministic simulation |
