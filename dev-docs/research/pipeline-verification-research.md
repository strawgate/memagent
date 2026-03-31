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
