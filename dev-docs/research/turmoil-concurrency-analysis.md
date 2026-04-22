# Turmoil Concurrency Analysis

> **Status:** Active
> **Date:** 2026-04-01
> **Context:** Concurrency bug analysis for nightly Turmoil simulation failures in pipeline orchestration.

---

## Executive Summary

The nightly Turmoil tests are failing due to several concurrency bugs in the pipeline's `run_async` select loop and worker pool ack handling. The core issue is **ack starvation** - under sustained load, the select loop prioritizes receiving new data over processing acks, causing:

1. Unbounded `in_flight` batch growth
2. Checkpoints never advance during normal processing
3. Potential deadlocks during shutdown
4. Race conditions in multi-worker scenarios

---

## Bug Analysis

### Bug 1: Ack Starvation in Select Loop (HIGH PRIORITY)

**Location:** `crates/logfwd-runtime/src/pipeline/mod.rs`, `run_async()` select loop

**Problem:**
The `tokio::select!` macro is **unbiased by default**. Under sustained input load:
- `rx.recv()` always has data ready (channel has messages)
- `ack_rx.recv()` gets starved - acks queue up
- `in_flight` grows without bound
- Checkpoints only advance after shutdown

**Code:**
```rust
loop {
    tokio::select! {
        () = shutdown.cancelled() => { break; }
        msg = rx.recv() => { /* ... */ }     // Always ready under load
        _ = flush_interval.tick() => { /* ... */ }
        ack = self.pool.ack_rx_mut().recv() => { /* rarely runs */ }  // STARVED
    }
}
```

**Evidence:** Test `ack_starvation_under_sustained_load` documents this behavior:
- With 200 delays of 50ms each and 100 lines
- Only 1 checkpoint update (final flush) vs expected >1 intermediate advances

**Fix Strategy:**
Add `biased` mode to prioritize ack processing:
```rust
loop {
    tokio::select! {
        biased;  // Process branches in order
        () = shutdown.cancelled() => { break; }
        ack = self.pool.ack_rx_mut().recv() => { /* process acks first */ }
        msg = rx.recv() => { /* ... */ }
        _ = flush_interval.tick() => { /* ... */ }
    }
}
```

---

### Bug 2: Worker Panic Leaks Batch Tickets (MEDIUM PRIORITY)

**Location:** `crates/logfwd-runtime/src/worker_pool/worker.rs` (worker task)

**Problem:**
If `Sink::send_batch()` panics:
1. Worker task dies
2. `BatchTicket<Sending>` is never acked/rejected
3. `PipelineMachine::in_flight` permanently contains the batch
4. `draining.stop()` returns `Err`
5. Pipeline falls to `force_stop()` with data loss

**Evidence:** Test `worker_panic_does_not_block_drain` confirms:
- Pipeline hangs or force-stops after worker panic
- Single worker with 1 panic causes complete pipeline failure

**Fix Strategy:**
Wrap worker task execution in `std::panic::catch_unwind` (or `AssertUnwindSafe` for async):
```rust
// In worker loop
let result = std::panic::catch_unwind(|| {
    // ... send_batch call
});
match result {
    Ok(send_result) => { /* normal handling */ }
    Err(_) => {
        // Worker panic - reject all tickets in this batch
        for ticket in work_item.tickets {
            let receipt = ticket.reject();
            // Send receipt through ack channel
        }
    }
}
```

**Alternative:** Use `tokio::spawn` + `JoinHandle::abort` pattern with panic recovery.

---

### Bug 3: Rejected Batch Advances Checkpoint (DESIGN ISSUE)

**Location:** `crates/logfwd-runtime/src/pipeline/mod.rs`, `ack_all_tickets()` path

**Problem:**
`record_ack_and_advance()` treats `reject()` identically to `ack()` for checkpoint advancement. When a batch is rejected:
1. Data is NOT delivered
2. Checkpoint advances past the undelivered data
3. On restart, the data is permanently skipped

**Code:**
```rust
fn ack_all_tickets(...) {
    for ticket in tickets {
        let receipt = if success {
            ticket.ack()
        } else {
            ticket.reject()  // Same effect on checkpoint!
        };
        let advance = machine.apply_ack(receipt);  // Advances either way
        // ...
    }
}
```

**Evidence:** Test `rejected_batch_checkpoint_behavior` confirms:
- Batch with 20 lines rejected → checkpoint advances to 520
- On restart, those 20 lines would be skipped

**Fix Strategy:**
This may be **intentional behavior** for some rejection reasons (e.g., 400 Bad Request where retry would fail). However, for transient errors, we should NOT advance.

Options:
1. **Conservative:** Don't advance checkpoint on reject - data will be re-read (at-least-once delivery)
2. **Context-aware:** Only advance on "permanent" rejections (400 Bad Request), retry on transient (429, 500)
3. **Configurable:** Add `reject_advances_checkpoint` config option

---

### Bug 4: Race Condition in Multi-Worker Out-of-Order Ack (LOW PRIORITY)

**Location:** `crates/logfwd-types/src/pipeline/lifecycle.rs`, `record_ack_and_advance()`

**Problem:**
With multiple workers:
- Worker 1 gets slow batch (3s delay)
- Worker 2 gets fast batches
- Worker 2's acks arrive before Worker 1's
- Checkpoint must NOT advance past Worker 1's batch

**Evidence:** Test `multi_worker_out_of_order_ack_checkpoint_ordering` exists but the race is in the `select!` loop, not the state machine.

**Current Status:** The `PipelineMachine` correctly handles out-of-order acks via `pending_acks` BTreeMap. The bug is in the **delivery** of acks to the machine (see Bug 1).

---

### Bug 5: TCP Server Crash Recovery Limited (KNOWN LIMITATION)

**Location:** `crates/logfwd/tests/turmoil_sim/network_sim.rs`, `tcp_server_crash_triggers_reconnect`

**Problem:**
After server crash + bounce:
- Worker pool exhausts `MAX_RETRIES=3` during crash window
- When server bounces, worker has already given up on in-flight batches
- Subsequent batches also fail if worker is in retry loop

**Evidence:** Test documents this as a "real limitation":
> "Transient server downtime longer than the retry window (~1s with default backoff) causes permanent batch loss."

**Fix Strategy:**
This is a **design limitation**, not a bug. Options:
1. Increase `MAX_RETRIES` or make configurable
2. Implement circuit breaker pattern with longer backoff
3. Add persistent retry queue for critical data

---

## Fix Implementation Plan

### Phase 1: Ack Starvation Fix (Critical)

**File:** `crates/logfwd-runtime/src/pipeline/mod.rs`

**Changes:**
1. Add `biased` to select! loop
2. Reorder branches: shutdown → ack_rx → rx → flush_interval

```rust
loop {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => { break; }
        ack = self.pool.ack_rx_mut().recv() => {
            if let Some(ack) = ack {
                self.apply_pool_ack(ack);
            }
        }
        msg = rx.recv() => { /* ... */ }
        _ = flush_interval.tick() => { /* ... */ }
    }
}
```

**Testing:**
- Run `ack_starvation_under_sustained_load` 100+ times with different seeds
- Verify checkpoint advances >1 times during processing

### Phase 2: Worker Panic Recovery (High)

**File:** `crates/logfwd-runtime/src/worker_pool/worker.rs`

**Changes:**
1. Wrap `sink.send_batch()` in panic catch
2. On panic, reject all tickets and report failure
3. Spawn new worker to replace dead one

**Testing:**
- Run `worker_panic_does_not_block_drain` 100+ times
- Verify pipeline completes without hanging

### Phase 3: Reject Behavior Review (Medium)

**File:** `crates/logfwd-runtime/src/pipeline/mod.rs`, `ack_all_tickets()`

**Decision needed:** Should reject advance checkpoint?

**Option A (Conservative - Recommended):**
```rust
fn ack_all_tickets(..., success: bool, is_reject: bool) {
    for ticket in tickets {
        if success {
            let receipt = ticket.ack();
            machine.apply_ack(receipt);  // Advances
        } else if is_reject {
            // Don't advance checkpoint on reject
            // Just remove from in_flight without advancing
            machine.remove_without_advance(ticket);
        } else {
            // Error - don't advance
        }
    }
}
```

### Phase 4: TCP Recovery Improvement (Low)

**Options:**
1. Make `MAX_RETRIES` configurable (default: 3 → 10)
2. Add exponential backoff up to 30s
3. Add "permanent failure" mode for circuit breaker

---

## Test Validation Matrix

| Test | Current Status | After Fix |
|------|---------------|-----------|
| `ack_starvation_under_sustained_load` | Flaky/Fails | Stable |
| `worker_panic_does_not_block_drain` | Fails/Hangs | Completes |
| `rejected_batch_checkpoint_behavior` | Documents bug | Fixed or documented |
| `multi_worker_out_of_order_ack_checkpoint_ordering` | Passes | Passes |
| `tcp_server_crash_triggers_reconnect` | Documents limitation | Improved or documented |
| `crash_checkpoint_consistency` | Passes | Passes |
| `multi_source_independent_checkpoint` | Passes | Passes |

---

## Verification Commands

```bash
# Run all Turmoil tests with multiple seeds
for seed in {1..100}; do
    TURMOIL_SEED=$seed cargo test --features turmoil --test turmoil_sim 2>&1 | tee -a turmoil_results.log
done

# Run specific test with detailed output
TURMOIL_SEED=42 cargo test --features turmoil --test turmoil_sim ack_starvation -- --nocapture

# Run with stress configuration
cargo test --features turmoil --test turmoil_sim --release
```

---

## Appendix: Key Code Locations

### Pipeline Select Loop
- **File:** `crates/logfwd-runtime/src/pipeline/mod.rs`
- **Search:** `run_async()`, `tokio::select!`
- **Function:** `run_async()`

### PipelineMachine

- **File:** `crates/logfwd-types/src/pipeline/lifecycle.rs`
- **Search:** `apply_ack()`, `record_ack_and_advance()`
- **Key Methods:** `apply_ack()`, `record_ack_and_advance()`

### BatchTicket

- **File:** `crates/logfwd-types/src/pipeline/batch.rs`
- **Search:** `ack()`, `reject()`, `fail()`
- **Key Methods:** `ack()`, `reject()`, `fail()`

### Worker Pool

- **File:** `crates/logfwd-runtime/src/worker_pool/pool.rs`
- **Search:** `submit()`, worker loop

---

## References

1. [Turmoil Documentation](https://docs.rs/turmoil/latest/turmoil/)
2. [Tokio Select Bias](https://docs.rs/tokio/latest/tokio/macro.select.html#fairness)
3. `dev-docs/references/turmoil-simulation.md` - Internal turmoil patterns
4. `crates/logfwd/tests/turmoil_sim/` - All simulation tests
