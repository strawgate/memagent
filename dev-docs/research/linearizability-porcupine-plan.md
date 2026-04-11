> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Plan linearizability checking for pipeline terminalization/checkpoint contracts.

# Linearizability / Porcupine Plan

## Current state

- We now capture runtime-origin transition events in turmoil scenarios (not synthetic trace events).
- We already enforce phase/order/checkpoint invariants with the transition validator.
- We do **not** currently run a linearizability checker (Porcupine or equivalent) in CI.

## Why add linearizability now

- Current checks validate local invariants and replay equivalence.
- They do not prove that concurrent histories are equivalent to a legal sequential execution of the intended API contract.
- Linearizability checking closes that gap for race-heavy paths (ack ordering, checkpoint persistence, hold/release, crash/restart).

## Target contract (v1)

Model operations over a single source checkpoint register:

1. `StartSend(batch_id, checkpoint)`
2. `Ack(batch_id, outcome)` where `outcome ∈ {Delivered, Rejected, RetryExhausted, TimedOut, PoolClosed, InternalFailure}`
3. `Flush(success)`
4. `ReadDurable() -> Option<offset>`

Core legality rules:

1. Durable checkpoint is monotonic.
2. Durable checkpoint never exceeds max contiguous acked/delivered (or explicitly rejected) frontier.
3. Failed flush does not advance durability.
4. Force-stop may abandon in-flight work but must not fabricate advancement.

## Porcupine integration options

1. **Recommended**: keep checker as a small Go tool under `scripts/linearizability/`.
   - Input: JSON history emitted by turmoil fault harness.
   - Output: pass/fail + minimal counterexample trace.
2. Alternative: keep our current Rust validator only (no true linearizability search).
3. Alternative: add another checker framework later if Porcupine constraints become limiting.

## Execution plan

1. Define operation/event schema used by turmoil history export.
2. Add history exporter in `turmoil_sim` fault harness.
3. Implement Porcupine model and checker CLI.
4. Add deterministic test cases:
   - expected pass histories
   - expected fail histories (regression corpus)
5. Wire CI job:
   - run selected turmoil scenarios
   - run Porcupine checker on emitted histories
6. Add docs:
   - contract definition
   - how to reproduce counterexamples locally

## Acceptance criteria

1. CI fails on injected non-linearizable history.
2. CI passes on current known-good seeded histories.
3. Counterexample output is operator-actionable (batch ids, checkpoints, operation order).
4. Runtime overhead remains test-only.

## Risks

1. Model mismatch: checker model diverges from intended runtime semantics.
2. History incompleteness: missing events can produce false positives/negatives.
3. Maintenance cost if model scope is too broad in v1.

## Scope guardrails

1. Start with single-source checkpoint model.
2. Keep all history capture/checking test-only.
3. Expand to multi-source and richer operations only after v1 counterexample quality is good.
