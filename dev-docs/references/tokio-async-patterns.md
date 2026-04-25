# Tokio Async Patterns (Repo-Scoped)

Operational patterns used in `ffwd-runtime` and pipeline shells.

## Boundary Rule

- Async orchestration belongs in runtime/binary crates.
- Pure transitions and invariants belong in lower pure crates.

## Preferred Patterns

- Bounded channels for backpressure.
- Explicit shutdown propagation.
- Clear ownership of worker lifecycle and ack flow.
- Isolate pure reducers from async shells when logic grows.

## Avoid

- Hidden unbounded queues.
- Implicit checkpoint advancement on ambiguous outcomes.
- Embedding business invariants directly in async select loops.

## Critical Gotchas

- Do not create nested runtimes or call `block_on` from runtime worker paths.
- In `tokio::select!`, ensure fairness assumptions are explicit; avoid starvation-prone loops.
- Timer branches in `select!` must be reset intentionally after state changes.
- `spawn_blocking` is for bounded CPU/offload only; avoid hiding core protocol transitions there.
- Bridge points (`blocking_send`/`blocking_recv`) should stay at explicit boundaries (thread handoff), not deep inside async logic.
- Cancellation must not silently drop checkpoint/delivery outcomes; propagate terminal state explicitly.

## Change Checklist

- If lifecycle semantics changed: update `dev-docs/ARCHITECTURE.md`, `dev-docs/VERIFICATION.md`,
  relevant TLA+ specs in `tla/`, and the affected Kani/proptest state-machine coverage.
- If checkpoint behavior changed: update `dev-docs/ADAPTER_CONTRACT.md` and related tests.
- If cancellation behavior changed: update TLA+ specs (`tla/`), state-machine coverage, Kani proof harnesses, proptest suites, and related docs.

## Canonical Docs

- Runtime boundaries: `dev-docs/CRATE_RULES.md`
- Lifecycle and data flow: `dev-docs/ARCHITECTURE.md`
- Protocol/invariant requirements: `dev-docs/VERIFICATION.md`

## Upstream

- https://tokio.rs/tokio/tutorial
