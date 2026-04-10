# Tokio Async Patterns (Repo-Scoped)

Operational patterns used in `logfwd-runtime` and pipeline shells.

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

## Change Checklist

- If lifecycle semantics changed: update `dev-docs/ARCHITECTURE.md` and `dev-docs/VERIFICATION.md`.
- If checkpoint behavior changed: update `dev-docs/ADAPTER_CONTRACT.md` and related tests.
- If cancellation behavior changed: update TLA+/state-machine coverage if required.

## Canonical Docs

- Runtime boundaries: `dev-docs/CRATE_RULES.md`
- Lifecycle and data flow: `dev-docs/ARCHITECTURE.md`
- Protocol/invariant requirements: `dev-docs/VERIFICATION.md`

## Upstream

- https://tokio.rs/tokio/tutorial
