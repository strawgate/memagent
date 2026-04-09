# Workstream 4 result: split tail module internals in `logfwd-io`

Date: 2026-04-09

## Summary

The legacy monolithic `tail.rs` implementation is now represented as a `tail/` module tree, and this workstream further reduced the top-level `tail` entrypoint to an index-style module file (`tail/mod.rs`) that only wires submodules and exports API types.

Primary change in this workstream:
- moved the large integrated `#[cfg(test)]` tail suite out of `tail/mod.rs` into `tail/tests.rs`
- moved Kani proof code out of `tail/mod.rs` into `tail/verification.rs`
- kept runtime logic in existing responsibility modules (`tailer.rs`, `reader.rs`, `discovery.rs`, `identity.rs`, `glob.rs`) unchanged

This makes `tail/mod.rs` navigable and keeps behavior stable while preserving checkpoint, duplicate/loss window, and diagnostics semantics.

## Chosen module map

Runtime modules:
- `tail/mod.rs`: module index + public exports only
- `tail/tailer.rs`: orchestration (`FileTailer`, poll loop, backoff, health reduction)
- `tail/discovery.rs`: fs event draining, glob rescans, rotation/new-file/deletion detection
- `tail/reader.rs`: byte-level I/O, offsets, truncation handling, EOF emission gating, LRU eviction
- `tail/identity.rs`: file identity + source-id derivation and fingerprinting
- `tail/glob.rs`: glob root/depth math and expansion

Verification/test modules:
- `tail/tests.rs`: integration-style unit test suite for tail behavior
- `tail/verification.rs`: Kani proofs for EOF emission state machine properties

## Alternative decomposition considered and rejected

Rejected alternative: split runtime behavior into many tiny micro-modules (for example, separate modules per event type, rotation detector, backoff policy, and LRU manager) in this same PR.

Why rejected now:
- higher mechanical churn and greater risk of subtle ordering regressions in event emission, especially around `Truncated`/`Data` sequencing and EOF signaling
- larger diff would increase review burden for this fanout workstream
- current module boundaries already map to coherent responsibilities; the highest remaining maintainability issue was top-level file navigability due embedded tests/proofs

## Changed file list

- `crates/logfwd-io/src/tail/mod.rs`
- `crates/logfwd-io/src/tail/tests.rs` (new)
- `crates/logfwd-io/src/tail/verification.rs` (new)

No changes were made to `otlp_receiver.rs` or non-`logfwd-io` crates.

## Behavior and contract-risk notes

Checkpoint/offset semantics risk assessment (explicit):
- No runtime offset logic was modified in this workstream; `reader.rs` code paths for truncation reset, offset advancement, LRU evicted offset restore, and source-id keyed updates were left intact.
- Event ordering behavior was not changed in runtime code. In particular, truncation-before-data ordering and EOF-once-per-streak semantics remain governed by existing implementations/tests.
- Because only module wiring and location of tests/proofs changed, expected checkpoint duplicate/loss windows are unchanged relative to prior behavior.

Diagnostics/control-plane risk assessment:
- No changes to diagnostics counters, health policy reducer, or control-plane endpoint logic.
- `FileTailer` health transition behavior remains in `tailer.rs` unchanged.

Net: low behavioral risk; primary risk is accidental compile/test drift from relocation, mitigated by required test/check runs below.

## Tests/check outcomes

Executed:
- `cargo test -p logfwd-io tail` ✅
- `cargo check -p logfwd-io` ✅

Both completed successfully after refactor.

## Recommendation label

**Recommendation: ACCEPT**

Rationale:
- materially improves module navigability by collapsing `tail/mod.rs` from thousands of lines to a concise module index
- preserves behavior by avoiding semantic edits in runtime hot paths
- keeps verification and regression coverage in place (relocated, not removed)

## What evidence would change my mind

I would downgrade/reject if any of the following appears:
- regression in truncation/rotation event ordering (`Truncated`, `Data`, `Rotated`, `EndOfFile`) under existing tests or new interleaving tests
- offset restoration regression after LRU eviction/reopen or copytruncate scenarios
- new readiness/health transition drift for file input under error-backoff conditions
- Kani proof integration no longer discovering or running `tail` EOF invariants after relocation
