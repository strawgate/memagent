# Formal Verification Coverage

Checklist for Kani/TLA+/proptest coverage quality.

## Hard Fails

Reject PRs when any required proof/test is missing for changed critical logic.

## Kani Required Areas

For `ffwd-core` logic that is pure/bounded:

- Parsers and framing logic.
- Encoding and size math.
- Bitmask/structural operations.
- Pure state transitions.

If changed and unproven, fail review unless explicitly exempt per `VERIFICATION.md`.

## Proof Quality Checklist

- Harnesses are in `#[cfg(kani)]` modules.
- Assertions verify behavior, not tautologies.
- Assumptions are minimal and non-vacuous.
- Loop bounds use explicit unwind where required.
- At least one `cover!` validates interesting-path reachability.

## Non-Kani Areas

Use proptest/integration/TLA+ when logic is async, IO-heavy, or temporal.

## TLA Hard Fails

Reject when:
- `tla/*.tla` changes without corresponding `.cfg` coverage and TLC evidence.
- Liveness properties change without updated fairness rationale.
- New invariants are added without vacuity/coverage witness intent.

## Documentation Sync

If proofs/tests change coverage expectations, update:

- `dev-docs/VERIFICATION.md`
- `dev-docs/verification/kani-boundary-contract.toml` (when boundary status changes)

## Evidence Commands

```bash
just kani
just kani-boundary
just test
# For TLA+-touching changes, run matching TLC models from tla/README.md
```

Required review evidence for verification-heavy changes:
- Harness names changed/added.
- Command list run locally.
- Coverage/vacuity notes when assumptions are constrained.
