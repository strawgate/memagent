# Kani Verification (Repo-Scoped)

Minimal guide for writing and reviewing Kani proofs in logfwd.

## What Kani Is Used For Here

Use Kani for bounded, pure, deterministic logic.

Primary targets in this repo:

- Parsers and framers in `logfwd-core`.
- Bitmask/structural logic.
- Encoding size/math invariants.
- Pure state transitions.

Do not use Kani for async orchestration or IO-heavy code.

## Mandatory Conventions

- Put harnesses in `#[cfg(kani)] mod verification` in the same file.
- Name harnesses `verify_<fn>_<property>`.
- Add unwind bounds for loops (`#[kani::unwind(N)]`).
- Add `kani::cover!` for at least one non-trivial path.
- Keep assumptions narrow and explicit.

## Proof Types We Require

- No panic: parser/encoder does not panic for bounded symbolic input.
- Oracle equivalence: output matches a simpler reference implementation.
- Invariant preservation: transition keeps invariant true.

## Common Failure Modes

- Vacuous proofs from over-constrained `assume`.
- Oracle that reuses production logic (not independent).
- Missing unwind on loops.
- Proving only shape, not behavior.

## Fast Workflow

```bash
just kani
RUSTC_WRAPPER="" cargo kani -p logfwd-core --harness verify_my_harness -Z function-contracts -Z mem-predicates -Z stubbing
RUSTC_WRAPPER="" cargo kani -p logfwd-core -Z function-contracts -Z mem-predicates -Z stubbing
```

## CI-Parity Commands

```bash
just kani
just kani-boundary
```

Use raw `cargo kani` only for targeted iteration; always finish with `just` recipes.

## Copy/Paste Harness Templates

```rust
#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    fn verify_my_fn_no_panic() {
        let input: [u8; 16] = kani::any();
        let _ = my_fn(&input);
        kani::cover!(true, "harness executed");
    }
}
```

```rust
#[cfg(kani)]
mod verification {
    use super::*;

    fn oracle(input: &[u8]) -> u64 {
        // simple independent reference implementation
        input.iter().map(|b| *b as u64).sum()
    }

    #[kani::proof]
    fn verify_my_fn_matches_oracle() {
        let input: [u8; 16] = kani::any();
        assert_eq!(my_fn(&input), oracle(&input));
        kani::cover!(input[0] != 0, "non-trivial path");
    }
}
```

```rust
#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    fn verify_transition_preserves_invariant() {
        let state = State::arbitrary();
        let next = step(state);
        assert!(next.invariant_holds());
        kani::cover!(next.changed(), "state transition happened");
    }
}
```

## Review Checklist

- Is this function in the Kani-required set from `dev-docs/VERIFICATION.md`?
- Is the property meaningful (not tautological)?
- Is there evidence the interesting path is reachable (`cover!`)?
- If proof changed behavior expectations, was `VERIFICATION.md` updated?

## Failure Triage

- `kani::cover!` UNSAT: assumptions are over-constrained; relax constraints.
- Timeout/slow solve: reduce bounds or add solver annotation.
- Loop-related failure: adjust `#[kani::unwind(N)]` to realistic bound.
- Oracle mismatch: verify oracle independence from production implementation.

## Canonical Docs

- Policy and required coverage: `dev-docs/VERIFICATION.md`
- Crate constraints: `dev-docs/CRATE_RULES.md`
- Boundary contract checks: `dev-docs/verification/kani-boundary-contract.toml`

## Upstream

- https://model-checking.github.io/kani/
