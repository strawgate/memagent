# Kani Verification (Repo-Scoped Deep Reference)

This reference is intentionally deep.
Kani proofs in this repository are a high-complexity, high-leverage area, and
agents need enough detail to make good proof design choices without re-research.

Use this doc for proof authoring and review in `ffwd-*` crates.

## Kani In The Verification Stack

Use `dev-docs/VERIFICATION.md` as policy source-of-truth. Use this file for
practical Kani execution details.

| Property shape | Primary tool | Why |
|---|---|---|
| Pure bounded logic, parser safety, arithmetic invariants | Kani | Exhaustive symbolic exploration inside bounds |
| Temporal/liveness/ordering over many interleavings | TLA+ | Kani is step-bounded, not temporal |
| Heap-heavy / async / IO-intensive behavior | proptest (+ Miri) | Kani scales poorly on unbounded heap and async shells |

## Mental Model (Read First)

1. `kani::any()` is universal quantification, not sampling.
   - A passing proof means all values in the bounded domain satisfy assertions.
2. Loops are bounded by unwind depth.
   - Use `#[kani::unwind(N)]` when loops exist.
3. `kani::assume()` shrinks the explored domain.
   - Over-constraining can make proofs vacuous.
4. Kani auto-checks memory-safety classes (panic/overflow/oob/etc) in harnesses,
   but behavioral correctness still needs explicit assertions and meaningful oracles.

## Repository Policy (Kani-Specific)

Follow these defaults unless `dev-docs/VERIFICATION.md` says otherwise:

- Put harnesses in `#[cfg(kani)] mod verification` in the same file.
- Name harnesses `verify_<function>_<property>`.
- Add `#[kani::unwind(N)]` on looped harnesses.
- Add `kani::cover!` in non-trivial harnesses.
- If using `kani::assume()`, add 2+ `kani::cover!()` guards for vacuity checks.
- Prefer `kani::any_where()` over broad `any()` plus many detached `assume()` calls.

Proofs are typically required for:
- Parser/framer primitives in `ffwd-core`
- Bitmask/structural operations
- Encoding math and bounds-sensitive format logic
- Pure state transitions

Not a Kani-first target:
- Async orchestration shells
- IO and network side effects
- Unbounded heap-heavy workflows

## Commands (Local + CI Parity)

Fast path:

```bash
just kani
just kani-boundary
```

Focused iteration:

```bash
RUSTC_WRAPPER="" cargo kani -p ffwd-core --harness verify_my_harness -Z function-contracts -Z mem-predicates -Z stubbing
RUSTC_WRAPPER="" cargo kani -p ffwd-core -Z function-contracts -Z mem-predicates -Z stubbing
```

Guardrail and contract checks:

```bash
python3 scripts/verify_kani_boundary_contract.py
just verification-guardrail
```

## Proof Patterns Used In This Repo

### 1) Crash-freedom + reachability

```rust
#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(N)] // Choose N from the maximum loop iterations (+1 or +2 margin)
    fn verify_my_fn_no_panic() {
        let input: [u8; 16] = kani::any();
        let _ = my_fn(&input);
        kani::cover!(true, "harness executed");
    }
}
```

### 2) Oracle equivalence (preferred for parser/math primitives)

```rust
#[cfg(kani)]
mod verification {
    use super::*;

    fn oracle(input: &[u8]) -> u64 {
        input.iter().map(|b| *b as u64).sum()
    }

    #[kani::proof]
    #[kani::unwind(N)] // Choose N from the maximum loop iterations (+1 or +2 margin)
    fn verify_my_fn_matches_oracle() {
        let input: [u8; 16] = kani::any();
        assert_eq!(my_fn(&input), oracle(&input));
        kani::cover!(input[0] != 0, "non-trivial path");
    }
}
```

### 3) State transition invariants

```rust
#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(N)] // Choose N from the maximum loop iterations (+1 or +2 margin)
    fn verify_transition_preserves_invariant() {
        let state = State::arbitrary();
        let next = step(state);
        assert!(next.invariant_holds());
        kani::cover!(next.changed(), "state transition happened");
    }
}
```

### 4) Symbolic ordering/permutation exploration

For order-sensitive logic, model operation ordering symbolically with
`any_where()` rather than hardcoding one sequence.

Repository examples:
- `crates/ffwd-types/src/pipeline/lifecycle.rs`

### 5) Compositional contracts (`proof_for_contract` + `stub_verified`)

Use contracts to keep deep call chains tractable.

Repository examples:
- `crates/ffwd-core/src/structural.rs`
- `crates/ffwd-core/src/otlp.rs`

Workflow:
1. Add `#[cfg_attr(kani, kani::requires(...))]` / `ensures(...)` contracts.
2. Add `#[kani::proof_for_contract(fn_name)]` harnesses.
3. In higher-level harnesses, use `#[kani::stub_verified(fn_name)]`.

This reduces solver state explosion by replacing re-proving callees with their
already-verified contracts.

## Solver And Bound Tuning

- Start with default solver.
- If a harness is consistently above ~10s, add `#[kani::solver(kissat)]` with a short
  inline comment explaining why the override is needed.
- Keep unwind bounds realistic and explicit; too high inflates solve time,
  too low yields inconclusive behavior.
- Prefer proving smaller pure components compositionally over one monolith.

## Failure Triage

- `cover!` unsat:
  - assumptions are over-constrained; relax domain.
- Timeout/very slow solve:
  - lower input width, split proof, add contracts/stubs, try `kissat`.
- Loop-related failure:
  - revisit `#[kani::unwind(N)]` and loop assumptions.
- Oracle mismatch:
  - validate oracle independence from production implementation.
- Proof passes but seems weak:
  - add branch-specific assertions and additional covers.

## Review Checklist (Kani PRs)

- Is Kani required for this changed logic per `dev-docs/VERIFICATION.md`?
- Is the property behavioral (not tautological)?
- Are assumptions minimal and visible?
- Do covers demonstrate interesting-path reachability?
- Are unwind bounds justified?
- If contracts/stubs are used, are there corresponding `proof_for_contract` harnesses?
- If behavior/invariants changed, were docs and guardrails updated?

## Shared Verification Utilities (`ffwd-kani`)

For fundamental oracles and assertions used across multiple crates, use the
`ffwd-kani` crate. Key exports:

- **`ffwd_kani::bytes::assert_bytes_eq`**: bounded loop-based slice comparison
- **`ffwd_kani::bytes::eq_ignore_case_match`**: case-insensitive variable-length ASCII comparison — oracle for `ffwd_core::otlp::eq_ignore_case_match`
- **`ffwd_kani::bytes::compute_real_quotes_oracle`**: reference quote-escape bitmask
- **`ffwd_kani::bytes::prefix_xor_oracle`**: reference running XOR
- **`ffwd_kani::datetime::jdn_days_from_epoch`**: Julian Day Number oracle
- **`ffwd_kani::hex::hex_nibble_oracle`**: hex nibble branch-based oracle
- **`ffwd_kani::hex::hex_decode_oracle`**: hex decode reference implementation
- **`ffwd_kani::iter::find_byte`**: linear-scan byte search oracle
- **`ffwd_kani::numeric::parse_int_oracle`**: i128-accumulator integer parser
- **`ffwd_kani::proto::varint_len_oracle`**: varint encoded length predictor — oracle for `ffwd_core::otlp::varint_len`
- **`ffwd_kani::proto::tag_size_oracle`**: protobuf tag size predictor — oracle for `ffwd_core::otlp::tag_size`
- **`ffwd_kani::proto::bytes_field_total_size_oracle`**: protobuf field size predictor — oracle for `ffwd_core::otlp::bytes_field_total_size`

Add `ffwd-kani` as a dependency:

```toml
[dependencies]
ffwd-kani = { version = "0.1.0", path = "../ffwd-kani" }
```

Oracle functions are only called from `#[cfg(kani)]` and `#[cfg(test)]` blocks—
never from production code paths.

## Repo Pointers

- Policy and coverage rules: `dev-docs/VERIFICATION.md`
- Crate constraints: `dev-docs/CRATE_RULES.md`
- Boundary contract source: `dev-docs/verification/kani-boundary-contract.toml`
- Boundary validator: `scripts/verify_kani_boundary_contract.py`

## Upstream

- https://model-checking.github.io/kani/
