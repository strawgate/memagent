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
   - A passing proof means ALL values in the bounded domain satisfy assertions.
2. Loops are bounded by unwind depth.
   - Use `#[kani::unwind(N)]` when loops exist. N = max_iterations + 1 (margin for termination check).
3. `kani::assume()` shrinks the explored domain — over-constraining makes proofs vacuous.
4. Kani auto-checks memory-safety classes (panic/overflow/oob/etc) in harnesses,
   but behavioral correctness still needs explicit assertions and meaningful oracles.
5. `kani::any_where(|x| predicate)` generates a value satisfying the predicate —
   prefer this over `kani::any()` + separate `assume()`.

## Repository Policy (Kani-Specific)

Follow these defaults unless `dev-docs/VERIFICATION.md` says otherwise:

- Put harnesses in `#[cfg(kani)] mod verification` in the same file.
- Name harnesses `verify_<function>_<property>`.
- Add `#[kani::unwind(N)]` on looped harnesses. For loop-free proofs, use `#[kani::unwind(0)]`.
- Add `kani::cover!` in non-trivial harnesses to confirm interesting paths are reachable.
- Prefer `kani::any_where(|v| predicate)` over `kani::any()` + `kani::assume(predicate)`.
- If using `kani::assume()`, add 2+ `kani::cover!()` guards for vacuity checks.
- `#[kani::proof_for_contract]` counts as a valid proof marker for boundary-contract purposes.

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
RUSTC_WRAPPER="" cargo kani -p ffwd-core --harness verify_my_harness \
  -Z function-contracts -Z mem-predicates -Z stubbing
RUSTC_WRAPPER="" cargo kani -p ffwd-core \
  -Z function-contracts -Z mem-predicates -Z stubbing
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
    #[kani::unwind(17)] // max 16 bytes + 1 for termination check
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
        // Independent reference implementation — must NOT call `my_fn`
        input.iter().map(|b| *b as u64).sum()
    }

    #[kani::proof]
    #[kani::unwind(17)]
    fn verify_my_fn_matches_oracle() {
        let input: [u8; 16] = kani::any();
        assert_eq!(my_fn(&input), oracle(&input));
        kani::cover!(input[0] != 0, "non-trivial path");
    }
}
```

### 3) Loop-free proof

```rust
#[kani::proof]
#[kani::unwind(0)] // no loops — no unwind needed
fn verify_my_const_fn() {
    let x: u64 = kani::any();
    assert!(my_const_fn(x) >= 1);
}
```

### 4) `any_where` pattern (preferred over `any` + `assume`)

```rust
// Prefer this:
let len: usize = kani::any_where(|&l: &usize| l <= 16);

// Over this:
let len: usize = kani::any();
kani::assume(len <= 16);
```

`any_where` keeps the constraint co-located with value generation and avoids
a two-step generate-then-constrain pattern.

### 5) State transition invariants

```rust
#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(N)]
    fn verify_transition_preserves_invariant() {
        let state = State::arbitrary();
        let next = step(state);
        assert!(next.invariant_holds());
        kani::cover!(next.changed(), "state transition happened");
    }
}
```

### 6) Compositional contracts (`proof_for_contract` + `stub_verified`)

Use contracts to keep deep call chains tractable.

Workflow:
1. Add `#[cfg_attr(kani, kani::requires(...))]` / `ensures(...)` contracts to the production function.
2. Add `#[kani::proof_for_contract(fn_name)]` harnesses to verify the contract.
3. In higher-level harnesses, use `#[kani::stub_verified(fn_name)]`.

This reduces solver state explosion by replacing re-proving callees with their
already-verified contracts.

```rust
// Production function with contract
#[cfg_attr(kani, kani::ensures(|result: &usize| *result >= 1 && *result <= 10))]
pub fn varint_len(value: u64) -> usize { ... }

// Proof harness
#[kani::proof_for_contract(varint_len)]
#[kani::unwind(12)]
fn verify_varint_len_contract() {
    let val: u64 = kani::any();
    let res = varint_len(val);
    kani::cover!(res == 1, "single-byte");
    kani::cover!(res == 10, "max-byte");
}

// Caller harness using stub
#[kani::proof]
#[kani::stub_verified(varint_len)]
fn verify_deep_caller() { ... }
```

Repository examples:
- `crates/ffwd-core/src/structural.rs` — `compute_real_quotes` + `prefix_xor`
- `crates/ffwd-core/src/otlp.rs` — `varint_len`, `tag_size`, `bytes_field_total_size`
- `crates/ffwd-core/src/json_scanner.rs` — `skip_whitespace`, `skip_bare_value`

## Solver And Bound Tuning

| Solver | Best for | Notes |
|--------|---------|-------|
| default (CADical) | General purpose | Default for most proofs |
| `kissat` | Arithmetic-heavy: varint, bitmask, shift-heavy code | 47% of slow proofs benefit; up to 265x speedup |
| `z3` | Bit-vector heavy, old proof infrastructure | Fallback |
| `bitwuzla` | Bit-vector arithmetic | Try if kissat/z3 slow |

```rust
#[kani::proof]
#[kani::solver(kissat)] // arithmetic-heavy varint bit ops
fn verify_varint_len() { ... }
```

Unwind bound guide:
- `#[kani::unwind(0)]` — loop-free proof, no unwind needed
- `#[kani::unwind(N)]` — N = max loop iterations + 1 (margin for termination check)
- Too low: Kani reports "underunwind" or inconclusive behavior
- Too high: solver time increases without benefit
- Add +1 or +2 margin over exact loop count to cover termination check

## Vacuity Detection

A proof can pass trivially if assumptions or constraints make the domain empty.
Detect vacuity with `kani::cover!`:

```rust
let len: usize = kani::any();
kani::assume(len <= 16);
// MUST have at least 2 cover statements:
kani::cover!(len == 0, "empty case reachable");
kani::cover!(len > 0, "non-empty case reachable");
```

If a cover reports **UNSATISFIABLE**, the proof is vacuous — the constrained
domain is empty and the assertions are meaningless.

When to specifically check for vacuity:
- After any `kani::assume()`
- When inputs are bounded with `kani::any_where()`
- In proofs with contracts (`#[requires]`, `#[ensures]`)

## Golden-Copy Oracle Limitation

Oracle equivalence proofs compare a production implementation against a reference
("oracle") implementation. The oracle must be **structurally independent** — if
both use the same algorithm or share internal logic, the proof cannot detect a
bug that infects both.

**Limitation:** A golden-copy oracle (structurally identical to production) will
NOT catch shared-logic bugs, implementation mistakes that both copies share, or
compiler bugs in shared helper functions.

**Mitigation strategies:**
1. **Encode-based verification** — prove properties by encoding and checking output
   (e.g., `encode_varint` then verify decoded length matches `varint_len`).
   This catches algorithmic mistakes without needing a second implementation.
2. **Cross-validation** — keep both encode-based and oracle-based proofs. If both
   pass, confidence is much higher.
3. **Different-algorithm oracle** — if possible, use a fundamentally different
   algorithm (e.g., Fliegel-Van Flandern JDN vs Hinnant date conversion).
4. **Document the limitation** in the oracle's doc comment when the oracle is
   golden-copy style.

Repository example of cross-validation:
- `verify_varint_len_matches_encode` (encode-based) + `verify_varint_len_vs_oracle`
  (oracle-based) both prove `varint_len` — neither alone is sufficient.

## Failure Triage

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `cover!` UNSATISFIABLE | Over-constrained domain | Relax `kani::assume()` bounds or use `any_where` |
| Timeout / very slow solve | Large input width or deep call chain | Lower input width, split proof, add contracts/stubs, try `kissat` |
| Loop-related failure | Under-unwind or missing `#[kani::unwind]` | Increase unwind bound |
| Oracle mismatch | Oracle/production logic mismatch | Validate oracle independence; check for off-by-one in position tracking |
| Proof passes but seems weak | Missing behavioral assertions | Add branch-specific assertions and additional covers |
| "No orbits found" / spurious alloc failures | `Vec::new()` or `vec![x; kani::any()]` in harness | Use `kani::any::<[u8; N]>()` or `kani::vec::any_vec::<T, N>()` |

## Review Checklist (Kani PRs)

- Is Kani required for this changed logic per `dev-docs/VERIFICATION.md`?
- Is the property behavioral (not tautological)?
- Are assumptions minimal and visible?
- Do covers demonstrate interesting-path reachability?
- Are unwind bounds justified? Is `#[kani::unwind(0)]` used for loop-free proofs?
- If contracts/stubs are used, are there corresponding `proof_for_contract` harnesses?
- Is the oracle independent from the production function (golden-copy limitation documented)?
- If behavior/invariants changed, were docs and guardrails updated?

## Shared Verification Utilities (`ffwd-kani`)

For fundamental oracles and assertions used across multiple crates, use the
`ffwd-kani` crate. All oracles are `no_std` compatible with zero external deps.

Add `ffwd-kani` as a dependency:

```toml
[dependencies]
ffwd-kani = { version = "0.1.0", path = "../ffwd-kani" }
```

Oracle functions are only called from `#[cfg(kani)]` and `#[cfg(test)]` blocks —
never from production code paths.

**Do not add an oracle inventory here.** The authoritative record of which oracles
exist and what they verify lives in the code:
- Each oracle is documented at its definition site in `crates/ffwd-kani/src/`
- Each oracle's proof is in the same file, co-located with the oracle
- Production linkages are documented in `dev-docs/VERIFICATION.md` per-module status,
  which is already kept up to date as part of PR review

When adding an oracle, document at the definition site:
- What it computes
- What production function it verifies (if any)
- Any known limitations (e.g., golden-copy limitation)

## Repo Pointers

- Policy and coverage rules: `dev-docs/VERIFICATION.md`
- Crate constraints: `dev-docs/CRATE_RULES.md`
- Boundary contract source: `dev-docs/verification/kani-boundary-contract.toml`
- Boundary validator: `scripts/verify_kani_boundary_contract.py`
- Oracle proof audit guide: `dev-docs/VERIFICATION.md` → per-module table

## Upstream

- https://model-checking.github.io/kani/
- Firecracker Kani patterns: `kani::any_where`, `#[kani::unwind(0)]`, stubbing at boundaries
