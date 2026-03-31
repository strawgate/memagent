# Kani Rust Verifier -- Agent Reference

Version: `kani-verifier 0.56+` (syncs monthly with Rust nightly)

Kani is an open-source bounded model checker for Rust that translates Rust MIR
into a SAT/SMT formula via CBMC. It proves safety and correctness properties
by exhaustively exploring all possible inputs within bounds.

**Install:** `cargo install --locked kani-verifier && cargo kani setup`
**Run:** `cargo kani` (all harnesses) or `cargo kani --harness <name>` (one harness)

---

## 1. Core Concepts

### How Kani works

Kani compiles Rust → MIR → GOTO (CBMC IR) → SAT formula. Every loop is
unrolled to a flat propositional constraint. Verification cost scales with
`unwind_depth × symbolic_variables × program_paths`.

Every `#[kani::proof]` harness automatically checks for:
- **Panics** (including `unwrap()` on `None`/`Err`)
- **Arithmetic overflow** (add, sub, mul, shift)
- **Division by zero**
- **Out-of-bounds array/slice access**
- **Null pointer dereference**
- **Pointer-outside-object-bounds** (unsafe code)

No explicit assertions needed for the above -- if any reachable path triggers
these, Kani reports failure. A successful verification is a mathematical proof
that no input causes the checked failures within the bounds.

### Proof harness structure

```rust
#[cfg(kani)]
#[kani::proof]
fn verify_my_function() {
    // 1. Generate symbolic (nondeterministic) inputs
    let x: u32 = kani::any();
    let y: u16 = kani::any();

    // 2. Constrain inputs (preconditions)
    kani::assume(x < 4096);
    kani::assume(y != 0);

    // 3. Call function under verification
    let result = my_function(x, y);

    // 4. Assert postconditions
    assert!(result <= x);
}
```

Key principle: `kani::any()` represents ALL possible values of the type
simultaneously. This is not random sampling -- it's exhaustive symbolic execution.

---

## 2. Attributes Reference

### `#[kani::proof]`

Marks a function as a proof harness. Must be a function with no parameters.
Only functions with this attribute are verified by `cargo kani`.

```rust
#[kani::proof]
fn my_harness() {
    assert!(1 + 1 == 2);
}
```

### `#[kani::unwind(N)]`

Sets the loop unwinding bound. N must be **one more than** the maximum number
of loop iterations (the extra iteration checks the exit condition). With
`break`/`continue`, add 2--3 more.

```rust
#[kani::proof]
#[kani::unwind(11)]  // handles up to 10 iterations
fn check_loop() {
    let data: [u8; 10] = kani::any();
    let mut sum: u32 = 0;
    for &b in data.iter() {
        sum += b as u32;
    }
    assert!(sum <= 2550);
}
```

**Soundness guarantee:** If N is too small, Kani reports an **unwinding assertion
failure** and marks all other properties as "undetermined." You cannot
accidentally "prove" something by under-unwinding.

**If N is not specified:** Kani attempts automatic loop unwinding, but this
doesn't always terminate. Use `--default-unwind <n>` as a global fallback.

### `#[kani::solver(solver)]`

Changes the SAT solver. Different solvers have dramatically different performance
on different harnesses.

Available solvers:
- `cadical` (default) -- good general-purpose performance
- `kissat` -- fastest 47% of the time for harnesses > 10s (s2n-quic benchmarks)
- `minisat` -- sometimes fastest, sometimes 265x slower than kissat
- `z3` -- SMT solver, good for arithmetic-heavy proofs
- `bitwuzla` -- bit-vector focused SMT solver
- `cvc5` -- another SMT option
- `bin="<path>"` -- custom solver binary

```rust
#[kani::proof]
#[kani::solver(kissat)]
fn check_with_kissat() {
    // ...
}
```

**Guideline:** Start with default (cadical). If slow, try kissat. If still slow,
try z3 for arithmetic or bitwuzla for bitwise. s2n-quic mixes solvers per-harness.

### `#[kani::should_panic]`

Verifies that the harness panics (negative testing):

```rust
#[kani::proof]
#[kani::should_panic]
fn double_init_panics() {
    let mut dev = Device::new();
    dev.init();
    dev.init(); // should panic
}
```

### `#[kani::stub(original, replacement)]`

Replaces a function during verification. Multiple stubs per harness allowed.

```rust
#[cfg(kani)]
fn mock_random<T: kani::Arbitrary>() -> T {
    kani::any()
}

#[kani::proof]
#[kani::stub(rand::random, mock_random)]
fn verify_with_stubbed_random() {
    let key: u32 = rand::random();
    let data: u32 = kani::any();
    let encrypted = data ^ key;
    let decrypted = encrypted ^ key;
    assert_eq!(data, decrypted);
}
```

Common stubbing reasons:
- **Unsupported features** (inline assembly, system calls)
- **Performance** (replace expensive code with simpler equivalent)
- **Compositional reasoning** (replace verified code with contract)

**Stubbing foreign (FFI) functions** is also supported:

```rust
extern "C" {
    fn my_c_function(input: u32) -> u32;
}

fn my_c_function_stub(input: u32) -> u32 { input + 1 }

#[kani::proof]
#[kani::stub(my_c_function, my_c_function_stub)]
fn check_ffi() {
    let result = unsafe { my_c_function(42) };
    assert_eq!(result, 43);
}
```

---

## 3. Nondeterministic Values

### `kani::any()` and the Arbitrary trait

`kani::any::<T>()` generates a symbolic value covering ALL valid values of type T.
For types with invariants (e.g., `NonZeroU32`), invalid values are excluded.

```rust
let x: u32 = kani::any();          // any u32
let y: bool = kani::any();         // true or false
let z: NonZeroU32 = kani::any();   // any u32 except 0
let arr: [u8; 16] = kani::any();   // all 2^128 possible arrays
```

Built-in Arbitrary implementations: all primitive types, `bool`, `char`,
`NonZero*`, `Option<T>`, `Result<T, E>`, arrays `[T; N]`, tuples.

### `kani::any_where()` -- constrained generation

```rust
let x: u32 = kani::any_where(|v| *v < 1000 && *v % 2 == 0);
// x is any even number from 0 to 998
```

### `kani::assume()` -- adding constraints

```rust
let x: u32 = kani::any();
kani::assume(x < 4096);  // restrict to values < 4096
```

**Warning:** Over-constraining with assume can make proofs vacuously true.
If assume eliminates all paths, Kani succeeds but proves nothing useful.

### Deriving Arbitrary for custom types

```rust
#[derive(Copy, Clone)]
#[cfg_attr(kani, derive(kani::Arbitrary))]
pub enum State {
    Idle,
    Processing(u32),
    Error { code: i32 },
}

#[cfg_attr(kani, derive(kani::Arbitrary))]
pub struct Config {
    pub max_retries: u8,
    pub timeout_ms: u32,
    pub enabled: bool,
}
```

All fields must implement `kani::Arbitrary`. Use `#[cfg_attr(kani, ...)]` because
the `kani` crate is only available during verification.

### Manual Arbitrary implementation

```rust
#[cfg(kani)]
impl kani::Arbitrary for MyType {
    fn any() -> Self {
        let val = u8::any();
        kani::assume(val != 0 && val <= 100);
        MyType { value: val }
    }
}
```

### BoundedArbitrary for collections

`Vec<T>`, `HashMap`, and other growable types don't implement Arbitrary (unbounded
size). Use `BoundedArbitrary` or `kani::bounded_any::<T, N>()`:

```rust
#[kani::proof]
#[kani::unwind(17)]
fn check_reverse() {
    let input: Vec<bool> = kani::bounded_any::<_, 16>();
    let double_reversed = reverse(reverse(input.clone()));
    for i in 0..input.len() {
        assert_eq!(input[i], double_reversed[i]);
    }
}
```

Derive BoundedArbitrary for custom types:

```rust
#[derive(BoundedArbitrary)]
struct MyVec<T> {
    #[bounded]
    vector: Vec<T>,
    capacity: usize,
}
```

---

## 4. Function Contracts (Compositional Verification)

**Enable with:** `cargo kani -Z function-contracts`

Contracts let you decompose verification into per-function proofs and compose
the results. This is essential for verifying code with deep call chains.

### Writing contracts

```rust
#[kani::requires(min != 0 && max != 0)]
#[kani::ensures(|result| *result != 0 && max % *result == 0 && min % *result == 0)]
#[kani::recursion]  // required for recursive functions
fn gcd(mut max: u8, mut min: u8) -> u8 {
    if min > max {
        std::mem::swap(&mut max, &mut min);
    }
    let rest = max % min;
    if rest == 0 { min } else { gcd(min, rest) }
}
```

- `#[kani::requires(cond)]` -- precondition (constrains valid inputs)
- `#[kani::ensures(|result| cond)]` -- postcondition (must hold after execution)
- `#[kani::modifies(ptr)]` -- declares mutable memory the function may modify
- `#[kani::recursion]` -- required on recursive functions with contracts
- Multiple requires/ensures clauses are joined with `&&`

### Verifying contracts

```rust
#[kani::proof_for_contract(gcd)]
fn check_gcd() {
    let max: u8 = kani::any();
    let min: u8 = kani::any();
    gcd(max, min);
}
```

`proof_for_contract` automatically injects pre/postcondition checks.
For recursive functions, Kani performs **inductive verification** automatically --
it replaces the recursive call with the contract, proving the function correct
in a single step regardless of recursion depth.

### Using verified contracts as stubs

```rust
#[kani::proof]
#[kani::stub_verified(gcd)]
fn verify_token_bucket_new() {
    let size = kani::any_where(|s| *s != 0);
    let time = kani::any_where(|t| *t != 0 && *t < u64::MAX / 1_000_000);
    let bucket = TokenBucket::new(size, kani::any(), time).unwrap();
    assert!(bucket.is_valid());
}
```

`stub_verified` replaces each call to `gcd` with its proven contract abstraction.
This collapses potentially 68+ recursive unrollings into a single check+assume.

### Contracts for external functions (double-stub trick)

```rust
use external_crate::gcd;

#[kani::ensures(|result| *result < max && *result != 0)]
fn local_gcd(max: i32, min: i32) -> i32 {
    gcd(max, min) // immediate delegation
}

#[kani::proof]
#[kani::stub(gcd, local_gcd)]       // replace external with local wrapper
#[kani::stub_verified(local_gcd)]    // then use the contract
fn harness() {
    function_that_calls_gcd(kani::any());
}
```

### Composing contracts for complex systems

The recommended pattern for complex parsing/encoding pipelines:

1. **Decompose** into small, pure functions (e.g., `parse_number`, `parse_string`,
   `encode_field`)
2. **Write contracts** for each component
3. **Verify** each component independently with `proof_for_contract`
4. **Compose** in higher-level proofs using `stub_verified` to replace verified
   components with their contracts

This decomposes an intractable whole-system proof into tractable per-component
proofs. Each component is verified within its bounds, and type contracts ensure
correct composition.

---

## 5. Loop Contracts (Unbounded Proofs)

**Enable with:** `cargo kani -Z loop-contracts`

Loop contracts abstract loops via invariants, enabling unbounded verification
without setting unwind bounds.

```rust
// These feature gates are required by Kani's loop contract macros (Kani uses nightly)
#![feature(stmt_expr_attributes)]
#![feature(proc_macro_hygiene)]

#[kani::proof]
fn verify_countdown() {
    let mut x: u64 = kani::any_where(|i| *i >= 1);

    #[kani::loop_invariant(x >= 1)]
    while x > 1 {
        x = x - 1;
    }

    assert!(x == 1);
}
```

Run: `cargo kani -Z loop-contracts`

### How loop contracts work

Kani uses mathematical induction:
1. **Base case:** Assert invariant holds before first iteration
2. **Inductive step:** Assume invariant on nondeterministic state, execute one
   iteration, assert invariant still holds
3. **Post-loop:** Invariant + negated guard gives postcondition

### Historic values

- `on_entry(expr)` -- value of expr before entering the loop
- `prev(expr)` -- value of expr in previous iteration

```rust
#[kani::loop_invariant(
    (i >= 2) && (i <= 100) && (i % 2 == 0)
    && (on_entry(i) == 100) && (prev(i) == i + 2)
)]
while i > 2 {
    i = i - 2;
}
```

### kani::index for for-loops

```rust
#[kani::proof]
fn verify_sum() {
    let mut sum: u32 = 0;
    let a: [u8; 10] = kani::any();
    kani::assume(kani::forall!(|i in (0,10)| a[i] <= 20));
    #[kani::loop_invariant(sum <= (kani::index as u32 * 20))]
    for x in a {
        sum = sum + x as u32;
    }
    assert!(sum <= 200);
}
```

### Loop modifies clauses

```rust
#[kani::loop_invariant(i <= 20)]
#[kani::loop_modifies(&i, &a)]
while i < 20 {
    a[i] = 1;
    i = i + 1;
}
```

Supports raw pointers, references, and slices for specifying modified memory.

### Loop contract limitations

- Supported: `while`, `loop`, `for` (over arrays, slices, Vec, Range, iterators)
- Not supported: `while let` loops
- Kani does **not** check loop termination -- non-terminating loops with valid
  invariants may produce unsound results
- Loop contracts must be side-effect free

---

## 6. Quantifiers

**Enable with:** `cargo kani -Z quantifiers`

### Universal quantifier (forall)

```rust
#[kani::proof]
fn test_forall() {
    let v: [u8; 10] = [10; 10];
    assert!(kani::forall!(|i in (0, 10)| v[i] == 10));
}
```

### Existential quantifier (exists)

```rust
#[kani::proof]
fn test_exists() {
    let v: [u8; 5] = [1, 2, 3, 4, 5];
    assert!(kani::exists!(|i in (0, v.len())| v[i] == 3));
}
```

**Limitation:** Quantified variables are currently `usize` only. Array indexing
in quantified expressions can cause deep call stacks -- use unsafe pointer
dereference for performance:

```rust
let ptr = v.as_ptr();
unsafe {
    kani::assert(kani::forall!(|i in (0, 128)|
        *ptr.wrapping_byte_offset(i as isize) == 10
    ), "all elements are 10");
}
```

---

## 7. Concrete Playback

**Enable with:** `cargo kani -Z concrete-playback --concrete-playback=[print|inplace]`

When verification fails, Kani generates a Rust unit test with the concrete
counterexample values:

```rust
// Generated by Kani:
#[test]
fn kani_concrete_playback_proof_harness_16220658101615121791() {
    let concrete_vals: Vec<Vec<u8>> = vec![
        vec![133],       // a = 133
        vec![135, 137],  // b = 35207
    ];
    kani::concrete_playback_run(concrete_vals, proof_harness);
}
```

- `print` -- outputs to stdout
- `inplace` -- inserts into source code

Run the generated test: `cargo kani playback -Z concrete-playback -- <test_name>`

Debug with: `rust-gdb` or `lldb` on the playback binary.

---

## 8. Practical Bounds and Sweet Spots

### What works well (Kani's sweet spots)

| Pattern | Practical max | Why |
|---------|--------------|-----|
| **u64 bitmask ops (no loops)** | **Full 64-bit range** | SAT solvers natively handle bit-vectors; 2^192 combinations in seconds |
| **Enum state machines** | All N×M variant pairs | Pure branching, no loops |
| **State transition sequences** | ~8 steps | s2n-quic uses InlineVec<Op, 8> |
| **Integer arithmetic (loop-free)** | Full type range | "runs in milliseconds" for 4× i64 params |
| **Roundtrip encode/decode** | Full type range for fixed-size types | s2n-quic packet number roundtrip: 2.87s |

### What has limits

| Pattern | Practical max | Workaround |
|---------|--------------|------------|
| Simple byte-slice loop | N ≈ 100 (3.8s) | Set explicit unwind bound |
| Complex byte parsing | N ≈ 10--20 | Compositional contracts |
| Vec operations | N ≈ 8--15 | Use InlineVec or BoundedArbitrary |
| HashMap entries | N ≈ 1--5 | Use u64 bitmask instead |
| Iterator chains | Same as loop bounds | Explicit unwind |
| memchr | Requires unwind ~5 | Stub or bound |
| Dynamic dispatch targets | ≤ ~50 | Restrict trait impls |

### What does NOT work

- **Concurrency** -- compiled as sequential code; no data race detection
- **Inline assembly** -- not supported at all
- **Async/await** -- not supported
- **Full parsers** -- JSON/text parsing intractable beyond ~20 bytes
- **Temporal properties** -- no liveness/fairness (use TLA+ instead)
- **Nondeterministic-size heap collections** -- scales poorly
- **Floating point precision** -- trig functions over-approximated to [-1, 1]

---

## 9. Bolero Integration (Unified Fuzzing + Proof)

The s2n-quic pattern: single harness runs as both fuzz test and Kani proof.

```rust
#[test]
#[cfg_attr(kani, kani::proof, kani::solver(kissat), kani::unwind(9))]
fn round_trip_test() {
    bolero::check!()
        .with_type::<Ops>()
        .for_each(|ops| round_trip(ops));
}
```

Under Kani: `bolero::check!()` redirects to `kani::any()`, iterates once
(symbolic covers all). Under fuzzers: generates millions of concrete inputs.

### Input size asymmetry

Fuzzers want large inputs, Kani needs tiny ones. Use conditional compilation:

```rust
#[cfg(kani)]
type Ops = InlineVec<Op, 8>;  // stack-allocated, Kani-friendly

#[cfg(not(kani))]
type Ops = Vec<Op>;           // heap-allocated, fuzzer-friendly
```

### Key pattern: differential testing

Compare optimized vs reference implementation over all inputs:

```rust
#[test]
#[cfg_attr(kani, kani::proof, kani::solver(kissat))]
fn differential_test() {
    bolero::check!()
        .with_type::<(u32, u32)>()
        .for_each(|(a, b)| {
            let expected = reference_impl(*a, *b);
            let actual = optimized_impl(*a, *b);
            assert_eq!(expected, actual);
        });
}
```

s2n-quic caught a real bug in packet number decoding this way -- fuzz testing
with 16M+ inputs missed it, Kani found it in 2.87 seconds.

---

## 10. CI Integration

### GitHub Actions

```yaml
name: Kani CI
on: [push, pull_request]
jobs:
  run-kani:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: model-checking/kani-github-action@v1
        with:
          working-directory: crates/my-crate
          args: --tests
```

**Hardware:** Standard GitHub runners have 7GB RAM, 2 cores. For memory-intensive
proofs, use larger runners or AWS CodeBuild.

**Cadence:** Run proofs on every PR if they complete quickly. Move slow proofs
to a nightly job. Start with per-PR and split if needed.

### Cargo.toml configuration

```toml
[workspace.metadata.kani.flags]
default-unwind = "1"  # force explicit unwind bounds everywhere
```

### Running specific harnesses

```bash
cargo kani --harness my_module::tests::my_harness  # single harness
cargo kani --tests                                  # include #[test] harnesses
cargo kani -Z function-contracts                    # enable contracts
cargo kani -Z loop-contracts                        # enable loop contracts
cargo kani -Z concrete-playback --concrete-playback=print  # get counterexamples
```

---

## 11. Verification Strategy for a Data Pipeline

Based on s2n-quic patterns and Kani's documented capabilities.

### Tier 1: Immediate (high-confidence, fast proofs)

- **Bitmask operations** -- full u64 range, milliseconds
- **Null bitmap invariants** -- exhaustive for ≤64 columns
- **Duplicate-key detection** -- all combinations for ≤8 fields/row
- **Integer/float parsing** -- bounded inputs ~16 bytes
- **State machine transitions** -- all enum pairs, seconds
- **Encode/decode roundtrips** -- full type range for fixed-size types

### Tier 2: Compositional (medium effort)

- **Parser decomposition** -- contracts for sub-parsers, compose with stub_verified
- **Format conversion roundtrip** -- partial roundtrip proofs via contracts
- **Bolero unified harnesses** -- every Kani proof doubles as fuzz test

### Tier 3: Design-level (TLA+ territory)

- **Pipeline liveness** -- "data eventually flows through"
- **Fairness** -- "no input source is starved"
- **Bounded latency** -- "data sits in buffer ≤ batch_timeout"
- **Shutdown ordering** -- "all buffered data flushed before exit"

---

## 12. Common Gotchas

### Vacuous proofs from over-constraining

If `kani::assume()` eliminates all execution paths, verification succeeds
trivially. Use `kani::cover!()` to verify that interesting paths are reachable:

```rust
#[kani::proof]
fn check_something() {
    let x: u32 = kani::any();
    kani::assume(x > 100 && x < 50);  // impossible! all paths eliminated
    assert!(false);  // THIS WILL PASS -- no paths reach it
}
```

Always add cover statements for critical paths.

### Unwinding bound too low

Kani reports `unwinding assertion failure` and marks checks as "undetermined."
This is a soundness safeguard, not a proof. Increase the bound.

### Choosing the wrong solver

Performance varies by 265x across solvers. Default cadical is good but not
always best. For Kani harnesses exceeding 10s: kissat wins 47%, cadical 24%,
minisat 29% of the time (s2n-quic benchmarks).

### BoundedArbitrary proofs are incomplete

Verifying Vec operations up to size N does NOT prove correctness for size N+1.
Use multiple bounds and consider if novel bugs could lurk beyond the bound.

### Unsoundness from constrained contract harnesses

If a `proof_for_contract` harness constrains inputs beyond the requires clause
(e.g., `kani::assume(max <= 255)` when the function accepts u64), the contract
is only verified for that subset. Using `stub_verified` with broader inputs is
then unsound. Always generate fully unconstrained inputs in contract harnesses.

### Stack unwinding not supported

Kani uses abort-on-panic semantics. Cleanup/drop logic that depends on stack
unwinding is not modeled. Use MIRI for testing unwinding-related resource safety.

### Floating point over-approximation

`sin()`, `cos()`, `sqrt()` return nondeterministic values in [-1, 1] or [0, ∞).
This preserves soundness but may cause spurious failures. Stub with tighter
approximations if needed, but be aware this can introduce unsoundness on
different platforms.

---

## 13. Feature Support Summary

### Fully supported
Macros, modules, functions, structs, enums, unions, traits, generics, closures,
loops, ranges, match, Box, Rc, Arc, Pin, Copy, Clone, unsafe blocks/functions.

### Partially supported
Patterns, closure types, pointer types, trait objects, impl Trait, DSTs,
type coercions, destructors, UnsafeCell, PhantomData, some intrinsics.

### Not supported
- **Concurrency** (compiled as sequential -- no thread/async verification)
- **Inline assembly** (asm!, global_asm!)
- **Await expressions** (async/await)
- **Data race detection**
- **Pointer aliasing rules** (use MIRI)
- **Unaligned pointer dereference** (partial via `-Z uninit-checks`)
- **Stack unwinding** (abort semantics only)

---

## 14. Command-Line Quick Reference

```bash
# Basic verification
cargo kani                                    # all harnesses
cargo kani --harness <name>                   # single harness
cargo kani --tests                            # include #[test] harnesses

# Unwind control
cargo kani --default-unwind 10                # global unwind bound
cargo kani --harness <name> --unwind 20       # override harness bound

# Experimental features
cargo kani -Z function-contracts              # enable contracts
cargo kani -Z loop-contracts                  # enable loop contracts
cargo kani -Z stubbing --harness <name>       # enable stubbing (single harness)
cargo kani -Z concrete-playback --concrete-playback=print

# Performance / debugging
cargo kani --visualize                        # generate HTML trace
cargo kani --cbmc-args --unwindset label:N    # per-loop unwind override

# Concrete playback
cargo kani playback -Z concrete-playback -- <test_name>
```

---

## 15. Real-World Examples

### Firecracker (AWS) -- VMM verification
- Verified virtio descriptor chain parser for all possible guest memory
- State machine verification: `{ReadOrWriteOk, OnlyWriteOk, Invalid}`
- `gcd` verified inductively via function contracts

### s2n-quic (AWS) -- QUIC protocol
- 30+ Bolero/Kani harnesses in CI on every pull request
- Packet number encode/decode roundtrip: all inputs, 2.87s
- RTT weighted average: all u32 × u32, 44.77s
- Caught real bug in optimized decode_packet_number that 16M fuzz inputs missed
- Variable-length integer codec, stream frame fitting -- all verified

### Rust Standard Library
- CStr verification: MAX_SIZE=32 with `#[kani::unwind(33)]`
- Raw pointer arithmetic operations verification
- Numeric primitive type safety verification

### propproof (academic)
- Found 2 bugs in PROST (Protocol Buffers) via proptest→Kani harnesses
- Nearest analogue to protobuf encoding verification

### Polkadot SCALE codec
- `decode(encode(x)) == x` verified for integer types up to u256
