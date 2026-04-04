# Formal verification limits for a Rust data processing kernel

> **Status:** Historical
> **Date:** 2026-03
> **Context:** Research on Kani's practical input-size bounds and TLA+ complementary role.

**Kani can prove meaningful safety and correctness properties for roughly 60–80% of a data pipeline's pure logic, but only within strict input-size bounds — typically 10–20 bytes for complex parsing, ~8 state transitions for protocol sequences, and full 64-bit range for loop-free bitwise operations.** Temporal properties like liveness and fairness fall entirely outside Kani's reach and require TLA+ at the design level. The most productive strategy is a two-layer architecture: decompose the kernel into small, pure, independently verifiable functions (bitmask logic, field accumulation, encode/decode roundtrips), prove those with Kani using compositional contracts, and model the pipeline's scheduling and progress guarantees in TLA+. Alternative tools — particularly Verus for unsafe systems code and EverParse for binary format parsing — extend the verification frontier beyond what bounded model checking alone can achieve.

---

## Kani's hard ceilings on input size and loop depth

Kani translates Rust MIR into a SAT formula via CBMC, unrolling every loop into a flat propositional constraint. This means verification cost scales with `unwind_depth × symbolic_variables × program_paths`, and the practical ceilings are surprisingly low for anything involving iteration over symbolic data.

**Byte slice iteration** (`&[u8]`): The Kani tutorial documents that a simple initialization loop over a slice completes in **~3.8 seconds at N=100** on commodity hardware. But the tutorial immediately warns: *"It's not uncommon for some proofs to scale poorly even to 5 iterations"* for complex per-byte logic, and *"Kani often scales poorly to 'big string problems' like parsing. Often a parser will need to consume inputs larger than 10–20 characters to exhibit strange behaviors."* For a JSON parser processing symbolic bytes, **N=10–20 is the realistic ceiling**. The CStr verification effort in the Rust standard library used MAX_SIZE=32 with `#[kani::unwind(33)]` — and that's for a simple null-terminated string check, not full parsing.

**Vec operations**: Kani provides `BoundedArbitrary for Vec<T>` that creates a symbolic vector up to a compile-time constant N elements. The s2n-quic codebase — Kani's most extensive production user — uses `InlineVec<Op, 8>` (a stack-allocated, fixed-capacity vector) for its Kani harnesses, confirming that **single-digit to low-double-digit Vec sizes** are the practical bound. Heap-allocated nondeterministic-size collections are explicitly called out: *"Kani does not (yet!) scale well to nondeterministic-size data structures involving heap allocations."*

**HashMap**: Kani does implement `BoundedArbitrary for HashMap`, but the hashing logic generates enormous SAT formula complexity. The implementation uses `DefaultHasher` specifically (not the default `RandomState`), and each symbolic insert creates many control-flow paths. Expect **N ≤ ~5 entries** before solver timeouts. For duplicate-key detection, a u64 bitmask is dramatically more verification-friendly.

**u64 bitmask operations** stand out as Kani's sweet spot. For **loop-free bitwise operations, Kani verifies over the full 64-bit range** efficiently because SAT solvers natively handle bit-vector constraints. The Kani announcement blog demonstrated verification over **2^192 input combinations** (three u64 fields) completing in seconds. A proof over four i64 parameters with pure arithmetic "runs in milliseconds." However, if you loop over individual bits (e.g., `for i in 0..64`), you still need `unwind(65)` and the formula grows proportionally.

**Closures, iterators, and memchr**: Iterator combinators like `.map().filter().fold()` are syntactic sugar over loops and face identical unwind bounds. The Kani test suite uses `#[kani::unwind(4)]` for a 3-element filtered iterator and `#[kani::unwind(3)]` for a 2-element `filter_map`. For memchr, Kani issue #1767 shows that the standard library's `memchr_general_case` creates unbounded loops — verification required `--unwind 5` and took **43.95 seconds** for the s2n-quic round-trip harness. Stubbing memchr or providing tight bounds is recommended.

**Nested function calls** have no inherent depth limit because CBMC inlines everything into a flat formula. The real bottleneck is **dynamic dispatch**: when CBMC encounters a trait object, it considers all possible implementations. In Firecracker, `std::io::Error` had **314 possible function targets** for a virtual drop call, causing a **4+ hour timeout** before Kani's trait-based restrictions reduced targets.

| Pattern | Practical max | Evidence |
|---------|--------------|----------|
| Simple byte-slice loop | N ≈ 100 (3.8s) | Kani tutorial |
| Complex byte parsing | N ≈ 10–20 | Kani tutorial, explicit warning |
| Vec operations | N ≈ 8–15 | s2n-quic InlineVec<Op, 8> |
| HashMap entries | N ≈ 1–5 | BoundedArbitrary impl + complexity |
| u64 bitwise (no loops) | **Full 64-bit range** | Blog: 2^192 combinations |
| Iterator chains | Same as loop bounds | Test suite: unwind(3–4) |
| memchr | Requires explicit unwind ~5 | Issue #1767 |
| Dynamic dispatch targets | ≤ ~50 feasible | ICSE paper: 314 → timeout |

---

## What unwind bounds actually mean for soundness

The `#[kani::unwind(N)]` attribute disables automatic loop discovery and unrolls all loops up to N iterations. **N must be one more than the maximum iterations** (the extra iteration checks the exit condition). With `break`/`continue`, add 2–3 more.

When N is **too small**, Kani is not silent — it reports an **unwinding assertion failure** and marks all other properties as "undetermined." The output explicitly states: `SUMMARY: ** 1 of 187 failed (186 undetermined) — Failed Checks: unwinding assertion loop 0`. This is a critical soundness guarantee: you cannot accidentally "prove" something by under-unwinding.

When N is **too large**, the SAT formula bloats proportionally. The s2n-quic performance benchmarks show dramatic solver-dependent variation: one harness took **1,460 seconds with MiniSat but only 5.5 seconds with Kissat** (265× speedup). For harnesses exceeding 10 seconds, **Kissat is fastest 47% of the time**, CaDiCaL 24%, MiniSat 29%. Choosing the right solver matters enormously.

Kani does attempt **automatic loop unwinding** by default, but the documentation warns this "doesn't always terminate." The `--default-unwind <n>` flag sets a global bound, and per-loop bounds are available via `--cbmc-args --unwindset label:bound`. Experimental **loop invariant support** (`#[kani::loop_invariant(...)]`) can enable unbounded verification for some loops — a significant capability for avoiding the unwind ceiling entirely.

---

## JSON parsing pushes Kani to its limits

No one has published a Kani verification of a JSON parser. The Kani documentation explicitly flags parsers as problematic, and the evidence supports this: **verifying `parse_json_line(line: &[u8])` for all inputs up to 64 bytes is almost certainly intractable**. Each symbolic byte at each position creates 256 possible values, and each branch in the parser creates a new SAT formula path. Nested JSON objects multiply the cost exponentially — depth-3 nesting with symbolic keys and values would overwhelm any SAT solver.

The recommended approach is **compositional verification using function contracts**, introduced in Kani in 2024. The pattern is:

1. Verify `detect_escape(bytes: &[u8]) -> Option<usize>` for inputs up to ~20 bytes
2. Verify `parse_number(bytes: &[u8]) -> Result<(Number, usize)>` for inputs up to ~16 bytes
3. Verify `parse_string(bytes: &[u8]) -> Result<(String, usize)>` for inputs up to ~16 bytes
4. Define `#[kani::requires]` / `#[kani::ensures]` contracts for each
5. Verify the top-level parser using `#[kani::stub_verified(parse_number)]` to replace sub-parsers with their proven contracts

This decomposes an intractable whole-parser proof into tractable per-component proofs. The Kani blog describes this pattern: *"Contracts let us safely break down the verification of a complex piece of code individually by function and efficiently compose the results, driving down the cost of verifying code with long call chains."* The Firecracker `gcd` function — which could take 96 iterations recursively — was verified inductively via contracts, then used as a stub in `TokenBucket::new`.

The closest real-world parser verification is **Firecracker's virtio descriptor chain parser**, which parses exactly 3 descriptors from symbolic guest memory. It completed "in seconds" — but the structure is fixed, not variable-length text. The **propproof** paper (ESEC/FSE 2023) found 2 bugs in PROST (Protocol Buffers library) using Kani via proptest harnesses, making it the nearest analogue to protobuf encoding verification.

---

## State machines are Kani's sweet spot

Kani excels at exhaustive (state, event) pair checking. With `#[derive(kani::Arbitrary)]` on enums, `kani::any::<State>()` generates a symbolic value covering all variants. For a transition function `fn next_state(state: State, event: Event) -> (State, Action)` with finite enums, Kani explores all N×M combinations in seconds — this is pure branching logic with no loops.

For **sequences of N transitions**, s2n-quic provides the benchmark: its Kani harnesses use `InlineVec<Op, 8>` with `#[kani::unwind(9)]`, confirming **~8 operations as the practical bound** for moderately complex state machines. Each transition is symbolic (any event from any state), so 8 steps already covers significant protocol behavior.

Kani **cannot verify true temporal properties** like "if event X occurs, action Y eventually follows." It is a bounded model checker that examines finite traces. However, you can encode **bounded temporal checks** manually: apply N arbitrary symbolic events after a trigger, assert the expected action occurs within those N steps, and let Kani find counterexamples. This proves the bounded variant ("Y follows X within 8 steps") but not the unbounded liveness property.

The **Firecracker virtio state machine verification** is the most detailed published example. Kani proved that Firecracker's `parse()` correctly enforces virtio requirement 2.6.4.2 (descriptor ordering) for all possible guest memory contents. The requirement was encoded as an explicit FSM with states `{ReadOrWriteOk, OnlyWriteOk, Invalid}`, and Kani verified the state machine property held across all symbolic inputs. **s2n-quic runs 30+ Bolero/Kani harnesses in CI on every pull request**, covering packet number encoding/decoding, variable-length integer codec, stream frame fitting, and RTT estimation — all verified exhaustively.

---

## Unified Bolero harnesses bridge fuzzing and proof

Bolero provides a single harness interface that runs with LibFuzzer, AFL, Honggfuzz, or Kani. The s2n-quic pattern is the canonical example:

```rust
#[test]
#[cfg_attr(kani, kani::proof, kani::solver(kissat), kani::unwind(9))]
fn round_trip_test() {
    bolero::check!()
        .with_type::<Ops>()
        .for_each(|ops| round_trip(ops));
}
```

Under Kani, `bolero::check!()` redirects value generation to `kani::any()` and iterates `for_each` exactly once (since the symbolic value already covers all possibilities). Under fuzzers, it generates millions of concrete inputs. Bolero supports `Vec<u8>` with bounded length via `gen::<Vec<u8>>().with().len(0..=8)`, but for Kani, s2n-quic uses **`InlineVec` (stack-allocated) rather than heap `Vec`** to avoid allocation overhead in the SAT formula.

The key limitation is **input size asymmetry**: fuzzers want large inputs, Kani needs tiny ones. s2n-quic handles this with conditional compilation: `#[cfg(kani)] type Ops = InlineVec<Op, 8>; #[cfg(not(kani))] type Ops = Vec<Op>;`. Different harnesses also need different SAT solvers — s2n-quic mixes Kissat, CaDiCaL, and MiniSat depending on the harness. Despite these pragmatic compromises, the unified harness approach means every property tested by fuzzing is also proven by Kani within its bounds.

---

## Builder logic and bitmask proofs are immediately tractable

Column accumulator logic — row counts, null bitmaps, field-value tracking, duplicate-key detection — is **ideal for Kani verification today**. These are bounded, pure data-structure operations on fixed-width integers and small arrays.

**Null bitmap consistency** ("bitmap bit is set iff value is present") is directly provable. For a u64-based bitmap covering up to 64 columns, a Kani harness can symbolically choose which rows receive values, execute the accumulator logic, and assert the invariant holds for all possible sequences of `add_value` calls — covering all 2^64 bitmap states in the u64 case without loops.

**Duplicate-key detection via u64 bitmask** is trivially verified. A harness generating two symbolic field IDs (0–63) and asserting that `mark_field_seen` returns true on the second call iff the IDs match covers all **4,096 combinations** exhaustively in seconds. This extends naturally to sequences of field insertions — for up to ~8 fields per row (matching the Kani sweet spot), full exhaustive verification is practical.

Apache Arrow's Rust column builders and DataFusion's `GroupsAccumulator` both cleanly separate **logical accumulation** (tracking values, nulls, counts) from **physical array construction** (buffer allocation, memory layout). DataFusion's `NullState` template encapsulates null-handling patterns across accumulators. This architectural separation is exactly what enables Kani verification: extract the logical layer as pure functions, prove them correct, and leave physical buffer management to testing/Miri.

---

## End-to-end roundtrips require compositional decomposition

Can Kani verify "for any JSON line up to N bytes, the OTLP protobuf output correctly represents the same field values"? **Not as a monolithic proof.** The parsing stage alone limits N to ~10–20 bytes, and adding protobuf encoding doubles the formula complexity. But **partial roundtrip proofs via function contracts are feasible and powerful**.

The s2n-quic **differential testing pattern** is the gold standard. Their packet number encode/decode roundtrip was verified across all inputs in **2.87 seconds** and caught a real bug during optimization. The RTT weighted average was verified across all u32 × u32 combinations in **44.77 seconds**. The Polkadot SCALE codec project explicitly verifies `decode(encode(x)) == x` for integer types up to u256 using Kani.

For a JSON-to-OTLP pipeline, the recommended decomposition is:

- **Verify parsing independently**: prove `parse` never panics and satisfies its contract for inputs up to ~16 bytes
- **Verify encoding independently**: prove `encode(decode(bytes)) == bytes` for typed field values (integers, strings up to ~16 bytes, booleans — full value range for fixed-size types)
- **Connect via type contracts**: the intermediate `TypedFields` type serves as the compositional contract; `#[kani::stub_verified(parse)]` in the encoding proof replaces the parser with its proven specification

This gives strong guarantees — each component is proven correct within bounds, and the type system ensures they compose correctly — without requiring the SAT solver to reason about the entire pipeline at once.

---

## Pipeline liveness demands TLA+, not Kani

Kani fundamentally cannot verify liveness ("if events arrive, actions are eventually produced"), fairness ("no input is starved"), or unbounded latency guarantees. These are properties of infinite behaviors, and Kani only examines finite traces. **Bounded latency** ("data sits in buffer for at most `batch_timeout`") is the exception — it reduces to a safety property checkable within a fixed time window.

**TLA+** is the right tool for pipeline-level temporal properties. FoundationDB, CockroachDB, AWS (S3, DynamoDB, EBS), and Datadog have all used TLA+ to verify distributed system designs. Datadog's TLA+ specification of their Courier queueing system (built on FoundationDB) verified `NoLostMsg` and found real bugs during design changes. CockroachDB's Parallel Commits TLA+ specification formally verified that *"all implicitly committed transactions will eventually become explicitly committed"* — a liveness property.

TLA+ natively supports `WF_vars(Action)` (weak fairness: if an action is continuously enabled, it eventually occurs) and `SF_vars(Action)` (strong fairness). For a batch pipeline, the specification would look like: `Spec == Init ∧ □[Next]_vars ∧ WF_vars(FlushBatch)` — asserting that batch flushing eventually occurs whenever enabled. TLC, the TLA+ model checker, verifies liveness by finding strongly connected components in the state graph using Tarjan's algorithm.

The recommended architecture is a **two-layer verification strategy**: TLA+ for design-level temporal properties (progress, fairness, bounded latency under failure), and Kani for implementation-level safety and correctness (no panics, correct encoding, bitmap invariants). AWS's 2024 ACM Queue paper describes exactly this pattern across their organization, increasingly supplemented by the P programming language (more approachable than TLA+ for engineers).

---

## Kani's automatic panic and overflow detection

Every `#[kani::proof]` harness automatically checks for **panics** (including `unwrap()` on `None`/`Err`), **arithmetic overflow** (addition, subtraction, multiplication, bit-shifting), **division by zero**, **out-of-bounds array/slice access**, **null pointer dereference**, and **pointer-outside-object-bounds** for unsafe code. No explicit assertions are needed — if any reachable path triggers these conditions, Kani reports failure with the exact check that failed.

This means a harness as simple as `let input: [u8; 16] = kani::any(); my_function(&input);` proves "this function never panics for any 16-byte input" — covering all 2^128 possible inputs. The Kani tutorial states: *"If Kani had run successfully on this harness, this amounts to a mathematical proof that there is no input that could cause a panic."*

**Bounded memory usage** is not directly provable — Kani has no built-in `assert!(total_allocated <= M)`. However, you can verify memory bounds indirectly by wrapping allocation calls in functions that assert size limits, or by using ghost state to track cumulative allocations. Kani's abstract machine uses a configurable maximum allocation size (default 2^47 bytes), so any allocation exceeding this triggers a verification failure.

---

## When Kani isn't enough: the alternative tool landscape

**Verus** (Microsoft Research/CMU) is the most capable alternative for a data processing kernel. It supports **full functional correctness including unsafe Rust** and has verified a memory allocator (mimalloc-based), OS page tables, a distributed key-value store (IronKV), Kubernetes controllers (Anvil — OSDI 2024 Best Paper), and confidential VM security modules (VeriSMo — also OSDI 2024 Best Paper). Critically for this kernel, **Vest** (USENIX Security 2025) is a Verus-verified parsing and serialization library for Rust — directly applicable to format parsing. Verus verifies code **3–61× faster** than comparable tools and uses Rust syntax for specifications. The annotation burden is medium-high (~5:1 proof-to-code ratio for complex systems) but lower than Dafny/F* equivalents.

**EverParse** (Microsoft Research) generates verified parsers from format specifications, deployed in production in Azure's Virtual Switch (parsing every network packet). It verifies memory safety, functional correctness, non-malleability, and absence of double-fetches. However, it targets **binary TLV formats** (TLS, QUIC, ASN.1, CBOR), not text formats like JSON or CRI logs. EverCBOR now generates verified Rust code, and EverCDDL supports a `--rust` output option.

**Flux** (UCSD) adds refinement types to Rust — expressing "returns value in range [0, input.len()]" directly as a type annotation like `i32{v: 0 <= v && v <= n}`. Its liquid inference **automatically synthesizes loop invariants**, slashing annotation overhead to near-zero (vs ~14% for Prusti). The TickTock project verified **22K lines of Tock OS** (used in Google's security chip) and found **7 previously unknown bugs**, 6 breaking process isolation. For a data kernel, Flux could type-check array index bounds and field-count invariants with minimal annotation.

**Creusot** (Inria) translates Rust to Why3 for full deductive verification. Its crown jewel is **CreuSAT**, a fully verified CDCL SAT solver — "the largest published verification effort of Rust code" at time of publication. Creusot recently published a framework for specifying and reasoning about Rust iterators in first-order logic, directly relevant to pipeline transform chains.

**Prusti** (ETH Zurich) offers pre/postcondition proofs via the Viper infrastructure. It auto-checks panics and overflow and handles loops with invariants. However, it's limited to safe Rust, and no evidence exists of parser or string-processing verification. Annotation overhead averages ~14% of code size.

**MIRI** complements all of these as a dynamic UB detector. It catches aliasing violations (Stacked Borrows/Tree Borrows), data races, memory leaks, and type invariants on concrete test inputs with zero annotation. It cannot prove functional correctness but covers undefined behavior categories that Kani doesn't model.

---

## How much of a data kernel can realistically be verified

Based on the evidence from verified systems projects, the Rust standard library verification effort, and tool capabilities as of 2025:

**~80–90% of pure data transformation logic** (field filtering, renaming, enrichment, type coercion, bitmask operations) can be verified for safety and bounded correctness using Kani with function contracts. These are the highest-value, lowest-effort targets.

**~90%+ of binary parsing/encoding** can be verified using EverParse (for binary formats) or Kani + Verus (for Rust implementations), at least within bounded input sizes. Text parsing (JSON, CRI logs) is harder — expect verification coverage for inputs up to ~16–20 bytes, with compositional proofs covering sub-components.

**~40–60% of buffer management and scheduling logic** can be verified for safety properties (no panics, no overflow, memory bounds). Liveness and fairness require TLA+ modeling at the design level.

**~0% of I/O integration code** (network reads, file system, system calls) falls within the verification frontier. This is the trusted computing base.

No fully verified data pipeline or ETL system exists anywhere. The closest analogues are **IronKV** (verified distributed key-value store in Dafny, SOSP 2015), **VeriBetrFS** (verified Bε-tree key-value store, OSDI 2020), and **EverParse** (verified parsing deployed in Azure). A formally verified data processing kernel — even partially verified — would be a novel contribution to the field.

**Micro-transforms** (field filter, rename, static enrichment) are excellent first verification targets. These are pure functions on bounded record types with simple, expressible specifications. A Kani harness with `kani::any()` input records can prove no-panic safety, output-schema correctness, and value preservation. For expression evaluation, bounded AST depth (≤ ~10 nodes) is tractable; algebraic properties like `filter(filter(x, p), q) == filter(x, p ∧ q)` can be checked for bounded cases. Creusot's verified iterator framework could extend these proofs to unbounded transforms.

---

## Conclusion

The practical verification strategy for a Rust data processing kernel has three tiers. **Tier 1** (immediate, high-confidence): use Kani to prove no-panic safety and correctness for all bitmask operations (full u64 range), null bitmap logic (exhaustive for ≤64 columns), duplicate-key detection, integer/float parsing for bounded inputs (~16 bytes), and state machine transition invariants (all enum pairs). **Tier 2** (compositional, medium effort): decompose JSON parsing and protobuf encoding into sub-components with function contracts, verify each independently for bounded inputs, and compose using `stub_verified`. Write Bolero unified harnesses so every Kani proof also runs as a fuzz test. **Tier 3** (design-level): model the pipeline's batching, timeout, and progress guarantees in TLA+ to prove liveness and fairness properties that no implementation-level tool can reach.

The critical insight is that **Kani's value scales inversely with function complexity** — verify many small, pure functions rather than few large ones. The s2n-quic project, with 30+ Kani proofs running in CI, demonstrates this philosophy works in production. The kernel's architecture should be designed to maximize the surface area of pure, bounded, independently verifiable functions — making formal verification not just possible, but practical.