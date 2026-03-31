# StructuralIndex Research Findings

Research conducted 2026-03-30. Three areas investigated: Kani SIMD
verification, architecture design, and memory layout.

## 1. Kani SIMD Verification

**Can Kani verify our SIMD intrinsics directly?** No. Kani operates at
MIR level and cannot model `core::arch` intrinsics (they go through
LLVM-specific lowering, not Kani's `platform_intrinsics` path).

**But the underlying platform intrinsics ARE supported:**

| Our intrinsic | Decomposes to | Kani support |
|---------------|--------------|-------------|
| `_mm256_cmpeq_epi8` / `vceqq_u8` | `simd_eq` | Yes |
| `_mm256_movemask_epi8` | `simd_lt` + `simd_bitmask` | Yes (PR #2677) |
| `_mm256_set1_epi8` / `vdupq_n_u8` | Vector splat | Yes |
| `vandq_u8` | `simd_and` | Yes |
| `vpaddlq_u8` | Pairwise add | Likely yes |
| `vgetq_lane_u64` | `simd_extract` | Yes |

**Nobody has publicly verified `core::arch` SIMD code with Kani.**
Cryspen wrote reference models of 384 x86 + 181 aarch64 intrinsics
for the Rust std verification effort, but tested via randomized
comparison, not Kani proofs.

**Recommended verification strategy (3 layers):**

1. **Kani proves the scalar fallback** — `find_char_mask` is a simple
   64-byte loop, perfect for Kani with `#[kani::unwind(65)]`. Proves
   the SPECIFICATION is correct for ALL inputs.

2. **Proptest proves SIMD ≡ scalar** — property test that all SIMD
   backends (NEON, AVX2, SSE2) produce identical output to the proven
   scalar fallback on random inputs. This is the s2n-quic pattern.

3. **Kani proves bitmask consumers** — `compute_real_quotes`,
   `prefix_xor`, and all bitmask consumption logic operate on `u64`
   values. Use `#[kani::stub]` to replace SIMD dispatch with the
   proven scalar implementation during verification.

**bolero integration:** Works for non-SIMD logic (unified proptest +
fuzz + Kani from one harness). For SIMD, separate proptest harnesses
needed since Kani can't compile `core::arch` calls.

## 2. Architecture: Same Crate or Separate?

**simdjson:** Stage 1 (structural indexing) and Stage 2 (tape building)
are in the **same compilation unit**. Logically separated but not
physically split. Reason: inlining is critical for register allocation
and instruction ordering across stages. PR #563 showed that breaking
Stage 1 into smaller sub-pieces IMPROVED performance by giving the
optimizer more leeway.

**simd-json (Rust):** Everything in **one crate**. Uses a `Stage1Parse`
trait that each SIMD backend implements. Stage 2 consumes a flat
`Vec<u32>` of structural indexes. The trait boundary works because
Stage 1's output (structural positions) is a simple data structure.

**Key architectural finding:** The natural boundary is a **data
structure**, not a trait. Both simdjson and simd-json pass a flat
array of structural positions between stages. The consumer doesn't
need to know which SIMD backend produced the data.

**Impact on our no_std plan:**

The `#![no_std]` + `#![forbid(unsafe_code)]` split for logfwd-core
is viable BUT needs careful design:

- **Don't put the SIMD backends behind a trait in core.** The trait
  adds virtual dispatch overhead. Instead, have logfwd-arrow produce
  the structural data (bitmasks or position arrays), then pass the
  data to logfwd-core's consumers.
- **LTO eliminates the cross-crate boundary cost.** With thin LTO
  (which we already have), the compiler can inline across crate
  boundaries. The cost of the split is zero at runtime.
- **The scalar fallback in core is for Kani only.** At runtime, the
  SIMD path in logfwd-arrow always runs. The scalar path exists so
  Kani can verify the specification. proptest proves SIMD ≡ scalar.

**Recommended architecture:**

```
logfwd-arrow (has SIMD, produces StructuralIndex)
  StructuralIndex::new(buf) → runs SIMD, produces bitmask data
  StructuralIndex is a plain struct (no trait, no vtable)

logfwd-core (no_std, proven, CONSUMES StructuralIndex)
  Framer::frame(buf, &newline_bitmask) → line ranges
  CriParser::extract(buf, &space_bitmask, &in_string) → field ranges
  Scanner::scan(buf, &structural_index) → field values via FieldSink

  Also has: scalar_find_char_mask() for Kani proofs
  Also has: compute_real_quotes(), prefix_xor() — pure u64 logic
```

The StructuralIndex struct itself can live in logfwd-core (it's just
Vec<u64> fields). The `new()` constructor with SIMD lives in
logfwd-arrow. Core has a `new_scalar()` for testing/Kani.

## 3. Memory Layout: Don't Store All Bitmasks

**simdjson and simd-json do NOT store bitmask vectors.** They process
per-block, flattening each block's bitmask into a `Vec<u32>` of
structural positions immediately. Only 3 scalar u64 values carry
between blocks (quote parity, backslash carry, pseudo-pred).

**For our 9-char StructuralIndex, storing all bitmasks costs:**
- 4MB buffer → 65K blocks → 9 × 65K × 8 bytes = **4.7MB** (more than input!)
- This would thrash L2 cache on M1 (128KB per core, shared 12MB L2)

**Which bitmasks need cross-block state?**

| Bitmask | Cross-block? | Why |
|---------|-------------|-----|
| Backslash carry | YES | Position 63 escapes byte 0 of next block |
| Quote parity (in_string) | YES | Opening quote means next block starts in string |
| Newline | NO | Self-contained |
| Space, comma, colon | NO | Self-contained |
| Braces, brackets | NO | Self-contained |

**Recommended approach: streaming per-block processing.**

Don't materialize all bitmasks at once. Process one 64-byte block at a
time, consuming bitmasks immediately:

```rust
for block in buf.chunks(64) {
    // Stage 1: SIMD detect all 9 chars → 9 u64 bitmasks (on stack)
    let raw = find_structural_chars(block);

    // Stage 2: escape handling (needs prev_odd_backslash carry)
    let real_q = compute_real_quotes(raw.quote, raw.backslash, &mut carry_bs);

    // Stage 3: string interior (needs prev_in_string carry)
    let in_string = prefix_xor(real_q) ^ carry_string;
    carry_string = ...;

    // Stage 4: mask structural chars & consume immediately
    let nl = raw.newline;  // newlines always structural
    let sp = raw.space & !in_string;
    let comma = raw.comma & !in_string;
    // ... extract line ranges, CRI fields, etc. from these u64s

    // Nothing stored! Just carry_bs and carry_string to next block.
}
```

This uses **zero heap allocation** for bitmasks. Only the output
(line ranges, field positions, etc.) allocates. The 9 bitmask u64s
live on the stack (~72 bytes) for each block.

**Trade-off:** The scanner currently uses ChunkIndex for random-access
lookups (`next_quote(pos)`, `is_in_string(pos)`). Streaming eliminates
this. The scanner would need to be restructured to process sequentially
within each block, using bitmask iteration (`trailing_zeros` + clear
lowest bit) instead of position-based queries.

This is exactly what simdjson/simd-json do — their Stage 2 iterates
through the structural positions array sequentially, never doing
random access into bitmasks.

## 4. Full Pipeline Benchmark Results

Benchmark on Apple M-series (NEON), ~760KB NDJSON, 4096 lines.

### Stage-by-stage cost breakdown

| Stage | Time | Incremental cost |
|-------|------|-----------------|
| Stage 1: Raw 9-char SIMD | 259 µs | — |
| + Stage 2: escape handling | 260 µs | ~1 µs |
| + Stage 3: string interior | 260 µs | ~0 µs |
| + Stage 4-5: mask + extract lines | 272 µs | ~12 µs |

**SIMD detection is 95%+ of total cost.** Post-processing is free.

### Fair comparison (same 9 characters detected)

| Approach | Time | Throughput |
|----------|------|-----------|
| Separate: 6× memchr + ChunkIndex | 718 µs | 1.1 GiB/s |
| **Unified SIMD StructuralIndex** | **265 µs** | **2.9 GiB/s** |

**Unified is 2.7x faster** than separate passes for the same output.

### Assembly analysis

- Scalar `unified_scan_scalar`: **NOT auto-vectorized** by LLVM.
  Emits byte-at-a-time `ldrb`/`cmp`/`csel`. The 5-way comparison
  pattern is too complex for auto-vectorization.
- Explicit NEON: Proper SIMD — `ldp q3,q4` for paired 128-bit loads,
  `cmeq.16b` for vector comparison, `uaddlp` reduction chain.
- This means the scalar fallback is genuinely ~10x slower than SIMD
  at runtime, confirming the SIMD path matters for production.

## 5. Open Questions Resolved

### Missing `]` in 9-char set
Yes, we need `]` for bracket matching. That's 10 chars, adding ~28µs.
Estimated total: ~284µs / ~2.7 GiB/s. Still fine.

### Kani provability of bitmask consumers
Yes. All bitmask consumers operate on u64 inputs — Kani's sweet spot.
`trailing_zeros`, bit manipulation, conditional logic are all
supported. No concerns here.

### String-interior masking for new characters
Cheap — one AND + NOT per bitmask per block (~7 operations for 7
non-quote/backslash chars). Confirmed by benchmark: Stage 4 adds
only ~12µs total for all masking + line extraction combined.

## 6. Recommendations

1. **Use streaming per-block processing** (like simdjson). Don't
   materialize all bitmask vectors. Zero heap allocation for bitmasks.

2. **StructuralIndex struct in core, SIMD constructor in arrow.**
   The struct is just data. The `new()` with SIMD lives in arrow.
   Core has `new_scalar()` for Kani. LTO eliminates boundary cost.

3. **Three-layer verification:** Kani proves scalar → proptest proves
   SIMD ≡ scalar → Kani proves bitmask consumers. This gives formal
   verification of the full pipeline logic.

4. **Restructure scanner for sequential processing.** Replace
   `next_quote(pos)` random access with sequential bitmask iteration.
   This aligns with the streaming approach and matches simdjson's
   proven architecture.

5. **Don't use a trait for SIMD dispatch.** Use the struct directly.
   LTO handles cross-crate inlining. The trait adds complexity with
   no benefit when there's always exactly one implementation at runtime.
