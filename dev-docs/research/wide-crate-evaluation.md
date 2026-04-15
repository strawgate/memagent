# `wide` Crate Evaluation for SIMD Character Detection

> **Status:** Completed
> **Date:** 2026-03-31
> **Context:** Evaluated `wide` crate for portable SIMD; decision made to adopt it.

Evaluated 2026-03-31 for replacing hand-rolled `std::arch` intrinsics
in logfwd's structural character detection.

## Decision: Use `wide` for portable SIMD

Replaces ~150 lines of unsafe platform-specific code (NEON, AVX2, SSE2
modules) with ~20 lines of safe, portable code. Same generated assembly.

## What `wide` is

A portable SIMD library by Lokathor. Provides `u8x16`, `u8x32` types
that compile to native SIMD on x86_64 (SSE2/AVX2), aarch64 (NEON),
and WASM (SIMD128). Scalar fallback on other targets.

- **Version**: 1.2.0, edition 2024
- **MSRV**: 1.89
- **License**: Zlib OR Apache-2.0 OR MIT
- **Dependencies**: `bytemuck` (direct), `safe_arch` (x86 only)
- **`no_std`**: Yes
- **GitHub**: https://github.com/Lokathor/wide (~484 stars)

## How it maps to our use case

Our pattern: load 64 bytes, compare against needle, extract u64 bitmask.

```rust
use wide::u8x16;

/// Detect one character in a 64-byte block. Returns u64 bitmask.
#[inline(always)]
fn mask64(block: &[u8; 64], needle: u8) -> u64 {
    let n = u8x32::splat(needle);
    let lo = u8x32::from(*<&[u8; 32]>::try_from(&block[..32]).unwrap());
    let hi = u8x32::from(*<&[u8; 32]>::try_from(&block[32..]).unwrap());
    (lo.simd_eq(n).to_bitmask() as u64) | ((hi.simd_eq(n).to_bitmask() as u64) << 32)
}

/// Detect all 10 structural characters. One function, all platforms.
pub fn find_structural_chars(block: &[u8; 64]) -> RawBlockMasks {
    RawBlockMasks {
        newline:       mask64(block, b'\n'),
        space:         mask64(block, b' '),
        quote:         mask64(block, b'"'),
        backslash:     mask64(block, b'\\'),
        comma:         mask64(block, b','),
        colon:         mask64(block, b':'),
        open_brace:    mask64(block, b'{'),
        close_brace:   mask64(block, b'}'),
        open_bracket:  mask64(block, b'['),
        close_bracket: mask64(block, b']'),
    }
}
```

This replaces three separate modules: `simd_neon` (~55 lines unsafe),
`simd_avx2` (~30 lines unsafe), `simd_sse2` (~40 lines unsafe).

## Platform dispatch

**Compile-time only.** No runtime `is_x86_feature_detected!()`.

| Platform | `u8x16` | `u8x32` |
|----------|---------|---------|
| x86_64 + AVX2 | SSE2 `m128i` | AVX2 `m256i` (native 256-bit) |
| x86_64 (default) | SSE2 `m128i` | **2× SSE2** (automatic fallback) |
| aarch64 | NEON `uint8x16_t` | **2× NEON** (no native 256-bit) |
| WASM | `v128` | 2× `v128` |
| Other | `[u8; 16]` scalar | `[u8; 32]` scalar |

**Key implication**: On aarch64, `u8x32` is always two `u8x16` ops.
This is fine — our current hand-rolled NEON code does the same thing
(4 × 16-byte loads for 64 bytes). No performance difference.

**For x86_64**: Compile with `-C target-cpu=native` or
`-C target-feature=+avx2` to get native 256-bit operations. Without
these flags, `u8x32` silently degrades to two SSE2 operations (still
correct, just slower). Our release profile already uses `target-cpu=native`
for deployment builds.

## API details

### Loading data
```rust
// From fixed array (zero-copy cast via bytemuck)
let v = u8x32::from([u8; 32]);

// From slice reference
let v = u8x32::from(*<&[u8; 32]>::try_from(&slice[..32]).unwrap());

// Broadcast single value
let needle = u8x32::splat(b'"');
```

### Comparison
```rust
// simd_eq is an inherent method, no trait import needed

let mask = data.simd_eq(needle);  // returns u8x32 (0xFF matched, 0x00 not)
```

### Bitmask extraction
```rust
let bits: u32 = mask.to_bitmask();  // u32 for both u8x16 AND u8x32
```

**Gotcha**: `to_bitmask()` returns `u32` for both `u8x16` and `u8x32`.
For `u8x16`, only bits 0..15 are meaningful. Be careful with
`trailing_zeros()` — if no bits are set, `u32::trailing_zeros()`
returns 32, not 16. (GitHub issue #165)

### No `u8x64`
There is no `u8x64` type for AVX-512. Not needed for our 64-byte
blocks since we process them as 2 × 32-byte halves regardless.

## Performance

### Assembly quality
`safe_arch` functions are all `#[inline(always)]` and are 1:1 wrappers
around `std::arch` intrinsics. `wide` methods are `#[inline]`. With
release optimizations, the generated assembly is identical to
hand-rolled intrinsics.

On x86_64 + AVX2:
```
wide:   u8x32::simd_eq() → _mm256_cmpeq_epi8 → vpcmpeqb
hand:   _mm256_cmpeq_epi8() → vpcmpeqb
```
Same instruction, same registers, same codegen.

On aarch64 NEON, `to_bitmask()` uses `vqtbl1q_u8` + `vaddvq_u16`
(a different reduction than our hand-rolled `vpaddl` chain, but
equivalent output and similar performance).

### No runtime multiversioning
`wide` does NOT support building a fat binary with runtime AVX2
detection. If we need this in the future, options:
- Compile two versions of the function with different `target_feature`
  attributes and dispatch with `is_x86_feature_detected!("avx2")`
- Use the `multiversion` crate (but issue #238 notes compatibility
  problems with `wide`)
- Accept SSE2-only for generic x86_64 builds (still faster than scalar)

For logfwd, we compile for known targets (`target-cpu=native` for
deployment, cross-compile for specific architectures), so this is
not a practical concern.

## Verification (Kani)

Same story as hand-rolled intrinsics: Kani cannot verify SIMD codepaths
(they're opaque compiler intrinsics regardless of wrapper). Our
verification strategy is unchanged:

1. **Kani proves `find_structural_chars_scalar`** (pure Rust, no SIMD)
2. **proptest proves `find_structural_chars` ≡ `_scalar`** (SIMD output
   matches scalar for random inputs)
3. **Kani proves `StreamingClassifier::process_block`** (pure u64 logic)

`wide` does not help or hurt Kani. The scalar fallback remains the
proven specification.

## `safe_arch` as alternative

`safe_arch` (by the same author) provides safe wrappers around
individual x86 intrinsics. We could use it directly for just the 4
operations we need. BUT:

- x86-only — no aarch64/NEON support (we'd still hand-roll NEON)
- Lower-level API (raw intrinsic names like `cmp_eq_mask_i8_m128i`)
- Same MSRV (1.89)

Since we target both x86_64 and aarch64, `wide` is strictly better.

## Comparison: before and after

### Before (hand-rolled, ~150 lines unsafe)
```text
structural.rs:
  mod simd_neon     — 55 lines, unsafe, NEON intrinsics
  mod simd_avx2     — 30 lines, unsafe, AVX2 intrinsics
  mod simd_sse2     — 40 lines, unsafe, SSE2 intrinsics
  fn find_structural_chars() — 15 lines, #[cfg] dispatch

chunk_classify.rs:
  mod aarch64_impl  — 70 lines, unsafe, NEON intrinsics
  mod x86           — 75 lines, unsafe, AVX2 + SSE2
  fn find_quotes_and_backslashes() — 15 lines, #[cfg] dispatch
```

### After (wide, ~25 lines safe)
```text
structural.rs:
  fn mask64()       — 6 lines, safe
  fn find_structural_chars() — 15 lines, safe, all platforms
```

Total unsafe SIMD code eliminated: ~285 lines → 0 lines.

## Known issues / caveats

1. **MSRV 1.89** — verify CI toolchain supports this
2. **`u8x32` without AVX2 = 2× SSE2** — not an error, just slower
3. **`to_bitmask()` returns `u32`** — watch `trailing_zeros()` edge case
4. **No runtime dispatch** — must choose target features at compile time
5. **20 open GitHub issues** — none are blockers for byte scanning
6. **Not used by any JSON parsers** — we'd be the first for this use case
   (faer uses it for linear algebra, bioinformatics projects for sequence
   alignment)

## Real-world users

- **faer** — production linear algebra library (flagship user)
- Bioinformatics researchers (sequence alignment)
- Various numeric/compression projects

## Alternatives considered and rejected

| Library | Why rejected |
|---------|-------------|
| `std::simd` | Nightly-only, indefinitely |
| `pulp` | No `cmpeq → bitmask` for u8. Designed for f32/f64 math |
| `safe_arch` | x86 only, no NEON. Same author as `wide` |
| `simdeez` | Barely used, uncertain maintenance |
| `packed_simd2` | Nightly-only, abandoned |
| `macerator` | Pre-1.0, too immature |
| memchr internals | Private API, not exposed |
| simd-json | Hand-rolls everything (no library) |
