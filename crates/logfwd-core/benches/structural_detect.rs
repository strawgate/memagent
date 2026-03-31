//! Benchmark: unified SIMD structural character detection vs separate passes.
//!
//! Compares three approaches for finding structural characters in log buffers:
//! 1. Separate passes: memchr for newlines, then ChunkIndex for quotes/backslashes
//! 2. Unified pass: one scan producing bitmasks for all characters at once
//! 3. Hybrid: newlines+spaces in pass 1, quotes+backslashes in pass 2
//!
//! See: https://github.com/strawgate/memagent/issues/313

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use logfwd_core::chunk_classify::ChunkIndex;

// ===========================================================================
// Data generators
// ===========================================================================

/// Generate NDJSON buffer (~4K lines, ~4MB).
fn gen_ndjson(line_count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(line_count * 256);
    for i in 0..line_count {
        let line = format!(
            r#"{{"level":"INFO","msg":"handling request number {}","path":"/api/v1/users/{}","status":{},"duration_ms":{:.2},"trace_id":"abc{}def","nested":{{"key":"value with \"escapes\" and \\backslashes"}}}}"#,
            i,
            i,
            if i % 10 == 0 { 500 } else { 200 },
            0.5 + (i as f64) * 0.1,
            i
        );
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
    }
    buf
}

/// Generate CRI-formatted buffer (~4K lines, ~4MB).
fn gen_cri(line_count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(line_count * 256);
    for i in 0..line_count {
        let line = format!(
            r#"2024-01-15T10:30:{:02}.{:09}Z {} F {{"level":"INFO","msg":"request {}","status":{}}}"#,
            i % 60,
            i % 1_000_000_000,
            if i % 3 == 0 { "stderr" } else { "stdout" },
            i,
            if i % 10 == 0 { 500 } else { 200 },
        );
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
    }
    buf
}

// ===========================================================================
// Approach 1: Separate passes (current architecture)
// ===========================================================================

/// Pass 1: find all newline positions with memchr.
fn find_newlines_memchr(buf: &[u8]) -> Vec<usize> {
    memchr::memchr_iter(b'\n', buf).collect()
}

/// Pass 2: ChunkIndex for quotes/backslashes (existing).
fn classify_chunk_index(buf: &[u8]) -> ChunkIndex {
    ChunkIndex::new(buf)
}

// ===========================================================================
// Approach 2: Unified pass — all structural characters in one scan
// ===========================================================================

/// Result of a unified structural scan over a 64-byte block.
struct UnifiedBlock {
    newline_bits: u64,
    space_bits: u64,
    quote_bits: u64,
    backslash_bits: u64,
    comma_bits: u64,
}

/// Scalar unified scan: find all 5 characters in one pass over a 64-byte block.
/// LLVM will auto-vectorize this on most targets.
fn unified_scan_scalar(data: &[u8; 64]) -> UnifiedBlock {
    let mut newline_bits: u64 = 0;
    let mut space_bits: u64 = 0;
    let mut quote_bits: u64 = 0;
    let mut backslash_bits: u64 = 0;
    let mut comma_bits: u64 = 0;

    for i in 0..64 {
        let b = data[i];
        let bit = 1u64 << i;
        if b == b'\n' {
            newline_bits |= bit;
        }
        if b == b' ' {
            space_bits |= bit;
        }
        if b == b'"' {
            quote_bits |= bit;
        }
        if b == b'\\' {
            backslash_bits |= bit;
        }
        if b == b',' {
            comma_bits |= bit;
        }
    }

    UnifiedBlock {
        newline_bits,
        space_bits,
        quote_bits,
        backslash_bits,
        comma_bits,
    }
}

/// Full unified scan over an entire buffer.
fn unified_scan_buffer(buf: &[u8]) -> Vec<UnifiedBlock> {
    let num_blocks = buf.len().div_ceil(64);
    let mut results = Vec::with_capacity(num_blocks);

    for block_idx in 0..num_blocks {
        let offset = block_idx * 64;
        let remaining = buf.len() - offset;

        let block: [u8; 64] = if remaining >= 64 {
            buf[offset..offset + 64].try_into().unwrap()
        } else {
            let mut padded = [b' '; 64];
            padded[..remaining].copy_from_slice(&buf[offset..]);
            padded
        };

        results.push(unified_scan_scalar(&block));
    }
    results
}

// ===========================================================================
// Approach 3: Hybrid — two specialized passes
// ===========================================================================

/// Pass 1: newlines + spaces (framing characters).
fn framing_scan_scalar(data: &[u8; 64]) -> (u64, u64) {
    let mut newline_bits: u64 = 0;
    let mut space_bits: u64 = 0;

    for i in 0..64 {
        let b = data[i];
        let bit = 1u64 << i;
        if b == b'\n' {
            newline_bits |= bit;
        }
        if b == b' ' {
            space_bits |= bit;
        }
    }
    (newline_bits, space_bits)
}

/// Pass 2: quotes + backslashes (classification characters).
fn classify_scan_scalar(data: &[u8; 64]) -> (u64, u64) {
    let mut quote_bits: u64 = 0;
    let mut backslash_bits: u64 = 0;

    for i in 0..64 {
        let b = data[i];
        let bit = 1u64 << i;
        if b == b'"' {
            quote_bits |= bit;
        }
        if b == b'\\' {
            backslash_bits |= bit;
        }
    }
    (quote_bits, backslash_bits)
}

fn hybrid_scan_buffer(buf: &[u8]) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
    let num_blocks = buf.len().div_ceil(64);
    let mut framing = Vec::with_capacity(num_blocks);
    let mut classify = Vec::with_capacity(num_blocks);

    for block_idx in 0..num_blocks {
        let offset = block_idx * 64;
        let remaining = buf.len() - offset;

        let block: [u8; 64] = if remaining >= 64 {
            buf[offset..offset + 64].try_into().unwrap()
        } else {
            let mut padded = [b' '; 64];
            padded[..remaining].copy_from_slice(&buf[offset..]);
            padded
        };

        framing.push(framing_scan_scalar(&block));
        classify.push(classify_scan_scalar(&block));
    }
    (framing, classify)
}

// ===========================================================================
// Approach 4: SIMD unified — extend NEON/AVX2 to detect 5 characters
// ===========================================================================

/// Unified SIMD scan result for a 64-byte block.
struct SimdUnifiedBlock {
    newline_bits: u64,
    space_bits: u64,
    quote_bits: u64,
    backslash_bits: u64,
    comma_bits: u64,
}

#[cfg(target_arch = "aarch64")]
mod simd_unified {
    use super::SimdUnifiedBlock;
    use std::arch::aarch64::*;

    #[inline(always)]
    unsafe fn movemask16(cmp: uint8x16_t) -> u16 {
        unsafe {
            const MASK: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
            let mask = vld1q_u8(MASK.as_ptr());
            let bits = vandq_u8(cmp, mask);
            let p16 = vpaddlq_u8(bits);
            let p32 = vpaddlq_u16(p16);
            let p64 = vpaddlq_u32(p32);
            let lo = vgetq_lane_u64(p64, 0) as u8;
            let hi = vgetq_lane_u64(p64, 1) as u8;
            (lo as u16) | ((hi as u16) << 8)
        }
    }

    #[inline(always)]
    unsafe fn extract_mask(
        c0: uint8x16_t,
        c1: uint8x16_t,
        c2: uint8x16_t,
        c3: uint8x16_t,
        needle: uint8x16_t,
    ) -> u64 {
        unsafe {
            (movemask16(vceqq_u8(c0, needle)) as u64)
                | ((movemask16(vceqq_u8(c1, needle)) as u64) << 16)
                | ((movemask16(vceqq_u8(c2, needle)) as u64) << 32)
                | ((movemask16(vceqq_u8(c3, needle)) as u64) << 48)
        }
    }

    #[target_feature(enable = "neon")]
    pub unsafe fn find_5chars_neon(data: &[u8; 64]) -> SimdUnifiedBlock {
        unsafe {
            let c0 = vld1q_u8(data.as_ptr());
            let c1 = vld1q_u8(data[16..].as_ptr());
            let c2 = vld1q_u8(data[32..].as_ptr());
            let c3 = vld1q_u8(data[48..].as_ptr());

            SimdUnifiedBlock {
                newline_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'\n')),
                space_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b' ')),
                quote_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'"')),
                backslash_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'\\')),
                comma_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b',')),
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod simd_unified {
    use super::SimdUnifiedBlock;
    use std::arch::x86_64::*;

    #[inline(always)]
    unsafe fn extract_mask_avx2(lo: __m256i, hi: __m256i, needle: __m256i) -> u64 {
        unsafe {
            let q0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, needle)) as u32 as u64;
            let q1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, needle)) as u32 as u64;
            q0 | (q1 << 32)
        }
    }

    #[target_feature(enable = "avx2")]
    pub unsafe fn find_5chars_avx2(data: &[u8; 64]) -> SimdUnifiedBlock {
        unsafe {
            let lo = _mm256_loadu_si256(data.as_ptr() as *const __m256i);
            let hi = _mm256_loadu_si256(data[32..].as_ptr() as *const __m256i);

            SimdUnifiedBlock {
                newline_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'\n' as i8)),
                space_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b' ' as i8)),
                quote_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'"' as i8)),
                backslash_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'\\' as i8)),
                comma_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b',' as i8)),
            }
        }
    }
}

fn simd_unified_scan_buffer(buf: &[u8]) -> Vec<SimdUnifiedBlock> {
    let num_blocks = buf.len().div_ceil(64);
    let mut results = Vec::with_capacity(num_blocks);

    for block_idx in 0..num_blocks {
        let offset = block_idx * 64;
        let remaining = buf.len() - offset;

        let block: [u8; 64] = if remaining >= 64 {
            buf[offset..offset + 64].try_into().unwrap()
        } else {
            let mut padded = [b' '; 64];
            padded[..remaining].copy_from_slice(&buf[offset..]);
            padded
        };

        #[cfg(target_arch = "aarch64")]
        {
            results.push(unsafe { simd_unified::find_5chars_neon(&block) });
        }
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                results.push(unsafe { simd_unified::find_5chars_avx2(&block) });
            } else {
                let s = unified_scan_scalar(&block);
                results.push(SimdUnifiedBlock {
                    newline_bits: s.newline_bits,
                    space_bits: s.space_bits,
                    quote_bits: s.quote_bits,
                    backslash_bits: s.backslash_bits,
                    comma_bits: s.comma_bits,
                });
            }
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let s = unified_scan_scalar(&block);
            results.push(SimdUnifiedBlock {
                newline_bits: s.newline_bits,
                space_bits: s.space_bits,
                quote_bits: s.quote_bits,
                backslash_bits: s.backslash_bits,
                comma_bits: s.comma_bits,
            });
        }
    }
    results
}

// ===========================================================================
// Approach 5: SIMD 9-char — full multiline JSON structural set
// ===========================================================================

/// 9-character structural scan: newline, space, quote, backslash, comma,
/// colon, open brace, close brace, open bracket, close bracket.
/// Covers everything needed for multiline JSON + CSV + CRI.
#[allow(dead_code)]
struct Simd9Block {
    newline_bits: u64,
    space_bits: u64,
    quote_bits: u64,
    backslash_bits: u64,
    comma_bits: u64,
    colon_bits: u64,
    open_brace_bits: u64,
    close_brace_bits: u64,
    open_bracket_bits: u64,
}

#[cfg(target_arch = "aarch64")]
mod simd_9char {
    use super::Simd9Block;
    use std::arch::aarch64::*;

    #[inline(always)]
    unsafe fn movemask16(cmp: uint8x16_t) -> u16 {
        unsafe {
            const MASK: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
            let mask = vld1q_u8(MASK.as_ptr());
            let bits = vandq_u8(cmp, mask);
            let p16 = vpaddlq_u8(bits);
            let p32 = vpaddlq_u16(p16);
            let p64 = vpaddlq_u32(p32);
            let lo = vgetq_lane_u64(p64, 0) as u8;
            let hi = vgetq_lane_u64(p64, 1) as u8;
            (lo as u16) | ((hi as u16) << 8)
        }
    }

    #[inline(always)]
    unsafe fn extract_mask(
        c0: uint8x16_t,
        c1: uint8x16_t,
        c2: uint8x16_t,
        c3: uint8x16_t,
        needle: uint8x16_t,
    ) -> u64 {
        unsafe {
            (movemask16(vceqq_u8(c0, needle)) as u64)
                | ((movemask16(vceqq_u8(c1, needle)) as u64) << 16)
                | ((movemask16(vceqq_u8(c2, needle)) as u64) << 32)
                | ((movemask16(vceqq_u8(c3, needle)) as u64) << 48)
        }
    }

    #[target_feature(enable = "neon")]
    pub unsafe fn find_9chars_neon(data: &[u8; 64]) -> Simd9Block {
        unsafe {
            let c0 = vld1q_u8(data.as_ptr());
            let c1 = vld1q_u8(data[16..].as_ptr());
            let c2 = vld1q_u8(data[32..].as_ptr());
            let c3 = vld1q_u8(data[48..].as_ptr());

            Simd9Block {
                newline_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'\n')),
                space_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b' ')),
                quote_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'"')),
                backslash_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'\\')),
                comma_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b',')),
                colon_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b':')),
                open_brace_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'{')),
                close_brace_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'}')),
                open_bracket_bits: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'[')),
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod simd_9char {
    use super::Simd9Block;
    use std::arch::x86_64::*;

    #[inline(always)]
    unsafe fn extract_mask_avx2(lo: __m256i, hi: __m256i, needle: __m256i) -> u64 {
        unsafe {
            let q0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, needle)) as u32 as u64;
            let q1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, needle)) as u32 as u64;
            q0 | (q1 << 32)
        }
    }

    #[target_feature(enable = "avx2")]
    pub unsafe fn find_9chars_avx2(data: &[u8; 64]) -> Simd9Block {
        unsafe {
            let lo = _mm256_loadu_si256(data.as_ptr() as *const __m256i);
            let hi = _mm256_loadu_si256(data[32..].as_ptr() as *const __m256i);

            Simd9Block {
                newline_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'\n' as i8)),
                space_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b' ' as i8)),
                quote_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'"' as i8)),
                backslash_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'\\' as i8)),
                comma_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b',' as i8)),
                colon_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b':' as i8)),
                open_brace_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'{' as i8)),
                close_brace_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'}' as i8)),
                open_bracket_bits: extract_mask_avx2(lo, hi, _mm256_set1_epi8(b'[' as i8)),
            }
        }
    }
}

fn simd_9char_scan_buffer(buf: &[u8]) -> Vec<Simd9Block> {
    let num_blocks = buf.len().div_ceil(64);
    let mut results = Vec::with_capacity(num_blocks);

    for block_idx in 0..num_blocks {
        let offset = block_idx * 64;
        let remaining = buf.len() - offset;

        let block: [u8; 64] = if remaining >= 64 {
            buf[offset..offset + 64].try_into().unwrap()
        } else {
            let mut padded = [b' '; 64];
            padded[..remaining].copy_from_slice(&buf[offset..]);
            padded
        };

        #[cfg(target_arch = "aarch64")]
        results.push(unsafe { simd_9char::find_9chars_neon(&block) });

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") {
            results.push(unsafe { simd_9char::find_9chars_avx2(&block) });
        }
    }
    results
}

// ===========================================================================
// Approach 6: Plain byte_search newline scan (what NewlineFramer uses)
// ===========================================================================

fn find_newlines_plain_loop(buf: &[u8]) -> Vec<usize> {
    let mut positions = Vec::new();
    for (i, &b) in buf.iter().enumerate() {
        if b == b'\n' {
            positions.push(i);
        }
    }
    positions
}

// ===========================================================================
// Benchmarks
// ===========================================================================

fn bench_separate_passes(c: &mut Criterion) {
    let ndjson = gen_ndjson(4096);
    let cri = gen_cri(4096);

    let mut group = c.benchmark_group("separate_passes");

    // NDJSON
    group.throughput(Throughput::Bytes(ndjson.len() as u64));
    group.bench_function("ndjson/memchr_newlines", |b| {
        b.iter(|| black_box(find_newlines_memchr(black_box(&ndjson))))
    });
    group.bench_function("ndjson/chunk_index", |b| {
        b.iter(|| black_box(classify_chunk_index(black_box(&ndjson))))
    });
    group.bench_function("ndjson/memchr_then_chunk_index", |b| {
        b.iter(|| {
            let nl = find_newlines_memchr(black_box(&ndjson));
            let ci = classify_chunk_index(black_box(&ndjson));
            black_box((nl, ci))
        })
    });
    group.bench_function("ndjson/plain_loop_newlines", |b| {
        b.iter(|| black_box(find_newlines_plain_loop(black_box(&ndjson))))
    });

    // CRI
    group.throughput(Throughput::Bytes(cri.len() as u64));
    group.bench_function("cri/memchr_newlines", |b| {
        b.iter(|| black_box(find_newlines_memchr(black_box(&cri))))
    });
    group.bench_function("cri/chunk_index", |b| {
        b.iter(|| black_box(classify_chunk_index(black_box(&cri))))
    });
    group.bench_function("cri/memchr_then_chunk_index", |b| {
        b.iter(|| {
            let nl = find_newlines_memchr(black_box(&cri));
            let ci = classify_chunk_index(black_box(&cri));
            black_box((nl, ci))
        })
    });

    group.finish();
}

fn bench_unified_pass(c: &mut Criterion) {
    let ndjson = gen_ndjson(4096);
    let cri = gen_cri(4096);

    let mut group = c.benchmark_group("unified_pass");

    group.throughput(Throughput::Bytes(ndjson.len() as u64));
    group.bench_function("ndjson/unified_5char_scalar", |b| {
        b.iter(|| black_box(unified_scan_buffer(black_box(&ndjson))))
    });
    group.bench_function("ndjson/unified_5char_simd", |b| {
        b.iter(|| black_box(simd_unified_scan_buffer(black_box(&ndjson))))
    });

    group.throughput(Throughput::Bytes(cri.len() as u64));
    group.bench_function("cri/unified_5char_scalar", |b| {
        b.iter(|| black_box(unified_scan_buffer(black_box(&cri))))
    });
    group.bench_function("cri/unified_5char_simd", |b| {
        b.iter(|| black_box(simd_unified_scan_buffer(black_box(&cri))))
    });

    group.finish();
}

fn bench_hybrid_pass(c: &mut Criterion) {
    let ndjson = gen_ndjson(4096);
    let cri = gen_cri(4096);

    let mut group = c.benchmark_group("hybrid_pass");

    group.throughput(Throughput::Bytes(ndjson.len() as u64));
    group.bench_function("ndjson/framing_then_classify", |b| {
        b.iter(|| black_box(hybrid_scan_buffer(black_box(&ndjson))))
    });

    group.throughput(Throughput::Bytes(cri.len() as u64));
    group.bench_function("cri/framing_then_classify", |b| {
        b.iter(|| black_box(hybrid_scan_buffer(black_box(&cri))))
    });

    group.finish();
}

fn bench_comparison(c: &mut Criterion) {
    let ndjson = gen_ndjson(4096);

    let mut group = c.benchmark_group("comparison_ndjson");
    group.throughput(Throughput::Bytes(ndjson.len() as u64));

    // Current: memchr newlines + ChunkIndex (2 passes, SIMD)
    group.bench_function("current_separate", |b| {
        b.iter(|| {
            let nl = find_newlines_memchr(black_box(&ndjson));
            let ci = classify_chunk_index(black_box(&ndjson));
            black_box((nl, ci))
        })
    });

    // Unified scalar: one pass, 5 characters, auto-vectorized
    group.bench_function("unified_5char_scalar", |b| {
        b.iter(|| black_box(unified_scan_buffer(black_box(&ndjson))))
    });

    // Unified SIMD: one pass, 5 characters, explicit NEON/AVX2
    group.bench_function("unified_5char_simd", |b| {
        b.iter(|| black_box(simd_unified_scan_buffer(black_box(&ndjson))))
    });

    // Unified SIMD 9 chars: one pass, full multiline JSON structural set
    group.bench_function("unified_9char_simd", |b| {
        b.iter(|| black_box(simd_9char_scan_buffer(black_box(&ndjson))))
    });

    // Hybrid: scalar framing + scalar classify (2 passes, auto-vectorized)
    group.bench_function("hybrid_2pass", |b| {
        b.iter(|| black_box(hybrid_scan_buffer(black_box(&ndjson))))
    });

    group.finish();
}

/// Benchmark scaling: how does throughput change as we detect more characters?
/// This helps determine if extending ChunkIndex for new formats is viable.
fn bench_char_count_scaling(c: &mut Criterion) {
    let ndjson = gen_ndjson(4096);

    let mut group = c.benchmark_group("char_count_scaling");
    group.throughput(Throughput::Bytes(ndjson.len() as u64));

    // 2 chars: current ChunkIndex (quotes + backslashes)
    group.bench_function("2_chars_simd", |b| {
        b.iter(|| black_box(classify_chunk_index(black_box(&ndjson))))
    });

    // 5 chars: unified SIMD (newline + space + quote + backslash + comma)
    group.bench_function("5_chars_simd", |b| {
        b.iter(|| black_box(simd_unified_scan_buffer(black_box(&ndjson))))
    });

    // 9 chars: full structural set (+ colon, braces, bracket)
    group.bench_function("9_chars_simd", |b| {
        b.iter(|| black_box(simd_9char_scan_buffer(black_box(&ndjson))))
    });

    // memchr (best-in-class single char SIMD)
    group.bench_function("1_char_memchr", |b| {
        b.iter(|| black_box(find_newlines_memchr(black_box(&ndjson))))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_separate_passes,
    bench_unified_pass,
    bench_hybrid_pass,
    bench_comparison,
    bench_char_count_scaling,
);
criterion_main!(benches);
