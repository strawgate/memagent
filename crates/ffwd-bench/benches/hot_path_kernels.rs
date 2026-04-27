//! Hot-path microbenchmarks for OTLP → Arrow optimization candidates.
//!
//! Each kernel isolates one cost so we can tell whether the optimization
//! plan's expected wins are real, not extrapolated from end-to-end numbers.
//!
//! Three questions:
//!   K1. trace_id storage: how much does hex-encoding-into-string-buffer cost
//!       vs FixedSizeBinary or raw bytes?
//!   K2. validity bitmap: is byte-level loop the bottleneck for sparse columns,
//!       or does u64 chunking actually help?
//!   K3. per-row column write: how much overhead does ColumnarBatchBuilder add
//!       over Arrow's primitive builders for a known-schema OTLP-shaped row?

use std::hint::black_box;

use arrow::array::{FixedSizeBinaryBuilder, Int64Builder, StringViewBuilder};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ffwd_arrow::columnar::builder::ColumnarBatchBuilder;
use ffwd_arrow::columnar::plan::{BatchPlan, FieldKind};

// ── K1: trace_id storage ───────────────────────────────────────────────────────

/// Replicates the per-byte hex loop used by `write_hex_bytes_lower` /
/// `encode_hex_lower_into` in `crates/ffwd-arrow/src/columnar/builder.rs`.
fn hex_encode_into(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let old = out.len();
    out.resize(old + bytes.len() * 2, 0);
    let dst = &mut out[old..];
    for (i, &b) in bytes.iter().enumerate() {
        dst[i * 2] = HEX[(b >> 4) as usize];
        dst[i * 2 + 1] = HEX[(b & 0xf) as usize];
    }
}

/// Branchless 8-bytes-at-a-time hex encoder using a u64 nibble trick.
/// Drop-in replacement for `encode_hex_lower_into` when input is 8-byte aligned.
/// Encodes 8 input bytes -> 16 output hex bytes per iteration.
#[inline]
fn hex_encode_u64_into(out: &mut Vec<u8>, bytes: &[u8]) {
    let old = out.len();
    out.resize(old + bytes.len() * 2, 0);
    let dst = &mut out[old..];
    let chunks = bytes.chunks_exact(8);
    let rem = chunks.remainder();
    for (i, chunk) in chunks.enumerate() {
        let v = u64::from_be_bytes(chunk.try_into().unwrap());
        // Spread 16 nibbles across 16 output bytes.
        let hi = (v >> 4) & 0x0f0f_0f0f_0f0f_0f0f;
        let lo = v & 0x0f0f_0f0f_0f0f_0f0f;
        // For each nibble n: '0'+n if n<10 else 'a'+n-10. Branchless:
        //   ascii = n + '0' + (((9 - n) >> 4) & ('a' - '0' - 10))
        // i.e. add 39 when n>=10, else 0. ('a'=97, '0'=48, 97-48-10=39).
        let add_hi = (((hi + 0x0606_0606_0606_0606) >> 4) & 0x0101_0101_0101_0101) * 0x27;
        let add_lo = (((lo + 0x0606_0606_0606_0606) >> 4) & 0x0101_0101_0101_0101) * 0x27;
        let h = hi + 0x3030_3030_3030_3030 + add_hi;
        let l = lo + 0x3030_3030_3030_3030 + add_lo;
        // Interleave: hi nibble of byte k and lo nibble of byte k.
        // h holds 8 hi-nibble ascii bytes (one per source byte); l holds 8 lo-nibble.
        // We need them written as h0 l0 h1 l1 ... in big-endian byte order.
        let h_bytes = h.to_be_bytes();
        let l_bytes = l.to_be_bytes();
        let base = i * 16;
        for k in 0..8 {
            dst[base + k * 2] = h_bytes[k];
            dst[base + k * 2 + 1] = l_bytes[k];
        }
    }
    // Tail (typically zero for trace_id (16) / span_id (8)).
    if !rem.is_empty() {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let base = bytes.len() - rem.len();
        for (i, &b) in rem.iter().enumerate() {
            dst[base * 2 + i * 2] = HEX[(b >> 4) as usize];
            dst[base * 2 + i * 2 + 1] = HEX[(b & 0xf) as usize];
        }
    }
}

fn bench_trace_id_storage(c: &mut Criterion) {
    let mut group = c.benchmark_group("trace_id_storage_8k");
    let n: usize = 8_000;
    let trace_ids: Vec<[u8; 16]> = (0..n)
        .map(|i| {
            let mut id = [0u8; 16];
            id[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            id[8..16]
                .copy_from_slice(&((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)).to_le_bytes());
            id
        })
        .collect();
    group.throughput(Throughput::Elements(n as u64));

    // Current production path: per-byte hex into shared string buffer.
    group.bench_function("hex_per_byte_string_buf", |b| {
        b.iter(|| {
            let mut buf: Vec<u8> = Vec::with_capacity(n * 32);
            for id in &trace_ids {
                hex_encode_into(&mut buf, id);
            }
            black_box(buf)
        });
    });

    // In-builder fix candidate: u64 nibble-spread hex encoder.
    group.bench_function("hex_u64_trick_string_buf", |b| {
        b.iter(|| {
            let mut buf: Vec<u8> = Vec::with_capacity(n * 32);
            for id in &trace_ids {
                hex_encode_u64_into(&mut buf, id);
            }
            black_box(buf)
        });
    });

    // Benchmark: FixedSizeBinaryBuilder throughput for trace/span ID columns (see TODO #1844).
    group.bench_function("fixed_size_binary_builder", |b| {
        b.iter(|| {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(n, 16);
            for id in &trace_ids {
                builder.append_value(id).unwrap();
            }
            black_box(builder.finish())
        });
    });

    // Floor: just push raw 16-byte arrays into a Vec.
    group.bench_function("raw_vec_arrays", |b| {
        b.iter(|| {
            let mut buf: Vec<[u8; 16]> = Vec::with_capacity(n);
            for &id in &trace_ids {
                buf.push(id);
            }
            black_box(buf)
        });
    });

    group.finish();
}

// ── K2: validity bitmap ────────────────────────────────────────────────────────

/// Current implementation in `accumulator.rs:554-563` — writes one bit per fact.
fn build_validity_bytes(facts: &[u32], num_rows: usize) -> Vec<u8> {
    let mut bits = vec![0u8; num_rows.div_ceil(8)];
    for &row in facts {
        let r = row as usize;
        if r < num_rows {
            bits[r >> 3] |= 1 << (r & 7);
        }
    }
    bits
}

/// Proposed: write into u64 words first, transmute to bytes at the end.
/// Same algorithmic complexity; smaller load/store width and fewer
/// dependencies per inner iteration.
fn build_validity_u64_chunked(facts: &[u32], num_rows: usize) -> Vec<u8> {
    let words_len = num_rows.div_ceil(64);
    let mut words = vec![0u64; words_len];
    for &row in facts {
        let r = row as usize;
        if r < num_rows {
            words[r >> 6] |= 1u64 << (r & 63);
        }
    }
    // Reinterpret as bytes (LE).
    let bits_len = num_rows.div_ceil(8);
    let mut out = Vec::with_capacity(bits_len);
    for w in &words {
        out.extend_from_slice(&w.to_le_bytes());
    }
    out.truncate(bits_len);
    out
}

fn bench_validity(c: &mut Criterion) {
    let mut group = c.benchmark_group("validity_bitmap_10k");
    let num_rows: usize = 10_000;

    let cases = [
        (
            "sparse_10pct",
            (0..num_rows as u32)
                .filter(|i| i % 10 == 0)
                .collect::<Vec<_>>(),
        ),
        (
            "sparse_30pct",
            (0..num_rows as u32)
                .filter(|i| i % 3 == 0)
                .collect::<Vec<_>>(),
        ),
        (
            "dense_90pct",
            (0..num_rows as u32)
                .filter(|i| i % 10 != 0)
                .collect::<Vec<_>>(),
        ),
    ];

    for (name, facts) in &cases {
        group.throughput(Throughput::Elements(facts.len() as u64));
        let id_byte = format!("byte_{name}");
        let id_u64 = format!("u64_{name}");
        group.bench_function(&id_byte, |b| {
            b.iter(|| black_box(build_validity_bytes(black_box(facts), num_rows)))
        });
        group.bench_function(&id_u64, |b| {
            b.iter(|| black_box(build_validity_u64_chunked(black_box(facts), num_rows)))
        });
    }
    group.finish();
}

// ── K3: column writer cost (the main event) ───────────────────────────────────

fn bench_column_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_writer_4field_10k");
    let n: usize = 10_000;
    let bodies: Vec<String> = (0..n)
        .map(|i| {
            format!("log message body number {i:06} with some realistic-ish length and a few words")
        })
        .collect();
    let trace_ids: Vec<[u8; 16]> = (0..n)
        .map(|i| {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&(i as u64).to_le_bytes());
            id
        })
        .collect();
    let span_ids: Vec<[u8; 8]> = (0..n).map(|i| (i as u64).to_le_bytes()).collect();

    group.throughput(Throughput::Elements(n as u64));

    // A: Current production path. Write through ColumnarBatchBuilder with
    //    Utf8View columns for trace_id/span_id (hex-encoded) and a
    //    detached body string. Mirrors `decode_log_record_wire` for the
    //    canonical columns.
    group.bench_function("columnar_batch_builder_current", |b| {
        b.iter(|| {
            let mut plan = BatchPlan::new();
            let h_ts = plan.declare_planned("timestamp", FieldKind::Int64).unwrap();
            let h_sev = plan
                .declare_planned("severity_number", FieldKind::Int64)
                .unwrap();
            let h_body = plan.declare_planned("body", FieldKind::Utf8View).unwrap();
            let h_tid = plan
                .declare_planned("trace_id", FieldKind::Utf8View)
                .unwrap();
            let h_sid = plan
                .declare_planned("span_id", FieldKind::Utf8View)
                .unwrap();
            let mut bldr = ColumnarBatchBuilder::new(plan);
            bldr.begin_batch();
            for i in 0..n {
                bldr.begin_row();
                bldr.write_i64(h_ts, i as i64);
                bldr.write_i64(h_sev, 9);
                bldr.write_str_bytes(h_body, bodies[i].as_bytes()).unwrap();
                bldr.write_hex_bytes_lower(h_tid, &trace_ids[i]).unwrap();
                bldr.write_hex_bytes_lower(h_sid, &span_ids[i]).unwrap();
                bldr.end_row();
            }
            black_box(bldr.finish_batch().unwrap())
        });
    });

    // B: Direct Arrow primitive builders. Same 5 columns; trace_id/span_id
    //    as FixedSizeBinary (no hex). Body as StringView with appended bytes.
    //    The "ceiling" we're trying to approach.
    group.bench_function("direct_arrow_fixedbinary", |b| {
        b.iter(|| {
            let mut ts = Int64Builder::with_capacity(n);
            let mut sev = Int64Builder::with_capacity(n);
            let mut body = StringViewBuilder::with_capacity(n);
            let mut tid = FixedSizeBinaryBuilder::with_capacity(n, 16);
            let mut sid = FixedSizeBinaryBuilder::with_capacity(n, 8);
            for i in 0..n {
                ts.append_value(i as i64);
                sev.append_value(9);
                body.append_value(&bodies[i]);
                tid.append_value(&trace_ids[i]).unwrap();
                sid.append_value(&span_ids[i]).unwrap();
            }
            black_box((
                ts.finish(),
                sev.finish(),
                body.finish(),
                tid.finish(),
                sid.finish(),
            ))
        });
    });

    // C: Same as B but trace_id/span_id as StringViewArray with hex encoding
    //    using the *same* byte-table encoder as production (hex_encode_into).
    //    Apples-to-apples: isolates schema (FixedSizeBinary vs Utf8View) cost
    //    without encoder algorithm confounds.
    group.bench_function("direct_arrow_hex_strings", |b| {
        b.iter(|| {
            let mut ts = Int64Builder::with_capacity(n);
            let mut sev = Int64Builder::with_capacity(n);
            let mut body = StringViewBuilder::with_capacity(n);
            let mut tid = StringViewBuilder::with_capacity(n);
            let mut sid = StringViewBuilder::with_capacity(n);
            let mut scratch_tid: Vec<u8> = Vec::with_capacity(32);
            let mut scratch_sid: Vec<u8> = Vec::with_capacity(16);
            for i in 0..n {
                ts.append_value(i as i64);
                sev.append_value(9);
                body.append_value(&bodies[i]);
                scratch_tid.clear();
                hex_encode_into(&mut scratch_tid, &trace_ids[i]);
                // SAFETY: hex_encode_into produces only ASCII hex digits.
                tid.append_value(unsafe { std::str::from_utf8_unchecked(&scratch_tid) });
                scratch_sid.clear();
                hex_encode_into(&mut scratch_sid, &span_ids[i]);
                // SAFETY: hex_encode_into produces only ASCII hex digits.
                sid.append_value(unsafe { std::str::from_utf8_unchecked(&scratch_sid) });
            }
            black_box((
                ts.finish(),
                sev.finish(),
                body.finish(),
                tid.finish(),
                sid.finish(),
            ))
        });
    });

    group.finish();
}

// ── K4: dynamic attribute write throughput ────────────────────────────────────
// Models attrs-heavy: 40 attribute writes per row, all of them String.
// Compares ColumnarBatchBuilder dynamic path vs a direct StringViewBuilder pool.

fn bench_dynamic_attrs(c: &mut Criterion) {
    let mut group = c.benchmark_group("dynamic_attrs_2k_rows_40_attrs");
    let rows: usize = 2_000;
    let attrs_per_row: usize = 40;
    let total_writes = (rows * attrs_per_row) as u64;
    // Stable attribute keys (simulates OTel semantic conventions).
    let keys: Vec<String> = (0..attrs_per_row)
        .map(|i| format!("attr.key_{i:02}.subdomain.dotted"))
        .collect();
    let values: Vec<String> = (0..attrs_per_row * rows)
        .map(|i| format!("value_{i:08}_short"))
        .collect();

    group.throughput(Throughput::Elements(total_writes));

    // A: ColumnarBatchBuilder dynamic resolution path (resolve_dynamic per attr).
    group.bench_function("columnar_builder_resolve_dynamic", |b| {
        b.iter(|| {
            let plan = BatchPlan::new();
            let mut bldr = ColumnarBatchBuilder::new(plan);
            bldr.begin_batch();
            for r in 0..rows {
                bldr.begin_row();
                for a in 0..attrs_per_row {
                    let h = bldr.resolve_dynamic(&keys[a], FieldKind::Utf8View).unwrap();
                    bldr.write_str_bytes(h, values[r * attrs_per_row + a].as_bytes())
                        .unwrap();
                }
                bldr.end_row();
            }
            black_box(bldr.finish_batch().unwrap())
        });
    });

    // B: Pre-resolve handles outside the row loop (simulates the position-cache
    //    after warm-up — what the OTLP projected path effectively achieves).
    group.bench_function("columnar_builder_preresolved", |b| {
        b.iter(|| {
            let plan = BatchPlan::new();
            let mut bldr = ColumnarBatchBuilder::new(plan);
            bldr.begin_batch();
            let handles: Vec<_> = keys
                .iter()
                .map(|k| bldr.resolve_dynamic(k, FieldKind::Utf8View).unwrap())
                .collect();
            for r in 0..rows {
                bldr.begin_row();
                for a in 0..attrs_per_row {
                    bldr.write_str_bytes(handles[a], values[r * attrs_per_row + a].as_bytes())
                        .unwrap();
                }
                bldr.end_row();
            }
            black_box(bldr.finish_batch().unwrap())
        });
    });

    // C: Direct Arrow — one StringViewBuilder per attribute key, no dispatch.
    group.bench_function("direct_arrow_per_key", |b| {
        b.iter(|| {
            let mut builders: Vec<StringViewBuilder> = (0..attrs_per_row)
                .map(|_| StringViewBuilder::with_capacity(rows))
                .collect();
            for r in 0..rows {
                for a in 0..attrs_per_row {
                    builders[a].append_value(&values[r * attrs_per_row + a]);
                }
            }
            let arrays: Vec<_> = builders.into_iter().map(|mut b| b.finish()).collect();
            black_box(arrays)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_trace_id_storage,
    bench_validity,
    bench_column_writer,
    bench_dynamic_attrs
);
criterion_main!(benches);
