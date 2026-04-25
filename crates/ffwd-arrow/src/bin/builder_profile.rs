//! Profiling harness for ColumnarBatchBuilder vs StreamingBuilder.
//!
//! Three workloads model different real producer patterns:
//!   - `zero-copy`: strings reference an input buffer (OTLP/scanner path)
//!   - `generated`: strings are written/copied into a generated buffer (JSON decode)
//!   - `mixed`: half zero-copy refs, half generated (realistic blend)
//!
//! Usage:
//!   cargo build -p ffwd-arrow --release --features _test-internals --bin builder_profile
//!   valgrind --tool=callgrind ./target/release/builder_profile columnar-zero-copy 50
//!   valgrind --tool=callgrind ./target/release/builder_profile streaming-zero-copy 50

use std::env;
use std::hint::black_box;

use arrow::buffer::Buffer;
use ffwd_arrow::columnar::accumulator::StringRef;
use ffwd_arrow::columnar::builder::ColumnarBatchBuilder;
use ffwd_arrow::columnar::plan::{BatchPlan, FieldHandle, FieldKind};
use ffwd_arrow::streaming_builder::StreamingBuilder;

const ROWS_PER_BATCH: u32 = 1000;

// Pre-built input buffer: 10 string fields × "example-value-0123456789" (24 bytes each).
// In a real pipeline this would be the protobuf wire bytes or JSON input line.
const STR_VALUE: &str = "example-value-0123456789";
const STR_COUNT: usize = 10;

fn build_input_buffer() -> Vec<u8> {
    let mut buf = Vec::with_capacity(STR_VALUE.len() * STR_COUNT);
    for _ in 0..STR_COUNT {
        buf.extend_from_slice(STR_VALUE.as_bytes());
    }
    buf
}

fn str_refs_for_row() -> [StringRef; STR_COUNT] {
    let len = STR_VALUE.len() as u32;
    core::array::from_fn(|i| StringRef {
        offset: (i as u32) * len,
        len,
    })
}

// ---------------------------------------------------------------------------
// Columnar: zero-copy (write_str_ref into input buffer)
// ---------------------------------------------------------------------------
fn run_columnar_zero_copy(num_batches: u64) {
    let input_buf = build_input_buffer();
    let input_arrow = Buffer::from_vec(input_buf);
    let refs = str_refs_for_row();

    let mut plan = BatchPlan::new();
    let int_handles: [FieldHandle; 4] = core::array::from_fn(|i| {
        plan.declare_planned(
            [
                "timestamp_ns",
                "severity_number",
                "flags",
                "http.status_code",
            ][i],
            FieldKind::Int64,
        )
        .unwrap()
    });
    let str_handles: [FieldHandle; STR_COUNT] = core::array::from_fn(|i| {
        plan.declare_planned(
            [
                "severity_text",
                "body",
                "trace_id",
                "span_id",
                "resource.service.name",
                "resource.host.name",
                "scope.name",
                "scope.version",
                "attributes.http.method",
                "attributes.http.path",
            ][i],
            FieldKind::Utf8View,
        )
        .unwrap()
    });
    let float_h = plan
        .declare_planned("duration_ms", FieldKind::Float64)
        .unwrap();

    let mut b = ColumnarBatchBuilder::new(plan);

    for batch_idx in 0..num_batches {
        b.begin_batch();
        b.set_original_buffer(input_arrow.clone());
        for row in 0..ROWS_PER_BATCH {
            b.begin_row();
            let base = (batch_idx as i64) * (ROWS_PER_BATCH as i64) + row as i64;
            for (i, &h) in int_handles.iter().enumerate() {
                b.write_i64(h, base + i as i64 * 1000);
            }
            for (i, &h) in str_handles.iter().enumerate() {
                b.write_str_ref(h, refs[i]);
            }
            b.write_f64(float_h, base as f64 * 0.001);
            b.end_row();
        }
        let batch = b.finish_batch().unwrap();
        black_box(&batch);
    }
}

// ---------------------------------------------------------------------------
// Columnar: generated strings (write_str copies into string_buf)
// ---------------------------------------------------------------------------
fn run_columnar_generated(num_batches: u64) {
    let mut plan = BatchPlan::new();
    let int_handles: [FieldHandle; 4] = core::array::from_fn(|i| {
        plan.declare_planned(
            [
                "timestamp_ns",
                "severity_number",
                "flags",
                "http.status_code",
            ][i],
            FieldKind::Int64,
        )
        .unwrap()
    });
    let str_handles: [FieldHandle; STR_COUNT] = core::array::from_fn(|i| {
        plan.declare_planned(
            [
                "severity_text",
                "body",
                "trace_id",
                "span_id",
                "resource.service.name",
                "resource.host.name",
                "scope.name",
                "scope.version",
                "attributes.http.method",
                "attributes.http.path",
            ][i],
            FieldKind::Utf8View,
        )
        .unwrap()
    });
    let float_h = plan
        .declare_planned("duration_ms", FieldKind::Float64)
        .unwrap();

    let mut b = ColumnarBatchBuilder::new(plan);

    for batch_idx in 0..num_batches {
        b.begin_batch();
        for row in 0..ROWS_PER_BATCH {
            b.begin_row();
            let base = (batch_idx as i64) * (ROWS_PER_BATCH as i64) + row as i64;
            for (i, &h) in int_handles.iter().enumerate() {
                b.write_i64(h, base + i as i64 * 1000);
            }
            for &h in &str_handles {
                b.write_str(h, STR_VALUE).unwrap();
            }
            b.write_f64(float_h, base as f64 * 0.001);
            b.end_row();
        }
        let batch = b.finish_batch().unwrap();
        black_box(&batch);
    }
}

// ---------------------------------------------------------------------------
// Columnar: mixed (5 zero-copy refs + 5 generated)
// ---------------------------------------------------------------------------
fn run_columnar_mixed(num_batches: u64) {
    let input_buf = build_input_buffer();
    let input_arrow = Buffer::from_vec(input_buf);
    let refs = str_refs_for_row();

    let mut plan = BatchPlan::new();
    let int_handles: [FieldHandle; 4] = core::array::from_fn(|i| {
        plan.declare_planned(
            [
                "timestamp_ns",
                "severity_number",
                "flags",
                "http.status_code",
            ][i],
            FieldKind::Int64,
        )
        .unwrap()
    });
    let str_handles: [FieldHandle; STR_COUNT] = core::array::from_fn(|i| {
        plan.declare_planned(
            [
                "severity_text",
                "body",
                "trace_id",
                "span_id",
                "resource.service.name",
                "resource.host.name",
                "scope.name",
                "scope.version",
                "attributes.http.method",
                "attributes.http.path",
            ][i],
            FieldKind::Utf8View,
        )
        .unwrap()
    });
    let float_h = plan
        .declare_planned("duration_ms", FieldKind::Float64)
        .unwrap();

    let mut b = ColumnarBatchBuilder::new(plan);

    for batch_idx in 0..num_batches {
        b.begin_batch();
        b.set_original_buffer(input_arrow.clone());
        for row in 0..ROWS_PER_BATCH {
            b.begin_row();
            let base = (batch_idx as i64) * (ROWS_PER_BATCH as i64) + row as i64;
            for (i, &h) in int_handles.iter().enumerate() {
                b.write_i64(h, base + i as i64 * 1000);
            }
            // First 5 fields: zero-copy from input buffer
            for (h, r) in str_handles[..5].iter().zip(&refs[..5]) {
                b.write_str_ref(*h, *r);
            }
            // Last 5 fields: generated/decoded strings
            for &h in &str_handles[5..] {
                b.write_str(h, STR_VALUE).unwrap();
            }
            b.write_f64(float_h, base as f64 * 0.001);
            b.end_row();
        }
        let batch = b.finish_batch().unwrap();
        black_box(&batch);
    }
}

// ---------------------------------------------------------------------------
// Streaming baselines
// ---------------------------------------------------------------------------

const FIELD_NAMES: [&str; 15] = [
    "timestamp_ns",
    "severity_number",
    "severity_text",
    "body",
    "flags",
    "trace_id",
    "span_id",
    "resource.service.name",
    "resource.host.name",
    "scope.name",
    "scope.version",
    "attributes.http.method",
    "attributes.http.status_code",
    "attributes.duration_ms",
    "attributes.http.path",
];

fn run_streaming_zero_copy(num_batches: u64) {
    let input_buf = build_input_buffer();
    let mut sb = StreamingBuilder::new(None);

    // StreamingBuilder clears its field_index each batch, so resolve_field must
    // be called per batch. Use a fixed-size array to avoid per-batch Vec alloc.
    let mut indices = [0usize; 15];

    for _batch in 0..num_batches {
        let buf = bytes::Bytes::copy_from_slice(&input_buf);
        sb.begin_batch(buf.clone());
        for (i, name) in FIELD_NAMES.iter().enumerate() {
            indices[i] = sb.resolve_field(name.as_bytes());
        }

        for row in 0..ROWS_PER_BATCH {
            sb.begin_row();
            for &idx in &[indices[0], indices[1], indices[4], indices[12]] {
                sb.append_i64_value_by_idx(idx, row as i64);
            }
            // Subslice directly from the Bytes so offset_of recognizes pointer identity.
            let str_indices = [
                indices[2],
                indices[3],
                indices[5],
                indices[6],
                indices[7],
                indices[8],
                indices[9],
                indices[10],
                indices[11],
                indices[14],
            ];
            for (i, &idx) in str_indices.iter().enumerate() {
                let start = i * STR_VALUE.len();
                let end = start + STR_VALUE.len();
                sb.append_str_by_idx(idx, &buf[start..end]);
            }
            sb.append_f64_value_by_idx(indices[13], row as f64 * 0.001);
            sb.end_row();
        }
        let batch = sb.finish_batch().unwrap();
        black_box(&batch);
    }
}

fn run_streaming_generated(num_batches: u64) {
    let mut sb = StreamingBuilder::new(None);
    let mut indices = [0usize; 15];

    for _batch in 0..num_batches {
        let dummy = bytes::Bytes::from_static(b"");
        sb.begin_batch(dummy);
        for (i, name) in FIELD_NAMES.iter().enumerate() {
            indices[i] = sb.resolve_field(name.as_bytes());
        }

        for row in 0..ROWS_PER_BATCH {
            sb.begin_row();
            for &idx in &[indices[0], indices[1], indices[4], indices[12]] {
                sb.append_i64_value_by_idx(idx, row as i64);
            }
            for &idx in &[
                indices[2],
                indices[3],
                indices[5],
                indices[6],
                indices[7],
                indices[8],
                indices[9],
                indices[10],
                indices[11],
                indices[14],
            ] {
                sb.append_decoded_str_by_idx(idx, STR_VALUE.as_bytes());
            }
            sb.append_f64_value_by_idx(indices[13], row as f64 * 0.001);
            sb.end_row();
        }
        let batch = sb.finish_batch().unwrap();
        black_box(&batch);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map_or("columnar-zero-copy", String::as_str);
    let num_batches: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20);

    let total_rows = ROWS_PER_BATCH as u64 * num_batches;
    eprintln!("Running {mode} for {num_batches} batches ({total_rows} rows)...");

    let start = std::time::Instant::now();

    match mode {
        "columnar-zero-copy" => run_columnar_zero_copy(num_batches),
        "columnar-generated" => run_columnar_generated(num_batches),
        "columnar-mixed" => run_columnar_mixed(num_batches),
        "streaming-zero-copy" => run_streaming_zero_copy(num_batches),
        "streaming-generated" => run_streaming_generated(num_batches),
        other => {
            eprintln!("Unknown mode: {other}");
            eprintln!("Modes: columnar-zero-copy, columnar-generated, columnar-mixed,");
            eprintln!("       streaming-zero-copy, streaming-generated");
            std::process::exit(1);
        }
    }

    let elapsed = start.elapsed();
    eprintln!(
        "Done in {:.1?} ({:.0} ns/row, {:.0} rows/sec)",
        elapsed,
        elapsed.as_nanos() as f64 / total_rows as f64,
        total_rows as f64 / elapsed.as_secs_f64(),
    );
}
