//! Profiling harness for ColumnarBatchBuilder vs StreamingBuilder.
//!
//! Usage:
//!   cargo build -p logfwd-arrow --release --features _test-internals --bin builder_profile
//!   valgrind --tool=callgrind ./target/release/builder_profile columnar 50
//!   valgrind --tool=callgrind ./target/release/builder_profile streaming 50
//!   valgrind --tool=dhat      ./target/release/builder_profile columnar 10
//!   valgrind --tool=massif    ./target/release/builder_profile columnar 10

use std::env;
use std::hint::black_box;

use logfwd_arrow::columnar::builder::ColumnarBatchBuilder;
use logfwd_arrow::columnar::plan::{BatchPlan, FieldHandle, FieldKind};
use logfwd_arrow::streaming_builder::StreamingBuilder;

const ROWS_PER_BATCH: u32 = 1000;

fn run_columnar_detached(num_batches: u64) {
    let mut plan = BatchPlan::new();
    let int_handles: [FieldHandle; 4] = [
        plan.declare_planned("timestamp_ns", FieldKind::Int64)
            .unwrap(),
        plan.declare_planned("severity_number", FieldKind::Int64)
            .unwrap(),
        plan.declare_planned("flags", FieldKind::Int64).unwrap(),
        plan.declare_planned("attributes.http.status_code", FieldKind::Int64)
            .unwrap(),
    ];
    let str_handles: [FieldHandle; 10] = [
        plan.declare_planned("severity_text", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("body", FieldKind::Utf8View).unwrap(),
        plan.declare_planned("trace_id", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("span_id", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("resource.service.name", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("resource.host.name", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("scope.name", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("scope.version", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("attributes.http.method", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("attributes.http.path", FieldKind::Utf8View)
            .unwrap(),
    ];
    let float_handles: [FieldHandle; 1] = [plan
        .declare_planned("attributes.duration_ms", FieldKind::Float64)
        .unwrap()];

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
                b.write_str(h, "example-value-0123456789").unwrap();
            }
            for &h in &float_handles {
                b.write_f64(h, base as f64 * 0.001);
            }
            b.end_row();
        }
        let batch = b.finish_batch().unwrap();
        black_box(&batch);
    }
}

fn run_columnar_view(num_batches: u64) {
    let mut plan = BatchPlan::new();
    let int_handles: [FieldHandle; 4] = [
        plan.declare_planned("timestamp_ns", FieldKind::Int64)
            .unwrap(),
        plan.declare_planned("severity_number", FieldKind::Int64)
            .unwrap(),
        plan.declare_planned("flags", FieldKind::Int64).unwrap(),
        plan.declare_planned("attributes.http.status_code", FieldKind::Int64)
            .unwrap(),
    ];
    let str_handles: [FieldHandle; 10] = [
        plan.declare_planned("severity_text", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("body", FieldKind::Utf8View).unwrap(),
        plan.declare_planned("trace_id", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("span_id", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("resource.service.name", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("resource.host.name", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("scope.name", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("scope.version", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("attributes.http.method", FieldKind::Utf8View)
            .unwrap(),
        plan.declare_planned("attributes.http.path", FieldKind::Utf8View)
            .unwrap(),
    ];
    let float_handles: [FieldHandle; 1] = [plan
        .declare_planned("attributes.duration_ms", FieldKind::Float64)
        .unwrap()];

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
                b.write_str(h, "example-value-0123456789").unwrap();
            }
            for &h in &float_handles {
                b.write_f64(h, base as f64 * 0.001);
            }
            b.end_row();
        }
        let batch = b
            .finish_batch_view(arrow::buffer::Buffer::from(b"" as &[u8]), 0)
            .unwrap();
        black_box(&batch);
    }
}

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

fn run_streaming_detached(num_batches: u64) {
    let mut sb = StreamingBuilder::new(None);

    for _batch in 0..num_batches {
        let dummy = bytes::Bytes::from_static(b"");
        sb.begin_batch(dummy);
        let indices: Vec<usize> = FIELD_NAMES
            .iter()
            .map(|name| sb.resolve_field(name.as_bytes()))
            .collect();

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
                sb.append_decoded_str_by_idx(idx, b"example-value-0123456789");
            }
            sb.append_f64_value_by_idx(indices[13], row as f64 * 0.001);
            sb.end_row();
        }
        let batch = sb.finish_batch_detached().unwrap();
        black_box(&batch);
    }
}

fn run_streaming_view(num_batches: u64) {
    let mut sb = StreamingBuilder::new(None);

    for _batch in 0..num_batches {
        let dummy = bytes::Bytes::from_static(b"");
        sb.begin_batch(dummy);
        let indices: Vec<usize> = FIELD_NAMES
            .iter()
            .map(|name| sb.resolve_field(name.as_bytes()))
            .collect();

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
                sb.append_decoded_str_by_idx(idx, b"example-value-0123456789");
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
    let mode = args.get(1).map_or("columnar", String::as_str);
    let num_batches: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20);

    let total_rows = ROWS_PER_BATCH as u64 * num_batches;
    eprintln!("Running {mode} for {num_batches} batches ({total_rows} rows)...");

    let start = std::time::Instant::now();

    match mode {
        "columnar" => run_columnar_detached(num_batches),
        "columnar-view" => run_columnar_view(num_batches),
        "streaming" => run_streaming_detached(num_batches),
        "streaming-view" => run_streaming_view(num_batches),
        other => {
            eprintln!("Unknown mode: {other}");
            eprintln!(
                "Usage: builder_profile <columnar|columnar-view|streaming|streaming-view> [batches]"
            );
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
