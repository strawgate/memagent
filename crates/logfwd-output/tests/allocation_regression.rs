//! Allocation regression tests for output encoding hot paths.
//!
//! OTLP encoding and JSON serialization run once per batch (~every 100ms),
//! not per-row. However, they should still have stable allocation profiles
//! across repeated batches (no leaks) and linear scaling with batch size.

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_output::{BatchMetadata, build_col_infos, write_row_json};

fn make_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("host_str", DataType::Utf8, true),
        Field::new("status_int", DataType::Int64, true),
        Field::new("latency_float", DataType::Float64, true),
    ]));

    let hosts: Vec<&str> = (0..n)
        .map(|i| if i % 2 == 0 { "web1" } else { "web2" })
        .collect();
    let statuses: Vec<i64> = (0..n).map(|i| 200 + (i as i64 % 5)).collect();
    let latencies: Vec<f64> = (0..n).map(|i| 1.0 + (i as f64) * 0.01).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(hosts)),
            Arc::new(Int64Array::from(statuses)),
            Arc::new(Float64Array::from(latencies)),
        ],
    )
    .unwrap()
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// write_row_json should have stable allocations across repeated calls.
/// It writes into a reusable buffer — no per-call allocation growth.
#[test]
fn write_row_json_stable_across_batches() {
    let batch = make_batch(100);
    let cols = build_col_infos(&batch);
    let mut buf = Vec::with_capacity(4096);

    // Warmup.
    for row in 0..batch.num_rows() {
        write_row_json(&batch, row, &cols, &mut buf);
    }
    buf.clear();

    // Window 1.
    let reg1 = Region::new(&GLOBAL);
    for row in 0..batch.num_rows() {
        write_row_json(&batch, row, &cols, &mut buf);
    }
    let stats1 = reg1.change();
    buf.clear();

    // Window 2.
    let reg2 = Region::new(&GLOBAL);
    for row in 0..batch.num_rows() {
        write_row_json(&batch, row, &cols, &mut buf);
    }
    let stats2 = reg2.change();

    // Both windows should allocate roughly the same (or zero — even better).
    if stats1.bytes_allocated == 0 && stats2.bytes_allocated == 0 {
        // Perfect: zero allocations per row when buffer is pre-allocated.
        return;
    }
    let growth = stats2.bytes_allocated as f64 / stats1.bytes_allocated.max(1) as f64;
    assert!(
        (0.5..2.0).contains(&growth),
        "write_row_json instability: window 1={} bytes, window 2={} bytes (ratio={growth:.2})",
        stats1.bytes_allocated,
        stats2.bytes_allocated,
    );
}

/// OTLP encode_batch should have stable allocation across repeated calls.
#[test]
fn otlp_encode_stable_across_batches() {
    use logfwd_output::{OtlpProtocol, OtlpSink};

    let mut sink = OtlpSink::new(
        "test".to_string(),
        "http://localhost:4318/v1/logs".to_string(),
        OtlpProtocol::Http,
        logfwd_output::Compression::None,
        vec![],
    );

    let batch = make_batch(500);
    let meta = BatchMetadata {
        resource_attrs: vec![],
        observed_time_ns: now_nanos(),
    };

    // Warmup.
    for _ in 0..3 {
        sink.encode_batch(&batch, &meta);
    }

    // Window 1.
    let reg1 = Region::new(&GLOBAL);
    for _ in 0..5 {
        sink.encode_batch(&batch, &meta);
    }
    let stats1 = reg1.change();

    // Window 2.
    let reg2 = Region::new(&GLOBAL);
    for _ in 0..5 {
        sink.encode_batch(&batch, &meta);
    }
    let stats2 = reg2.change();

    // Both windows should allocate roughly the same (or zero — even better).
    if stats1.bytes_allocated == 0 && stats2.bytes_allocated == 0 {
        return;
    }
    let growth = stats2.bytes_allocated as f64 / stats1.bytes_allocated.max(1) as f64;
    assert!(
        (0.5..2.0).contains(&growth),
        "OTLP encode instability: window 1={} bytes, window 2={} bytes (ratio={growth:.2})",
        stats1.bytes_allocated,
        stats2.bytes_allocated,
    );
}
