//! Allocation regression tests for output encoding hot paths.
//!
//! Tests use `#[serial]` because the global counting allocator is shared.

use serial_test::serial;
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
        Field::new("host", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("latency", DataType::Float64, true),
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

fn assert_stable_or_zero(label: &str, alloc1: usize, alloc2: usize) {
    if alloc1 == 0 && alloc2 == 0 {
        return;
    }
    // Small one-off allocations (< 4 KiB) from the system allocator or
    // runtime (glibc tcache, thread-local state) are noise, not regressions.
    // Only check the ratio when both windows report meaningful work.
    const NOISE_FLOOR: usize = 4096;
    if alloc1 < NOISE_FLOOR && alloc2 < NOISE_FLOOR {
        return;
    }
    let growth = alloc2 as f64 / alloc1.max(1) as f64;
    assert!(
        (0.5..2.0).contains(&growth),
        "{label}: window 1={alloc1} bytes, window 2={alloc2} bytes (ratio={growth:.2})",
    );
}

/// write_row_json should have stable allocations across repeated calls.
#[test]
#[serial]
fn write_row_json_stable_across_batches() {
    let batch = make_batch(100);
    let cols = build_col_infos(&batch);
    let mut buf = Vec::with_capacity(4096);

    // Warmup: run several passes to flush lazy one-time allocations (Arrow
    // downcast caches, thread-local state, glibc tcache warming, etc.).
    // Two passes were not always sufficient on Linux CI — glibc can defer
    // internal bookkeeping allocations past the second pass.
    for _ in 0..5 {
        for row in 0..batch.num_rows() {
            let _ = write_row_json(&batch, row, &cols, &mut buf, false);
        }
        buf.clear();
    }

    let reg1 = Region::new(GLOBAL);
    for row in 0..batch.num_rows() {
        let _ = write_row_json(&batch, row, &cols, &mut buf, false);
    }
    let stats1 = reg1.change();
    buf.clear();

    let reg2 = Region::new(GLOBAL);
    for row in 0..batch.num_rows() {
        let _ = write_row_json(&batch, row, &cols, &mut buf, false);
    }
    let stats2 = reg2.change();

    assert_stable_or_zero(
        "write_row_json",
        stats1.bytes_allocated,
        stats2.bytes_allocated,
    );
}

/// OTLP encode_batch should have stable allocations across repeated calls.
#[test]
#[serial]
fn otlp_encode_stable_across_batches() {
    use logfwd_output::{OtlpProtocol, OtlpSink};

    let mut sink = OtlpSink::new(
        "test".to_string(),
        "http://localhost:4318/v1/logs".to_string(),
        OtlpProtocol::Http,
        logfwd_output::Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
    )
    .unwrap();

    let batch = make_batch(500);
    let meta = BatchMetadata {
        resource_attrs: Arc::default(),
        observed_time_ns: now_nanos(),
    };

    for _ in 0..3 {
        sink.encode_batch(&batch, &meta);
    }

    let reg1 = Region::new(GLOBAL);
    for _ in 0..5 {
        sink.encode_batch(&batch, &meta);
    }
    let stats1 = reg1.change();

    let reg2 = Region::new(GLOBAL);
    for _ in 0..5 {
        sink.encode_batch(&batch, &meta);
    }
    let stats2 = reg2.change();

    assert_stable_or_zero(
        "OTLP encode_batch",
        stats1.bytes_allocated,
        stats2.bytes_allocated,
    );
}
