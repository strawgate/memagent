//! Allocation regression tests for the scanner hot path.
//!
//! These tests use a counting allocator to verify that scanning does not
//! introduce allocation regressions. Specifically:
//! - Repeated scans of same-sized data should have stable allocation counts
//!   (catches buffer leaks where capacity grows unboundedly across batches)
//! - Allocations should not grow super-linearly with input size
//!
//! This is a separate integration test binary because `#[global_allocator]`
//! is per-binary — it cannot coexist with other allocator overrides (e.g. dhat).

use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use logfwd_arrow::scanner::{SimdScanner, StreamingSimdScanner};
use logfwd_core::scan_config::ScanConfig;

/// Generate N NDJSON lines with consistent fields.
fn make_ndjson(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 80);
    for i in 0..n {
        use std::io::Write;
        write!(
            buf,
            r#"{{"host":"web{i}","status":{status},"lat":1.{frac}}}"#,
            status = 200 + (i % 5),
            frac = i % 10
        )
        .unwrap();
        buf.push(b'\n');
    }
    buf
}

/// Repeated scans should not grow memory. Compare the allocation delta
/// between two consecutive windows of batches — they should be nearly equal.
/// This catches buffer capacity that grows across batches without bound.
#[test]
fn storage_scanner_no_leak_across_batches() {
    let mut scanner = SimdScanner::new(ScanConfig::default());
    let data = make_ndjson(500);

    // Warmup: 5 batches to stabilize.
    for _ in 0..5 {
        drop(scanner.scan(&data).unwrap());
    }

    // Window 1: 10 batches.
    let reg1 = Region::new(&GLOBAL);
    for _ in 0..10 {
        drop(scanner.scan(&data).unwrap());
    }
    let stats1 = reg1.change();

    // Window 2: 10 more batches.
    let reg2 = Region::new(&GLOBAL);
    for _ in 0..10 {
        drop(scanner.scan(&data).unwrap());
    }
    let stats2 = reg2.change();

    // Both windows should allocate roughly the same amount.
    // If window 2 allocates significantly more, buffers are growing.
    let growth = stats2.bytes_allocated as f64 / stats1.bytes_allocated.max(1) as f64;
    assert!(
        (0.5..2.0).contains(&growth),
        "allocation instability: window 1 allocated {} bytes, window 2 allocated {} bytes \
         (ratio={growth:.2}). Expected ~1.0.",
        stats1.bytes_allocated,
        stats2.bytes_allocated,
    );
}

/// Same stability test for the streaming (zero-copy) scanner.
#[test]
fn streaming_scanner_no_leak_across_batches() {
    let mut scanner = StreamingSimdScanner::new(ScanConfig::default());
    let data = make_ndjson(500);

    // Warmup.
    for _ in 0..5 {
        drop(scanner.scan(bytes::Bytes::from(data.clone())).unwrap());
    }

    // Window 1.
    let reg1 = Region::new(&GLOBAL);
    for _ in 0..10 {
        drop(scanner.scan(bytes::Bytes::from(data.clone())).unwrap());
    }
    let stats1 = reg1.change();

    // Window 2.
    let reg2 = Region::new(&GLOBAL);
    for _ in 0..10 {
        drop(scanner.scan(bytes::Bytes::from(data.clone())).unwrap());
    }
    let stats2 = reg2.change();

    let growth = stats2.bytes_allocated as f64 / stats1.bytes_allocated.max(1) as f64;
    assert!(
        (0.5..2.0).contains(&growth),
        "streaming scanner instability: window 1={} bytes, window 2={} bytes \
         (ratio={growth:.2}). Expected ~1.0.",
        stats1.bytes_allocated,
        stats2.bytes_allocated,
    );
}

/// StorageBuilder allocates per-row (copies string values). This test
/// documents the current allocation profile and catches SUPER-linear
/// regressions (O(n²) or worse) while accepting the expected O(n) growth.
#[test]
fn storage_scanner_allocs_are_linear_not_quadratic() {
    let mut scanner = SimdScanner::new(ScanConfig::default());

    // Warmup.
    let _ = scanner.scan(&make_ndjson(1000)).unwrap();

    // Measure 500-row batch.
    let data_500 = make_ndjson(500);
    let reg_500 = Region::new(&GLOBAL);
    let _ = scanner.scan(&data_500).unwrap();
    let stats_500 = reg_500.change();

    // Measure 5000-row batch (10x).
    let data_5000 = make_ndjson(5000);
    let reg_5000 = Region::new(&GLOBAL);
    let _ = scanner.scan(&data_5000).unwrap();
    let stats_5000 = reg_5000.change();

    // Linear growth is expected (StorageBuilder copies values).
    // Quadratic (>15x for 10x data) would indicate a bug.
    let count_ratio =
        stats_5000.allocations as f64 / stats_500.allocations.max(1) as f64;
    assert!(
        count_ratio < 15.0,
        "super-linear allocation growth: 10x rows caused {count_ratio:.1}x more allocations \
         (500-row={} allocs, 5000-row={} allocs). Expected ~10x (linear).",
        stats_500.allocations,
        stats_5000.allocations,
    );

    // Also verify streaming scanner is sub-linear (zero-copy path).
    let mut streaming = StreamingSimdScanner::new(ScanConfig::default());
    let _ = streaming.scan(bytes::Bytes::from(make_ndjson(1000))).unwrap();

    let reg_s500 = Region::new(&GLOBAL);
    let _ = streaming.scan(bytes::Bytes::from(data_500)).unwrap();
    let stats_s500 = reg_s500.change();

    let reg_s5000 = Region::new(&GLOBAL);
    let _ = streaming.scan(bytes::Bytes::from(data_5000)).unwrap();
    let stats_s5000 = reg_s5000.change();

    let streaming_ratio =
        stats_s5000.allocations as f64 / stats_s500.allocations.max(1) as f64;
    assert!(
        streaming_ratio < 5.0,
        "streaming scanner should be sub-linear: 10x rows caused {streaming_ratio:.1}x \
         more allocations (500-row={}, 5000-row={}). Zero-copy path should not \
         allocate per-row.",
        stats_s500.allocations,
        stats_s5000.allocations,
    );
}
