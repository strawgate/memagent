//! Allocation regression tests for the scanner hot path.
//!
//! Tests use `#[serial]` because the global counting allocator is shared
//! across all tests in this binary — parallel execution would cause
//! measurements to interfere.

use serial_test::serial;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use ffwd_arrow::scanner::Scanner;
use ffwd_core::scan_config::ScanConfig;

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

fn assert_stable_windows(label: &str, alloc1: usize, alloc2: usize) {
    if alloc1 == 0 && alloc2 == 0 {
        return;
    }
    let growth = alloc2 as f64 / alloc1.max(1) as f64;
    assert!(
        (0.5..2.0).contains(&growth),
        "{label}: window 1={alloc1} bytes, window 2={alloc2} bytes \
         (ratio={growth:.2}). Expected ~1.0.",
    );
}

/// Repeated StreamingBuilder scans should have stable allocation profiles.
#[test]
#[serial]
fn streaming_scanner_no_leak_across_batches() {
    let mut scanner = Scanner::new(ScanConfig::default());
    let input: bytes::Bytes = make_ndjson(500).into();

    for _ in 0..5 {
        drop(scanner.scan(input.clone()).unwrap());
    }

    let reg1 = Region::new(GLOBAL);
    for _ in 0..10 {
        drop(scanner.scan(input.clone()).unwrap());
    }
    let stats1 = reg1.change();

    let reg2 = Region::new(GLOBAL);
    for _ in 0..10 {
        drop(scanner.scan(input.clone()).unwrap());
    }
    let stats2 = reg2.change();

    assert_stable_windows(
        "StreamingBuilder leak",
        stats1.bytes_allocated,
        stats2.bytes_allocated,
    );
}

/// Repeated scan_detached scans should have stable allocation profiles.
#[test]
#[serial]
fn detached_scanner_no_leak_across_batches() {
    let mut scanner = Scanner::new(ScanConfig::default());
    let data = make_ndjson(500);

    for _ in 0..5 {
        drop(
            scanner
                .scan_detached(bytes::Bytes::from(data.clone()))
                .unwrap(),
        );
    }

    let reg1 = Region::new(GLOBAL);
    for _ in 0..10 {
        drop(
            scanner
                .scan_detached(bytes::Bytes::from(data.clone()))
                .unwrap(),
        );
    }
    let stats1 = reg1.change();

    let reg2 = Region::new(GLOBAL);
    for _ in 0..10 {
        drop(
            scanner
                .scan_detached(bytes::Bytes::from(data.clone()))
                .unwrap(),
        );
    }
    let stats2 = reg2.change();

    assert_stable_windows(
        "Detached scanner leak",
        stats1.bytes_allocated,
        stats2.bytes_allocated,
    );
}

/// Scanner allocations should scale linearly (not quadratically).
/// StreamingBuilder should be sub-linear (zero-copy path).
#[test]
#[serial]
fn scanner_allocs_scale_linearly() {
    // StreamingBuilder should be sub-linear (zero-copy path).
    let mut streaming = Scanner::new(ScanConfig::default());
    let _ = streaming
        .scan(bytes::Bytes::from(make_ndjson(1000)))
        .unwrap();

    let data_500 = make_ndjson(500);
    let data_5000 = make_ndjson(5000);

    let reg_s500 = Region::new(GLOBAL);
    let _ = streaming.scan(bytes::Bytes::from(data_500)).unwrap();
    let stats_s500 = reg_s500.change();

    let reg_s5000 = Region::new(GLOBAL);
    let _ = streaming.scan(bytes::Bytes::from(data_5000)).unwrap();
    let stats_s5000 = reg_s5000.change();

    let streaming_ratio = stats_s5000.allocations as f64 / stats_s500.allocations.max(1) as f64;
    assert!(
        streaming_ratio < 5.0,
        "StreamingBuilder not sub-linear: 10x rows caused {streaming_ratio:.1}x allocs \
         (500={}, 5000={}). Zero-copy path should not allocate per-row.",
        stats_s500.allocations,
        stats_s5000.allocations,
    );
}
