//! Allocation profiling: ColumnarBatchBuilder vs StreamingBuilder.
//!
//! Run with: cargo test -p ffwd-arrow --features _test-internals \
//!           --test columnar_alloc_profile -- --nocapture
#![cfg(feature = "_test-internals")]
//!
//! Uses `stats_alloc` global allocator to count every heap allocation.
//! Tests use `#[serial]` because the global counting allocator is shared.

use serial_test::serial;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use ffwd_arrow::columnar::builder::ColumnarBatchBuilder;
use ffwd_arrow::columnar::plan::{BatchPlan, FieldHandle, FieldKind};
use ffwd_arrow::streaming_builder::StreamingBuilder;

const ROWS_PER_BATCH: u32 = 1000;
const NUM_BATCHES: u64 = 5;
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn print_stats(label: &str, stats: &stats_alloc::Stats, total_rows: u64) {
    let allocs_per_row = stats.allocations as f64 / total_rows as f64;
    let bytes_per_row = stats.bytes_allocated as f64 / total_rows as f64;
    let reallocs_per_row = stats.reallocations as f64 / total_rows as f64;
    eprintln!("  {label}:");
    eprintln!(
        "    allocations:   {:>8} total  ({:.2}/row)",
        stats.allocations, allocs_per_row
    );
    eprintln!(
        "    bytes alloc'd: {:>8}        ({:.0} B/row)",
        stats.bytes_allocated, bytes_per_row
    );
    eprintln!(
        "    reallocations: {:>8} total  ({:.3}/row)",
        stats.reallocations, reallocs_per_row
    );
    eprintln!(
        "    bytes freed:   {:>8}        ({:.0} B/row)",
        stats.bytes_deallocated,
        stats.bytes_deallocated as f64 / total_rows as f64
    );
}

// ---------------------------------------------------------------------------
// StreamingBuilder workload runners
// ---------------------------------------------------------------------------

fn run_streaming_detached(warmup: bool) -> stats_alloc::Stats {
    let mut sb = StreamingBuilder::new(None);

    if warmup {
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
        let _ = sb.finish_batch_detached().unwrap();
    }

    let region = Region::new(GLOBAL);

    for _batch in 0..NUM_BATCHES {
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
        assert_eq!(batch.num_rows(), ROWS_PER_BATCH as usize);
    }

    region.change()
}

fn run_streaming_view(warmup: bool) -> stats_alloc::Stats {
    let mut sb = StreamingBuilder::new(None);

    if warmup {
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
        let _ = sb.finish_batch().unwrap();
    }

    let region = Region::new(GLOBAL);

    for _batch in 0..NUM_BATCHES {
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
        assert_eq!(batch.num_rows(), ROWS_PER_BATCH as usize);
    }

    region.change()
}

// ---------------------------------------------------------------------------
// ColumnarBatchBuilder workload runners
// ---------------------------------------------------------------------------

fn make_columnar_plan() -> (
    BatchPlan,
    [FieldHandle; 4],
    [FieldHandle; 10],
    [FieldHandle; 1],
) {
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
    (plan, int_handles, str_handles, float_handles)
}

fn run_columnar_detached(warmup: bool) -> stats_alloc::Stats {
    let (plan, int_handles, str_handles, float_handles) = make_columnar_plan();
    let mut b = ColumnarBatchBuilder::new(plan);

    if warmup {
        // Full warmup batch to prime all vec capacities
        b.begin_batch();
        for row in 0..ROWS_PER_BATCH {
            b.begin_row();
            for &h in &int_handles {
                b.write_i64(h, row as i64);
            }
            for &h in &str_handles {
                b.write_str(h, "example-value-0123456789").unwrap();
            }
            for &h in &float_handles {
                b.write_f64(h, row as f64 * 0.001);
            }
            b.end_row();
        }
        let _ = b.finish_batch().unwrap();
    }

    let region = Region::new(GLOBAL);

    for _batch in 0..NUM_BATCHES {
        b.begin_batch();
        for row in 0..ROWS_PER_BATCH {
            b.begin_row();
            for &h in &int_handles {
                b.write_i64(h, row as i64);
            }
            for &h in &str_handles {
                b.write_str(h, "example-value-0123456789").unwrap();
            }
            for &h in &float_handles {
                b.write_f64(h, row as f64 * 0.001);
            }
            b.end_row();
        }
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), ROWS_PER_BATCH as usize);
    }

    region.change()
}

fn run_columnar_view(warmup: bool) -> stats_alloc::Stats {
    let (plan, int_handles, str_handles, float_handles) = make_columnar_plan();
    let mut b = ColumnarBatchBuilder::new(plan);

    if warmup {
        b.begin_batch();
        for row in 0..ROWS_PER_BATCH {
            b.begin_row();
            for &h in &int_handles {
                b.write_i64(h, row as i64);
            }
            for &h in &str_handles {
                b.write_str(h, "example-value-0123456789").unwrap();
            }
            for &h in &float_handles {
                b.write_f64(h, row as f64 * 0.001);
            }
            b.end_row();
        }
        let _ = b.finish_batch().unwrap();
    }

    let region = Region::new(GLOBAL);

    for _batch in 0..NUM_BATCHES {
        b.begin_batch();
        for row in 0..ROWS_PER_BATCH {
            b.begin_row();
            for &h in &int_handles {
                b.write_i64(h, row as i64);
            }
            for &h in &str_handles {
                b.write_str(h, "example-value-0123456789").unwrap();
            }
            for &h in &float_handles {
                b.write_f64(h, row as f64 * 0.001);
            }
            b.end_row();
        }
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), ROWS_PER_BATCH as usize);
    }

    region.change()
}

// ---------------------------------------------------------------------------
// Profile tests
// ---------------------------------------------------------------------------

/// Full allocation profile for both builders, cold and warm.
#[test]
#[serial]
fn allocation_profile() {
    let total_rows = u64::from(ROWS_PER_BATCH) * NUM_BATCHES;

    eprintln!("\n=== StreamingBuilder ({total_rows} rows, {NUM_BATCHES} batches) ===\n");
    eprintln!("  -- Cold start --");
    print_stats(
        "detached (cold)",
        &run_streaming_detached(false),
        total_rows,
    );
    print_stats("view (cold)", &run_streaming_view(false), total_rows);
    eprintln!("\n  -- Steady state (warmed) --");
    print_stats("detached (warm)", &run_streaming_detached(true), total_rows);
    print_stats("view (warm)", &run_streaming_view(true), total_rows);

    eprintln!("\n=== ColumnarBatchBuilder ({total_rows} rows, {NUM_BATCHES} batches) ===\n");
    eprintln!("  -- Cold start --");
    print_stats("detached (cold)", &run_columnar_detached(false), total_rows);
    print_stats("view (cold)", &run_columnar_view(false), total_rows);
    eprintln!("\n  -- Steady state (warmed) --");
    print_stats("detached (warm)", &run_columnar_detached(true), total_rows);
    print_stats("view (warm)", &run_columnar_view(true), total_rows);
    eprintln!();
}

/// Side-by-side warm-state comparison table.
#[test]
#[serial]
fn allocation_comparison() {
    let total_rows = u64::from(ROWS_PER_BATCH) * NUM_BATCHES;

    let sb_det = run_streaming_detached(true);
    let sb_view = run_streaming_view(true);
    let cb_det = run_columnar_detached(true);
    let cb_view = run_columnar_view(true);

    eprintln!("\n=== Allocation Comparison — warm, {total_rows} rows ===\n");
    eprintln!(
        "  {:>32} {:>10} {:>12} {:>10}",
        "Builder", "Allocs", "Bytes", "Reallocs"
    );
    eprintln!(
        "  {:>32} {:>10} {:>12} {:>10}",
        "StreamingBuilder detached",
        sb_det.allocations,
        sb_det.bytes_allocated,
        sb_det.reallocations
    );
    eprintln!(
        "  {:>32} {:>10} {:>12} {:>10}",
        "StreamingBuilder view",
        sb_view.allocations,
        sb_view.bytes_allocated,
        sb_view.reallocations
    );
    eprintln!(
        "  {:>32} {:>10} {:>12} {:>10}",
        "ColumnarBatch detached", cb_det.allocations, cb_det.bytes_allocated, cb_det.reallocations
    );
    eprintln!(
        "  {:>32} {:>10} {:>12} {:>10}",
        "ColumnarBatch view", cb_view.allocations, cb_view.bytes_allocated, cb_view.reallocations
    );

    // Summarize
    if sb_det.bytes_allocated > 0 {
        let byte_ratio = cb_det.bytes_allocated as f64 / sb_det.bytes_allocated as f64;
        let alloc_ratio = cb_det.allocations as f64 / sb_det.allocations as f64;
        eprintln!(
            "\n  Columnar/Streaming (detached): {:.1}x bytes, {:.1}x allocs",
            byte_ratio, alloc_ratio
        );
    }
    if sb_view.bytes_allocated > 0 {
        let byte_ratio = cb_view.bytes_allocated as f64 / sb_view.bytes_allocated as f64;
        let alloc_ratio = cb_view.allocations as f64 / sb_view.allocations as f64;
        eprintln!(
            "  Columnar/Streaming (view):     {:.1}x bytes, {:.1}x allocs\n",
            byte_ratio, alloc_ratio
        );
    }
}

/// Allocation scaling: verify sub-linear growth with row count.
#[test]
#[serial]
fn columnar_alloc_scaling() {
    let (plan, int_handles, str_handles, float_handles) = make_columnar_plan();
    let mut b = ColumnarBatchBuilder::new(plan);

    // Warmup
    b.begin_batch();
    b.begin_row();
    b.end_row();
    let _ = b.finish_batch().unwrap();

    let measure = |b: &mut ColumnarBatchBuilder, rows: u32| -> stats_alloc::Stats {
        let region = Region::new(GLOBAL);
        b.begin_batch();
        for row in 0..rows {
            b.begin_row();
            for &h in &int_handles {
                b.write_i64(h, row as i64);
            }
            for &h in &str_handles {
                b.write_str(h, "example-value-0123456789").unwrap();
            }
            for &h in &float_handles {
                b.write_f64(h, row as f64 * 0.001);
            }
            b.end_row();
        }
        let _ = b.finish_batch().unwrap();
        region.change()
    };

    let r500 = measure(&mut b, 500);
    let r5000 = measure(&mut b, 5000);

    let alloc_ratio = r5000.allocations as f64 / r500.allocations.max(1) as f64;
    let bytes_ratio = r5000.bytes_allocated as f64 / r500.bytes_allocated.max(1) as f64;

    eprintln!("\n=== ColumnarBatchBuilder Scaling ===");
    eprintln!(
        "  500 rows:  {} allocs, {} bytes",
        r500.allocations, r500.bytes_allocated,
    );
    eprintln!(
        "  5000 rows: {} allocs, {} bytes",
        r5000.allocations, r5000.bytes_allocated,
    );
    eprintln!(
        "  10x rows → {:.1}x allocs, {:.1}x bytes\n",
        alloc_ratio, bytes_ratio,
    );

    assert!(
        alloc_ratio < 15.0,
        "Allocations grew super-linearly: {alloc_ratio:.1}x for 10x rows"
    );
}
