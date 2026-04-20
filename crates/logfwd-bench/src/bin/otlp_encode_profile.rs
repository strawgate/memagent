//! CPU-profile harness for the Arrow→OTLP encoder.
//!
//! Runs `encode_batch` / `encode_batch_generated_fast` in a tight loop while
//! collecting a pprof flamegraph.  Designed to be run with a release binary so
//! the flamegraph reflects production-representative inlining decisions.
//!
//! # Usage
//!
//! ```text
//! # Dense wide batch (no nulls) — generated fast path
//! cargo run -p logfwd-bench --release --bin otlp_encode_profile -- \
//!     wide 10000 5000 generated_fast /tmp/otlp-wide-fast.svg
//!
//! # Sparse wide batch (~25% nulls) — generated fast path
//! cargo run -p logfwd-bench --release --bin otlp_encode_profile -- \
//!     wide_sparse 10000 5000 generated_fast /tmp/otlp-wide-sparse.svg
//!
//! # Manual encoder (encode_batch)
//! cargo run -p logfwd-bench --release --bin otlp_encode_profile -- \
//!     wide 10000 5000 manual /tmp/otlp-wide-manual.svg
//! ```
//!
//! Arguments (all positional):
//!   1. `fixture`    — `narrow` | `wide` | `wide_sparse`
//!   2. `rows`       — rows per batch (e.g. 10000)
//!   3. `iterations` — encode loop count (e.g. 5000)
//!   4. `encoder`    — `manual` | `generated_fast`
//!   5. `svg_path`   — output flamegraph SVG path
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::fs::File;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use logfwd_bench::{generators, make_otlp_sink};
use logfwd_output::Compression;

fn usage() -> ! {
    eprintln!("Usage: otlp_encode_profile <fixture> <rows> <iterations> <encoder> <svg_path>");
    eprintln!("  fixture:    narrow | wide | wide_sparse");
    eprintln!("  rows:       rows per batch (e.g. 10000)");
    eprintln!("  iterations: encode loop count (e.g. 5000)");
    eprintln!("  encoder:    manual | generated_fast");
    eprintln!("  svg_path:   output flamegraph SVG path");
    std::process::exit(1);
}

fn make_batch(fixture: &str, rows: usize) -> RecordBatch {
    match fixture {
        "narrow" => generators::gen_narrow_batch(rows, 42),
        "wide" => generators::gen_wide_batch(rows, 42),
        "wide_sparse" => generators::gen_wide_sparse_batch(rows, 42),
        other => {
            eprintln!("Unknown fixture '{other}'; expected narrow | wide | wide_sparse");
            usage();
        }
    }
}

fn run_iterations(batch: &RecordBatch, encoder: &str, iterations: usize) -> Duration {
    let meta = generators::make_metadata();
    let mut sink = make_otlp_sink(Compression::None);

    // Warm up — two passes to stabilise allocator state.
    for _ in 0..2 {
        match encoder {
            "manual" => {
                sink.encode_batch(batch, &meta);
                black_box(&sink);
            }
            "generated_fast" => {
                sink.encode_batch_generated_fast(batch, &meta);
                black_box(&sink);
            }
            other => {
                eprintln!("Unknown encoder '{other}'; expected manual | generated_fast");
                usage();
            }
        }
    }

    let start = Instant::now();
    for _ in 0..iterations {
        match encoder {
            "manual" => {
                sink.encode_batch(batch, &meta);
                black_box(&sink);
            }
            _ => {
                sink.encode_batch_generated_fast(batch, &meta);
                black_box(&sink);
            }
        }
    }
    start.elapsed()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        usage();
    }

    let fixture = &args[1];
    let rows: usize = args[2].parse().unwrap_or_else(|_| {
        eprintln!("rows must be a positive integer");
        usage();
    });
    let iterations: usize = args[3].parse().unwrap_or_else(|_| {
        eprintln!("iterations must be a positive integer");
        usage();
    });
    let encoder = &args[4];
    let svg_path = PathBuf::from(&args[5]);

    let batch = make_batch(fixture, rows);

    // Report batch null stats so callers can verify the fixture.
    let null_cols: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .filter(|(_, arr)| arr.null_count() > 0)
        .map(|(f, arr)| format!("{}({})", f.name(), arr.null_count()))
        .collect();
    eprintln!("fixture={fixture} rows={rows} encoder={encoder} iterations={iterations}");
    if null_cols.is_empty() {
        eprintln!("nullable_cols=none (all columns are fully dense)");
    } else {
        eprintln!("nullable_cols={}", null_cols.join(", "));
    }

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1_000)
        .blocklist(&["libc", "libgcc", "pthread"])
        .build()
        .expect("pprof guard");

    let elapsed = run_iterations(&batch, encoder, iterations);

    if let Ok(report) = guard.report().build() {
        let file = File::create(&svg_path).expect("create flamegraph SVG");
        report.flamegraph(file).expect("write flamegraph");
        eprintln!("flamegraph={}", svg_path.display());
    } else {
        eprintln!("warning: pprof report build failed");
    }

    let rows_per_sec = (rows * iterations) as f64 / elapsed.as_secs_f64();
    eprintln!(
        "elapsed={:.2}s rows_encoded={} throughput={:.2}M rows/s",
        elapsed.as_secs_f64(),
        rows * iterations,
        rows_per_sec / 1_000_000.0,
    );
}
