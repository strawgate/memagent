//! Throughput ceiling benchmarks: generator → scan → null.
//!
//! Determines the absolute maximum throughput ffwd can achieve on a single
//! core with no I/O and no transform — pure generator → scan → null.  This is
//! the theoretical ceiling that all other scenarios are bounded by.
//!
//! **Dimensions varied:**
//! - Input schema: narrow (~120 B, 5 fields), production_mixed (~250 B, variable), wide (~600 B, 20+ fields)
//! - Scan config: extract_all (default), 3-field pushdown, extract_all with line_capture off
//! - Batch size: 1K → 100K rows
//! - Scan mode: streaming (StringViewArray) vs scan_detached (StringArray)
//!
//! **Metrics:** lines/sec (Throughput::Elements) and bytes/sec (Throughput::Bytes)
//!
//! Run with: `cargo bench -p ffwd-bench --bench throughput_ceiling`

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ffwd_core::scan_config::{FieldSpec, ScanConfig};

use ffwd_arrow::scanner::Scanner;
use ffwd_bench::{NullSink, generators};

// ---------------------------------------------------------------------------
// Scan configs
// ---------------------------------------------------------------------------

fn config_extract_all() -> ScanConfig {
    ScanConfig::default()
}

fn config_3_fields() -> ScanConfig {
    ScanConfig {
        wanted_fields: vec![
            FieldSpec {
                name: "level".into(),
                aliases: vec![],
            },
            FieldSpec {
                name: "status".into(),
                aliases: vec![],
            },
            FieldSpec {
                name: "duration_ms".into(),
                aliases: vec![],
            },
        ],
        extract_all: false,
        line_field_name: None,
        validate_utf8: false,
        row_predicate: None,
    }
}

fn config_no_raw() -> ScanConfig {
    ScanConfig {
        line_field_name: None,
        ..ScanConfig::default()
    }
}

// ---------------------------------------------------------------------------
// 1. Ceiling by schema — scan_detached → null, extract_all, 50K rows
//    Reports both lines/sec and bytes/sec for each schema.
// ---------------------------------------------------------------------------

fn bench_ceiling_by_schema(c: &mut Criterion) {
    let mut group = c.benchmark_group("ceiling_by_schema");
    group.sample_size(30);

    let n = 50_000usize;
    let meta = generators::make_metadata();

    let schemas: Vec<(&str, Vec<u8>)> = vec![
        ("narrow_5f", generators::gen_narrow(n, 42)),
        ("mixed_var", generators::gen_production_mixed(n, 42)),
        ("wide_20f", generators::gen_wide(n, 42)),
    ];

    for (schema_name, data) in &schemas {
        let buf = Bytes::from(data.clone());
        let bytes_per_batch = data.len() as u64;

        // Report lines/sec
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("lines", schema_name), &buf, |b, buf| {
            let mut scanner = Scanner::new(config_extract_all());
            let mut sink = NullSink;
            b.iter(|| {
                let batch = scanner
                    .scan_detached(buf.clone())
                    .expect("scan should not fail");
                sink.send_batch(&batch, &meta).unwrap();
                std::hint::black_box(());
            });
        });

        // Report bytes/sec
        group.throughput(Throughput::Bytes(bytes_per_batch));
        group.bench_with_input(BenchmarkId::new("bytes", schema_name), &buf, |b, buf| {
            let mut scanner = Scanner::new(config_extract_all());
            let mut sink = NullSink;
            b.iter(|| {
                let batch = scanner
                    .scan_detached(buf.clone())
                    .expect("scan should not fail");
                sink.send_batch(&batch, &meta).unwrap();
                std::hint::black_box(());
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Ceiling by batch size — production_mixed, extract_all, scan_detached → null
// ---------------------------------------------------------------------------

fn bench_ceiling_by_batch_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("ceiling_by_batch_size");
    group.sample_size(30);

    let meta = generators::make_metadata();
    let batch_sizes: &[usize] = &[1_000, 5_000, 10_000, 50_000, 100_000];

    for &n in batch_sizes {
        let data = generators::gen_production_mixed(n, 42);
        let buf = Bytes::from(data.clone());

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("lines", n), &buf, |b, buf| {
            let mut scanner = Scanner::new(config_extract_all());
            let mut sink = NullSink;
            b.iter(|| {
                let batch = scanner
                    .scan_detached(buf.clone())
                    .expect("scan should not fail");
                sink.send_batch(&batch, &meta).unwrap();
                std::hint::black_box(());
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Ceiling by scan config — production_mixed, 50K rows, scan_detached → null
// ---------------------------------------------------------------------------

fn bench_ceiling_by_scan_config(c: &mut Criterion) {
    let mut group = c.benchmark_group("ceiling_by_scan_config");
    group.sample_size(30);

    let n = 50_000usize;
    let data = generators::gen_production_mixed(n, 42);
    let buf = Bytes::from(data);
    let meta = generators::make_metadata();

    // extract_all (default)
    group.throughput(Throughput::Elements(n as u64));
    group.bench_with_input(BenchmarkId::new("lines", "extract_all"), &buf, |b, buf| {
        let mut scanner = Scanner::new(config_extract_all());
        let mut sink = NullSink;
        b.iter(|| {
            let batch = scanner
                .scan_detached(buf.clone())
                .expect("scan should not fail");
            sink.send_batch(&batch, &meta).unwrap();
            std::hint::black_box(());
        });
    });

    // 3-field pushdown
    group.throughput(Throughput::Elements(n as u64));
    group.bench_with_input(BenchmarkId::new("lines", "3_fields"), &buf, |b, buf| {
        let mut scanner = Scanner::new(config_3_fields());
        let mut sink = NullSink;
        b.iter(|| {
            let batch = scanner
                .scan_detached(buf.clone())
                .expect("scan should not fail");
            sink.send_batch(&batch, &meta).unwrap();
            std::hint::black_box(());
        });
    });

    // extract_all with line_capture off
    group.throughput(Throughput::Elements(n as u64));
    group.bench_with_input(
        BenchmarkId::new("lines", "line_capture_off"),
        &buf,
        |b, buf| {
            let mut scanner = Scanner::new(config_no_raw());
            let mut sink = NullSink;
            b.iter(|| {
                let batch = scanner
                    .scan_detached(buf.clone())
                    .expect("scan should not fail");
                sink.send_batch(&batch, &meta).unwrap();
                std::hint::black_box(());
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Ceiling by scan mode — streaming vs scan_detached, 50K production_mixed
// ---------------------------------------------------------------------------

fn bench_ceiling_by_scan_mode(c: &mut Criterion) {
    let mut group = c.benchmark_group("ceiling_by_scan_mode");
    group.sample_size(30);

    let n = 50_000usize;
    let data = generators::gen_production_mixed(n, 42);
    let buf = Bytes::from(data);
    let meta = generators::make_metadata();

    group.throughput(Throughput::Elements(n as u64));

    group.bench_with_input(BenchmarkId::new("streaming", n), &buf, |b, buf| {
        let mut scanner = Scanner::new(config_extract_all());
        let mut sink = NullSink;
        b.iter(|| {
            let batch = scanner.scan(buf.clone()).expect("scan should not fail");
            sink.send_batch(&batch, &meta).unwrap();
            std::hint::black_box(());
        });
    });

    group.bench_with_input(BenchmarkId::new("scan_detached", n), &buf, |b, buf| {
        let mut scanner = Scanner::new(config_extract_all());
        let mut sink = NullSink;
        b.iter(|| {
            let batch = scanner
                .scan_detached(buf.clone())
                .expect("scan should not fail");
            sink.send_batch(&batch, &meta).unwrap();
            std::hint::black_box(());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Bottleneck isolation — separate generator, scan, and sink costs
// ---------------------------------------------------------------------------

fn bench_bottleneck_isolation(c: &mut Criterion) {
    let mut group = c.benchmark_group("bottleneck_isolation");
    group.sample_size(30);

    let n = 50_000usize;
    let meta = generators::make_metadata();

    // (a) Generator only: measure data generation cost
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function(BenchmarkId::new("generator_only", n), |b| {
        b.iter(|| {
            let data = generators::gen_production_mixed(n, 42);
            std::hint::black_box(data.len());
        });
    });

    // (b) Scan only: measure scan without sink dispatch
    let data = generators::gen_production_mixed(n, 42);
    let buf = Bytes::from(data);
    group.bench_with_input(BenchmarkId::new("scan_only", n), &buf, |b, buf| {
        let mut scanner = Scanner::new(config_extract_all());
        b.iter(|| {
            let batch = scanner
                .scan_detached(buf.clone())
                .expect("scan should not fail");
            std::hint::black_box(batch.num_rows());
        });
    });

    // (c) Null sink only: measure dispatch cost on pre-built batch
    let data = generators::gen_production_mixed(n, 42);
    let buf = Bytes::from(data);
    let mut scanner = Scanner::new(config_extract_all());
    let batch = scanner.scan_detached(buf).expect("scan should not fail");

    group.bench_function(BenchmarkId::new("null_sink_only", n), |b| {
        let mut sink = NullSink;
        b.iter(|| {
            sink.send_batch(&batch, &meta).unwrap();
            std::hint::black_box(())
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_ceiling_by_schema,
    bench_ceiling_by_batch_size,
    bench_ceiling_by_scan_config,
    bench_ceiling_by_scan_mode,
    bench_bottleneck_isolation,
);
criterion_main!(benches);
