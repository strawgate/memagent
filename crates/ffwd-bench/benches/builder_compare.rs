//! Criterion benchmarks: streaming (StringViewArray) vs scan_detached (StringArray)
//! across different data shapes, sizes, and pipeline stages.
//!
//! All variants within a group run back-to-back for reliable relative comparison.
//!
//! Run with: cargo bench -p ffwd-bench --bench builder_compare

use std::fmt::Write;

use arrow::ipc::CompressionType;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ffwd_arrow::materialize::detach_if_attached;
use ffwd_arrow::scanner::Scanner;
use ffwd_core::scan_config::ScanConfig;
use ffwd_transform::SqlTransform;

// ---------------------------------------------------------------------------
// Test data generators
// ---------------------------------------------------------------------------

/// Simple 7-field JSON (~180 bytes/line). Mix of strings and ints.
fn gen_simple(n: usize) -> Vec<u8> {
    let levels = ["INFO", "ERROR", "DEBUG", "WARN"];
    let paths = [
        "/api/users",
        "/api/orders",
        "/api/health",
        "/api/auth/login",
        "/api/metrics",
    ];
    let mut s = String::with_capacity(n * 260);
    for i in 0..n {
        let _ = write!(
            s,
            r#"{{"timestamp":"2024-01-15T10:30:{:02}.{:09}Z","level":"{}","message":"GET {} HTTP/1.1","status":{},"duration_ms":{},"request_id":"req-{:08x}","service":"api-gateway"}}"#,
            i % 60,
            i % 1_000_000_000,
            levels[i % levels.len()],
            paths[i % paths.len()],
            [200, 200, 200, 500, 404][i % 5],
            (i % 500) + 1,
            i,
        );
        s.push('\n');
    }
    s.into_bytes()
}

/// Narrow 3-field JSON (~50 bytes/line). Mostly short strings.
fn gen_narrow(n: usize) -> Vec<u8> {
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let mut s = String::with_capacity(n * 60);
    for i in 0..n {
        let _ = write!(
            s,
            r#"{{"ts":"{}","lvl":"{}","msg":"event {}"}}"#,
            i,
            levels[i % 4],
            i
        );
        s.push('\n');
    }
    s.into_bytes()
}

/// Wide 20-field JSON (~550 bytes/line). Many string columns.
fn gen_wide(n: usize) -> Vec<u8> {
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let methods = ["GET", "POST", "PUT", "DELETE", "PATCH"];
    let regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"];
    let namespaces = ["default", "kube-system", "monitoring", "logging"];
    let mut s = String::with_capacity(n * 600);
    for i in 0..n {
        let _ = write!(
            s,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request {}","duration_ms":{},"service":"myapp","host":"node-{}","pod":"app-{:04}","namespace":"{}","method":"{}","status_code":{},"region":"{}","user_id":"user-{}","trace_id":"{:032x}","response_bytes":{},"latency_p99_ms":{},"error_count":{},"cache_hit":{},"db_query_ms":{},"upstream":"svc-{}","version":"v{}.{}"}}"#,
            i % 1000,
            levels[i % 4],
            i,
            1 + (i * 13) % 500,
            i % 10,
            i % 100,
            namespaces[i % 4],
            methods[i % 5],
            [200, 201, 400, 404, 500][i % 5],
            regions[i % 4],
            i % 1000,
            (i as u64).wrapping_mul(0x517cc1b727220a95),
            100 + (i * 37) % 10000,
            10 + (i * 11) % 1000,
            if i % 20 == 0 { 1 } else { 0 },
            if i % 3 == 0 { "true" } else { "false" },
            (i * 7) % 200,
            i % 4,
            1 + i % 5,
            i % 10,
        );
        s.push('\n');
    }
    s.into_bytes()
}

/// Long-string JSON (~1KB/line). Few fields but large message values.
fn gen_long_strings(n: usize) -> Vec<u8> {
    let mut s = String::with_capacity(n * 1100);
    let long_msg: String = "abcdefghij".repeat(80); // 800 chars
    for i in 0..n {
        let _ = write!(
            s,
            r#"{{"id":{},"body":"{}","tag":"item-{}"}}"#,
            i,
            long_msg,
            i % 100,
        );
        s.push('\n');
    }
    s.into_bytes()
}

/// Int-heavy JSON (~120 bytes/line). Mostly numeric, few strings.
fn gen_int_heavy(n: usize) -> Vec<u8> {
    let mut s = String::with_capacity(n * 130);
    for i in 0..n {
        let _ = write!(
            s,
            r#"{{"ts":{},"a":{},"b":{},"c":{},"d":{},"e":{},"f":{},"g":{},"label":"x{}"}}"#,
            i,
            i * 2,
            i * 3,
            i * 5,
            i * 7,
            i * 11,
            i * 13,
            i * 17,
            i % 10,
        );
        s.push('\n');
    }
    s.into_bytes()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_ipc_zstd(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let opts = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap();
    let mut writer = FileWriter::try_new_with_options(&mut buf, &batch.schema(), opts).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    buf
}

fn read_ipc(data: &[u8]) -> RecordBatch {
    use arrow::ipc::reader::FileReader;
    let cursor = std::io::Cursor::new(data);
    let reader = FileReader::try_new(cursor, None).unwrap();
    reader.into_iter().next().unwrap().unwrap()
}

fn fmt_bytes(n: usize) -> String {
    if n >= 1_073_741_824 {
        format!("{:.1} GB", n as f64 / 1_073_741_824.0)
    } else if n >= 1_048_576 {
        format!("{:.1} MB", n as f64 / 1_048_576.0)
    } else if n >= 1024 {
        format!("{:.1} KB", n as f64 / 1024.0)
    } else {
        format!("{} B", n)
    }
}

// ---------------------------------------------------------------------------
// 1. Scan only: streaming (StringViewArray) vs scan_detached (StringArray)
// ---------------------------------------------------------------------------

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    group.sample_size(30);

    let datasets: Vec<(&str, Vec<u8>)> = vec![
        ("narrow_3f_100k", gen_narrow(100_000)),
        ("simple_7f_100k", gen_simple(100_000)),
        ("wide_20f_100k", gen_wide(100_000)),
        ("long_str_100k", gen_long_strings(100_000)),
        ("int_heavy_100k", gen_int_heavy(100_000)),
        ("simple_7f_10k", gen_simple(10_000)),
        ("simple_7f_500k", gen_simple(500_000)),
    ];

    for (name, data) in &datasets {
        let buf = Bytes::from(data.clone());
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_function(BenchmarkId::new("streaming", name), |b| {
            let mut scanner = Scanner::new(ScanConfig::default());
            b.iter(|| std::hint::black_box(scanner.scan(buf.clone()).unwrap()));
        });

        group.bench_function(BenchmarkId::new("scan_detached", name), |b| {
            let mut scanner = Scanner::new(ScanConfig::default());
            b.iter(|| std::hint::black_box(scanner.scan_detached(buf.clone()).unwrap()));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Scan + IPC zstd persist
// ---------------------------------------------------------------------------

fn bench_persist(c: &mut Criterion) {
    let mut group = c.benchmark_group("persist");
    group.sample_size(20);

    let datasets: Vec<(&str, Vec<u8>)> = vec![
        ("narrow_3f_100k", gen_narrow(100_000)),
        ("simple_7f_100k", gen_simple(100_000)),
        ("wide_20f_100k", gen_wide(100_000)),
        ("long_str_100k", gen_long_strings(100_000)),
        ("int_heavy_100k", gen_int_heavy(100_000)),
    ];

    for (name, data) in &datasets {
        let buf = Bytes::from(data.clone());
        group.throughput(Throughput::Bytes(data.len() as u64));

        // streaming scan → detach → IPC zstd
        group.bench_function(BenchmarkId::new("streaming", name), |b| {
            let mut scanner = Scanner::new(ScanConfig::default());
            b.iter(|| {
                let batch = std::hint::black_box(scanner.scan(buf.clone()).unwrap());
                let owned = ffwd_arrow::materialize::detach(&batch);
                let compressed = write_ipc_zstd(&owned);
                std::hint::black_box(&compressed);
            });
        });

        // scan_detached → IPC zstd (no intermediate StringViewArray)
        group.bench_function(BenchmarkId::new("scan_detached", name), |b| {
            let mut scanner = Scanner::new(ScanConfig::default());
            b.iter(|| {
                let batch = std::hint::black_box(scanner.scan_detached(buf.clone()).unwrap());
                let compressed = write_ipc_zstd(&batch);
                std::hint::black_box(&compressed);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Full pipeline: scan → SQL → detach_if_attached → IPC zstd
// ---------------------------------------------------------------------------

fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline");
    group.sample_size(20);

    let n = 100_000;
    let data = gen_simple(n);
    let buf = Bytes::from(data.clone());
    group.throughput(Throughput::Bytes(data.len() as u64));

    // --- Passthrough (SELECT *) ---

    group.bench_function(BenchmarkId::new("passthrough", "streaming"), |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        b.iter(|| {
            let batch = std::hint::black_box(scanner.scan(buf.clone()).unwrap());
            let transformed = transform.execute_blocking(batch).unwrap();
            let owned = detach_if_attached(&transformed, &buf);
            let compressed = write_ipc_zstd(&owned);
            std::hint::black_box(&compressed);
        });
    });

    group.bench_function(BenchmarkId::new("passthrough", "scan_detached"), |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        b.iter(|| {
            let batch = std::hint::black_box(scanner.scan_detached(buf.clone()).unwrap());
            let transformed = transform.execute_blocking(batch).unwrap();
            let compressed = write_ipc_zstd(&transformed);
            std::hint::black_box(&compressed);
        });
    });

    // --- WHERE filter (25% selectivity) ---

    group.bench_function(BenchmarkId::new("where_filter", "streaming"), |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
        b.iter(|| {
            let batch = std::hint::black_box(scanner.scan(buf.clone()).unwrap());
            let transformed = transform.execute_blocking(batch).unwrap();
            let owned = detach_if_attached(&transformed, &buf);
            let compressed = write_ipc_zstd(&owned);
            std::hint::black_box(&compressed);
        });
    });

    group.bench_function(BenchmarkId::new("where_filter", "scan_detached"), |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
        b.iter(|| {
            let batch = std::hint::black_box(scanner.scan_detached(buf.clone()).unwrap());
            let transformed = transform.execute_blocking(batch).unwrap();
            let compressed = write_ipc_zstd(&transformed);
            std::hint::black_box(&compressed);
        });
    });

    // --- Projection + filter ---

    group.bench_function(BenchmarkId::new("proj_filter", "streaming"), |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new(
            "SELECT level, message, status, duration_ms FROM logs WHERE status >= 400",
        )
        .unwrap();
        b.iter(|| {
            let batch = std::hint::black_box(scanner.scan(buf.clone()).unwrap());
            let transformed = transform.execute_blocking(batch).unwrap();
            let owned = detach_if_attached(&transformed, &buf);
            let compressed = write_ipc_zstd(&owned);
            std::hint::black_box(&compressed);
        });
    });

    group.bench_function(BenchmarkId::new("proj_filter", "scan_detached"), |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new(
            "SELECT level, message, status, duration_ms FROM logs WHERE status >= 400",
        )
        .unwrap();
        b.iter(|| {
            let batch = std::hint::black_box(scanner.scan_detached(buf.clone()).unwrap());
            let transformed = transform.execute_blocking(batch).unwrap();
            let compressed = write_ipc_zstd(&transformed);
            std::hint::black_box(&compressed);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Decompress (read back for query)
// ---------------------------------------------------------------------------

fn bench_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("decompress");

    let datasets: Vec<(&str, Vec<u8>)> = vec![
        ("simple_7f_100k", gen_simple(100_000)),
        ("wide_20f_100k", gen_wide(100_000)),
        ("long_str_100k", gen_long_strings(100_000)),
    ];

    for (name, data) in &datasets {
        let buf = Bytes::from(data.clone());
        let mut scanner = Scanner::new(ScanConfig::default());
        let batch = scanner.scan_detached(buf).unwrap();
        let compressed = write_ipc_zstd(&batch);

        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_function(BenchmarkId::new("ipc_zstd_read", name), |b| {
            b.iter(|| {
                let batch = read_ipc(&compressed);
                std::hint::black_box(batch.num_rows());
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Print sizes
// ---------------------------------------------------------------------------

fn bench_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sizes");
    group.sample_size(10);

    let datasets: Vec<(&str, Vec<u8>)> = vec![
        ("narrow_3f_100k", gen_narrow(100_000)),
        ("simple_7f_100k", gen_simple(100_000)),
        ("wide_20f_100k", gen_wide(100_000)),
        ("long_str_100k", gen_long_strings(100_000)),
        ("int_heavy_100k", gen_int_heavy(100_000)),
    ];

    eprintln!();
    eprintln!(
        "{:<20} {:>10} {:>12} {:>12} {:>12} {:>12} {:>8}",
        "Dataset", "Raw", "Streaming", "scan_detached", "IPC(stream)", "IPC(owned)", "Ratio"
    );
    eprintln!("{}", "-".repeat(95));

    for (name, data) in &datasets {
        let buf = Bytes::from(data.clone());
        let raw_size = data.len();

        let mut s1 = Scanner::new(ScanConfig::default());
        let streaming_batch = s1.scan(buf.clone()).unwrap();
        let streaming_mem = streaming_batch.get_array_memory_size();
        let streaming_ipc = write_ipc_zstd(&ffwd_arrow::materialize::detach(&streaming_batch));

        let mut s2 = Scanner::new(ScanConfig::default());
        let owned_batch = s2.scan_detached(buf).unwrap();
        let owned_mem = owned_batch.get_array_memory_size();
        let owned_ipc = write_ipc_zstd(&owned_batch);

        eprintln!(
            "{:<20} {:>10} {:>12} {:>12} {:>12} {:>12} {:>7.1}x",
            name,
            fmt_bytes(raw_size),
            fmt_bytes(streaming_mem),
            fmt_bytes(owned_mem),
            fmt_bytes(streaming_ipc.len()),
            fmt_bytes(owned_ipc.len()),
            raw_size as f64 / owned_ipc.len() as f64,
        );
    }
    eprintln!();

    group.bench_function("noop", |b| b.iter(|| std::hint::black_box(42)));
    group.finish();
}

criterion_group!(
    benches,
    bench_scan,
    bench_persist,
    bench_pipeline,
    bench_decompress,
    bench_sizes,
);
criterion_main!(benches);
