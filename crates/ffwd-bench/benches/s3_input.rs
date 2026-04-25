//! S3 input benchmarks.
//!
//! Requires a running MinIO (or real AWS S3) instance. Set the
//! `MINIO_ENDPOINT` environment variable (default: `http://localhost:9000`).
//! Uses `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (default: `minioadmin`).
//!
//! The benchmark is skipped automatically when MinIO is unreachable.
//!
//! Run with:
//! ```
//! MINIO_ENDPOINT=http://localhost:9000 just bench --bench s3_input --features s3
//! ```

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ffwd_io::s3_input::client::S3Client;

// ── MinIO connection helpers ───────────────────────────────────────────────

fn minio_endpoint() -> String {
    std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string())
}

fn minio_access_key() -> String {
    std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "minioadmin".to_string())
}

fn minio_secret_key() -> String {
    std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string())
}

const BENCH_BUCKET: &str = "ffwd-bench";

/// Returns `None` if MinIO is unreachable.
fn try_connect_minio() -> Option<Arc<S3Client>> {
    let client = S3Client::new(
        BENCH_BUCKET,
        "us-east-1",
        Some(&minio_endpoint()),
        minio_access_key(),
        minio_secret_key(),
        None,
        64,
    )
    .ok()?;
    Some(Arc::new(client))
}

/// Create a test bucket and upload objects for benchmarking.
async fn setup_bench_objects() -> Option<()> {
    let endpoint = minio_endpoint();
    let access_key = minio_access_key();
    let secret_key = minio_secret_key();

    let s3 = S3Client::new(
        BENCH_BUCKET,
        "us-east-1",
        Some(&endpoint),
        access_key,
        secret_key,
        None,
        64,
    )
    .ok()?;

    if s3.create_bucket().await.is_err() {
        eprintln!("s3_bench: MinIO not reachable at {endpoint} — skipping");
        return None;
    }

    for (name, size_bytes) in &[
        ("bench/1mb.log", 1_000_000usize),
        ("bench/10mb.log", 10_000_000),
        ("bench/100mb.log", 100_000_000),
    ] {
        if s3.head_object(name).await.is_ok() {
            continue;
        }
        let data = generate_log_bytes(*size_bytes);
        s3.put_object(name, &data).await.ok()?;
    }

    Some(())
}

/// Generate synthetic log lines.
fn generate_log_bytes(size: usize) -> Vec<u8> {
    let line =
        b"{\"ts\":\"2024-01-01T00:00:00Z\",\"level\":\"info\",\"msg\":\"benchmark log line\"}\n";
    let mut out = Vec::with_capacity(size);
    while out.len() < size {
        let remaining = size - out.len();
        let to_write = remaining.min(line.len());
        out.extend_from_slice(&line[..to_write]);
    }
    out
}

// ── Benchmarks ────────────────────────────────────────────────────────────

fn bench_s3_download(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    let minio_up = rt.block_on(async {
        let client = reqwest::Client::builder().use_rustls_tls().build().ok()?;
        client
            .get(format!("{}/", minio_endpoint()))
            .send()
            .await
            .ok()
            .map(|_| ())
    });

    if minio_up.is_none() {
        eprintln!("s3_bench: MinIO not available — skipping S3 benchmarks");
        return;
    }

    let setup_ok = rt.block_on(async { setup_bench_objects().await });
    if setup_ok.is_none() {
        eprintln!("s3_bench: setup failed — skipping S3 benchmarks");
        return;
    }

    let client = match try_connect_minio() {
        Some(c) => c,
        None => return,
    };

    let mut group = c.benchmark_group("s3_download");

    for (label, key, size) in &[
        ("1mb", "bench/1mb.log", 1_000_000u64),
        ("10mb", "bench/10mb.log", 10_000_000u64),
    ] {
        group.throughput(Throughput::Bytes(*size));

        group.bench_with_input(BenchmarkId::new("single_get", label), label, |b, _| {
            let client = Arc::clone(&client);
            let key = *key;
            b.to_async(&rt).iter(|| async {
                client
                    .get_object(key)
                    .await
                    .expect("benchmark single_get download should succeed");
            });
        });

        group.bench_with_input(
            BenchmarkId::new("parallel_range_get", label),
            label,
            |b, _| {
                let client = Arc::clone(&client);
                let key = *key;
                let size = *size;
                b.to_async(&rt).iter(|| async {
                    ffwd_io::s3_input::fetch_parallel_bench(
                        Arc::clone(&client),
                        key,
                        size,
                        ffwd_io::s3_input::DEFAULT_PART_SIZE,
                        ffwd_io::s3_input::DEFAULT_MAX_CONCURRENT_FETCHES,
                    )
                    .await
                    .expect("parallel_range_get should succeed");
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("stream_parallel", label), label, |b, _| {
            let client = Arc::clone(&client);
            let key = *key;
            let size = *size;
            b.to_async(&rt).iter(|| async {
                let received = ffwd_io::s3_input::fetch_parallel_stream_bench(
                    Arc::clone(&client),
                    key,
                    size,
                    ffwd_io::s3_input::DEFAULT_PART_SIZE,
                    ffwd_io::s3_input::DEFAULT_MAX_CONCURRENT_FETCHES,
                )
                .await
                .expect("streaming bench should succeed");
                assert_eq!(
                    received, size as usize,
                    "received bytes should match object size"
                );
            });
        });
    }

    // Part-size sweep on 10 MB: vary part_size × concurrency at ~16 MiB memory.
    let sweep_key = "bench/10mb.log";
    let sweep_size = 10_000_000u64;
    group.throughput(Throughput::Bytes(sweep_size));

    for (part_label, part_size, concurrency) in &[
        ("1m_x16", 1u64 * 1024 * 1024, 16usize), // 16 MiB peak
        ("2m_x8", 2 * 1024 * 1024, 8),           // 16 MiB peak
        ("4m_x4", 4 * 1024 * 1024, 4),           // 16 MiB peak
        ("8m_x2", 8 * 1024 * 1024, 2),           // 16 MiB peak
    ] {
        group.bench_with_input(BenchmarkId::new("sweep", part_label), part_label, |b, _| {
            let client = Arc::clone(&client);
            let part_size = *part_size;
            let concurrency = *concurrency;
            b.to_async(&rt).iter(|| async {
                let received = ffwd_io::s3_input::fetch_parallel_stream_bench(
                    Arc::clone(&client),
                    sweep_key,
                    sweep_size,
                    part_size,
                    concurrency,
                )
                .await
                .expect("sweep bench should succeed");
                assert_eq!(
                    received, sweep_size as usize,
                    "sweep received bytes should match"
                );
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_s3_download);
criterion_main!(benches);
