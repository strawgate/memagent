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
//! MINIO_ENDPOINT=http://localhost:9000 cargo bench --bench s3_input --features s3
//! ```

use std::io::Write;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

#[cfg(feature = "s3")]
use logfwd_io::s3_input::client::S3Client;

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

const BENCH_BUCKET: &str = "logfwd-bench";

/// Returns `None` if MinIO is unreachable.
#[cfg(feature = "s3")]
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
#[cfg(feature = "s3")]
async fn setup_bench_objects(rt: &Runtime) -> Option<()> {
    let endpoint = minio_endpoint();
    let access_key = minio_access_key();
    let secret_key = minio_secret_key();

    // Use reqwest to create the bucket via MinIO API.
    let client = reqwest::Client::builder().use_rustls_tls().build().ok()?;

    // Try a simple request to see if MinIO is up.
    let probe = client.get(format!("{endpoint}/")).send().await;

    if probe.is_err() {
        eprintln!("s3_bench: MinIO not reachable at {endpoint} — skipping");
        return None;
    }

    // Create bucket (ignore errors if it already exists).
    let _create = client
        .put(format!("{endpoint}/{BENCH_BUCKET}"))
        .header("Content-Length", "0")
        .send()
        .await;

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

    // Upload test objects of various sizes.
    for (name, size_bytes) in &[
        ("bench/1mb.log", 1_000_000usize),
        ("bench/10mb.log", 10_000_000),
        ("bench/100mb.log", 100_000_000),
    ] {
        // Check if the object already exists to avoid re-uploading.
        if s3.head_object(name).await.is_ok() {
            continue;
        }
        let data = generate_log_bytes(*size_bytes);
        upload_object_raw(&client, &endpoint, BENCH_BUCKET, name, &data).await?;
    }

    rt.block_on(async { Some(()) })
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

/// Upload bytes to MinIO using a raw PUT request (no SigV4 needed for local MinIO
/// with anonymous PUT if bucket policy allows, but we use basic auth here).
async fn upload_object_raw(
    client: &reqwest::Client,
    endpoint: &str,
    bucket: &str,
    key: &str,
    data: &[u8],
) -> Option<()> {
    let url = format!("{endpoint}/{bucket}/{key}");
    let resp = client
        .put(&url)
        .header("Content-Length", data.len().to_string())
        .body(data.to_vec())
        .send()
        .await
        .ok()?;
    if resp.status().is_success() || resp.status().as_u16() == 200 {
        Some(())
    } else {
        None
    }
}

// ── Benchmarks ────────────────────────────────────────────────────────────

#[cfg(feature = "s3")]
fn bench_s3_download(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    // Skip benchmark if MinIO is not available.
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

    // Setup objects.
    let setup_ok = rt.block_on(async { setup_bench_objects(&rt).await });
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
                let _ = client.get_object(key).await;
            });
        });

        // Parallel range-GET benchmark.
        group.bench_with_input(
            BenchmarkId::new("parallel_range_get", label),
            label,
            |b, _| {
                let client = Arc::clone(&client);
                let key = *key;
                let size = *size;
                b.to_async(&rt).iter(|| async {
                    let part = 2 * 1024 * 1024u64; // 2 MiB parts
                    let _ = logfwd_io::s3_input::fetch_parallel_bench(
                        Arc::clone(&client),
                        key,
                        size,
                        part,
                        8,
                    )
                    .await;
                });
            },
        );
    }

    group.finish();
}

#[cfg(not(feature = "s3"))]
fn bench_s3_download(_c: &mut Criterion) {
    eprintln!("s3_bench: compiled without 's3' feature — no S3 benchmarks");
}

criterion_group!(benches, bench_s3_download);
criterion_main!(benches);
