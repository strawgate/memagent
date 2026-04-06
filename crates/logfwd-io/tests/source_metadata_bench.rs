//! Benchmark-style POCs for source metadata attachment in the input format layer.
//!
//! These are intentionally narrow:
//! - baseline passthrough of JSON lines
//! - row-level `_source_id` injection
//! - row-level `_source_id` + `_source_path` + `_input` injection
//!
//! Run with:
//! `cargo test --release -p logfwd-io --test source_metadata_bench -- --ignored --nocapture`

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes;
use logfwd_arrow::Scanner;
use logfwd_core::scan_config::ScanConfig;

fn generate_json_chunk(target_bytes: usize, payload_size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(target_bytes + payload_size * 2);
    let payload = "x".repeat(payload_size);
    let mut seq = 0u64;
    while out.len() < target_bytes {
        out.extend_from_slice(
            format!(
                "{{\"seq\":{seq},\"level\":\"INFO\",\"service\":\"bench\",\"message\":\"{payload}\"}}\n"
            )
            .as_bytes(),
        );
        seq += 1;
    }
    out
}

fn passthrough_copy(chunk: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(chunk);
}

fn inject_source_id_only(chunk: &[u8], source_id: &str, out: &mut Vec<u8>) {
    out.clear();
    let sid = source_id.as_bytes();
    let mut pos = 0;
    while pos < chunk.len() {
        let eol = memchr::memchr(b'\n', &chunk[pos..]).map_or(chunk.len(), |off| pos + off);
        let line = &chunk[pos..eol];
        if !line.is_empty() {
            debug_assert_eq!(line.first(), Some(&b'{'));
            out.extend_from_slice(b"{\"_source_id\":\"");
            out.extend_from_slice(sid);
            out.extend_from_slice(b"\",");
            out.extend_from_slice(&line[1..]);
            out.push(b'\n');
        }
        pos = eol + 1;
    }
}

fn inject_source_identity(
    chunk: &[u8],
    source_id: &str,
    source_path: &str,
    input_name: &str,
    out: &mut Vec<u8>,
) {
    out.clear();
    let sid = source_id.as_bytes();
    let path = source_path.as_bytes();
    let input = input_name.as_bytes();
    let mut pos = 0;
    while pos < chunk.len() {
        let eol = memchr::memchr(b'\n', &chunk[pos..]).map_or(chunk.len(), |off| pos + off);
        let line = &chunk[pos..eol];
        if !line.is_empty() {
            debug_assert_eq!(line.first(), Some(&b'{'));
            out.extend_from_slice(b"{\"_source_id\":\"");
            out.extend_from_slice(sid);
            out.extend_from_slice(b"\",\"_source_path\":\"");
            out.extend_from_slice(path);
            out.extend_from_slice(b"\",\"_input\":\"");
            out.extend_from_slice(input);
            out.extend_from_slice(b"\",");
            out.extend_from_slice(&line[1..]);
            out.push(b'\n');
        }
        pos = eol + 1;
    }
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn bench_case<F>(label: &str, chunk: &[u8], iterations: usize, mut f: F)
where
    F: FnMut(&[u8], &mut Vec<u8>),
{
    let mut out = Vec::with_capacity(chunk.len() * 2);

    for _ in 0..2 {
        f(chunk, &mut out);
        black_box(&out);
    }

    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        f(chunk, &mut out);
        black_box(&out);
        samples.push(start.elapsed());
    }

    let med = median(samples.clone());
    let avg = samples.iter().map(Duration::as_secs_f64).sum::<f64>() / iterations as f64;
    let mib = chunk.len() as f64 / (1024.0 * 1024.0);
    let line_count = memchr::memchr_iter(b'\n', chunk).count() as f64;

    println!(
        "{label:36} median={:7.3} ms avg={:7.3} ms thr={:7.1} MiB/s line_rate={:9.0} lines/s out_bytes={}",
        med.as_secs_f64() * 1000.0,
        avg * 1000.0,
        mib / med.as_secs_f64(),
        line_count / med.as_secs_f64(),
        out.len()
    );
}

fn bench_scan_case(label: &str, chunk: &[u8], iterations: usize) {
    let mut warmup_scanner = Scanner::new(ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        keep_raw: false,
        validate_utf8: false,
    });
    black_box(warmup_scanner.scan(Bytes::copy_from_slice(chunk)).unwrap());

    let mut samples = Vec::with_capacity(iterations);
    let mut rows = 0usize;
    for _ in 0..iterations {
        let mut scanner = Scanner::new(ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
            validate_utf8: false,
        });
        let start = Instant::now();
        let batch = scanner.scan(Bytes::copy_from_slice(chunk)).unwrap();
        let elapsed = start.elapsed();
        rows = batch.num_rows();
        black_box(batch);
        samples.push(elapsed);
    }

    let med = median(samples.clone());
    let avg = samples.iter().map(Duration::as_secs_f64).sum::<f64>() / iterations as f64;
    let mib = chunk.len() as f64 / (1024.0 * 1024.0);
    println!(
        "{label:36} median={:7.3} ms avg={:7.3} ms thr={:7.1} MiB/s rows={rows}",
        med.as_secs_f64() * 1000.0,
        avg * 1000.0,
        mib / med.as_secs_f64(),
    );
}

#[test]
#[ignore = "benchmark: run manually in release mode for investigation"]
fn source_metadata_injection_benchmark() {
    let chunk_1m = generate_json_chunk(1024 * 1024, 64);
    let chunk_4m = generate_json_chunk(4 * 1024 * 1024, 64);
    let mut injected_id_4m = Vec::new();
    let mut injected_full_4m = Vec::new();
    inject_source_id_only(&chunk_4m, "1432949071", &mut injected_id_4m);
    inject_source_identity(
        &chunk_4m,
        "1432949071",
        "/var/log/pods/ns_pod_uid/container/0.log",
        "pods",
        &mut injected_full_4m,
    );

    println!("\n== Source Metadata Injection Bench ==");
    println!(
        "1 MiB chunk: {} lines, {} bytes",
        memchr::memchr_iter(b'\n', &chunk_1m).count(),
        chunk_1m.len()
    );
    bench_case("baseline passthrough (1 MiB)", &chunk_1m, 9, passthrough_copy);
    bench_case("inject _source_id (1 MiB)", &chunk_1m, 9, |chunk, out| {
        inject_source_id_only(chunk, "1432949071", out);
    });
    bench_case(
        "inject id+path+input (1 MiB)",
        &chunk_1m,
        9,
        |chunk, out| {
            inject_source_identity(
                chunk,
                "1432949071",
                "/var/log/pods/ns_pod_uid/container/0.log",
                "pods",
                out,
            );
        },
    );

    println!(
        "\n4 MiB chunk: {} lines, {} bytes",
        memchr::memchr_iter(b'\n', &chunk_4m).count(),
        chunk_4m.len()
    );
    bench_case("baseline passthrough (4 MiB)", &chunk_4m, 9, passthrough_copy);
    bench_case("inject _source_id (4 MiB)", &chunk_4m, 9, |chunk, out| {
        inject_source_id_only(chunk, "1432949071", out);
    });
    bench_case(
        "inject id+path+input (4 MiB)",
        &chunk_4m,
        9,
        |chunk, out| {
            inject_source_identity(
                chunk,
                "1432949071",
                "/var/log/pods/ns_pod_uid/container/0.log",
                "pods",
                out,
            );
        },
    );

    println!("\nscanner throughput on 4 MiB-family payloads");
    bench_scan_case("scan baseline 4 MiB", &chunk_4m, 7);
    bench_scan_case("scan with _source_id", &injected_id_4m, 7);
    bench_scan_case("scan with id+path+input", &injected_full_4m, 7);
}
