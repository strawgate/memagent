//! File I/O and framing throughput benchmarks.
//!
//! Measures the CPU cost of different framing strategies on data read from
//! disk via tmpfile:
//!
//! - **Direct read** — `std::fs::read` baseline
//! - **Newline framing** — `NewlineFramer::frame` on read buffer
//! - **CRI parse + reassemble** — `parse_cri_line` + `CriReassembler`
//!
//! Varies: line length distribution, file size (10 MB and 100 MB).

use std::io::Write;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use logfwd_bench::generators;
use logfwd_core::cri::parse_cri_line;
use logfwd_core::framer::NewlineFramer;
use logfwd_core::reassembler::{AggregateResult, CriReassembler};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write data to a temp file and return its path.
fn write_tmpfile(data: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().expect("create tmpfile");
    f.write_all(data).expect("write tmpfile");
    f.flush().expect("flush tmpfile");
    f
}

// ---------------------------------------------------------------------------
// Framing benchmarks
// ---------------------------------------------------------------------------

fn bench_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_io_framing");
    group.sample_size(20);

    // Generate data at different sizes with different line length distributions.
    let scenarios: Vec<(&str, Vec<u8>)> = vec![
        // Narrow lines (~130 bytes each)
        ("narrow_80k", generators::gen_narrow(80_000, 42)),
        // Production mixed (~250 bytes avg)
        ("mixed_40k", generators::gen_production_mixed(40_000, 42)),
        // CRI K8s lines (~350 bytes avg)
        ("cri_30k", generators::gen_cri_k8s(30_000, 42)),
    ];

    let framer = NewlineFramer;

    for (name, data) in &scenarios {
        let file = write_tmpfile(data);
        let file_path = file.path();
        let file_size = data.len() as u64;

        group.throughput(Throughput::Bytes(file_size));

        // Baseline: read file into memory
        group.bench_with_input(
            BenchmarkId::new("read_only", name),
            &file_path.to_path_buf(),
            |b, path| {
                b.iter(|| {
                    let buf = std::fs::read(path).expect("read file");
                    std::hint::black_box(buf);
                });
            },
        );

        // Newline framing: read + frame (loop to consume all lines)
        group.bench_with_input(
            BenchmarkId::new("newline_frame", name),
            &file_path.to_path_buf(),
            |b, path| {
                b.iter(|| {
                    let buf = std::fs::read(path).expect("read file");
                    let mut total_lines = 0usize;
                    let mut offset = 0;
                    while offset < buf.len() {
                        let output = framer.frame(&buf[offset..]);
                        total_lines += output.len();
                        if output.remainder_offset == 0 {
                            break;
                        }
                        offset += output.remainder_offset;
                    }
                    std::hint::black_box(total_lines);
                });
            },
        );

        // In-memory framing only (no I/O cost, loop to consume all lines)
        group.bench_with_input(BenchmarkId::new("frame_only", name), data, |b, data| {
            b.iter(|| {
                let mut total_lines = 0usize;
                let mut offset = 0;
                while offset < data.len() {
                    let output = framer.frame(&data[offset..]);
                    total_lines += output.len();
                    if output.remainder_offset == 0 {
                        break;
                    }
                    offset += output.remainder_offset;
                }
                std::hint::black_box(total_lines);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// CRI parse throughput
// ---------------------------------------------------------------------------

fn bench_cri_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_io_cri");
    group.sample_size(20);

    // CRI data at different sizes
    let sizes: Vec<(usize, &str)> = vec![(10_000, "10k_lines"), (100_000, "100k_lines")];

    for (count, label) in &sizes {
        let data = generators::gen_cri_k8s(*count, 42);
        let file = write_tmpfile(&data);
        let file_path = file.path();

        group.throughput(Throughput::Bytes(data.len() as u64));

        // Read + CRI parse + reassemble
        group.bench_with_input(
            BenchmarkId::new("read_parse_reassemble", label),
            &file_path.to_path_buf(),
            |b, path| {
                let mut reassembler = CriReassembler::new(1024 * 1024);
                let mut json_buf = Vec::with_capacity(data.len());
                b.iter(|| {
                    let buf = std::fs::read(path).expect("read file");
                    reassembler.reset();
                    json_buf.clear();
                    let mut count = 0usize;
                    let mut start = 0;
                    while start < buf.len() {
                        let end =
                            memchr::memchr(b'\n', &buf[start..]).map_or(buf.len(), |p| start + p);
                        if let Some(cri) = parse_cri_line(&buf[start..end]) {
                            match reassembler.feed(cri.message, cri.is_full) {
                                AggregateResult::Complete(msg)
                                | AggregateResult::Truncated(msg) => {
                                    json_buf.extend_from_slice(msg);
                                    json_buf.push(b'\n');
                                    count += 1;
                                    reassembler.reset();
                                }
                                AggregateResult::Pending => {}
                            }
                        }
                        start = end + 1;
                    }
                    std::hint::black_box(count);
                });
            },
        );

        // Parse only (in-memory, no I/O, no reassembly allocation)
        group.bench_with_input(BenchmarkId::new("parse_only", label), &data, |b, data| {
            b.iter(|| {
                let mut count = 0usize;
                let mut start = 0;
                while start < data.len() {
                    let end =
                        memchr::memchr(b'\n', &data[start..]).map_or(data.len(), |p| start + p);
                    if parse_cri_line(&data[start..end]).is_some() {
                        count += 1;
                    }
                    start = end + 1;
                }
                std::hint::black_box(count);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

criterion_group!(benches, bench_framing, bench_cri_framing);
criterion_main!(benches);
