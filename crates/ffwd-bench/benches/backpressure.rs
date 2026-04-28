//! Backpressure behavior benchmarks: measures how pipeline throughput degrades
//! as output latency increases.
//!
//! Uses a producer-consumer model to simulate the pipeline's bounded channel
//! behavior. A producer adds realistic CPU work (scanning) and sends raw bytes
//! through a bounded mpsc channel while a consumer simulates slow output with
//! configurable delays.
//!
//! **Design note:** We scan to add CPU work but send raw bytes (not RecordBatch)
//! through the channel. RecordBatch is not Sync, so it cannot be sent through
//! std::sync::mpsc::sync_channel. This design measures channel backpressure
//! dynamics, not scanner throughput.
//!
//! **Metrics captured:**
//! - Throughput degradation curve (lines/sec vs output delay)
//! - Producer stall frequency (how often the producer blocks on a full channel)
//! - Stall rate (stalls per batch sent)
//!
//! **Scenarios tested:**
//! - Data sizes: mixed (default), narrow, wide
//! - Channel capacities: 1, 16 (default), 256
//! - Producer counts: 1 (default), 2, 4
//! - Delays: 0ms, 10ms, 50ms, 200ms per batch
//!
//! **Baseline workflow:**
//! ```bash
//! # Save initial baseline (run once after merging or significant changes)
//! just bench-backpressure-save
//!
//! # Compare against baseline (CI / pre-release)
//! just bench-backpressure-compare
//! ```
//!
//! Criterion will report "Benchmark changed" with percentage difference if
//! throughput deviates significantly from the baseline.
//!
//! Run with: `cargo bench -p ffwd-bench --bench backpressure`

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ffwd_arrow::scanner::Scanner;
use ffwd_bench::generators;
use ffwd_core::scan_config::ScanConfig;

const DELAYS_MS: &[u64] = &[0, 10, 50, 200];

const CHANNEL_CAPACITIES: &[usize] = &[1, 16, 256];

const PRODUCER_COUNTS: &[usize] = &[1, 2, 4];

const RUNTIME_SECS: f64 = 2.0;

struct SlowConsumer {
    delay: Duration,
}

impl SlowConsumer {
    fn new(delay_ms: u64) -> Self {
        Self {
            delay: Duration::from_millis(delay_ms),
        }
    }

    fn consume(&self) {
        if self.delay > Duration::ZERO {
            std::thread::sleep(self.delay);
        }
    }
}

struct ProducerStats {
    batches_sent: std::sync::atomic::AtomicUsize,
    stalls: std::sync::atomic::AtomicUsize,
    total_bytes: std::sync::atomic::AtomicU64,
}

impl ProducerStats {
    fn new() -> Self {
        Self {
            batches_sent: std::sync::atomic::AtomicUsize::new(0),
            stalls: std::sync::atomic::AtomicUsize::new(0),
            total_bytes: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

fn bytes_per_line_estimate(data: &[u8]) -> f64 {
    let newline_count = memchr::memchr_iter(b'\n', data).count();
    if newline_count > 0 {
        data.len() as f64 / newline_count as f64
    } else {
        250.0
    }
}

fn run_backpressure_benchmark(
    delay_ms: u64,
    data_bytes: &Bytes,
    channel_capacity: usize,
    num_producers: usize,
    runtime_secs: f64,
) -> (f64, usize, usize, f64) {
    let consumer = SlowConsumer::new(delay_ms);
    let stats = Arc::new(ProducerStats::new());

    let (tx, rx) = std::sync::mpsc::sync_channel::<Bytes>(channel_capacity);

    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(runtime_secs);

    let mut producer_handles = Vec::new();

    for _ in 0..num_producers {
        let tx_clone = tx.clone();
        let stats_clone = Arc::clone(&stats);
        let data_for_thread = data_bytes.clone();
        let deadline_clone = deadline;

        let handle = std::thread::spawn(move || {
            let mut batches_sent = 0usize;
            let mut stalls = 0usize;
            let mut total_bytes = 0u64;

            while Instant::now() < deadline_clone {
                let mut scanner = Scanner::new(ScanConfig::default());
                scanner
                    .scan_detached(data_for_thread.clone())
                    .expect("benchmark data should scan successfully");
                let batch_bytes = data_for_thread.len() as u64;

                match tx_clone.try_send(data_for_thread.clone()) {
                    Ok(()) => {}
                    Err(std::sync::mpsc::TrySendError::Full(_)) => {
                        stalls += 1;
                        if tx_clone.send(data_for_thread.clone()).is_err() {
                            break;
                        }
                    }
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        break;
                    }
                }

                batches_sent += 1;
                total_bytes += batch_bytes;
            }

            stats_clone
                .batches_sent
                .fetch_add(batches_sent, std::sync::atomic::Ordering::Relaxed);
            stats_clone
                .stalls
                .fetch_add(stalls, std::sync::atomic::Ordering::Relaxed);
            stats_clone
                .total_bytes
                .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);
        });
        producer_handles.push(handle);
    }

    drop(tx);

    let consumer_handle = std::thread::spawn(move || {
        while rx.recv().is_ok() {
            consumer.consume();
        }
    });

    for handle in producer_handles {
        handle.join().unwrap();
    }
    consumer_handle.join().unwrap();

    let elapsed = start.elapsed();
    let batches_sent = stats
        .batches_sent
        .load(std::sync::atomic::Ordering::Relaxed);
    let stalls = stats.stalls.load(std::sync::atomic::Ordering::Relaxed);
    let total_bytes = stats.total_bytes.load(std::sync::atomic::Ordering::Relaxed);

    let bytes_per_sec = total_bytes as f64 / elapsed.as_secs_f64();
    let bpl = bytes_per_line_estimate(data_bytes);
    let lines_per_sec = bytes_per_sec / bpl;

    let stall_rate = if batches_sent > 0 {
        stalls as f64 / batches_sent as f64
    } else {
        0.0
    };

    (lines_per_sec, stalls, batches_sent, stall_rate)
}

fn bench_backpressure_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure_mixed");

    let n = 10_000;
    let data = generators::gen_production_mixed(n, 42);
    let data_bytes = Bytes::from(data);

    group.throughput(Throughput::Bytes(data_bytes.len() as u64));
    group.sample_size(10);

    for &delay_ms in DELAYS_MS {
        for &capacity in CHANNEL_CAPACITIES {
            for &producers in PRODUCER_COUNTS {
                let id = BenchmarkId::new(
                    format!("d{delay_ms}_c{capacity}_p{producers}"),
                    format!("{delay_ms}_{capacity}_{producers}"),
                );
                group.bench_with_input(id, &(delay_ms, capacity, producers), |b, &(delay, capacity, producers)| {
                    let bytes = data_bytes.clone();
                    b.iter_custom(|iters| {
                        let start = Instant::now();
                        for _ in 0..iters {
                            let result = run_backpressure_benchmark(delay, &bytes, capacity, producers, RUNTIME_SECS);
                            std::hint::black_box(result);
                        }
                        start.elapsed()
                    });
                });
            }
        }
    }

    if std::env::var("BACKPRESSURE_SUMMARY").is_ok() {
        #[allow(clippy::print_stderr)]
        {
            eprintln!("\n=== Backpressure Summary (mixed, runtime: {RUNTIME_SECS}s) ===");
            eprintln!("{:>8} {:>8} {:>8} {:>15} {:>12} {:>12} {:>12}", "Delay", "Cap", "Prods", "Lines/sec", "Stalls", "Batches", "Stall Rate");
            for &delay_ms in DELAYS_MS {
                for &capacity in CHANNEL_CAPACITIES {
                    for &producers in PRODUCER_COUNTS {
                        let (lines_per_sec, stalls, batches_sent, stall_rate) =
                            run_backpressure_benchmark(delay_ms, &data_bytes, capacity, producers, RUNTIME_SECS);
                        eprintln!("{:>8} {:>8} {:>8} {:>15.0} {:>12} {:>12} {:>12.3}",
                            format!("{delay_ms}ms"), capacity, producers, lines_per_sec, stalls, batches_sent, stall_rate);
                    }
                }
            }
        }
    }
}

fn bench_backpressure_narrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure_narrow");

    let n = 10_000;
    let data = generators::gen_narrow(n, 42);
    let data_bytes = Bytes::from(data);

    group.throughput(Throughput::Bytes(data_bytes.len() as u64));
    group.sample_size(10);

    for &delay_ms in DELAYS_MS {
        let id = BenchmarkId::new("delay_ms", delay_ms);
        group.bench_with_input(id, &delay_ms, |b, &delay| {
            let bytes = data_bytes.clone();
            b.iter_custom(|iters| {
                let start = Instant::now();
                for _ in 0..iters {
                    let result = run_backpressure_benchmark(delay, &bytes, 16, 1, RUNTIME_SECS);
                    std::hint::black_box(result);
                }
                start.elapsed()
            });
        });
    }
}

fn bench_backpressure_wide(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure_wide");

    let n = 10_000;
    let data = generators::gen_wide(n, 42);
    let data_bytes = Bytes::from(data);

    group.throughput(Throughput::Bytes(data_bytes.len() as u64));
    group.sample_size(10);

    for &delay_ms in DELAYS_MS {
        let id = BenchmarkId::new("delay_ms", delay_ms);
        group.bench_with_input(id, &delay_ms, |b, &delay| {
            let bytes = data_bytes.clone();
            b.iter_custom(|iters| {
                let start = Instant::now();
                for _ in 0..iters {
                    let result = run_backpressure_benchmark(delay, &bytes, 16, 1, RUNTIME_SECS);
                    std::hint::black_box(result);
                }
                start.elapsed()
            });
        });
    }
}

fn bench_backpressure_capacity(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure_capacity");

    let n = 10_000;
    let data = generators::gen_production_mixed(n, 42);
    let data_bytes = Bytes::from(data);

    group.throughput(Throughput::Bytes(data_bytes.len() as u64));
    group.sample_size(10);

    for &capacity in CHANNEL_CAPACITIES {
        let id = BenchmarkId::new("capacity", capacity);
        group.bench_with_input(id, &capacity, |b, &capacity| {
            let bytes = data_bytes.clone();
            b.iter_custom(|iters| {
                let start = Instant::now();
                for _ in 0..iters {
                    let result = run_backpressure_benchmark(50, &bytes, capacity, 1, RUNTIME_SECS);
                    std::hint::black_box(result);
                }
                start.elapsed()
            });
        });
    }
}

fn bench_backpressure_producers(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure_producers");

    let n = 10_000;
    let data = generators::gen_production_mixed(n, 42);
    let data_bytes = Bytes::from(data);

    group.throughput(Throughput::Bytes(data_bytes.len() as u64));
    group.sample_size(10);

    for &producers in PRODUCER_COUNTS {
        let id = BenchmarkId::new("producers", producers);
        group.bench_with_input(id, &producers, |b, &producers| {
            let bytes = data_bytes.clone();
            b.iter_custom(|iters| {
                let start = Instant::now();
                for _ in 0..iters {
                    let result = run_backpressure_benchmark(50, &bytes, 16, producers, RUNTIME_SECS);
                    std::hint::black_box(result);
                }
                start.elapsed()
            });
        });
    }
}

criterion_group!(
    benches,
    bench_backpressure_mixed,
    bench_backpressure_narrow,
    bench_backpressure_wide,
    bench_backpressure_capacity,
    bench_backpressure_producers
);
criterion_main!(benches);
