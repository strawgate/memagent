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
//! **Delays tested:** 0ms, 10ms, 50ms, 200ms per batch
//!
//! Run with: `cargo bench -p ffwd-bench --bench backpressure`

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ffwd_arrow::scanner::Scanner;
use ffwd_bench::generators;
use ffwd_core::scan_config::ScanConfig;

const CHANNEL_CAPACITY: usize = 16;

/// Approximate bytes per line for the production_mixed generator output.
/// Used only for throughput estimation (lines/sec) since production_mixed varies.
const ESTIMATED_BYTES_PER_LINE: f64 = 250.0;

const DELAYS_MS: &[u64] = &[0, 10, 50, 200];

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

fn run_backpressure_benchmark(
    delay_ms: u64,
    data_bytes: &Bytes,
    runtime_secs: f64,
) -> (f64, usize, usize, f64) {
    let consumer = SlowConsumer::new(delay_ms);
    let stats = Arc::new(ProducerStats::new());

    // std::sync::mpsc::sync_channel is bounded (like the pipeline's mpsc channel)
    let (tx, rx) = std::sync::mpsc::sync_channel::<Bytes>(CHANNEL_CAPACITY);

    let stats_clone = Arc::clone(&stats);
    // Clone once so the thread owns its copy (Bytes clone is cheap — refcount bump)
    let data_for_thread = data_bytes.clone();

    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(runtime_secs);

    // Producer thread: sends batches until deadline
    let producer_handle = std::thread::spawn(move || {
        let mut batches_sent = 0usize;
        let mut stalls = 0usize;
        let mut total_bytes = 0u64;

        while Instant::now() < deadline {
            // Scan to add realistic CPU work (simulates scanner in the pipeline).
            // RecordBatch is not Sync so we cannot send it through std::sync::mpsc.
            // We send raw bytes instead — this measures channel backpressure behavior,
            // not scanner throughput. The scan call above is the CPU work simulation.
            let mut scanner = Scanner::new(ScanConfig::default());
            // Panic on scan failure to avoid misleading partial results
            scanner
                .scan_detached(data_for_thread.clone())
                .expect("benchmark data should scan successfully");
            let batch_bytes = data_for_thread.len() as u64;

            // Try non-blocking send first to count stalls, then blocking send if needed
            match tx.try_send(data_for_thread.clone()) {
                Ok(()) => {
                    // Sent immediately — no stall
                }
                Err(std::sync::mpsc::TrySendError::Full(_)) => {
                    // Channel full — this is a stall; block until sent
                    stalls += 1;
                    if tx.send(data_for_thread.clone()).is_err() {
                        break; // channel closed (consumer done)
                    }
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    // Consumer exited — producer is done
                    break;
                }
            }

            batches_sent += 1;
            total_bytes += batch_bytes;
        }

        stats_clone
            .batches_sent
            .store(batches_sent, std::sync::atomic::Ordering::Relaxed);
        stats_clone
            .stalls
            .store(stalls, std::sync::atomic::Ordering::Relaxed);
        stats_clone
            .total_bytes
            .store(total_bytes, std::sync::atomic::Ordering::Relaxed);
    });

    // Consumer thread: receives and "processes" (sleeps) until producer done
    let consumer_handle = std::thread::spawn(move || {
        while rx.recv().is_ok() {
            // Simulate slow output
            consumer.consume();
        }
    });

    // Wait for producer to finish (it closes tx by dropping when done)
    producer_handle.join().unwrap();
    // Consumer will exit when tx is dropped
    consumer_handle.join().unwrap();

    let elapsed = start.elapsed();
    let batches_sent = stats
        .batches_sent
        .load(std::sync::atomic::Ordering::Relaxed);
    let stalls = stats.stalls.load(std::sync::atomic::Ordering::Relaxed);
    let total_bytes = stats.total_bytes.load(std::sync::atomic::Ordering::Relaxed);

    // Throughput in bytes/sec and lines/sec
    let bytes_per_sec = total_bytes as f64 / elapsed.as_secs_f64();
    let lines_per_sec = bytes_per_sec / ESTIMATED_BYTES_PER_LINE;

    // Stall rate
    let stall_rate = if batches_sent > 0 {
        stalls as f64 / batches_sent as f64
    } else {
        0.0
    };

    (lines_per_sec, stalls, batches_sent, stall_rate)
}

fn bench_backpressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure");

    // Fixed dataset: 10K rows of production_mixed data
    let n = 10_000;
    let data = generators::gen_production_mixed(n, 42);
    // Pre-construct Bytes once to avoid repeated allocation inside b.iter
    let data_bytes = Bytes::from(data.clone());
    let runtime_secs = 2.0; // Short runs for benchmarking

    group.throughput(Throughput::Bytes(data_bytes.len() as u64));
    group.sample_size(10);

    for &delay_ms in DELAYS_MS {
        group.bench_with_input(
            BenchmarkId::new("delay_ms", delay_ms),
            &delay_ms,
            |b, &delay| {
                // Clone Bytes cheaply (ref count bump) inside the timed iteration
                let bytes = data_bytes.clone();
                // Use iter_custom so Criterion times the actual function call.
                // The function self-times for runtime_secs and returns metrics;
                // we return the wall-clock elapsed time so Criterion reports accurately.
                // Loop iters times and return total elapsed so Criterion computes
                // per-iteration time correctly (divides total by iteration count).
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let result = run_backpressure_benchmark(delay, &bytes, runtime_secs);
                        std::hint::black_box(result);
                    }
                    start.elapsed()
                });
            },
        );
    }

    // Summary table: printed to stderr so it shows up in benchmark output.
    // Re-running the benchmarks is intentional — criterion's measured times include
    // all thread coordination overhead, while this summary shows aggregate behavior.
    // Gate behind BACKPRESSURE_SUMMARY=1 for CI environments where extra runtime matters.
    if std::env::var("BACKPRESSURE_SUMMARY").is_ok() {
        eprintln!(
            "\n=== Backpressure Summary (runtime: {}s per config) ===",
            runtime_secs
        );
        eprintln!(
            "{:<12} {:>15} {:>12} {:>12} {:>12}",
            "Delay", "Lines/sec", "Stalls", "Batches", "Stall Rate"
        );

        for &delay_ms in DELAYS_MS {
            let (lines_per_sec, stalls, batches_sent, stall_rate) =
                run_backpressure_benchmark(delay_ms, &data_bytes, runtime_secs);

            eprintln!(
                "{:<12} {:>15.0} {:>12} {:>12} {:>12.3}",
                format!("{}ms", delay_ms),
                lines_per_sec,
                stalls,
                batches_sent,
                stall_rate
            );
        }
    }
}

criterion_group!(benches, bench_backpressure);
criterion_main!(benches);
