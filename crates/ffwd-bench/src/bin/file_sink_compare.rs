#![allow(clippy::print_stdout, clippy::print_stderr)]

//! Comprehensive file output strategy comparison.
//!
//! Tests three key dimensions:
//! 1. Serialization+I/O threading strategies (serial vs threaded pipeline)
//! 2. Durability modes (no-sync, periodic sync, every-batch sync)
//! 3. Sustained load (enough volume to exhaust page cache benefit)
//!
//! Run with: cargo run --release -p ffwd-bench --bin file_sink_compare -- [OPTIONS]
//!   --schema narrow|wide|mixed   Data schema (default: wide)
//!   --batches N                  Number of batches (default: 200)
//!   --batch-size N               Rows per batch (default: 10000)
//!   --sync-interval N            Fsync every N batches, 0=never (default: 0)
//!   --scenario all|basic|sync|sustained  Which test set (default: all)

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use ffwd_bench::generators;
use ffwd_output::{BatchMetadata, FileSinkFactory, SinkFactory, StdoutFormat, StdoutSink};
use ffwd_types::diagnostics::ComponentStats;
use tempfile::NamedTempFile;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

const TOKIO_BUF_CAPACITY: usize = 16 * 1024 * 1024;
const STD_BUF_CAPACITY: usize = 16 * 1024 * 1024;

struct StrategyResult {
    name: &'static str,
    elapsed: Duration,
}

fn parse_flag(args: &[String], flag: &str, default: &str) -> String {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .map_or_else(|| default.to_string(), String::clone)
}

fn parse_usize_flag(args: &[String], flag: &str, default: usize) -> usize {
    parse_flag(args, flag, &default.to_string())
        .parse()
        .unwrap_or(default)
}

fn fmt_throughput(total_rows: u64, total_bytes: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return "N/A".to_owned();
    }
    let lines_per_sec = total_rows as f64 / secs;
    let mb_per_sec = total_bytes as f64 / 1_048_576.0 / secs;
    format!("{lines_per_sec:.0} lines/sec ({mb_per_sec:.1} MB/sec)")
}

fn fmt_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn generate_batch(schema: &str, batch_size: usize) -> (RecordBatch, BatchMetadata, &'static str) {
    match schema {
        "wide" => (
            generators::gen_wide_batch(batch_size, 42),
            generators::make_metadata(),
            "wide",
        ),
        "mixed" => (
            generators::gen_production_mixed_batch(batch_size, 42),
            generators::make_metadata(),
            "mixed",
        ),
        _ => (
            generators::gen_narrow_batch(batch_size, 42),
            generators::make_metadata(),
            "narrow",
        ),
    }
}

fn serialize_batch(batch: &RecordBatch, meta: &BatchMetadata) -> io::Result<Vec<u8>> {
    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
    let mut out = Vec::with_capacity(batch.num_rows() * 300);
    sink.write_batch_to(batch, meta, &mut out)?;
    Ok(out)
}

fn run_serialize_only(
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
    let mut out = Vec::with_capacity(batch.num_rows() * 300);
    let start = Instant::now();
    for _ in 0..num_batches {
        out.clear();
        sink.write_batch_to(batch, meta, &mut out)?;
        std::hint::black_box(&out);
    }
    Ok(StrategyResult {
        name: "serialize_only",
        elapsed: start.elapsed(),
    })
}

fn run_serialize_std_nosync(
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
    let mut out = Vec::with_capacity(batch.num_rows() * 300);
    let tmp = NamedTempFile::new()?;
    let mut file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let start = Instant::now();
    for _ in 0..num_batches {
        out.clear();
        sink.write_batch_to(batch, meta, &mut out)?;
        file.write_all(&out)?;
    }
    Ok(StrategyResult {
        name: "serialize_std_nosync",
        elapsed: start.elapsed(),
    })
}

fn run_serialize_tokio_nosync(
    rt: &Runtime,
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
    let mut out = Vec::with_capacity(batch.num_rows() * 300);
    let tmp = NamedTempFile::new()?;
    let std_file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let mut file = tokio::fs::File::from_std(std_file);
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..num_batches {
            out.clear();
            sink.write_batch_to(batch, meta, &mut out)?;
            file.write_all(&out).await?;
        }
        Ok::<(), io::Error>(())
    })?;
    Ok(StrategyResult {
        name: "serialize_tokio_nosync",
        elapsed: start.elapsed(),
    })
}

fn run_serialize_tokio_mutex_nosync(
    rt: &Runtime,
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
    let mut out = Vec::with_capacity(batch.num_rows() * 300);
    let tmp = NamedTempFile::new()?;
    let std_file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let file = Arc::new(Mutex::new(tokio::fs::File::from_std(std_file)));
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..num_batches {
            out.clear();
            sink.write_batch_to(batch, meta, &mut out)?;
            let mut guard = file.lock().await;
            guard.write_all(&out).await?;
        }
        Ok::<(), io::Error>(())
    })?;
    Ok(StrategyResult {
        name: "serialize_tokio_mutex",
        elapsed: start.elapsed(),
    })
}

fn run_std_direct(payload: &[u8], num_batches: usize) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let mut file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let start = Instant::now();
    for _ in 0..num_batches {
        file.write_all(payload)?;
    }
    file.flush()?;
    file.sync_data()?;
    Ok(StrategyResult {
        name: "std_direct",
        elapsed: start.elapsed(),
    })
}

fn run_std_direct_nosync(payload: &[u8], num_batches: usize) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let mut file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let start = Instant::now();
    for _ in 0..num_batches {
        file.write_all(payload)?;
    }
    Ok(StrategyResult {
        name: "std_direct_nosync",
        elapsed: start.elapsed(),
    })
}

fn run_std_bufwriter(payload: &[u8], num_batches: usize) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let mut file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let start = Instant::now();
    {
        let mut writer = io::BufWriter::with_capacity(STD_BUF_CAPACITY, &mut file);
        for _ in 0..num_batches {
            writer.write_all(payload)?;
        }
        writer.flush()?;
    }
    file.sync_data()?;
    Ok(StrategyResult {
        name: "std_bufwriter_16m",
        elapsed: start.elapsed(),
    })
}

fn run_tokio_direct(
    rt: &Runtime,
    payload: &[u8],
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let std_file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let mut file = tokio::fs::File::from_std(std_file);
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..num_batches {
            file.write_all(payload).await?;
        }
        file.flush().await?;
        file.sync_data().await
    })?;
    Ok(StrategyResult {
        name: "tokio_direct",
        elapsed: start.elapsed(),
    })
}

fn run_tokio_direct_nosync(
    rt: &Runtime,
    payload: &[u8],
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let std_file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let mut file = tokio::fs::File::from_std(std_file);
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..num_batches {
            file.write_all(payload).await?;
        }
        Ok::<(), io::Error>(())
    })?;
    Ok(StrategyResult {
        name: "tokio_direct_nosync",
        elapsed: start.elapsed(),
    })
}

fn run_tokio_bufwriter(
    rt: &Runtime,
    payload: &[u8],
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let std_file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let file = tokio::fs::File::from_std(std_file);
    let mut writer = BufWriter::with_capacity(TOKIO_BUF_CAPACITY, file);
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..num_batches {
            writer.write_all(payload).await?;
        }
        writer.flush().await?;
        writer.get_mut().sync_data().await
    })?;
    Ok(StrategyResult {
        name: "tokio_bufwriter_16m",
        elapsed: start.elapsed(),
    })
}

fn run_file_sink(
    rt: &Runtime,
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let stats = Arc::new(ComponentStats::new());
    let factory = FileSinkFactory::new(
        "bench".into(),
        tmp.path().to_string_lossy().into_owned(),
        StdoutFormat::Json,
        stats,
    )?;
    let mut sink = factory.create()?;
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..num_batches {
            match sink.send_batch(batch, meta).await {
                ffwd_output::SendResult::Ok => {}
                other => {
                    return Err(io::Error::other(format!(
                        "file sink send_batch failed: {other:?}"
                    )));
                }
            }
        }
        sink.flush().await
    })?;
    Ok(StrategyResult {
        name: "file_sink_full",
        elapsed: start.elapsed(),
    })
}

fn run_file_sink_send_only(
    rt: &Runtime,
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let stats = Arc::new(ComponentStats::new());
    let factory = FileSinkFactory::new(
        "bench".into(),
        tmp.path().to_string_lossy().into_owned(),
        StdoutFormat::Json,
        stats,
    )?;
    let mut sink = factory.create()?;
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..num_batches {
            match sink.send_batch(batch, meta).await {
                ffwd_output::SendResult::Ok => {}
                other => {
                    return Err(io::Error::other(format!(
                        "file sink send_batch failed: {other:?}"
                    )));
                }
            }
        }
        Ok::<(), io::Error>(())
    })?;
    Ok(StrategyResult {
        name: "file_sink_send_only",
        elapsed: start.elapsed(),
    })
}

fn return_buffer(tx: &SyncSender<Vec<u8>>, mut buf: Vec<u8>) -> io::Result<()> {
    buf.clear();
    tx.send(buf)
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "buffer return channel closed"))
}

/// Threaded pipeline: serialization on caller, I/O on dedicated OS thread.
/// Double-buffered with sync_channel.
fn run_threaded_pipeline(
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
    sync_interval: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let payload_capacity = batch.num_rows() * 300;

    // Use 3 buffers for triple-buffering: writer can drain while serializer fills next
    let (filled_tx, filled_rx) = sync_channel::<Vec<u8>>(3);
    let (empty_tx, empty_rx) = sync_channel::<Vec<u8>>(3);
    for _ in 0..3 {
        empty_tx
            .send(Vec::with_capacity(payload_capacity))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "failed to seed empty buffer")
            })?;
    }

    let sync_iv = sync_interval;
    let writer_empty_tx = empty_tx.clone();
    let writer = thread::spawn(move || -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        let mut count = 0usize;
        while let Ok(buf) = filled_rx.recv() {
            file.write_all(&buf)?;
            return_buffer(&writer_empty_tx, buf)?;
            count += 1;
            if sync_iv > 0 && count % sync_iv == 0 {
                file.sync_data()?;
            }
        }
        file.flush()?;
        file.sync_data()?;
        Ok(())
    });

    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
    let start = Instant::now();
    for _ in 0..num_batches {
        let mut buf = empty_rx.recv().map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "empty buffer channel closed")
        })?;
        sink.write_batch_to(batch, meta, &mut buf)?;
        filled_tx.send(buf).map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "filled buffer channel closed")
        })?;
    }
    drop(filled_tx);
    drop(empty_tx);
    writer
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))??;

    let name = if sync_interval > 0 {
        "threaded_sync"
    } else {
        "threaded_nosync"
    };
    Ok(StrategyResult {
        name,
        elapsed: start.elapsed(),
    })
}

/// Serial pipeline with periodic sync: serialize then write sequentially, fsync every N.
fn run_serial_periodic_sync(
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
    sync_interval: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let mut file = std::fs::OpenOptions::new().append(true).open(tmp.path())?;
    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
    let mut out = Vec::with_capacity(batch.num_rows() * 300);

    let start = Instant::now();
    for i in 0..num_batches {
        out.clear();
        sink.write_batch_to(batch, meta, &mut out)?;
        file.write_all(&out)?;
        if sync_interval > 0 && (i + 1) % sync_interval == 0 {
            file.sync_data()?;
        }
    }
    file.flush()?;
    file.sync_data()?;

    let name = if sync_interval > 0 {
        "serial_sync"
    } else {
        "serial_nosync"
    };
    Ok(StrategyResult {
        name,
        elapsed: start.elapsed(),
    })
}

/// Multi-producer threaded pipeline: N serializer threads feed one writer thread.
/// Simulates real pipeline with multiple workers.
fn run_multi_producer_threaded(
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
    num_producers: usize,
    sync_interval: usize,
) -> io::Result<StrategyResult> {
    use std::sync::Mutex as StdMutex;

    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let payload_capacity = batch.num_rows() * 300;

    // Shared channels: all producers write filled buffers, shared empty pool
    let (filled_tx, filled_rx) = sync_channel::<Vec<u8>>(num_producers * 2);
    let (empty_tx, empty_rx) = sync_channel::<Vec<u8>>(num_producers * 2);
    for _ in 0..(num_producers * 2) {
        empty_tx
            .send(Vec::with_capacity(payload_capacity))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "failed to seed empty buffer")
            })?;
    }

    // Wrap receiver in Arc<Mutex> for sharing among producers
    let empty_rx = Arc::new(StdMutex::new(empty_rx));

    let sync_iv = sync_interval;
    let writer_empty_tx = empty_tx.clone();
    let writer = thread::spawn(move || -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        let mut count = 0usize;
        while let Ok(buf) = filled_rx.recv() {
            file.write_all(&buf)?;
            return_buffer(&writer_empty_tx, buf)?;
            count += 1;
            if sync_iv > 0 && count % sync_iv == 0 {
                file.sync_data()?;
            }
        }
        file.flush()?;
        file.sync_data()?;
        Ok(())
    });

    let batches_per_producer = num_batches / num_producers;
    let start = Instant::now();

    // Spawn producer threads
    let producers: Vec<_> = (0..num_producers)
        .map(|_| {
            let batch = batch.clone();
            let meta = meta.clone();
            let filled_tx = filled_tx.clone();
            let empty_rx = Arc::clone(&empty_rx);
            thread::spawn(move || -> io::Result<()> {
                let stats = Arc::new(ComponentStats::new());
                let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
                for _ in 0..batches_per_producer {
                    let mut buf = empty_rx
                        .lock()
                        .map_err(|_| io::Error::other("mutex poisoned"))?
                        .recv()
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "empty buffer channel closed",
                            )
                        })?;
                    sink.write_batch_to(&batch, &meta, &mut buf)?;
                    filled_tx.send(buf).map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "filled buffer channel closed")
                    })?;
                }
                Ok(())
            })
        })
        .collect();

    // Drop our copies so the writer sees EOF when all producers finish
    drop(filled_tx);
    drop(empty_tx);

    for p in producers {
        p.join()
            .map_err(|_| io::Error::other("producer thread panicked"))??;
    }
    writer
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))??;

    Ok(StrategyResult {
        name: "multi_producer",
        elapsed: start.elapsed(),
    })
}

/// Threaded pipeline with BufWriter on the writer thread.
/// Reduces syscall count by coalescing small writes.
fn run_threaded_bufwriter(
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
    sync_interval: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let payload_capacity = batch.num_rows() * 300;

    let (filled_tx, filled_rx) = sync_channel::<Vec<u8>>(3);
    let (empty_tx, empty_rx) = sync_channel::<Vec<u8>>(3);
    for _ in 0..3 {
        empty_tx
            .send(Vec::with_capacity(payload_capacity))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "failed to seed empty buffer")
            })?;
    }

    let sync_iv = sync_interval;
    let writer_empty_tx = empty_tx.clone();
    let writer = thread::spawn(move || -> io::Result<()> {
        let file = std::fs::OpenOptions::new().append(true).open(&path)?;
        let mut writer = io::BufWriter::with_capacity(STD_BUF_CAPACITY, file);
        let mut count = 0usize;
        while let Ok(buf) = filled_rx.recv() {
            writer.write_all(&buf)?;
            return_buffer(&writer_empty_tx, buf)?;
            count += 1;
            if sync_iv > 0 && count % sync_iv == 0 {
                writer.flush()?;
                writer.get_mut().sync_data()?;
            }
        }
        writer.flush()?;
        writer.get_mut().sync_data()?;
        Ok(())
    });

    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, stats);
    let start = Instant::now();
    for _ in 0..num_batches {
        let mut buf = empty_rx.recv().map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "empty buffer channel closed")
        })?;
        sink.write_batch_to(batch, meta, &mut buf)?;
        filled_tx.send(buf).map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "filled buffer channel closed")
        })?;
    }
    drop(filled_tx);
    drop(empty_tx);
    writer
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))??;

    let name = if sync_interval > 0 {
        "threaded_buf_sync"
    } else {
        "threaded_buf_nosync"
    };
    Ok(StrategyResult {
        name,
        elapsed: start.elapsed(),
    })
}

/// FileSink with periodic flush (simulates real-world durability).
fn run_file_sink_periodic_sync(
    rt: &Runtime,
    batch: &RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
    sync_interval: usize,
) -> io::Result<StrategyResult> {
    let tmp = NamedTempFile::new()?;
    let stats = Arc::new(ComponentStats::new());
    let factory = FileSinkFactory::new(
        "bench".into(),
        tmp.path().to_string_lossy().into_owned(),
        StdoutFormat::Json,
        stats,
    )?;
    let mut sink = factory.create()?;
    let start = Instant::now();
    rt.block_on(async {
        for i in 0..num_batches {
            match sink.send_batch(batch, meta).await {
                ffwd_output::SendResult::Ok => {}
                other => {
                    return Err(io::Error::other(format!(
                        "file sink send_batch failed: {other:?}"
                    )));
                }
            }
            if sync_interval > 0 && (i + 1) % sync_interval == 0 {
                sink.flush().await?;
            }
        }
        sink.flush().await
    })?;

    let name = if sync_interval > 0 {
        "filesink_sync"
    } else {
        "filesink_nosync"
    };
    Ok(StrategyResult {
        name,
        elapsed: start.elapsed(),
    })
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: file_sink_compare [OPTIONS]");
        eprintln!("  --schema narrow|wide|mixed  Data schema (default: wide)");
        eprintln!("  --batches N                 Number of batches (default: 200)");
        eprintln!("  --batch-size N              Rows per batch (default: 10000)");
        eprintln!("  --sync-interval N           Fsync every N batches, 0=never (default: 0)");
        eprintln!("  --scenario all|basic|sync|sustained|threaded");
        eprintln!("                              Which test set (default: all)");
        return Ok(());
    }

    let schema = parse_flag(&args, "--schema", "wide");
    let num_batches = parse_usize_flag(&args, "--batches", 200);
    let batch_size = parse_usize_flag(&args, "--batch-size", 10_000);
    let sync_interval = parse_usize_flag(&args, "--sync-interval", 0);
    let scenario = parse_flag(&args, "--scenario", "all");

    let (batch, meta, schema_name) = generate_batch(&schema, batch_size);
    let payload = serialize_batch(&batch, &meta)?;
    let total_rows = (num_batches * batch.num_rows()) as u64;
    let total_bytes = payload.len() as u64 * num_batches as u64;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    println!("=== File Sink Compare ===");
    println!("Schema:        {schema_name}");
    println!("Batch rows:    {}", batch.num_rows());
    println!("Batches:       {num_batches}");
    println!(
        "Payload/batch: {} ({:.0} bytes/row)",
        fmt_bytes(payload.len() as u64),
        payload.len() as f64 / batch.num_rows() as f64
    );
    println!("Total output:  {}", fmt_bytes(total_bytes));
    if sync_interval > 0 {
        println!("Sync interval: every {sync_interval} batches");
    }
    println!();

    let mut results: Vec<StrategyResult> = Vec::new();

    // === Basic strategies (original set) ===
    if scenario == "all" || scenario == "basic" {
        println!("--- Basic Strategies ---");
        results.push(run_serialize_only(&batch, &meta, num_batches)?);
        results.push(run_serialize_std_nosync(&batch, &meta, num_batches)?);
        results.push(run_serialize_tokio_nosync(&rt, &batch, &meta, num_batches)?);
        results.push(run_serialize_tokio_mutex_nosync(&rt, &batch, &meta, num_batches)?);
        results.push(run_std_direct_nosync(&payload, num_batches)?);
        results.push(run_std_direct(&payload, num_batches)?);
        results.push(run_std_bufwriter(&payload, num_batches)?);
        results.push(run_tokio_direct_nosync(&rt, &payload, num_batches)?);
        results.push(run_tokio_direct(&rt, &payload, num_batches)?);
        results.push(run_tokio_bufwriter(&rt, &payload, num_batches)?);
        results.push(run_file_sink_send_only(&rt, &batch, &meta, num_batches)?);
        results.push(run_file_sink(&rt, &batch, &meta, num_batches)?);
    }

    // === Threaded pipeline comparison ===
    if scenario == "all" || scenario == "threaded" {
        println!("--- Threaded Pipeline Strategies ---");
        // No sync
        results.push(run_serial_periodic_sync(&batch, &meta, num_batches, 0)?);
        results.push(run_threaded_pipeline(&batch, &meta, num_batches, 0)?);
        results.push(run_threaded_bufwriter(&batch, &meta, num_batches, 0)?);
        results.push(run_file_sink_periodic_sync(
            &rt, &batch, &meta, num_batches, 0,
        )?);
    }

    // === Sync-interval comparison (the money test) ===
    if scenario == "all" || scenario == "sync" {
        let intervals = if sync_interval > 0 {
            vec![sync_interval]
        } else {
            vec![10, 50]
        };
        for iv in &intervals {
            println!("--- Sync Every {iv} Batches ---");
            results.push(run_serial_periodic_sync(&batch, &meta, num_batches, *iv)?);
            results.push(run_threaded_pipeline(&batch, &meta, num_batches, *iv)?);
            results.push(run_threaded_bufwriter(&batch, &meta, num_batches, *iv)?);
            results.push(run_file_sink_periodic_sync(
                &rt, &batch, &meta, num_batches, *iv,
            )?);
        }
    }

    // === Sustained high-volume test ===
    if scenario == "all" || scenario == "sustained" {
        // Use 2000 batches (20M rows) to exhaust page cache benefit
        let sustained_batches = 2000;
        let sustained_total_rows = (sustained_batches * batch.num_rows()) as u64;
        let sustained_total_bytes = payload.len() as u64 * sustained_batches as u64;
        println!(
            "--- Sustained Volume ({} / {} rows) ---",
            fmt_bytes(sustained_total_bytes),
            sustained_total_rows
        );
        let s1 = run_serial_periodic_sync(&batch, &meta, sustained_batches, 50)?;
        let s2 = run_threaded_pipeline(&batch, &meta, sustained_batches, 50)?;
        let s3 = run_threaded_bufwriter(&batch, &meta, sustained_batches, 50)?;
        let s4 = run_multi_producer_threaded(&batch, &meta, sustained_batches, 2, 50)?;
        let s5 = run_multi_producer_threaded(&batch, &meta, sustained_batches, 4, 50)?;

        println!("| Strategy | Elapsed | Per Batch | Throughput |");
        println!("|----------|---------|-----------|------------|");
        for result in [&s1, &s2, &s3, &s4, &s5] {
            let per_batch = result.elapsed / sustained_batches as u32;
            println!(
                "| {} | {:>7.1?} | {:>9.1?} | {} |",
                result.name,
                result.elapsed,
                per_batch,
                fmt_throughput(sustained_total_rows, sustained_total_bytes, result.elapsed),
            );
        }
        println!();
    }

    // Print main results table
    if !results.is_empty() {
        println!("| Strategy | Elapsed | Per Batch | Throughput |");
        println!("|----------|---------|-----------|------------|");
        for result in &results {
            let per_batch = result.elapsed / num_batches as u32;
            println!(
                "| {} | {:>7.1?} | {:>9.1?} | {} |",
                result.name,
                result.elapsed,
                per_batch,
                fmt_throughput(total_rows, total_bytes, result.elapsed),
            );
        }
    }

    Ok(())
}
