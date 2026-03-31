#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::env;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use logfwd::chunk::ChunkConfig;
use logfwd::compress::ChunkCompressor;
use logfwd::otlp;
#[cfg(unix)]
use logfwd::daemon::{self, DaemonConfig};
use logfwd::pipeline::{self, OutputMode, PipelineConfig, PipelineStats};
#[cfg(unix)]
use logfwd::tail::{FileTailer, TailConfig, TailEvent};

fn main() -> io::Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: logfwd <file> [options]");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --mode <raw|otlp|passthrough>   Output mode (default: raw)");
        eprintln!("  --chunk-size <bytes>             Initial chunk size (default: 262144)");
        eprintln!("  --no-adaptive                    Disable adaptive chunk tuning");
        eprintln!("  --level <1-3>                    Zstd compression level (default: 1)");
        eprintln!("  --tail                           Live tail mode (follow file, Ctrl-C to stop)");
        eprintln!();
        eprintln!("V2 pipeline (from YAML config):");
        eprintln!("  logfwd --config <config.yaml> [--validate] [--dry-run]");
        eprintln!();
        eprintln!("Daemon mode (for K8s DaemonSet):");
        eprintln!("  logfwd --daemon [--glob PATTERN] [--endpoint URL] [--collector NAME]");
        eprintln!();
        eprintln!("Blackhole OTLP collector (for benchmarks):");
        eprintln!("  logfwd --blackhole [bind_addr]                  (default: 127.0.0.1:4318)");
        eprintln!();
        eprintln!("Generate synthetic data:");
        eprintln!("  logfwd --generate <num_lines> <output_file>");
        eprintln!("  logfwd --generate-json <num_lines> <output_file>");
        std::process::exit(1);
    }

    if args[1] == "--generate" || args[1] == "--generate-json" {
        if args.len() < 4 {
            eprintln!("Usage: logfwd {} <num_lines> <output_file>", args[1]);
            std::process::exit(1);
        }
        let num_lines: usize = args[2].parse().expect("invalid num_lines");
        let output = &args[3];
        return if args[1] == "--generate-json" {
            generate_json_log_file(num_lines, output)
        } else {
            generate_log_file(num_lines, output)
        };
    }

    // Handle --blackhole mode: fake OTLP collector that accepts and discards
    if args[1] == "--blackhole" {
        let addr = args.get(2).map(|s| s.as_str()).unwrap_or("127.0.0.1:4318");
        return run_blackhole(addr);
    }

    // Handle --e2e mode: end-to-end benchmark with per-stage timing
    if args[1] == "--e2e" {
        if args.len() < 3 {
            eprintln!("Usage: logfwd --e2e <cri_file> [mode]");
            eprintln!("  Modes: cri-only, jsonlines, jsonlines-zstd, otlp, otlp-zstd (default)");
            eprintln!();
            eprintln!("  Generate CRI test data from JSON:");
            eprintln!("    logfwd --generate-json 5000000 /tmp/json.txt");
            eprintln!("    logfwd --e2e --wrap-cri /tmp/json.txt /tmp/cri.txt");
            std::process::exit(1);
        }

        // Handle --wrap-cri subcommand
        if args[2] == "--wrap-cri" {
            if args.len() < 5 {
                eprintln!("Usage: logfwd --e2e --wrap-cri <json_input> <cri_output>");
                std::process::exit(1);
            }
            eprintln!("Wrapping {} in CRI format -> {}", args[3], args[4]);
            logfwd::e2e_bench::generate_cri_file(&args[3], &args[4])?;
            let size = std::fs::metadata(&args[4])?.len();
            eprintln!("Done: {:.1} MB", size as f64 / (1024.0 * 1024.0));
            return Ok(());
        }

        let cri_file = &args[2];
        let mode_str = args.get(3).map(|s| s.as_str()).unwrap_or("otlp-zstd");
        let mode = match mode_str {
            "cri-only" => logfwd::e2e_bench::E2eMode::CriOnly,
            "jsonlines" => logfwd::e2e_bench::E2eMode::JsonLines,
            "jsonlines-zstd" => logfwd::e2e_bench::E2eMode::JsonLinesCompressed,
            "otlp" => logfwd::e2e_bench::E2eMode::OtlpProto,
            "otlp-zstd" => logfwd::e2e_bench::E2eMode::OtlpCompressed,
            "otlp-raw" => logfwd::e2e_bench::E2eMode::OtlpRaw,
            "otlp-raw-zstd" => logfwd::e2e_bench::E2eMode::OtlpRawCompressed,
            other => {
                eprintln!("Unknown e2e mode: {other}");
                std::process::exit(1);
            }
        };

        eprintln!("E2E benchmark: {} (mode: {mode_str})", cri_file);
        eprintln!("Warming cache...");
        let _ = logfwd::e2e_bench::run_e2e(cri_file, logfwd::e2e_bench::E2eMode::CriOnly)?;

        eprintln!("Running 3 iterations...");
        let mut best: Option<logfwd::e2e_bench::E2eTimings> = None;
        for run in 1..=3 {
            let t = logfwd::e2e_bench::run_e2e(cri_file, mode.clone())?;
            let lps = t.total_lines as f64 / (t.elapsed_ns as f64 / 1e9);
            eprintln!("  Run {run}: {:.0} lines/sec", lps);
            if best.as_ref().map_or(true, |b| t.elapsed_ns < b.elapsed_ns) {
                best = Some(t);
            }
        }
        eprintln!();
        best.unwrap().print_report();
        return Ok(());
    }

    // Handle --config mode (v2 Arrow pipeline from YAML config)
    #[cfg(unix)]
    if args[1] == "--config" {
        if args.len() < 3 {
            eprintln!("Usage: logfwd --config <config.yaml> [--validate] [--dry-run]");
            std::process::exit(1);
        }
        let config_path = &args[2];
        let validate_only = args.iter().any(|a| a == "--validate");
        let dry_run = args.iter().any(|a| a == "--dry-run");

        let config = logfwd::config::Config::load(config_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        if validate_only {
            eprintln!("Config OK: {} pipeline(s)", config.pipelines.len());
            return Ok(());
        }

        return run_v2_pipelines(config, dry_run);
    }

    // Handle --daemon mode
    #[cfg(unix)]
    if args[1] == "--daemon" {
        let mut dc = DaemonConfig::default();
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str() {
                "--glob" => { i += 1; dc.glob_pattern = args[i].clone(); }
                "--endpoint" => { i += 1; dc.endpoint = args[i].clone(); }
                "--collector" => { i += 1; dc.collector_name = args[i].clone(); }
                _ => { eprintln!("Unknown daemon arg: {}", args[i]); std::process::exit(1); }
            }
            i += 1;
        }
        return daemon::run_daemon(dc);
    }

    // Parse arguments
    let path = PathBuf::from(&args[1]);
    let mut chunk_size = 256 * 1024usize;
    let mut adaptive = true;
    let mut level = 1i32;
    let mut mode = OutputMode::RawChunk;
    let mut tail_mode = false;

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--chunk-size" => {
                i += 1;
                chunk_size = args[i].parse().expect("invalid chunk size");
            }
            "--no-adaptive" => adaptive = false,
            "--tail" => tail_mode = true,
            "--mode" => {
                i += 1;
                mode = match args[i].as_str() {
                    "raw" => OutputMode::RawChunk,
                    "otlp" => OutputMode::Otlp,
                    "passthrough" => OutputMode::Passthrough,
                    other => {
                        eprintln!("Unknown mode: {other} (expected: raw, otlp, passthrough)");
                        std::process::exit(1);
                    }
                };
            }
            "--level" => {
                i += 1;
                level = args[i].parse().expect("invalid level");
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    #[cfg(unix)]
    if tail_mode {
        return run_tail(&path, &mode, level);
    }
    #[cfg(not(unix))]
    if tail_mode {
        eprintln!("Tail mode is not supported on this platform");
        std::process::exit(1);
    }

    let mode_name = match mode {
        OutputMode::RawChunk => "raw (compress only)",
        OutputMode::Otlp => "otlp (JSON → protobuf → compress)",
        OutputMode::Passthrough => "passthrough (read + count only)",
    };

    eprintln!("logfwd pipeline benchmark");
    eprintln!("  file: {}", path.display());
    eprintln!("  mode: {mode_name}");
    eprintln!("  chunk_size: {} KB", chunk_size / 1024);
    eprintln!("  adaptive: {adaptive}");
    eprintln!("  zstd level: {level}");
    eprintln!();

    // Warm up the page cache
    eprint!("Warming page cache... ");
    io::stderr().flush()?;
    let file_size = std::fs::metadata(&path)?.len();
    let _ = pipeline::run_file(
        &path,
        PipelineConfig {
            chunk: ChunkConfig {
                target_size: 256 * 1024,
                min_size: 32 * 1024,
                max_size: 1024 * 1024,
                flush_timeout_us: 0,
            },
            compression_level: level,
            adaptive: false,
            mode: OutputMode::Passthrough,
        },
    )?;
    eprintln!("done ({:.1} MB)", file_size as f64 / (1024.0 * 1024.0));

    // Run 3 iterations, take the best
    let mut best_stats: Option<PipelineStats> = None;
    for run in 1..=3 {
        eprint!("Run {run}/3... ");
        io::stderr().flush()?;

        let stats = pipeline::run_file(
            &path,
            PipelineConfig {
                chunk: ChunkConfig {
                    target_size: chunk_size,
                    min_size: 32 * 1024,
                    max_size: 16 * 1024 * 1024,
                    flush_timeout_us: 0,
                },
                compression_level: level,
                adaptive,
                mode: mode.clone(),
            },
        )?;

        let lps = stats.lines_per_sec();
        let mbps = stats.throughput_mbps();
        eprintln!(
            "{:.0} lines/sec, {:.1} MB/sec, ratio {:.2}x, chunks: {}, final_chunk: {}KB",
            lps,
            mbps,
            stats.avg_ratio(),
            stats.total_chunks,
            stats.final_chunk_size / 1024
        );

        if best_stats
            .as_ref()
            .map_or(true, |b| stats.lines_per_sec() > b.lines_per_sec())
        {
            best_stats = Some(stats);
        }
    }

    let best = best_stats.unwrap();
    eprintln!();
    eprintln!("=== Best result ===");
    eprintln!("  Mode:           {mode_name}");
    eprintln!("  Lines:          {}", best.total_lines);
    eprintln!(
        "  Raw input:      {:.1} MB",
        best.total_raw_bytes as f64 / (1024.0 * 1024.0)
    );
    eprintln!(
        "  Output:         {:.1} MB",
        best.total_output_bytes as f64 / (1024.0 * 1024.0)
    );
    eprintln!("  Ratio:          {:.2}x", best.avg_ratio());
    eprintln!("  Chunks:         {}", best.total_chunks);
    eprintln!("  Chunk size:     {} KB", best.final_chunk_size / 1024);
    eprintln!(
        "  Elapsed:        {:.3} sec",
        best.elapsed_ns as f64 / 1_000_000_000.0
    );
    eprintln!("  Throughput:     {:.1} MB/sec", best.throughput_mbps());
    eprintln!("  Lines/sec:      {:.0}", best.lines_per_sec());
    eprintln!();

    let target = 1_000_000.0f64;
    if best.lines_per_sec() >= target {
        eprintln!(
            "  >>> TARGET HIT: {:.1}x above 1M lines/sec",
            best.lines_per_sec() / target
        );
    } else {
        eprintln!(
            "  Below target: {:.1}% of 1M lines/sec",
            best.lines_per_sec() / target * 100.0
        );
    }

    // Print tuner exploration
    if !best.tuner_scores.is_empty() {
        eprintln!();
        eprintln!("=== Tuner exploration ===");
        eprintln!(
            "  {:>8}  {:>12}  {:>10}  {:>8}  {}",
            "Size", "Score", "MB/sec", "Ratio", ""
        );

        let max_score = best
            .tuner_scores
            .iter()
            .map(|(_, s, _, _)| *s)
            .fold(0.0f64, f64::max);

        for &(size, score, throughput, ratio) in &best.tuner_scores {
            if score == 0.0 {
                continue;
            }
            let bar_len = if max_score > 0.0 {
                ((score / max_score) * 40.0) as usize
            } else {
                0
            };
            let bar: String = "#".repeat(bar_len);
            let marker = if size == best.final_chunk_size {
                " <-- best"
            } else {
                ""
            };
            eprintln!(
                "  {:>6}KB  {:>12.0}  {:>8.1}  {:>8.2}x  {}{marker}",
                size / 1024,
                score,
                throughput / (1024.0 * 1024.0),
                ratio,
                bar,
            );
        }
    }

    Ok(())
}

fn run_blackhole(addr: &str) -> io::Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};

    eprintln!("logfwd blackhole collector listening on {addr}");
    eprintln!("  Accepts any POST, returns 200, counts bytes.");
    eprintln!("  Ctrl-C to stop.");

    let server = tiny_http::Server::http(addr)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    let total_requests = AtomicU64::new(0);
    let total_bytes = AtomicU64::new(0);
    let start = Instant::now();

    // Stats reporter thread
    let req_ref = &total_requests as *const AtomicU64 as usize;
    let bytes_ref = &total_bytes as *const AtomicU64 as usize;
    std::thread::spawn(move || {
        let mut prev_bytes = 0u64;
        let mut prev_reqs = 0u64;
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let reqs = unsafe { &*(req_ref as *const AtomicU64) }.load(Ordering::Relaxed);
            let bytes = unsafe { &*(bytes_ref as *const AtomicU64) }.load(Ordering::Relaxed);
            let d_reqs = reqs - prev_reqs;
            let d_bytes = bytes - prev_bytes;
            if d_reqs > 0 {
                eprint!(
                    "\r  {} reqs ({}/s) | {:.1} MB ({:.1} MB/s)    ",
                    reqs,
                    d_reqs,
                    bytes as f64 / (1024.0 * 1024.0),
                    d_bytes as f64 / (1024.0 * 1024.0),
                );
                io::stderr().flush().ok();
            }
            prev_reqs = reqs;
            prev_bytes = bytes;
        }
    });

    for mut request in server.incoming_requests() {
        // Read the full body to simulate a real collector.
        let content_len = request.body_length().unwrap_or(0);
        let mut body = Vec::with_capacity(content_len);
        request.as_reader().read_to_end(&mut body).ok();

        total_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        total_requests.fetch_add(1, Ordering::Relaxed);

        let resp = tiny_http::Response::from_string("{}")
            .with_status_code(200)
            .with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
        let _ = request.respond(resp);
    }

    let elapsed = start.elapsed().as_secs_f64();
    let reqs = total_requests.load(Ordering::Relaxed);
    let bytes = total_bytes.load(Ordering::Relaxed);
    eprintln!(
        "\nDone: {} requests, {:.1} MB in {:.1}s",
        reqs,
        bytes as f64 / (1024.0 * 1024.0),
        elapsed,
    );
    Ok(())
}

#[cfg(unix)]
fn run_v2_pipelines(config: logfwd::config::Config, dry_run: bool) -> io::Result<()> {
    use logfwd::diagnostics::DiagnosticsServer;
    use logfwd::pipeline_v2::Pipeline;

    let shutdown = Arc::new(AtomicBool::new(false));

    // Build pipelines.
    let mut pipelines = Vec::new();
    for (name, pipe_cfg) in &config.pipelines {
        let pipeline = Pipeline::from_config(name, pipe_cfg)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        eprintln!("  pipeline '{}' ready", name);
        pipelines.push(pipeline);
    }

    if dry_run {
        eprintln!("Dry run: {} pipeline(s) constructed successfully", pipelines.len());
        return Ok(());
    }

    // Start diagnostics server if configured.
    let _diag_handle = if let Some(ref addr) = config.server.diagnostics {
        let mut server = DiagnosticsServer::new(addr);
        for p in &pipelines {
            server.add_pipeline(Arc::clone(p.metrics()));
        }
        eprintln!("  diagnostics: http://{addr}");
        Some(server.start())
    } else {
        None
    };

    eprintln!("logfwd v2 starting ({} pipeline(s))", pipelines.len());

    // Run each pipeline on its own thread.
    let mut handles = Vec::new();
    let main_pipeline = pipelines.pop();

    for mut pipeline in pipelines {
        let sd = shutdown.clone();
        handles.push(std::thread::spawn(move || pipeline.run(&sd)));
    }

    if let Some(mut main_pipe) = main_pipeline {
        main_pipe.run(&shutdown)?;
    }

    for h in handles {
        let _ = h.join();
    }

    Ok(())
}

#[cfg(unix)]
fn run_tail(path: &Path, mode: &OutputMode, level: i32) -> io::Result<()> {
    let mode_name = match mode {
        OutputMode::RawChunk => "raw",
        OutputMode::Otlp => "otlp",
        OutputMode::Passthrough => "passthrough",
    };
    eprintln!("Tailing {} (mode: {mode_name}, Ctrl-C to stop)", path.display());

    let config = TailConfig {
        start_from_end: true,
        poll_interval_ms: 50,
        read_buf_size: 256 * 1024,
        ..Default::default()
    };

    let mut tailer = FileTailer::new(&[path.to_path_buf()], config)?;
    let mut compressor = ChunkCompressor::new(level);

    // Stats tracking.
    let mut total_bytes = 0u64;
    let mut total_lines = 0u64;
    let mut total_output = 0u64;
    let mut last_report = Instant::now();
    let mut interval_bytes = 0u64;
    let mut interval_lines = 0u64;

    // Ctrl-C handling.
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc_flag(&r);

    while running.load(Ordering::Relaxed) {
        let events = tailer.poll()?;
        let had_events = !events.is_empty();

        for event in events {
            match event {
                TailEvent::Data { bytes, .. } => {
                    let data = &bytes;
                    let line_count = memchr::memchr_iter(b'\n', data).count();

                    let output_size = match mode {
                        OutputMode::Passthrough => data.len(),
                        OutputMode::RawChunk => {
                            let c = compressor.compress(data)?;
                            c.compressed_size as usize
                        }
                        OutputMode::Otlp => {
                            let observed_ns = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_nanos() as u64;

                            let line_refs: Vec<&[u8]> = data
                                .split(|&b| b == b'\n')
                                .filter(|l| !l.is_empty())
                                .collect();
                            let otlp_bytes = otlp::encode_batch(&line_refs, observed_ns);
                            let c = compressor.compress(&otlp_bytes)?;
                            c.compressed_size as usize
                        }
                    };

                    total_bytes += data.len() as u64;
                    total_lines += line_count as u64;
                    total_output += output_size as u64;
                    interval_bytes += data.len() as u64;
                    interval_lines += line_count as u64;
                }
                TailEvent::Rotated { path } => {
                    eprintln!("[rotated] {}", path.display());
                }
                TailEvent::Truncated { path } => {
                    eprintln!("[truncated] {}", path.display());
                }
            }
        }

        // Print stats every second.
        if last_report.elapsed() >= Duration::from_secs(1) {
            let elapsed = last_report.elapsed().as_secs_f64();
            let lps = interval_lines as f64 / elapsed;
            let mbps = interval_bytes as f64 / (1024.0 * 1024.0) / elapsed;
            let ratio = if total_output > 0 {
                total_bytes as f64 / total_output as f64
            } else {
                0.0
            };

            eprint!(
                "\r  {:.0} lines/sec | {:.1} MB/sec | {:.1}x ratio | {} total lines    ",
                lps, mbps, ratio, total_lines,
            );
            io::stderr().flush()?;

            interval_bytes = 0;
            interval_lines = 0;
            last_report = Instant::now();
        }

        // Sleep briefly if no events to avoid busy-spinning.
        if !had_events {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    eprintln!();
    eprintln!("Stopped. Total: {} lines, {:.1} MB read, {:.1} MB output",
        total_lines,
        total_bytes as f64 / (1024.0 * 1024.0),
        total_output as f64 / (1024.0 * 1024.0),
    );

    Ok(())
}

/// Set up Ctrl-C to flip an AtomicBool. Portable, no sigaction needed.
#[cfg(unix)]
fn ctrlc_flag(flag: &Arc<AtomicBool>) {
    let f = flag.clone();
    let _ = std::thread::spawn(move || {
        // Block on reading a line from a channel that never sends —
        // but check the signal flag. This is a simple portable approach.
        // In production we'd use signal_hook or tokio::signal.
    });
    // For now, just let the OS default SIGINT handler kill us.
    // The AtomicBool approach requires signal_hook crate; skip for prototype.
}

fn generate_log_file(num_lines: usize, output: &str) -> io::Result<()> {
    use std::io::BufWriter;

    eprintln!("Generating {num_lines} plain-text log lines to {output}...");
    let file = std::fs::File::create(output)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let templates = [
        "2024-01-15T10:30:00.{ms:03}Z INFO  service.handler processed request path=/api/v1/users/{id} status=200 duration_ms={dur} request_id={rid}\n",
        "2024-01-15T10:30:00.{ms:03}Z DEBUG service.db query completed table=users rows=42 duration_ms={dur} connection_id={cid}\n",
        "2024-01-15T10:30:00.{ms:03}Z WARN  service.pool connection pool utilization high current=85 max=100 wait_ms={dur}\n",
        "2024-01-15T10:30:00.{ms:03}Z INFO  service.auth token validated user_id={id} scope=read,write ip=10.0.{ip2}.{ip3}\n",
        "2024-01-15T10:30:00.{ms:03}Z ERROR service.handler request failed path=/api/v1/orders/{id} error=\"timeout after {dur}ms\" trace_id={rid}\n",
    ];

    for i in 0..num_lines {
        let template_idx = i % templates.len();
        let ms = i % 1000;
        let id = 10000 + (i * 7) % 90000;
        let dur = 1 + (i * 13) % 500;
        let rid = format!("{:016x}", (i as u64).wrapping_mul(0x517cc1b727220a95));
        let cid = (i * 3) % 100;
        let ip2 = (i * 5) % 256;
        let ip3 = (i * 11) % 256;

        let line = templates[template_idx]
            .replace("{ms:03}", &format!("{ms:03}"))
            .replace("{id}", &id.to_string())
            .replace("{dur}", &dur.to_string())
            .replace("{rid}", &rid)
            .replace("{cid}", &cid.to_string())
            .replace("{ip2}", &ip2.to_string())
            .replace("{ip3}", &ip3.to_string());

        writer.write_all(line.as_bytes())?;
    }

    writer.flush()?;
    let size = std::fs::metadata(output)?.len();
    eprintln!(
        "Done: {:.1} MB, avg {:.0} bytes/line",
        size as f64 / (1024.0 * 1024.0),
        size as f64 / num_lines as f64
    );
    Ok(())
}

fn generate_json_log_file(num_lines: usize, output: &str) -> io::Result<()> {
    use std::io::BufWriter;

    eprintln!("Generating {num_lines} JSON log lines to {output}...");
    let file = std::fs::File::create(output)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = ["/api/v1/users", "/api/v1/orders", "/api/v2/products", "/health", "/api/v1/auth"];

    for i in 0..num_lines {
        let level = levels[i % 4];
        let path = paths[i % 5];
        let id = 10000 + (i * 7) % 90000;
        let dur = 1 + (i * 13) % 500;
        let rid = format!("{:016x}", (i as u64).wrapping_mul(0x517cc1b727220a95));

        write!(
            writer,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request handled {} {}/{}","duration_ms":{},"request_id":"{}","service":"myapp"}}"#,
            i % 1000, level, "GET", path, id, dur, rid,
        )?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    let size = std::fs::metadata(output)?.len();
    eprintln!(
        "Done: {:.1} MB, avg {:.0} bytes/line",
        size as f64 / (1024.0 * 1024.0),
        size as f64 / num_lines as f64
    );
    Ok(())
}
