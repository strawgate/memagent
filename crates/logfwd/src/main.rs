#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::env;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

fn main() -> io::Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage:");
        eprintln!();
        eprintln!("  Run pipeline from YAML config:");
        eprintln!("    logfwd --config <config.yaml> [--validate] [--dry-run]");
        eprintln!();
        eprintln!("  Blackhole OTLP collector (for benchmarks):");
        eprintln!("    logfwd --blackhole [bind_addr]                  (default: 127.0.0.1:4318)");
        eprintln!();
        eprintln!("  Generate synthetic data:");
        eprintln!("    logfwd --generate-json <num_lines> <output_file>");
        std::process::exit(1);
    }

    if args[1] == "--generate-json" {
        if args.len() < 4 {
            eprintln!("Usage: logfwd --generate-json <num_lines> <output_file>");
            std::process::exit(1);
        }
        let num_lines: usize = args[2].parse().expect("invalid num_lines");
        let output = &args[3];
        return generate_json_log_file(num_lines, output);
    }

    if args[1] == "--blackhole" {
        let addr = args.get(2).map(|s| s.as_str()).unwrap_or("127.0.0.1:4318");
        return run_blackhole(addr);
    }

    if args[1] == "--config" {
        if args.len() < 3 {
            eprintln!("Usage: logfwd --config <config.yaml> [--validate] [--dry-run]");
            std::process::exit(1);
        }
        let config_path = &args[2];
        let validate_only = args.iter().any(|a| a == "--validate");
        let dry_run = args.iter().any(|a| a == "--dry-run");

        let config = logfwd_config::Config::load(config_path)
            .map_err(|e| io::Error::other(e.to_string()))?;

        if validate_only {
            eprintln!("Config OK: {} pipeline(s)", config.pipelines.len());
            return Ok(());
        }

        return run_v2_pipelines(config, dry_run);
    }

    eprintln!("Unknown command: {}", args[1]);
    eprintln!("Run 'logfwd' with no arguments for usage.");
    std::process::exit(1);
}

fn run_blackhole(addr: &str) -> io::Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};

    eprintln!("logfwd blackhole collector listening on {addr}");
    eprintln!("  Accepts any POST, returns 200, counts bytes/lines.");
    eprintln!("  Requests to /_bulk get an ES-compatible response.");
    eprintln!("  GET /stats returns JSON counters.");
    eprintln!("  Ctrl-C to stop.");

    let server = tiny_http::Server::http(addr).map_err(|e| io::Error::other(e.to_string()))?;

    let total_requests = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let total_lines = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Stats reporter thread
    let reqs_clone = Arc::clone(&total_requests);
    let bytes_clone = Arc::clone(&total_bytes);
    let lines_clone = Arc::clone(&total_lines);
    std::thread::spawn(move || {
        let mut prev_lines = 0u64;
        let mut prev_bytes = 0u64;
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let reqs = reqs_clone.load(Ordering::Relaxed);
            let lines = lines_clone.load(Ordering::Relaxed);
            let bytes = bytes_clone.load(Ordering::Relaxed);
            let d_lines = lines - prev_lines;
            let d_bytes = bytes - prev_bytes;
            if d_lines > 0 || d_bytes > 0 {
                eprint!(
                    "\r  {} reqs | {} lines ({}/s) | {:.1} MB ({:.1} MB/s)    ",
                    reqs,
                    lines,
                    d_lines,
                    bytes as f64 / (1024.0 * 1024.0),
                    d_bytes as f64 / (1024.0 * 1024.0),
                );
                io::stderr().flush().ok();
            }
            prev_lines = lines;
            prev_bytes = bytes;
        }
    });

    // ES bulk response: minimal valid response that says "everything succeeded".
    let es_bulk_response = r#"{"took":0,"errors":false,"items":[]}"#;

    let stats_reqs = Arc::clone(&total_requests);
    let stats_bytes = Arc::clone(&total_bytes);
    let stats_lines = Arc::clone(&total_lines);

    for mut request in server.incoming_requests() {
        // GET /stats — return JSON counters for programmatic polling
        if request.method() == &tiny_http::Method::Get && request.url() == "/stats" {
            let body = format!(
                r#"{{"requests":{},"lines":{},"bytes":{}}}"#,
                stats_reqs.load(Ordering::Relaxed),
                stats_lines.load(Ordering::Relaxed),
                stats_bytes.load(Ordering::Relaxed),
            );
            let resp = tiny_http::Response::from_string(body)
                .with_status_code(200)
                .with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
            let _ = request.respond(resp);
            continue;
        }

        let content_len = request.body_length().unwrap_or(0);
        let mut body = Vec::with_capacity(content_len);
        request.as_reader().read_to_end(&mut body).ok();

        let line_count = memchr::memchr_iter(b'\n', &body).count() as u64;
        total_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        total_lines.fetch_add(line_count, Ordering::Relaxed);
        total_requests.fetch_add(1, Ordering::Relaxed);

        let is_bulk = request.url().contains("/_bulk");
        let resp_body = if is_bulk { es_bulk_response } else { "{}" };

        let resp = tiny_http::Response::from_string(resp_body)
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
    let lines = total_lines.load(Ordering::Relaxed);
    eprintln!(
        "\nDone: {} requests, {} lines, {:.1} MB in {:.1}s",
        reqs,
        lines,
        bytes as f64 / (1024.0 * 1024.0),
        elapsed,
    );
    Ok(())
}

fn run_v2_pipelines(config: logfwd_config::Config, dry_run: bool) -> io::Result<()> {
    use logfwd::pipeline_v2::Pipeline;
    use logfwd_core::diagnostics::DiagnosticsServer;

    let shutdown = Arc::new(AtomicBool::new(false));

    // Build pipelines.
    let mut pipelines = Vec::new();
    for (name, pipe_cfg) in &config.pipelines {
        let pipeline = Pipeline::from_config(name, pipe_cfg).map_err(io::Error::other)?;
        eprintln!("  pipeline '{}' ready", name);
        pipelines.push(pipeline);
    }

    if dry_run {
        eprintln!(
            "Dry run: {} pipeline(s) constructed successfully",
            pipelines.len()
        );
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

fn generate_json_log_file(num_lines: usize, output: &str) -> io::Result<()> {
    use std::io::BufWriter;

    eprintln!("Generating {num_lines} JSON log lines to {output}...");
    let file = std::fs::File::create(output)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v2/products",
        "/health",
        "/api/v1/auth",
    ];

    for i in 0..num_lines {
        let level = levels[i % 4];
        let path = paths[i % 5];
        let id = 10000 + (i * 7) % 90000;
        let dur = 1 + (i * 13) % 500;
        let rid = format!("{:016x}", (i as u64).wrapping_mul(0x517cc1b727220a95));

        write!(
            writer,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request handled GET {}/{}","duration_ms":{},"request_id":"{}","service":"myapp"}}"#,
            i % 1000,
            level,
            path,
            id,
            dur,
            rid,
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
