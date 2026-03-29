//! Blackhole HTTP sink: accepts any POST, counts bytes/lines, exposes /stats.
//!
//! Protocol-aware line counting:
//! - NDJSON (default): count `\n` characters
//! - Elasticsearch `/_bulk`: count `\n` / 2 (action+document pairs)
//! - OTLP `/v1/logs` JSON: count `"body":` occurrences per log record

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Blackhole {
    stats: Arc<Stats>,
    _server: Arc<tiny_http::Server>,
    _handle: std::thread::JoinHandle<()>,
}

struct Stats {
    lines: AtomicU64,
    bytes: AtomicU64,
}

impl Blackhole {
    pub fn start(addr: &str) -> io::Result<Self> {
        let server =
            Arc::new(tiny_http::Server::http(addr).map_err(|e| io::Error::other(e.to_string()))?);
        let stats = Arc::new(Stats {
            lines: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        });

        let srv = Arc::clone(&server);
        let st = Arc::clone(&stats);
        let handle = std::thread::spawn(move || serve(srv, st));

        Ok(Blackhole {
            stats,
            _server: server,
            _handle: handle,
        })
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.stats.lines.load(Ordering::Relaxed),
            self.stats.bytes.load(Ordering::Relaxed),
        )
    }

    pub fn reset(&self) {
        self.stats.lines.store(0, Ordering::Relaxed);
        self.stats.bytes.store(0, Ordering::Relaxed);
    }
}

fn serve(server: Arc<tiny_http::Server>, stats: Arc<Stats>) {
    let es_bulk_ok = r#"{"took":0,"errors":false,"items":[]}"#;
    let json_hdr =
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();

    for mut req in server.incoming_requests() {
        // GET /stats — polling endpoint for the runner.
        if *req.method() == tiny_http::Method::Get && req.url() == "/stats" {
            let body = format!(
                r#"{{"lines":{},"bytes":{}}}"#,
                stats.lines.load(Ordering::Relaxed),
                stats.bytes.load(Ordering::Relaxed),
            );
            let _ = req.respond(
                tiny_http::Response::from_string(body)
                    .with_status_code(200)
                    .with_header(json_hdr.clone()),
            );
            continue;
        }

        // Read body.
        let mut body = Vec::with_capacity(req.body_length().unwrap_or(0));
        let _ = req.as_reader().read_to_end(&mut body);
        stats.bytes.fetch_add(body.len() as u64, Ordering::Relaxed);

        // Count lines using protocol-appropriate strategy.
        let url = req.url();
        let lines = count_lines(&body, url);
        stats.lines.fetch_add(lines, Ordering::Relaxed);

        // Respond with protocol-appropriate body.
        let resp_body = if url.contains("/_bulk") {
            es_bulk_ok
        } else {
            "{}"
        };
        let _ = req.respond(
            tiny_http::Response::from_string(resp_body)
                .with_status_code(200)
                .with_header(json_hdr.clone()),
        );
    }
}

fn count_lines(body: &[u8], url: &str) -> u64 {
    let newlines = memchr::memchr_iter(b'\n', body).count() as u64;

    if url.contains("/_bulk") {
        // ES bulk: alternating action + document lines → each log = 2 newlines.
        return newlines / 2;
    }

    if url.contains("/v1/logs") {
        // OTLP JSON: each log record contains a "body" key. Count occurrences.
        // Falls back to newline count if no "body" keys found (e.g., protobuf).
        let otlp_count = count_otlp_body_keys(body);
        return if otlp_count > 0 { otlp_count } else { newlines };
    }

    // Default: NDJSON — one newline per log line.
    newlines
}

/// Count `"body":` occurrences in OTLP JSON — one per log record.
fn count_otlp_body_keys(body: &[u8]) -> u64 {
    memchr::memmem::find_iter(body, b"\"body\":").count() as u64
}
