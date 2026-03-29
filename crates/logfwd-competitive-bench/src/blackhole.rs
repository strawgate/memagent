//! Blackhole HTTP sink: accepts any POST, counts bytes/lines, exposes /stats.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counters exposed by the blackhole server.
pub struct BlackholeStats {
    pub lines: AtomicU64,
    pub bytes: AtomicU64,
}

/// A running blackhole server. Stops when dropped.
pub struct Blackhole {
    stats: Arc<BlackholeStats>,
    // Keep the server alive — tiny_http stops accepting when dropped.
    _server: Arc<tiny_http::Server>,
    _handle: std::thread::JoinHandle<()>,
}

impl Blackhole {
    /// Start a blackhole HTTP server on `addr`. Returns immediately.
    pub fn start(addr: &str) -> io::Result<Self> {
        let server = Arc::new(
            tiny_http::Server::http(addr).map_err(|e| io::Error::other(e.to_string()))?,
        );

        let stats = Arc::new(BlackholeStats {
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

    /// Get current stats (lines, bytes).
    pub fn stats(&self) -> (u64, u64) {
        (
            self.stats.lines.load(Ordering::Relaxed),
            self.stats.bytes.load(Ordering::Relaxed),
        )
    }

    /// Reset counters to zero.
    pub fn reset(&self) {
        self.stats.lines.store(0, Ordering::Relaxed);
        self.stats.bytes.store(0, Ordering::Relaxed);
    }
}

fn serve(server: Arc<tiny_http::Server>, stats: Arc<BlackholeStats>) {
    let es_bulk_response = r#"{"took":0,"errors":false,"items":[]}"#;
    let json_header =
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();

    for mut request in server.incoming_requests() {
        // GET /stats — JSON counters for programmatic polling.
        if *request.method() == tiny_http::Method::Get && request.url() == "/stats" {
            let body = format!(
                r#"{{"lines":{},"bytes":{}}}"#,
                stats.lines.load(Ordering::Relaxed),
                stats.bytes.load(Ordering::Relaxed),
            );
            let resp = tiny_http::Response::from_string(body)
                .with_status_code(200)
                .with_header(json_header.clone());
            let _ = request.respond(resp);
            continue;
        }

        // Any POST: read body, count newlines, respond 200.
        let content_len = request.body_length().unwrap_or(0);
        let mut body = Vec::with_capacity(content_len);
        let _ = request.as_reader().read_to_end(&mut body);

        let line_count = memchr::memchr_iter(b'\n', &body).count() as u64;
        stats.bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        stats.lines.fetch_add(line_count, Ordering::Relaxed);

        let is_bulk = request.url().contains("/_bulk");
        let resp_body = if is_bulk { es_bulk_response } else { "{}" };

        let resp = tiny_http::Response::from_string(resp_body)
            .with_status_code(200)
            .with_header(json_header.clone());
        let _ = request.respond(resp);
    }
}
