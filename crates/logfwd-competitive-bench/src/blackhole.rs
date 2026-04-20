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
        // OTLP JSON: count exact logRecords from nested arrays when possible.
        // Falls back to lightweight body-key scanning or newline-based estimates.
        let otlp_count = match count_otlp_log_records(body) {
            0 => count_otlp_body_keys(body),
            n => n,
        };
        if otlp_count > 0 {
            return otlp_count;
        }
        return ndjson_line_count(body, newlines);
    }

    // Default: NDJSON — tolerate missing trailing newline.
    ndjson_line_count(body, newlines)
}

/// Count `"body":` occurrences in OTLP JSON — one per log record.
fn count_otlp_body_keys(body: &[u8]) -> u64 {
    memchr::memmem::find_iter(body, b"\"body\":").count() as u64
}

/// Count OTLP log records by parsing the JSON payload shape:
/// `resourceLogs[].scopeLogs[].logRecords[]`.
fn count_otlp_log_records(body: &[u8]) -> u64 {
    let v: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return 0,
    };
    v.get("resourceLogs")
        .and_then(serde_json::Value::as_array)
        .map_or(0, |resource_logs| {
            resource_logs
                .iter()
                .filter_map(|r| r.get("scopeLogs").and_then(serde_json::Value::as_array))
                .map(|scope_logs| {
                    scope_logs
                        .iter()
                        .filter_map(|s| s.get("logRecords").and_then(serde_json::Value::as_array))
                        .map(|records| records.len() as u64)
                        .sum::<u64>()
                })
                .sum::<u64>()
        })
}

fn ndjson_line_count(body: &[u8], newlines: u64) -> u64 {
    if body.is_empty() {
        return 0;
    }
    if body.last() == Some(&b'\n') {
        newlines
    } else {
        newlines + 1
    }
}

#[cfg(test)]
mod tests {
    use super::count_lines;

    #[test]
    fn count_ndjson_with_and_without_trailing_newline() {
        assert_eq!(count_lines(b"{\"a\":1}\n{\"a\":2}\n", "/"), 2);
        assert_eq!(count_lines(b"{\"a\":1}\n{\"a\":2}", "/"), 2);
        assert_eq!(count_lines(b"{\"a\":1}", "/"), 1);
    }

    #[test]
    fn count_es_bulk_pairs() {
        let body = br#"{"index":{}}
{"msg":"a"}
{"index":{}}
{"msg":"b"}
"#;
        assert_eq!(count_lines(body, "/_bulk"), 2);
    }

    #[test]
    fn count_otlp_json_log_records() {
        let body = br#"{
  "resourceLogs": [
    {
      "scopeLogs": [
        { "logRecords": [{ "body": { "stringValue": "a" } }, { "body": { "stringValue": "b" } }] },
        { "logRecords": [{ "body": { "stringValue": "c" } }] }
      ]
    }
  ]
}"#;
        assert_eq!(count_lines(body, "/v1/logs"), 3);
    }
}
