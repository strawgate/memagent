//! HTTP input source for newline-delimited payload ingestion.
//!
//! Listens for requests on a configurable route (default `/ingest`), accepts
//! optionally compressed request bodies, and forwards newline-delimited bytes
//! to the pipeline scanner path as [`crate::input::InputEvent::Data`].

use std::io;
use std::io::Read as _;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{ALLOW, CONTENT_ENCODING, RETRY_AFTER};
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use flate2::read::GzDecoder;
use logfwd_types::diagnostics::ComponentHealth;
use tokio::sync::oneshot;

use crate::InputError;
use crate::input::{InputEvent, InputSource};
use crate::receiver_http::{parse_content_length, read_limited_body};

/// Default max request body size (10 MiB).
const DEFAULT_MAX_REQUEST_BODY_SIZE: usize = 10 * 1024 * 1024;
/// Default max bytes drained per `poll()` call (1 GiB).
const DEFAULT_MAX_DRAINED_BYTES_PER_POLL: usize = 1024 * 1024 * 1024;

/// Bounded channel capacity — limits memory when the pipeline falls behind.
const CHANNEL_BOUND: usize = 4096;

/// Accepted HTTP method for the input endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpInputMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
}

impl HttpInputMethod {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Patch => "PATCH",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
        }
    }

    #[must_use]
    pub fn matches(self, method: &Method) -> bool {
        match self {
            Self::Get => method == Method::GET,
            Self::Post => method == Method::POST,
            Self::Put => method == Method::PUT,
            Self::Delete => method == Method::DELETE,
            Self::Patch => method == Method::PATCH,
            Self::Head => method == Method::HEAD,
            Self::Options => method == Method::OPTIONS,
        }
    }
}

/// HTTP input behavior and limits.
#[derive(Debug, Clone)]
pub struct HttpInputOptions {
    /// Route path to match, e.g. `/ingest`.
    pub path: String,
    /// When true, only exact path matches are accepted.
    pub strict_path: bool,
    /// Accepted HTTP method for ingest requests.
    pub method: HttpInputMethod,
    /// Max request body size in bytes.
    pub max_request_body_size: usize,
    /// Max bytes drained from the internal queue per `poll()` call.
    pub max_drained_bytes_per_poll: usize,
    /// HTTP response code for accepted requests.
    pub response_code: u16,
    /// Optional static response body for accepted requests.
    pub response_body: Option<String>,
}

impl Default for HttpInputOptions {
    fn default() -> Self {
        Self {
            path: "/ingest".to_string(),
            strict_path: true,
            method: HttpInputMethod::Post,
            max_request_body_size: DEFAULT_MAX_REQUEST_BODY_SIZE,
            max_drained_bytes_per_poll: DEFAULT_MAX_DRAINED_BYTES_PER_POLL,
            response_code: 200,
            response_body: None,
        }
    }
}

/// HTTP NDJSON receiver that forwards bytes to the scanner pipeline.
pub struct HttpInput {
    name: String,
    rx: Option<mpsc::Receiver<Vec<u8>>>,
    /// The address the HTTP server is bound to.
    addr: std::net::SocketAddr,
    /// Keep the server thread handle alive.
    handle: Option<std::thread::JoinHandle<()>>,
    /// Shutdown signal for the background server task.
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Shutdown mechanism for the background thread.
    is_running: Arc<AtomicBool>,
    /// Source-owned health snapshot for readiness and diagnostics.
    health: Arc<AtomicU8>,
    /// Max bytes drained from internal request queue on each `poll()`.
    max_drained_bytes_per_poll: usize,
    /// Deferred request bytes that did not fit in the previous poll budget.
    deferred_bytes: Option<DeferredBytes>,
}

#[derive(Debug)]
struct DeferredBytes {
    bytes: Vec<u8>,
    offset: usize,
}

#[derive(Clone)]
struct HttpServerState {
    route_path: String,
    strict_path: bool,
    accepted_method: HttpInputMethod,
    max_request_body_size: usize,
    success_response_code: StatusCode,
    success_response_body: Option<String>,
    tx: mpsc::SyncSender<Vec<u8>>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
}

impl HttpInput {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:8081").
    /// Uses default options with `path` override when supplied.
    pub fn new(name: impl Into<String>, addr: &str, path: Option<&str>) -> io::Result<Self> {
        let mut options = HttpInputOptions::default();
        if let Some(path) = path {
            options.path = path.to_string();
        }
        Self::new_with_options(name, addr, options)
    }

    /// Bind an HTTP server with explicit options.
    pub fn new_with_options(
        name: impl Into<String>,
        addr: &str,
        options: HttpInputOptions,
    ) -> io::Result<Self> {
        Self::new_with_capacity_inner(name, addr, CHANNEL_BOUND, options)
    }

    #[cfg(test)]
    fn new_with_capacity(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        options: HttpInputOptions,
    ) -> io::Result<Self> {
        Self::new_with_capacity_inner(name, addr, capacity, options)
    }

    fn new_with_capacity_inner(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        options: HttpInputOptions,
    ) -> io::Result<Self> {
        let options = normalize_options(options)?;
        let success_response_code = StatusCode::from_u16(options.response_code)
            .expect("normalize_options validated response code");

        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| io::Error::other(format!("HTTP input bind {addr}: {e}")))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!("HTTP input set_nonblocking {bound_addr}: {e}"))
        })?;

        let (tx, rx) = mpsc::sync_channel(capacity);

        let is_running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let state = Arc::new(HttpServerState {
            route_path: options.path,
            strict_path: options.strict_path,
            accepted_method: options.method,
            max_request_body_size: options.max_request_body_size,
            success_response_code,
            success_response_body: options.response_body,
            tx,
            is_running: Arc::clone(&is_running),
            health: Arc::clone(&health),
        });
        let state_for_server = Arc::clone(&state);
        let is_running_for_server = Arc::clone(&is_running);
        let health_for_server = Arc::clone(&health);

        let handle = std::thread::Builder::new()
            .name("http-input".into())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    health_for_server.store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                    return;
                };

                runtime.block_on(async move {
                    let Ok(listener) = tokio::net::TcpListener::from_std(std_listener) else {
                        health_for_server
                            .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                        return;
                    };

                    let app = axum::Router::new()
                        .fallback(any(handle_request))
                        .with_state(state_for_server);

                    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    });

                    if server.await.is_err() && is_running_for_server.load(Ordering::Relaxed) {
                        health_for_server
                            .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                    }
                });
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name: name.into(),
            rx: Some(rx),
            addr: bound_addr,
            handle: Some(handle),
            shutdown_tx: Some(shutdown_tx),
            is_running,
            health,
            max_drained_bytes_per_poll: options.max_drained_bytes_per_poll,
            deferred_bytes: None,
        })
    }

    /// Returns the local address the HTTP server is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }
}

impl Drop for HttpInput {
    fn drop(&mut self) {
        self.health
            .store(ComponentHealth::Stopping.as_repr(), Ordering::Relaxed);
        self.is_running.store(false, Ordering::Relaxed);
        self.rx.take();
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.health
            .store(ComponentHealth::Stopped.as_repr(), Ordering::Relaxed);
    }
}

impl InputSource for HttpInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        if self.rx.is_none() {
            return Ok(vec![]);
        }

        let mut all = Vec::new();
        if !append_deferred_capped(
            &mut all,
            self.max_drained_bytes_per_poll,
            &mut self.deferred_bytes,
        ) {
            return Ok(vec![InputEvent::Data {
                accounted_bytes: all.len() as u64,
                bytes: all,
                source_id: None,
                cri_metadata: None,
            }]);
        }

        loop {
            if all.len() >= self.max_drained_bytes_per_poll {
                break;
            }

            let recv_result = {
                let Some(rx) = self.rx.as_ref() else {
                    break;
                };
                rx.try_recv()
            };
            let Ok(bytes) = recv_result else {
                break;
            };
            if !append_capped(
                &mut all,
                bytes,
                self.max_drained_bytes_per_poll,
                &mut self.deferred_bytes,
            ) {
                break;
            }
        }

        if all.is_empty() {
            return Ok(vec![]);
        }
        let accounted_bytes = all.len() as u64;
        Ok(vec![InputEvent::Data {
            bytes: all,
            source_id: None,
            accounted_bytes,
            cri_metadata: None,
        }])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        let stored = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        if self
            .handle
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
            && self.is_running.load(Ordering::Relaxed)
        {
            ComponentHealth::Failed
        } else {
            stored
        }
    }
}

fn append_capped(
    out: &mut Vec<u8>,
    bytes: Vec<u8>,
    max_drained_bytes_per_poll: usize,
    deferred_bytes: &mut Option<DeferredBytes>,
) -> bool {
    if out.len() >= max_drained_bytes_per_poll {
        *deferred_bytes = Some(DeferredBytes { bytes, offset: 0 });
        return false;
    }

    let remaining = max_drained_bytes_per_poll - out.len();
    if bytes.len() <= remaining {
        out.extend_from_slice(&bytes);
        return true;
    }

    out.extend_from_slice(&bytes[..remaining]);
    *deferred_bytes = Some(DeferredBytes {
        bytes,
        offset: remaining,
    });
    false
}

fn append_deferred_capped(
    out: &mut Vec<u8>,
    max_drained_bytes_per_poll: usize,
    deferred_bytes: &mut Option<DeferredBytes>,
) -> bool {
    let Some(deferred) = deferred_bytes.as_mut() else {
        return true;
    };

    if out.len() >= max_drained_bytes_per_poll {
        return false;
    }

    let remaining = max_drained_bytes_per_poll - out.len();
    let available = deferred.bytes.len().saturating_sub(deferred.offset);
    let take = available.min(remaining);
    out.extend_from_slice(&deferred.bytes[deferred.offset..deferred.offset + take]);
    deferred.offset += take;

    if deferred.offset >= deferred.bytes.len() {
        *deferred_bytes = None;
        true
    } else {
        false
    }
}

async fn handle_request(
    State(state): State<Arc<HttpServerState>>,
    request: Request<Body>,
) -> Response {
    let success_response = || {
        if state.success_response_code == StatusCode::NO_CONTENT {
            return StatusCode::NO_CONTENT.into_response();
        }
        if let Some(body) = &state.success_response_body {
            (state.success_response_code, body.clone()).into_response()
        } else {
            state.success_response_code.into_response()
        }
    };

    if !path_matches(request.uri().path(), &state.route_path, state.strict_path) {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }

    if !state.accepted_method.matches(request.method()) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(ALLOW, state.accepted_method.as_str())],
            "method not allowed",
        )
            .into_response();
    }

    let content_length = parse_content_length(request.headers());
    if content_length.is_some_and(|body_len| body_len > state.max_request_body_size as u64) {
        return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response();
    }

    let content_encoding = match parse_content_encoding(request.headers()) {
        Ok(content_encoding) => content_encoding,
        Err(status) => return (status, "invalid content-encoding header").into_response(),
    };
    if let Some(encoding) = content_encoding.as_deref()
        && !is_supported_content_encoding(encoding)
    {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported content-encoding: {encoding}"),
        )
            .into_response();
    }

    let mut body = match read_limited_body(
        request.into_body(),
        state.max_request_body_size,
        content_length,
    )
    .await
    {
        Ok(body) => body,
        Err(status) => {
            let message = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload too large"
            } else {
                "read error"
            };
            return (status, message).into_response();
        }
    };

    body = match decode_content(
        body,
        content_encoding.as_deref(),
        state.max_request_body_size,
    ) {
        Ok(decoded) => decoded,
        Err(InputError::Receiver(msg)) => return (StatusCode::BAD_REQUEST, msg).into_response(),
        Err(InputError::Io(e)) if e.kind() == io::ErrorKind::InvalidData => {
            return (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response();
        }
        Err(_) => return (StatusCode::BAD_REQUEST, "decode failed").into_response(),
    };

    if body.is_empty() {
        state
            .health
            .store(ComponentHealth::Healthy.as_repr(), Ordering::Relaxed);
        return success_response();
    }

    if !body.ends_with(b"\n") {
        body.push(b'\n');
    }

    match state.tx.try_send(body) {
        Ok(()) => {
            state
                .health
                .store(ComponentHealth::Healthy.as_repr(), Ordering::Relaxed);
            success_response()
        }
        Err(mpsc::TrySendError::Full(_)) => {
            state
                .health
                .store(ComponentHealth::Degraded.as_repr(), Ordering::Relaxed);
            (
                StatusCode::TOO_MANY_REQUESTS,
                [(RETRY_AFTER, "1")],
                "too many requests: pipeline backpressure",
            )
                .into_response()
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            if state.is_running.load(Ordering::Relaxed) {
                state
                    .health
                    .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
            }
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(RETRY_AFTER, "1")],
                "service unavailable: pipeline disconnected",
            )
                .into_response()
        }
    }
}

fn parse_content_encoding(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let Some(value) = headers.get(CONTENT_ENCODING) else {
        return Ok(None);
    };
    let parsed = value.to_str().map_err(|_e| StatusCode::BAD_REQUEST)?.trim();
    if parsed.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(parsed.to_ascii_lowercase()))
}

fn normalize_options(mut options: HttpInputOptions) -> io::Result<HttpInputOptions> {
    options.path = normalize_route(&options.path)?;
    if options.max_request_body_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "http input max_request_body_size must be >= 1",
        ));
    }
    if options.max_drained_bytes_per_poll == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "http input max_drained_bytes_per_poll must be >= 1",
        ));
    }
    if !is_valid_success_response_code(options.response_code) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "http input response_code must be one of: 200, 201, 202, 204",
        ));
    }
    if options.response_code == 204 && options.response_body.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "http input response_body is not allowed when response_code is 204",
        ));
    }
    Ok(options)
}

fn is_valid_success_response_code(code: u16) -> bool {
    matches!(code, 200 | 201 | 202 | 204)
}

fn normalize_route(path: &str) -> io::Result<String> {
    if path.trim().is_empty() || !path.starts_with('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "http input path must start with '/'",
        ));
    }
    Ok(path.to_string())
}

fn path_matches(path: &str, route: &str, strict_path: bool) -> bool {
    if strict_path {
        return path == route;
    }
    if route == "/" {
        return true;
    }
    path == route
        || path
            .strip_prefix(route)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn decode_content(
    body: Vec<u8>,
    content_encoding: Option<&str>,
    max_request_body_size: usize,
) -> Result<Vec<u8>, InputError> {
    match content_encoding {
        Some("zstd") => decompress_zstd(&body, max_request_body_size),
        Some("gzip") => decompress_gzip(&body, max_request_body_size),
        None | Some("identity") => Ok(body),
        Some(other) => Err(InputError::Receiver(format!(
            "unsupported content-encoding: {other}"
        ))),
    }
}

fn is_supported_content_encoding(content_encoding: &str) -> bool {
    matches!(content_encoding, "identity" | "gzip" | "zstd")
}

fn decompress_zstd(body: &[u8], max_request_body_size: usize) -> Result<Vec<u8>, InputError> {
    let decoder = zstd::Decoder::new(body)
        .map_err(|_e| InputError::Receiver("zstd decompression failed".to_string()))?;
    read_decompressed_body(
        decoder,
        body.len(),
        max_request_body_size,
        "zstd decompression failed",
    )
}

fn decompress_gzip(body: &[u8], max_request_body_size: usize) -> Result<Vec<u8>, InputError> {
    let decoder = GzDecoder::new(body);
    read_decompressed_body(
        decoder,
        body.len(),
        max_request_body_size,
        "gzip decompression failed",
    )
}

fn read_decompressed_body(
    reader: impl io::Read,
    compressed_len: usize,
    max_request_body_size: usize,
    error_label: &str,
) -> Result<Vec<u8>, InputError> {
    let mut decompressed = Vec::with_capacity(compressed_len.min(max_request_body_size));
    match reader
        .take(max_request_body_size as u64 + 1)
        .read_to_end(&mut decompressed)
    {
        Ok(n) if n > max_request_body_size => Err(InputError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "payload too large",
        ))),
        Ok(_) => Ok(decompressed),
        Err(_) => Err(InputError::Receiver(error_label.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;
    use std::sync::mpsc as std_mpsc;
    use std::time::{Duration, Instant};

    use proptest::prelude::*;

    use super::*;

    fn poll_until_data(input: &mut dyn InputSource, timeout: Duration) -> Vec<u8> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let mut out = Vec::new();
            for event in input.poll().expect("poll should succeed") {
                if let InputEvent::Data { bytes, .. } = event {
                    out.extend_from_slice(&bytes);
                }
            }
            if !out.is_empty() {
                return out;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        vec![]
    }

    #[test]
    fn http_ndjson_roundtrip() {
        let options = HttpInputOptions {
            path: "/ingest".to_string(),
            ..HttpInputOptions::default()
        };
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", options).expect("http input binds");
        let url = format!("http://{}/ingest", input.local_addr());

        let body = b"{\"msg\":\"hello\"}\n{\"msg\":\"world\"}\n";
        let resp = ureq::post(&url)
            .header("Content-Type", "application/x-ndjson")
            .send(body)
            .expect("POST should succeed");
        assert_eq!(resp.status(), 200);

        let data = poll_until_data(&mut input, Duration::from_secs(2));
        let text = String::from_utf8_lossy(&data);
        assert!(
            text.contains("\"msg\":\"hello\""),
            "expected first row: {text}"
        );
        assert!(
            text.contains("\"msg\":\"world\""),
            "expected second row: {text}"
        );
    }

    #[test]
    fn http_default_route_accepts_ingest() {
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", HttpInputOptions::default())
                .expect("http input binds");
        let url = format!("http://{}/ingest", input.local_addr());

        let resp = ureq::post(&url)
            .send(b"{\"msg\":\"root\"}")
            .expect("POST should succeed");
        assert_eq!(resp.status(), 200);

        let data = poll_until_data(&mut input, Duration::from_secs(2));
        let text = String::from_utf8_lossy(&data);
        assert!(
            text.contains("\"msg\":\"root\""),
            "expected root row: {text}"
        );
    }

    #[test]
    fn http_default_route_rejects_root() {
        let input = HttpInput::new_with_options("test", "127.0.0.1:0", HttpInputOptions::default())
            .expect("http input binds");
        let url = format!("http://{}/", input.local_addr());
        let status = match ureq::post(&url).send(b"{\"x\":1}\n") {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(err) => panic!("unexpected request failure: {err}"),
        };
        assert_eq!(status, 404, "default route should reject root path");
    }

    #[test]
    fn http_appends_newline_when_missing() {
        let options = HttpInputOptions {
            path: "/ingest".to_string(),
            ..HttpInputOptions::default()
        };
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", options).expect("http input binds");
        let url = format!("http://{}/ingest", input.local_addr());

        let body = b"{\"msg\":\"no-newline\"}";
        let resp = ureq::post(&url).send(body).expect("POST should succeed");
        assert_eq!(resp.status(), 200);

        let data = poll_until_data(&mut input, Duration::from_secs(2));
        assert!(
            data.ends_with(b"\n"),
            "receiver must append trailing newline"
        );
    }

    #[test]
    fn http_rejects_wrong_path() {
        let options = HttpInputOptions {
            path: "/ingest".to_string(),
            ..HttpInputOptions::default()
        };
        let input =
            HttpInput::new_with_options("test", "127.0.0.1:0", options).expect("http input binds");
        let url = format!("http://{}/not-ingest", input.local_addr());

        let status = match ureq::post(&url).send(b"{\"x\":1}\n") {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(err) => panic!("unexpected request failure: {err}"),
        };
        assert_eq!(status, 404, "wrong path should return 404");
    }

    #[test]
    fn http_prefix_path_when_not_strict() {
        let options = HttpInputOptions {
            path: "/ingest".to_string(),
            strict_path: false,
            ..HttpInputOptions::default()
        };
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", options).expect("http input binds");
        let url = format!("http://{}/ingest/team-a", input.local_addr());

        let resp = ureq::post(&url)
            .send(b"{\"msg\":\"prefix\"}\n")
            .expect("POST should succeed");
        assert_eq!(resp.status(), 200);

        let data = poll_until_data(&mut input, Duration::from_secs(2));
        let text = String::from_utf8_lossy(&data);
        assert!(
            text.contains("\"msg\":\"prefix\""),
            "expected prefix row: {text}"
        );
    }

    #[test]
    fn http_rejects_method_mismatch() {
        let options = HttpInputOptions {
            path: "/ingest".to_string(),
            method: HttpInputMethod::Put,
            ..HttpInputOptions::default()
        };
        let input =
            HttpInput::new_with_options("test", "127.0.0.1:0", options).expect("http input binds");
        let url = format!("http://{}/ingest", input.local_addr());

        let status = match ureq::post(&url).send(b"{\"x\":1}\n") {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(err) => panic!("unexpected request failure: {err}"),
        };
        assert_eq!(status, 405, "POST should be rejected for PUT-only endpoint");
    }

    #[test]
    fn http_returns_configured_success_body() {
        let options = HttpInputOptions {
            path: "/_bulk".to_string(),
            strict_path: false,
            response_body: Some("{\"errors\":false}".to_string()),
            ..HttpInputOptions::default()
        };
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", options).expect("http input binds");
        let url = format!("http://{}/_bulk", input.local_addr());

        let resp = ureq::post(&url)
            .send(b"{\"index\":{}}\n{\"message\":\"ok\"}\n")
            .expect("POST should succeed");
        assert_eq!(resp.status(), 200);
        let body = resp
            .into_body()
            .read_to_string()
            .expect("response body should be readable");
        assert_eq!(body, "{\"errors\":false}");

        let data = poll_until_data(&mut input, Duration::from_secs(2));
        let text = String::from_utf8_lossy(&data);
        assert!(
            text.contains("\"message\":\"ok\""),
            "expected payload row: {text}"
        );
    }

    #[test]
    fn http_rejects_response_body_with_204() {
        let options = HttpInputOptions {
            path: "/ingest".to_string(),
            response_code: 204,
            response_body: Some("{\"ok\":true}".to_string()),
            ..HttpInputOptions::default()
        };
        let err = match HttpInput::new_with_options("test", "127.0.0.1:0", options) {
            Ok(_) => panic!("204 with response body should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("response_body is not allowed when response_code is 204"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn http_returns_429_when_channel_full() {
        let options = HttpInputOptions {
            path: "/ingest".to_string(),
            ..HttpInputOptions::default()
        };
        let mut input = HttpInput::new_with_capacity("test", "127.0.0.1:0", 1, options)
            .expect("http input binds");
        let url = format!("http://{}/ingest", input.local_addr());

        let first = ureq::post(&url).send(b"{\"seq\":1}\n").expect("first POST");
        assert_eq!(first.status(), 200);

        let status = match ureq::post(&url).send(b"{\"seq\":2}\n") {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(err) => panic!("unexpected request failure: {err}"),
        };
        assert_eq!(
            status, 429,
            "expected backpressure response (429), got {status}"
        );

        let _ = poll_until_data(&mut input, Duration::from_secs(2));

        let third = ureq::post(&url)
            .send(b"{\"seq\":3}\n")
            .expect("third POST should succeed after drain");
        assert_eq!(third.status(), 200);
    }

    #[test]
    fn http_rejects_malformed_content_encoding_header() {
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", HttpInputOptions::default())
                .expect("http input binds");
        let mut stream = TcpStream::connect(input.local_addr()).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("timeout");
        let request = b"POST /ingest HTTP/1.1\r\n\
Host: localhost\r\n\
Content-Type: application/x-ndjson\r\n\
Content-Encoding: \xff\r\n\
Content-Length: 8\r\n\
Connection: close\r\n\
\r\n\
{\"x\":1}\n";
        stream.write_all(request).expect("write request");
        stream.flush().expect("flush request");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).expect("read response");
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 400"),
            "expected 400 status line, got: {text}"
        );
        let data = poll_until_data(&mut input, Duration::from_millis(100));
        assert!(data.is_empty(), "malformed requests must not enqueue data");
    }

    #[test]
    fn http_rejects_unsupported_content_encoding_before_enqueue() {
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", HttpInputOptions::default())
                .expect("http input binds");
        let url = format!("http://{}/ingest", input.local_addr());

        let status = match ureq::post(&url)
            .header("Content-Encoding", "br")
            .send(b"{\"x\":1}\n")
        {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(err) => panic!("unexpected request failure: {err}"),
        };

        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16());
        let data = poll_until_data(&mut input, Duration::from_millis(100));
        assert!(
            data.is_empty(),
            "unsupported encodings must not enqueue data"
        );
    }

    #[test]
    fn http_rejects_empty_content_encoding_header_before_enqueue() {
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", HttpInputOptions::default())
                .expect("http input binds");
        let mut stream = TcpStream::connect(input.local_addr()).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("timeout");
        let request = b"POST /ingest HTTP/1.1\r\n\
Host: localhost\r\n\
Content-Type: application/x-ndjson\r\n\
Content-Encoding:   \r\n\
Content-Length: 8\r\n\
Connection: close\r\n\
\r\n\
{\"x\":1}\n";
        stream.write_all(request).expect("write request");
        stream.flush().expect("flush request");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).expect("read response");
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 400"),
            "expected 400 status line, got: {text}"
        );
        let data = poll_until_data(&mut input, Duration::from_millis(100));
        assert!(
            data.is_empty(),
            "empty content-encoding must not enqueue data"
        );
    }

    #[test]
    fn content_encoding_trims_optional_whitespace() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_ENCODING,
            " gzip ".parse().expect("valid content-encoding header"),
        );

        assert_eq!(
            parse_content_encoding(&headers)
                .expect("header parses")
                .as_deref(),
            Some("gzip")
        );
    }

    #[test]
    fn content_encoding_absent_header_is_accepted() {
        let headers = HeaderMap::new();
        assert_eq!(
            parse_content_encoding(&headers).expect("absent header must parse"),
            None
        );
    }

    #[test]
    fn http_poll_respects_max_drained_bytes_per_poll() {
        let options = HttpInputOptions {
            max_drained_bytes_per_poll: 8,
            ..HttpInputOptions::default()
        };
        let mut input =
            HttpInput::new_with_options("test", "127.0.0.1:0", options).expect("http input binds");
        let mut first = TcpStream::connect(input.local_addr()).expect("connect first");
        first
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("timeout first");
        first
            .write_all(
                b"POST /ingest HTTP/1.1\r\n\
Host: localhost\r\n\
Content-Type: application/x-ndjson\r\n\
Content-Length: 6\r\n\
Connection: close\r\n\
\r\n\
11111\n",
            )
            .expect("write first request");
        first.flush().expect("flush first request");
        let mut first_response = Vec::new();
        first
            .read_to_end(&mut first_response)
            .expect("read first response");
        assert!(
            String::from_utf8_lossy(&first_response).starts_with("HTTP/1.1 200"),
            "first request should succeed"
        );

        let mut second = TcpStream::connect(input.local_addr()).expect("connect second");
        second
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("timeout second");
        second
            .write_all(
                b"POST /ingest HTTP/1.1\r\n\
Host: localhost\r\n\
Content-Type: application/x-ndjson\r\n\
Content-Length: 6\r\n\
Connection: close\r\n\
\r\n\
22222\n",
            )
            .expect("write second request");
        second.flush().expect("flush second request");
        let mut second_response = Vec::new();
        second
            .read_to_end(&mut second_response)
            .expect("read second response");
        assert!(
            String::from_utf8_lossy(&second_response).starts_with("HTTP/1.1 200"),
            "second request should succeed"
        );

        let first_poll = poll_until_data(&mut input, Duration::from_secs(2));
        assert_eq!(
            first_poll.len(),
            8,
            "poll should stop at max_drained_bytes_per_poll"
        );

        let second_poll = poll_until_data(&mut input, Duration::from_secs(2));
        assert_eq!(
            second_poll.len(),
            4,
            "remainder should be deferred to the next poll"
        );

        let mut merged = first_poll;
        merged.extend_from_slice(&second_poll);
        assert_eq!(merged, b"11111\n22222\n");
    }

    fn decode_observed_seq(bytes: &[u8]) -> BTreeSet<u64> {
        let mut observed = BTreeSet::new();
        for line in bytes.split(|&b| b == b'\n').filter(|line| !line.is_empty()) {
            let value: serde_json::Value =
                serde_json::from_slice(line).expect("observed payload line should be valid JSON");
            let seq = value
                .get("seq")
                .and_then(serde_json::Value::as_u64)
                .expect("observed payload line should include numeric seq");
            assert!(observed.insert(seq), "duplicate seq observed: {seq}");
        }
        observed
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 16,
            .. ProptestConfig::default()
        })]
        #[test]
        fn http_post_poll_drop_interleavings(
            delays_ms in prop::collection::vec(0u16..40, 1..10),
            drop_after_ms in prop::option::of(0u16..40),
        ) {
            let options = HttpInputOptions {
                path: "/ingest".to_string(),
                ..HttpInputOptions::default()
            };
            let mut maybe_input = Some(
                HttpInput::new_with_capacity("test", "127.0.0.1:0", 2, options)
                    .expect("http input binds"),
            );
            let addr = maybe_input
                .as_ref()
                .expect("input should exist before optional drop")
                .local_addr();
            let url = format!("http://{addr}/ingest");

            let (tx, rx) = std_mpsc::channel();
            let mut handles = Vec::with_capacity(delays_ms.len());
            for (idx, delay_ms) in delays_ms.iter().copied().enumerate() {
                let tx = tx.clone();
                let url = url.clone();
                handles.push(std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(u64::from(delay_ms)));
                    let body = format!("{{\"seq\":{idx}}}\n");
                    let status = match ureq::post(&url).send(body.as_bytes()) {
                        Ok(resp) => resp.status().as_u16(),
                        Err(ureq::Error::StatusCode(code)) => code,
                        Err(_) => 0,
                    };
                    tx.send((idx as u64, status)).expect("status send should succeed");
                }));
            }
            drop(tx);

            let start = Instant::now();
            let deadline = start + Duration::from_secs(3);
            let mut dropped = false;
            let mut outcomes = Vec::new();
            let mut observed_bytes = Vec::new();

            while Instant::now() < deadline && outcomes.len() < delays_ms.len() {
                while let Ok(outcome) = rx.try_recv() {
                    outcomes.push(outcome);
                }

                if !dropped
                    && let Some(drop_ms) = drop_after_ms
                    && start.elapsed() >= Duration::from_millis(u64::from(drop_ms))
                {
                    drop(maybe_input.take());
                    dropped = true;
                }

                if let Some(input) = maybe_input.as_mut() {
                    for event in input.poll().expect("poll should succeed") {
                        if let InputEvent::Data { bytes, .. } = event {
                            observed_bytes.extend_from_slice(&bytes);
                        }
                    }
                }

                std::thread::sleep(Duration::from_millis(5));
            }

            while let Ok(outcome) = rx.try_recv() {
                outcomes.push(outcome);
            }
            for handle in handles {
                handle.join().expect("POST worker should join");
            }

            if let Some(input) = maybe_input.as_mut() {
                let drain_deadline = Instant::now() + Duration::from_millis(200);
                while Instant::now() < drain_deadline {
                    let mut drained_any = false;
                    for event in input.poll().expect("poll should succeed") {
                        if let InputEvent::Data { bytes, .. } = event {
                            observed_bytes.extend_from_slice(&bytes);
                            drained_any = true;
                        }
                    }
                    if !drained_any {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }

            prop_assert_eq!(
                outcomes.len(),
                delays_ms.len(),
                "all POST workers should report one terminal status"
            );

            let accepted: BTreeSet<u64> = outcomes
                .iter()
                .filter_map(|(seq, status)| {
                    if matches!(*status, 200 | 201 | 202 | 204) {
                        Some(*seq)
                    } else {
                        None
                    }
                })
                .collect();

            for (_, status) in &outcomes {
                prop_assert!(
                    matches!(*status, 200 | 201 | 202 | 204 | 429 | 503 | 0),
                    "unexpected status code: {status}"
                );
                if !dropped {
                    prop_assert_ne!(*status, 0, "transport errors are unexpected without drop");
                }
            }

            let observed = decode_observed_seq(&observed_bytes);
            prop_assert!(
                observed.is_subset(&accepted),
                "observed seqs must be subset of accepted seqs; observed={observed:?} accepted={accepted:?}"
            );

            if !dropped {
                prop_assert_eq!(
                    observed, accepted,
                    "without drop, all accepted records should be drained"
                );
            }
        }
    }
}
