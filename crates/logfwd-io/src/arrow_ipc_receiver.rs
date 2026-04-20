//! Arrow IPC stream HTTP receiver.
//!
//! Accepts POST requests containing Arrow IPC stream bytes, deserializes them
//! into RecordBatches, and sends them through a channel. This bypasses the
//! scanner entirely — Arrow data enters the pipeline in native form.
//!
//! Endpoint: POST /v1/arrow
//!
//! Content-Type: `application/vnd.apache.arrow.stream` (uncompressed),
//! `application/vnd.apache.arrow.stream+zstd` (zstd compressed), or
//! `application/vnd.apache.arrow.stream+lz4` (lz4 compressed).
//! Also supports `Content-Encoding: zstd` and `Content-Encoding: lz4` headers.

use std::io;
use std::io::Read as _;
use std::sync::mpsc;
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicU8, Ordering},
};

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};
use tokio::sync::oneshot;

use crate::InputError;
use crate::background_http_task::BackgroundHttpTask;
use crate::input::{InputEvent, InputSource};
use crate::receiver_health::{ReceiverHealthEvent, reduce_receiver_health};
use crate::receiver_http::{MAX_REQUEST_BODY_SIZE, parse_content_length, read_limited_body};

/// Bounded channel capacity — limits memory when the pipeline falls behind.
const CHANNEL_BOUND: usize = 256;

#[derive(Debug, Clone)]
pub struct ArrowIpcReceiverOptions {
    /// Maximum concurrent connections processing requests. Defaults to `1024`.
    pub max_connections: usize,
    /// Maximum payload size in bytes. Defaults to `MAX_REQUEST_BODY_SIZE` (10 MiB).
    pub max_message_size_bytes: usize,
}

impl Default for ArrowIpcReceiverOptions {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            max_message_size_bytes: MAX_REQUEST_BODY_SIZE,
        }
    }
}

/// Arrow IPC receiver that listens for Arrow stream data via HTTP POST.
///
/// Produces `RecordBatch` directly, bypassing the JSON scanner. Each POST
/// can contain a single IPC stream with one or more batches.
pub struct ArrowIpcReceiver {
    name: String,
    rx: Option<mpsc::Receiver<DecodedBatch>>,
    in_flight_batches: Arc<std::sync::atomic::AtomicUsize>,
    slot_accounting: Arc<Mutex<()>>,
    /// The address the HTTP server is bound to.
    addr: std::net::SocketAddr,
    background_task: BackgroundHttpTask,
    shutdown: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
}

#[derive(Clone)]
struct ArrowIpcServerState {
    tx: mpsc::SyncSender<DecodedBatch>,
    in_flight_batches: Arc<std::sync::atomic::AtomicUsize>,
    slot_accounting: Arc<Mutex<()>>,
    channel_capacity: usize,
    shutdown: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    stats: Option<Arc<ComponentStats>>,
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
    max_connections: usize,
    max_message_size_bytes: usize,
}

struct ConnectionGuard {
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct DecodedBatch {
    batch: RecordBatch,
    accounted_bytes: u64,
}

impl ArrowIpcReceiver {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4319").
    /// Spawns a background thread to handle requests.
    pub fn new(
        name: impl Into<String>,
        addr: &str,
        options: ArrowIpcReceiverOptions,
    ) -> io::Result<Self> {
        Self::new_with_capacity_and_stats(name, addr, options, CHANNEL_BOUND, None)
    }

    /// Like [`Self::new`] but wires receiver diagnostics counters.
    pub fn new_with_stats(
        name: impl Into<String>,
        addr: &str,
        options: ArrowIpcReceiverOptions,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_and_stats(name, addr, options, CHANNEL_BOUND, Some(stats))
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    pub fn new_with_capacity(
        name: impl Into<String>,
        addr: &str,
        options: ArrowIpcReceiverOptions,
        capacity: usize,
    ) -> io::Result<Self> {
        Self::new_with_capacity_and_stats(name, addr, options, capacity, None)
    }

    /// Like [`Self::new_with_capacity`] but wires receiver diagnostics counters.
    pub fn new_with_capacity_and_stats(
        name: impl Into<String>,
        addr: &str,
        options: ArrowIpcReceiverOptions,
        capacity: usize,
        stats: Option<Arc<ComponentStats>>,
    ) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Arrow IPC receiver channel capacity must be greater than zero",
            ));
        }
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| io::Error::other(format!("Arrow IPC receiver bind {addr}: {e}")))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!(
                "Arrow IPC receiver set_nonblocking {bound_addr}: {e}"
            ))
        })?;

        let (tx, rx) = mpsc::sync_channel(capacity);
        let in_flight_batches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let slot_accounting = Arc::new(Mutex::new(()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        let state = Arc::new(ArrowIpcServerState {
            tx,
            in_flight_batches: Arc::clone(&in_flight_batches),
            slot_accounting: Arc::clone(&slot_accounting),
            channel_capacity: capacity,
            shutdown: Arc::clone(&shutdown),
            health: Arc::clone(&health),
            stats,
            active_connections: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max_connections: options.max_connections,
            max_message_size_bytes: options.max_message_size_bytes,
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown_for_server = Arc::clone(&shutdown);
        let health_for_server = Arc::clone(&health);
        let state_for_server = Arc::clone(&state);

        let handle = std::thread::Builder::new()
            .name("arrow-ipc-receiver".into())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    store_health_event(&health_for_server, ReceiverHealthEvent::FatalFailure);
                    return;
                };

                runtime.block_on(async move {
                    let Ok(listener) = tokio::net::TcpListener::from_std(std_listener) else {
                        store_health_event(&health_for_server, ReceiverHealthEvent::FatalFailure);
                        return;
                    };

                    let app = axum::Router::new()
                        .route("/v1/arrow", post(handle_arrow_ipc_request))
                        .with_state(state_for_server);

                    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    });

                    if server.await.is_err() && !shutdown_for_server.load(Ordering::Relaxed) {
                        store_health_event(&health_for_server, ReceiverHealthEvent::FatalFailure);
                    }
                });
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name: name.into(),
            rx: Some(rx),
            in_flight_batches,
            slot_accounting,
            addr: bound_addr,
            background_task: BackgroundHttpTask::new_axum(shutdown_tx, handle),
            shutdown,
            health,
        })
    }

    /// Returns the local address the HTTP server is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// Try to receive all available RecordBatches (non-blocking).
    pub fn try_recv_all(&self) -> Vec<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Vec::new();
        };
        let mut batches = Vec::new();
        loop {
            match try_recv_accounted(rx, &self.in_flight_batches, &self.slot_accounting) {
                Ok(Some(decoded)) => batches.push(decoded.batch),
                Ok(None) | Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => unreachable!(),
            }
        }
        batches
    }

    /// Blocking receive of the next RecordBatch.
    pub fn recv(&self) -> io::Result<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Err(io::Error::other("Arrow IPC receiver: already closed"));
        };
        loop {
            match try_recv_accounted(rx, &self.in_flight_batches, &self.slot_accounting) {
                Ok(Some(decoded)) => return Ok(decoded.batch),
                Ok(None) => std::thread::sleep(std::time::Duration::from_millis(1)),
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(io::Error::other("Arrow IPC receiver: channel disconnected"));
                }
                Err(mpsc::TryRecvError::Empty) => unreachable!(),
            }
        }
    }

    /// Receive the next RecordBatch with a timeout.
    ///
    /// Unlike `std::sync::mpsc::Receiver::recv_timeout`, this method polls the
    /// channel in sleep chunks of up to roughly 1ms, so callers should not rely
    /// on sub-millisecond or exact timeout precision. Returns
    /// `io::ErrorKind::TimedOut` when no batch arrives before the timeout, and
    /// `io::Error::other` when the receiver is closed or disconnected.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> io::Result<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Err(io::Error::other("Arrow IPC receiver: already closed"));
        };
        let start = std::time::Instant::now();
        let mut is_first_poll = true;
        loop {
            if !is_first_poll && start.elapsed() >= timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Arrow IPC receiver: timed out",
                ));
            }
            is_first_poll = false;
            match try_recv_accounted(rx, &self.in_flight_batches, &self.slot_accounting) {
                Ok(Some(decoded)) => return Ok(decoded.batch),
                Ok(None) => {
                    let elapsed = start.elapsed();
                    if elapsed >= timeout {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "Arrow IPC receiver: timed out",
                        ));
                    }
                    let remaining = timeout.saturating_sub(elapsed);
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(1)));
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(io::Error::other("Arrow IPC receiver: channel disconnected"));
                }
                Err(mpsc::TryRecvError::Empty) => unreachable!(),
            }
        }
    }

    /// Return the name of this receiver.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Coarse runtime health for readiness and diagnostics integration.
    pub fn health(&self) -> ComponentHealth {
        let stored = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        if self.background_task.is_finished() && !self.shutdown.load(Ordering::Relaxed) {
            ComponentHealth::Failed
        } else {
            stored
        }
    }
}

fn record_error(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_errors();
    }
}

fn record_parse_error(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_parse_errors(1);
    }
}

/// Decompress zstd body with size limit.
fn decompress_zstd(body: &[u8], max_message_size_bytes: usize) -> Result<Vec<u8>, InputError> {
    let decoder = zstd::Decoder::new(body).map_err(|_e| {
        InputError::Receiver("zstd decompression failed: invalid header".to_string())
    })?;
    let mut decompressed = Vec::with_capacity(body.len().min(max_message_size_bytes));
    match decoder
        .take(max_message_size_bytes as u64 + 1)
        .read_to_end(&mut decompressed)
    {
        Ok(n) if n > max_message_size_bytes => Err(InputError::Receiver(
            "decompressed payload too large".to_string(),
        )),
        Ok(_) => Ok(decompressed),
        Err(e) => Err(InputError::Receiver(format!(
            "zstd decompression failed: {e}"
        ))),
    }
}

/// Decompress lz4 (size-prepended block format) body with size limit.
///
/// `lz4_flex::decompress_size_prepended` reads the first 4 bytes as a LE u32
/// declared size and pre-allocates that much memory. We validate the declared
/// size against `max_message_size_bytes` *before* calling it to prevent a
/// forged prefix from triggering an unbounded allocation (DoS vector).
fn decompress_lz4(body: &[u8], max_message_size_bytes: usize) -> Result<Vec<u8>, InputError> {
    if body.len() < 4 {
        return Err(InputError::Receiver(
            "lz4 decompression failed: missing size prefix".to_string(),
        ));
    }
    let declared_len = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    if declared_len > max_message_size_bytes {
        return Err(InputError::Receiver(
            "decompressed payload too large".to_string(),
        ));
    }
    lz4_flex::decompress_size_prepended(body)
        .map_err(|e| InputError::Receiver(format!("lz4 decompression failed: {e}")))
}

/// Decode an Arrow IPC stream from bytes into RecordBatches.
fn decode_ipc_stream(body: &[u8]) -> Result<Vec<RecordBatch>, InputError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = io::Cursor::new(body);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| InputError::Receiver(format!("invalid Arrow IPC stream: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| InputError::Receiver(format!("failed to read Arrow IPC batch: {e}")))
}

fn store_health_event(health: &AtomicU8, event: ReceiverHealthEvent) {
    let mut current = health.load(Ordering::Relaxed);
    loop {
        let current_health = ComponentHealth::from_repr(current);
        let next = reduce_receiver_health(current_health, event).as_repr();
        match health.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

// Serializes non-blocking dequeue accounting with reservation so a freed slot
// and its counter decrement become visible to POST handlers together.
fn lock_slot_accounting(slot_accounting: &Mutex<()>) -> MutexGuard<'_, ()> {
    slot_accounting
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn try_recv_accounted(
    rx: &mpsc::Receiver<DecodedBatch>,
    in_flight_batches: &std::sync::atomic::AtomicUsize,
    slot_accounting: &Mutex<()>,
) -> Result<Option<DecodedBatch>, mpsc::TryRecvError> {
    let _guard = lock_slot_accounting(slot_accounting);
    match rx.try_recv() {
        Ok(decoded) => {
            in_flight_batches.fetch_sub(1, Ordering::Relaxed);
            Ok(Some(decoded))
        }
        Err(mpsc::TryRecvError::Empty) => Ok(None),
        Err(mpsc::TryRecvError::Disconnected) => Err(mpsc::TryRecvError::Disconnected),
    }
}

fn try_reserve_channel_slots(state: &ArrowIpcServerState, needed: usize) -> bool {
    if needed == 0 {
        return true;
    }
    let _guard = lock_slot_accounting(&state.slot_accounting);
    let mut observed = state.in_flight_batches.load(Ordering::Relaxed);
    loop {
        let Some(next) = observed.checked_add(needed) else {
            return false;
        };
        if next > state.channel_capacity {
            return false;
        }
        match state.in_flight_batches.compare_exchange_weak(
            observed,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => observed = actual,
        }
    }
}

async fn handle_arrow_ipc_request(
    State(state): State<Arc<ArrowIpcServerState>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if state.active_connections.fetch_add(1, Ordering::Relaxed) >= state.max_connections {
        state.active_connections.fetch_sub(1, Ordering::Relaxed);
        record_error(state.stats.as_ref());
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "too many concurrent connections",
        )
            .into_response();
    }
    let _guard = ConnectionGuard {
        active_connections: Arc::clone(&state.active_connections),
    };
    if state.shutdown.load(Ordering::Relaxed) {
        record_error(state.stats.as_ref());
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "service unavailable: pipeline disconnected",
        )
            .into_response();
    }

    let content_length = parse_content_length(&headers);
    if content_length.is_some_and(|body_len| body_len > state.max_message_size_bytes as u64) {
        record_error(state.stats.as_ref());
        return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response();
    }

    let content_encodings = match parse_content_encoding(&headers) {
        Ok(content_encoding) => content_encoding,
        Err(status) => {
            record_error(state.stats.as_ref());
            return (status, "invalid content-encoding header").into_response();
        }
    };
    let content_type = match parse_content_type(&headers) {
        Ok(content_type) => content_type,
        Err(status) => {
            record_error(state.stats.as_ref());
            return (status, "invalid content-type header").into_response();
        }
    };

    let body = match read_limited_body(body, state.max_message_size_bytes, content_length).await {
        Ok(body) => body,
        Err(status) => {
            record_error(state.stats.as_ref());
            let message = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload too large"
            } else {
                "read error"
            };
            return (status, message).into_response();
        }
    };
    let raw_body_len = body.len() as u64;

    let has_zstd_content_encoding = content_encodings
        .as_ref()
        .is_some_and(|encoding| encoding.has_zstd);
    let has_lz4_content_encoding = content_encodings
        .as_ref()
        .is_some_and(|encoding| encoding.has_lz4);
    let is_zstd = has_zstd_content_encoding
        || content_type.as_ref().is_some_and(|media_type| {
            media_type.eq_ignore_ascii_case("application/vnd.apache.arrow.stream+zstd")
        });
    let is_lz4 = has_lz4_content_encoding
        || content_type.as_ref().is_some_and(|media_type| {
            media_type.eq_ignore_ascii_case("application/vnd.apache.arrow.stream+lz4")
        });

    let body = if is_zstd {
        match decompress_zstd(&body, state.max_message_size_bytes) {
            Ok(body) => body,
            Err(InputError::Receiver(msg)) => {
                record_parse_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            Err(_) => {
                record_parse_error(state.stats.as_ref());
                return (
                    StatusCode::BAD_REQUEST,
                    "zstd decompression failed: invalid header",
                )
                    .into_response();
            }
        }
    } else if is_lz4 {
        match decompress_lz4(&body, state.max_message_size_bytes) {
            Ok(body) => body,
            Err(InputError::Receiver(msg)) => {
                record_parse_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            Err(_) => {
                record_parse_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, "lz4 decompression failed").into_response();
            }
        }
    } else {
        body
    };

    let batches = match decode_ipc_stream(&body) {
        Ok(batches) => batches,
        Err(InputError::Receiver(msg)) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg).into_response();
        }
        Err(_) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, "invalid Arrow IPC stream").into_response();
        }
    };

    let total_batch_count = batches.iter().filter(|batch| batch.num_rows() > 0).count() as u64;
    if state.shutdown.load(Ordering::Relaxed) {
        record_error(state.stats.as_ref());
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "service unavailable: pipeline disconnected",
        )
            .into_response();
    }
    if !try_reserve_channel_slots(&state, total_batch_count as usize) {
        record_error(state.stats.as_ref());
        store_health_event(&state.health, ReceiverHealthEvent::Backpressure);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "too many requests: pipeline backpressure (accepted_batches=0, rejected_batches={total_batch_count})"
            ),
        )
            .into_response();
    }

    let mut send_error: Option<StatusCode> = None;
    let mut sent_rows = false;
    let per_batch_accounted_bytes = raw_body_len.checked_div(total_batch_count).unwrap_or(0);
    let mut emitted_count = 0_u64;
    let mut accepted_batches = 0_u64;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        sent_rows = true;
        emitted_count = emitted_count.saturating_add(1);
        let accounted_bytes = if emitted_count == total_batch_count {
            raw_body_len.saturating_sub(
                per_batch_accounted_bytes.saturating_mul(total_batch_count.saturating_sub(1)),
            )
        } else {
            per_batch_accounted_bytes
        };
        let payload = DecodedBatch {
            batch,
            accounted_bytes,
        };
        match state.tx.try_send(payload) {
            Ok(()) => {
                accepted_batches = accepted_batches.saturating_add(1);
            }
            Err(mpsc::TrySendError::Full(_)) => {
                record_error(state.stats.as_ref());
                send_error = Some(StatusCode::INTERNAL_SERVER_ERROR);
                break;
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                record_error(state.stats.as_ref());
                send_error = Some(StatusCode::SERVICE_UNAVAILABLE);
                break;
            }
        }
    }
    if send_error.is_some() {
        let rejected_batches = total_batch_count.saturating_sub(accepted_batches);
        state
            .in_flight_batches
            .fetch_sub(rejected_batches as usize, Ordering::Relaxed);
    }

    match send_error {
        Some(StatusCode::SERVICE_UNAVAILABLE) => {
            if !state.shutdown.load(Ordering::Relaxed) {
                store_health_event(&state.health, ReceiverHealthEvent::FatalFailure);
            }
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "service unavailable: pipeline disconnected",
            )
                .into_response()
        }
        Some(StatusCode::INTERNAL_SERVER_ERROR) => {
            store_health_event(&state.health, ReceiverHealthEvent::FatalFailure);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error: Arrow IPC channel reservation invariant violated",
            )
                .into_response()
        }
        Some(_) => unreachable!(),
        None => {
            store_health_event(
                &state.health,
                if sent_rows {
                    ReceiverHealthEvent::DeliveryAccepted
                } else {
                    ReceiverHealthEvent::DeliveryNoop
                },
            );
            StatusCode::OK.into_response()
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct ContentEncodingFlags {
    has_zstd: bool,
    has_lz4: bool,
}

fn parse_content_encoding(headers: &HeaderMap) -> Result<Option<ContentEncodingFlags>, StatusCode> {
    let Some(value) = headers.get(CONTENT_ENCODING) else {
        return Ok(None);
    };
    let parsed = value.to_str().map_err(|_e| StatusCode::BAD_REQUEST)?;
    let mut flags = ContentEncodingFlags::default();
    let mut has_token = false;
    for token in parsed.split(',').map(str::trim) {
        if token.is_empty() {
            return Err(StatusCode::BAD_REQUEST);
        }
        if token.eq_ignore_ascii_case("zstd") {
            flags.has_zstd = true;
        } else if token.eq_ignore_ascii_case("lz4") {
            flags.has_lz4 = true;
        } else if !token.eq_ignore_ascii_case("identity") {
            return Err(StatusCode::BAD_REQUEST);
        }
        has_token = true;
    }
    if !has_token {
        return Err(StatusCode::BAD_REQUEST);
    }
    if flags.has_zstd && flags.has_lz4 {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(flags))
}

fn parse_content_type(headers: &HeaderMap) -> Result<Option<&str>, StatusCode> {
    let Some(value) = headers.get(CONTENT_TYPE) else {
        return Ok(None);
    };
    let parsed = value.to_str().map_err(|_e| StatusCode::BAD_REQUEST)?;
    let media_type = parsed.split(';').next().unwrap_or(parsed).trim();
    if media_type.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(media_type))
}

impl Drop for ArrowIpcReceiver {
    fn drop(&mut self) {
        let current = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        self.health.store(
            reduce_receiver_health(current, ReceiverHealthEvent::ShutdownRequested).as_repr(),
            Ordering::Relaxed,
        );
        self.shutdown.store(true, Ordering::Relaxed);
        self.rx.take();
        let current = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        self.health.store(
            reduce_receiver_health(current, ReceiverHealthEvent::ShutdownCompleted).as_repr(),
            Ordering::Relaxed,
        );
    }
}

impl InputSource for ArrowIpcReceiver {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let Some(rx) = self.rx.as_ref() else {
            return Ok(vec![]);
        };
        let mut events = Vec::new();
        loop {
            match try_recv_accounted(rx, &self.in_flight_batches, &self.slot_accounting) {
                Ok(Some(decoded)) => events.push(InputEvent::Batch {
                    batch: decoded.batch,
                    source_id: None,
                    accounted_bytes: decoded.accounted_bytes,
                }),
                Ok(None) | Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => unreachable!(),
            }
        }
        Ok(events)
    }

    fn name(&self) -> &str {
        ArrowIpcReceiver::name(self)
    }

    fn health(&self) -> ComponentHealth {
        ArrowIpcReceiver::health(self)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use logfwd_types::diagnostics::ComponentStats;

    // Regression test for issue #1142: clean shutdown
    #[test]
    fn clean_shutdown_releases_port() {
        let addr = "127.0.0.1:0";
        let receiver =
            ArrowIpcReceiver::new("test", addr, ArrowIpcReceiverOptions::default()).unwrap();
        let port = receiver.local_addr().port();

        // Wait briefly for thread to start blocking
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Drop it
        drop(receiver);

        // Wait briefly for the OS to actually release the port
        std::thread::sleep(std::time::Duration::from_millis(50));

        // The port should now be free to bind to immediately
        let new_addr = format!("127.0.0.1:{}", port);
        let result = tiny_http::Server::http(&new_addr);
        assert!(result.is_ok(), "Failed to bind to port {} after drop", port);
    }
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::Instant;

    fn loopback_http_client() -> ureq::Agent {
        ureq::Agent::config_builder()
            .proxy(None)
            .timeout_global(Some(std::time::Duration::from_secs(5)))
            .build()
            .into()
    }

    fn loopback_http_client_with_status_body() -> ureq::Agent {
        ureq::Agent::config_builder()
            .proxy(None)
            .timeout_global(Some(std::time::Duration::from_secs(5)))
            .http_status_as_error(false)
            .build()
            .into()
    }

    fn wait_until<F>(timeout: std::time::Duration, mut predicate: F, failure_message: &str)
    where
        F: FnMut() -> bool,
    {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(predicate(), "{failure_message}");
    }

    fn wait_for_server(addr: std::net::SocketAddr) {
        wait_until(
            std::time::Duration::from_secs(2),
            || std::net::TcpStream::connect(addr).is_ok(),
            "Arrow IPC receiver did not accept connections",
        );
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]))
    }

    fn make_test_batch() -> RecordBatch {
        let schema = test_schema();
        let msg = StringArray::from(vec![Some("alpha"), Some("beta")]);
        let code = Int64Array::from(vec![Some(1), Some(2)]);
        RecordBatch::try_new(schema, vec![Arc::new(msg), Arc::new(code)])
            .expect("test batch creation should succeed")
    }

    /// A second test batch with different content so tests can distinguish it
    /// from the batch produced by `make_test_batch`.
    fn make_test_batch_b() -> RecordBatch {
        let schema = test_schema();
        let msg = StringArray::from(vec![Some("gamma"), Some("delta"), Some("epsilon")]);
        let code = Int64Array::from(vec![Some(10), Some(20), Some(30)]);
        RecordBatch::try_new(schema, vec![Arc::new(msg), Arc::new(code)])
            .expect("test batch B creation should succeed")
    }

    fn assert_batch_messages(batch: &RecordBatch, expected: &[&str]) {
        let msg_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 0 should be StringArray");
        assert_eq!(msg_col.len(), expected.len());
        for (idx, value) in expected.iter().enumerate() {
            assert_eq!(msg_col.value(idx), *value);
        }
    }

    fn serialize_batch(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .expect("writer init");
        writer.write(batch).expect("write batch");
        writer.finish().expect("finish");
        buf
    }

    fn serialize_batches(batches: &[RecordBatch]) -> Vec<u8> {
        let Some(first) = batches.first() else {
            return Vec::new();
        };
        let mut buf = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &first.schema())
            .expect("writer init");
        for batch in batches {
            writer.write(batch).expect("write batch");
        }
        writer.finish().expect("finish");
        buf
    }

    #[test]
    fn test_arrow_ipc_custom_options() {
        let mut options = ArrowIpcReceiverOptions::default();
        options.max_connections = 5;
        options.max_message_size_bytes = 10;

        let addr = "127.0.0.1:0";
        let receiver = ArrowIpcReceiver::new("test_custom", addr, options).unwrap();
        let local_addr = receiver.local_addr();

        // Send a request exceeding max_message_size_bytes
        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        assert!(ipc_bytes.len() > 10);

        let url = format!("http://{local_addr}/v1/arrow");
        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(ipc_bytes.as_slice());
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(
            status, 413,
            "expected PAYLOAD_TOO_LARGE due to custom max_message_size_bytes"
        );
    }

    #[test]
    fn receiver_rejects_zero_channel_capacity() {
        let result = ArrowIpcReceiver::new_with_capacity(
            "test-zero-capacity",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            0,
        );
        let err = match result {
            Ok(_) => panic!("zero channel capacity should be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn receiver_accepts_arrow_ipc_post() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        assert_eq!(receiver.health(), ComponentHealth::Healthy);

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);

        // POST Arrow IPC data.
        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        // Receive the batch.
        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
        assert_eq!(received.num_columns(), 2);
        assert_eq!(received.schema(), batch.schema());
        assert_eq!(receiver.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn poll_emits_batch_event_with_accounted_bytes() {
        let mut receiver = ArrowIpcReceiver::new_with_capacity(
            "test-poll",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let events = receiver.poll().expect("poll should succeed");
        assert_eq!(events.len(), 1, "poll should emit one batch event");
        match &events[0] {
            InputEvent::Batch {
                batch,
                source_id,
                accounted_bytes,
            } => {
                assert_eq!(batch.num_rows(), 2);
                assert_eq!(*source_id, None);
                assert_eq!(*accounted_bytes, ipc_bytes.len() as u64);
            }
            _ => panic!("expected InputEvent::Batch"),
        }
    }

    #[test]
    fn receiver_accepts_zstd_compressed_arrow_ipc() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-zstd",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = zstd::bulk::compress(&ipc_bytes, 1).expect("zstd compress should succeed");

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream+zstd")
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_accepts_zstd_content_type_with_parameters() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-zstd-params",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = zstd::bulk::compress(&ipc_bytes, 1).expect("zstd compress should succeed");

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header(
                "Content-Type",
                "application/vnd.apache.arrow.stream+zstd; charset=binary",
            )
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_accepts_tokenized_content_encoding_with_zstd() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-zstd-tokenized",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = zstd::bulk::compress(&ipc_bytes, 1).expect("zstd compress should succeed");

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "identity, zstd")
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_rejects_unsupported_content_encoding() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-unsupported-encoding",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);

        let url = format!("http://{addr}/v1/arrow");
        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "gzip")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 400);
    }

    #[test]
    fn receiver_rejects_empty_content_encoding_token() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-empty-encoding-token",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);

        let url = format!("http://{addr}/v1/arrow");
        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "identity,,zstd")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 400);
    }

    #[test]
    fn receiver_rejects_wrong_path() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-404",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let url = format!("http://{addr}/v1/logs");
        let result = ureq::post(&url).send(b"data" as &[u8]);
        match result {
            Err(ureq::Error::StatusCode(code)) => assert_eq!(code, 404),
            other => panic!("expected 404, got {other:?}"),
        }
    }

    #[test]
    fn receiver_rejects_get_method() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-405",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        wait_for_server(addr);

        let url = format!("http://{addr}/v1/arrow");
        let result = ureq::get(&url).call();
        match result {
            Err(ureq::Error::StatusCode(code)) => assert_eq!(code, 405),
            other => panic!("expected 405, got {other:?}"),
        }
    }

    #[test]
    fn receiver_reports_degraded_on_backpressure_and_recovers() {
        let stats = Arc::new(ComponentStats::new());
        let receiver = ArrowIpcReceiver::new_with_capacity_and_stats(
            "test-429",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            1,
            Some(Arc::clone(&stats)),
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        wait_for_server(addr);

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let url = format!("http://{addr}/v1/arrow");

        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("first POST should succeed");
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(receiver.health(), ComponentHealth::Healthy);

        let result = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 429);
        assert_eq!(stats.errors(), 1);
        assert_eq!(receiver.health(), ComponentHealth::Degraded);

        let _ = receiver.try_recv_all();

        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("recovery POST should succeed");
        assert_eq!(response.status().as_u16(), 200);
        let _ = receiver.recv_timeout(std::time::Duration::from_secs(2));
        assert_eq!(receiver.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn receiver_rejects_multi_batch_request_before_enqueue_when_full() {
        // Regression for #1046/#1704:
        // when remaining channel capacity cannot fit a multi-batch request,
        // the receiver must reject before enqueuing any request batch.
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-partial",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            2,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        let url = format!("http://{addr}/v1/arrow");

        let prefilled_batch = make_test_batch_b();
        let prefilled_ipc_bytes = serialize_batch(&prefilled_batch);
        let response = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&prefilled_ipc_bytes)
            .expect("prefill POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let batches = vec![make_test_batch(), make_test_batch()];
        let ipc_bytes = serialize_batches(&batches);

        let response = loopback_http_client_with_status_body()
            .post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("backpressure POST should return a response");
        let status = response.status().as_u16();
        let body = response
            .into_body()
            .read_to_string()
            .expect("backpressure response body should be readable");
        assert_eq!(
            status, 429,
            "POST should return 429 when channel has insufficient capacity"
        );
        assert!(
            body.contains("accepted_batches=0"),
            "429 body should report zero accepted batches: {body}"
        );

        let received = receiver.try_recv_all();
        assert_eq!(
            received.len(),
            1,
            "rejected request must leave the existing queued batch unchanged"
        );
        assert_batch_messages(&received[0], &["gamma", "delta", "epsilon"]);
        assert_eq!(receiver.health(), ComponentHealth::Degraded);
    }

    #[test]
    fn rejected_retry_does_not_duplicate_batches() {
        // Duplicate-risk regression for #1046/#1704:
        // 1) the channel is partially full
        // 2) first POST is rejected with 429 before enqueue
        // 3) retry of identical payload is also rejected
        // 4) downstream sees only the prefilled batch, with no duplicated prefix
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-dup-risk",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            2,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        let url = format!("http://{addr}/v1/arrow");

        let prefilled_batch = make_test_batch_b();
        let prefilled_ipc_bytes = serialize_batch(&prefilled_batch);
        let response = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&prefilled_ipc_bytes)
            .expect("prefill POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let batches = vec![make_test_batch(), make_test_batch()];
        let ipc_bytes = serialize_batches(&batches);

        let first_response = loopback_http_client_with_status_body()
            .post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("first rejected POST should return a response");
        let first_status = first_response.status().as_u16();
        let first_body = first_response
            .into_body()
            .read_to_string()
            .expect("first rejection body should be readable");
        assert_eq!(first_status, 429);
        assert!(
            first_body.contains("accepted_batches=0"),
            "first 429 body should report zero accepted batches: {first_body}"
        );

        let retry_response = loopback_http_client_with_status_body()
            .post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("retry rejection should return a response");
        let retry_status = retry_response.status().as_u16();
        let retry_body = retry_response
            .into_body()
            .read_to_string()
            .expect("retry rejection body should be readable");
        assert_eq!(retry_status, 429, "retry should also be rejected with 429");
        assert!(
            retry_body.contains("accepted_batches=0"),
            "retry 429 body should report zero accepted batches: {retry_body}"
        );

        let queued_batches = receiver.try_recv_all();
        assert_eq!(
            queued_batches.len(),
            1,
            "retry must not duplicate an accepted prefix from either rejected request"
        );
        assert_batch_messages(&queued_batches[0], &["gamma", "delta", "epsilon"]);
    }

    #[test]
    fn decode_ipc_stream_empty_body() {
        let batches = decode_ipc_stream(b"").expect("empty body should succeed");
        assert!(batches.is_empty());
    }

    #[test]
    fn empty_request_does_not_clear_failed_health() {
        let mut receiver = ArrowIpcReceiver::new_with_capacity(
            "test-empty",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        // Drop the consumer side so the receiver reports pipeline disconnection.
        receiver.rx.take();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let url = format!("http://{addr}/v1/arrow");

        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 503);
        assert_eq!(receiver.health(), ComponentHealth::Failed);

        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(b"" as &[u8])
            .expect("empty request should still succeed");
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(receiver.health(), ComponentHealth::Failed);
    }

    #[test]
    fn decode_ipc_stream_invalid_body() {
        let result = decode_ipc_stream(b"not arrow data");
        assert!(result.is_err());
    }

    #[test]
    fn receiver_accepts_lz4_compressed_arrow_ipc() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-lz4",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = lz4_flex::compress_prepend_size(&ipc_bytes);

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "lz4")
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_accepts_lz4_content_type() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-lz4-ct",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = lz4_flex::compress_prepend_size(&ipc_bytes);

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream+lz4")
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn decompress_lz4_rejects_oversized_declared_length() {
        // Forge a 4-byte prefix claiming 1 GB, followed by minimal data.
        let mut forged = Vec::new();
        forged.extend_from_slice(&(1_073_741_824_u32).to_le_bytes());
        forged.extend_from_slice(b"tiny");

        let result = decompress_lz4(&forged, 10 * 1024 * 1024);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("too large"),
            "expected 'too large' error, got: {err_msg}"
        );
    }

    #[test]
    fn decompress_lz4_rejects_missing_prefix() {
        let result = decompress_lz4(b"abc", 10 * 1024 * 1024);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("missing size prefix"),
            "expected 'missing size prefix' error, got: {err_msg}"
        );
    }

    #[test]
    fn receiver_rejects_conflicting_zstd_and_lz4_content_encoding() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-conflict-encoding",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);

        let url = format!("http://{addr}/v1/arrow");
        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "zstd, lz4")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 400);
    }

    #[test]
    fn malformed_arrow_lz4_payload_increments_parse_errors_when_stats_hooked() {
        let stats = Arc::new(ComponentStats::new());
        let receiver = ArrowIpcReceiver::new_with_capacity_and_stats(
            "test-stats-parse",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
            Some(Arc::clone(&stats)),
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        wait_for_server(addr);

        let url = format!("http://{addr}/v1/arrow");
        let result = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "lz4")
            .send(b"bad" as &[u8]);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 400);
        assert_eq!(stats.parse_errors(), 1);
        assert_eq!(stats.errors(), 0);
    }

    #[test]
    fn disconnected_pipeline_increments_errors_when_stats_hooked() {
        let stats = Arc::new(ComponentStats::new());
        let mut receiver = ArrowIpcReceiver::new_with_capacity_and_stats(
            "test-stats-errors",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
            Some(Arc::clone(&stats)),
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        wait_for_server(addr);
        receiver.rx.take();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let url = format!("http://{addr}/v1/arrow");
        let result = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 503);
        assert_eq!(stats.errors(), 1);
    }
}
