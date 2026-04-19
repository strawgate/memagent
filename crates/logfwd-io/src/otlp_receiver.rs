//! OTLP HTTP receiver input source.
//!
//! Listens for OTLP ExportLogsServiceRequest via HTTP POST, decodes the
//! protobuf or JSON payload, and produces structured Arrow record batches.
//!
//! Endpoint: POST /v1/logs (protobuf or JSON)

mod convert;
mod decode;
#[cfg(any(feature = "otlp-research", test))]
mod projection;
mod server;
#[cfg(test)]
mod tests;
#[cfg(any(feature = "otlp-research", test))]
use bytes::Bytes;
#[cfg(test)]
use convert::*;
use decode::decode_otlp_logs_to_batch;
#[cfg(any(feature = "otlp-research", test))]
use decode::decode_otlp_protobuf_bytes_with_mode;
#[cfg(any(feature = "otlp-research", test))]
use decode::decode_otlp_protobuf_with_prost;
#[cfg(test)]
use decode::*;
#[cfg(any(feature = "otlp-research", test))]
use projection::ProjectionError;

use std::io;
#[cfg(any(feature = "otlp-research", test))]
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};

use arrow::record_batch::RecordBatch;
use axum::routing::post;
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};
use logfwd_types::field_names;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;

use crate::InputError;
use crate::background_http_task::BackgroundHttpTask;
use crate::input::{InputEvent, InputSource};

const CHANNEL_BOUND: usize = 4096;
const FALLBACK_PROTOBUF_DECODE_TASKS: usize = 4;
/// Max payloads drained from the internal channel in a single `poll()` call.
///
/// This bounds per-poll work and prevents one call from aggregating an
/// arbitrarily deep queue into memory.
const MAX_DRAINED_PAYLOADS_PER_POLL: usize = 256;

/// Protobuf decode strategy used by the OTLP HTTP receiver.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OtlpProtobufDecodeMode {
    /// Decode through the generated prost OTLP model.
    #[default]
    Prost,
    /// Decode supported primitive OTLP log shapes directly into Arrow.
    ///
    /// Valid but unsupported semantic cases fall back to prost. Malformed
    /// protobuf remains an error so the fast path cannot accidentally accept
    /// data the reference decoder rejects.
    #[cfg(any(feature = "otlp-research", test))]
    ProjectedFallback,
    /// Decode only through the experimental projection path.
    ///
    /// This is for parity tests and benchmarks that need to isolate projection
    /// coverage without hiding unsupported cases behind the reference decoder.
    #[cfg(any(feature = "otlp-research", test))]
    ProjectedOnly,
}

/// OTLP receiver that listens for log exports via HTTP.
pub struct OtlpReceiverInput {
    name: String,
    rx: Option<mpsc::Receiver<ReceiverPayload>>,
    /// The address the HTTP server is bound to.
    addr: std::net::SocketAddr,
    background_task: BackgroundHttpTask,
    /// Shutdown mechanism for the background thread.
    is_running: Arc<AtomicBool>,
    /// Source-owned health snapshot for readiness and diagnostics.
    health: Arc<AtomicU8>,
}

struct ReceiverPayload {
    batch: RecordBatch,
    accounted_bytes: u64,
}

struct OtlpServerState {
    tx: mpsc::SyncSender<ReceiverPayload>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    resource_prefix: String,
    protobuf_decode_mode: OtlpProtobufDecodeMode,
    protobuf_decode_permits: Arc<Semaphore>,
    #[cfg(any(feature = "otlp-research", test))]
    projected_decoder: Option<Mutex<ProjectedOtlpDecoder>>,
    stats: Option<Arc<ComponentStats>>,
    /// Maximum request body size. Defaults to `MAX_REQUEST_BODY_SIZE` (10 MiB).
    max_message_size_bytes: usize,
}

impl OtlpReceiverInput {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4318").
    /// Spawns a background thread to handle requests.
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_resource_prefix(name, addr, field_names::DEFAULT_RESOURCE_PREFIX)
    }

    /// Like [`Self::new`], but with a custom resource attribute prefix used
    /// when materializing OTLP resource attributes into flat columns.
    pub fn new_with_resource_prefix(
        name: impl Into<String>,
        addr: &str,
        resource_prefix: impl Into<String>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_stats_and_prefix(
            name,
            addr,
            CHANNEL_BOUND,
            None,
            resource_prefix.into(),
        )
    }

    /// Like [`Self::new`], but wires input diagnostics into receiver-side
    /// transport and decode failures.
    pub fn new_with_stats(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_stats_and_resource_prefix(
            name,
            addr,
            stats,
            field_names::DEFAULT_RESOURCE_PREFIX,
        )
    }

    /// Like [`Self::new_with_stats`], but allows configuring the resource
    /// attribute prefix used when flattening OTLP resource attributes.
    pub fn new_with_stats_and_resource_prefix(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ComponentStats>,
        resource_prefix: impl Into<String>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_stats_and_prefix(
            name,
            addr,
            CHANNEL_BOUND,
            Some(stats),
            resource_prefix.into(),
        )
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    #[cfg(test)]
    fn new_with_capacity(name: impl Into<String>, addr: &str, capacity: usize) -> io::Result<Self> {
        Self::new_with_capacity_stats_and_prefix(
            name,
            addr,
            capacity,
            None,
            field_names::DEFAULT_RESOURCE_PREFIX.to_string(),
        )
    }

    #[cfg(test)]
    fn new_with_capacity_and_stats(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        stats: Option<Arc<ComponentStats>>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_stats_and_prefix(
            name,
            addr,
            capacity,
            stats,
            field_names::DEFAULT_RESOURCE_PREFIX.to_string(),
        )
    }

    fn new_with_capacity_stats_and_prefix(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        stats: Option<Arc<ComponentStats>>,
        resource_prefix: String,
    ) -> io::Result<Self> {
        Self::new_with_capacity_stats_prefix_and_decode_mode(
            name,
            addr,
            capacity,
            stats,
            resource_prefix,
            OtlpProtobufDecodeMode::Prost,
            None,
        )
    }

    /// Like [`Self::new_with_stats_and_resource_prefix`], but allows overriding
    /// the maximum request body size. Pass `None` to keep the 10 MiB default.
    pub fn new_with_stats_resource_prefix_and_max_size(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ComponentStats>,
        resource_prefix: impl Into<String>,
        max_message_size_bytes: Option<usize>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_stats_prefix_and_decode_mode(
            name,
            addr,
            CHANNEL_BOUND,
            Some(stats),
            resource_prefix.into(),
            OtlpProtobufDecodeMode::Prost,
            max_message_size_bytes,
        )
    }

    #[cfg(any(feature = "otlp-research", test))]
    #[doc(hidden)]
    pub fn new_with_protobuf_decode_mode_experimental(
        name: impl Into<String>,
        addr: &str,
        stats: Option<Arc<ComponentStats>>,
        resource_prefix: impl Into<String>,
        protobuf_decode_mode: OtlpProtobufDecodeMode,
        max_message_size_bytes: Option<usize>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_stats_prefix_and_decode_mode(
            name,
            addr,
            CHANNEL_BOUND,
            stats,
            resource_prefix.into(),
            protobuf_decode_mode,
            max_message_size_bytes,
        )
    }

    fn new_with_capacity_stats_prefix_and_decode_mode(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        stats: Option<Arc<ComponentStats>>,
        resource_prefix: String,
        protobuf_decode_mode: OtlpProtobufDecodeMode,
        max_message_size_bytes: Option<usize>,
    ) -> io::Result<Self> {
        use crate::receiver_http::MAX_REQUEST_BODY_SIZE;
        let max_message_size_bytes = max_message_size_bytes.unwrap_or(MAX_REQUEST_BODY_SIZE);
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| io::Error::other(format!("OTLP receiver bind {addr}: {e}")))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!("OTLP receiver set_nonblocking {bound_addr}: {e}"))
        })?;

        let (tx, rx) = mpsc::sync_channel(capacity);
        let is_running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        #[cfg(any(feature = "otlp-research", test))]
        let projected_decoder = if protobuf_decode_mode == OtlpProtobufDecodeMode::Prost {
            None
        } else {
            Some(Mutex::new(ProjectedOtlpDecoder::new(&resource_prefix)))
        };
        let state = Arc::new(OtlpServerState {
            tx,
            is_running: Arc::clone(&is_running),
            health: Arc::clone(&health),
            resource_prefix,
            protobuf_decode_mode,
            protobuf_decode_permits: Arc::new(Semaphore::new(protobuf_decode_task_limit())),
            #[cfg(any(feature = "otlp-research", test))]
            projected_decoder,
            stats,
            max_message_size_bytes,
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let state_for_server = Arc::clone(&state);
        let is_running_for_server = Arc::clone(&is_running);
        let health_for_server = Arc::clone(&health);

        let handle = std::thread::Builder::new()
            .name("otlp-receiver".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        server::record_error(state_for_server.stats.as_ref());
                        health_for_server
                            .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                        return;
                    }
                };

                runtime.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(std_listener) {
                        Ok(listener) => listener,
                        Err(_) => {
                            server::record_error(state_for_server.stats.as_ref());
                            health_for_server
                                .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                            return;
                        }
                    };

                    let app = axum::Router::new()
                        .route("/v1/logs", post(server::handle_otlp_request))
                        .with_state(state_for_server);

                    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    });

                    if server.await.is_err() && is_running_for_server.load(Ordering::Relaxed) {
                        server::record_error(state.stats.as_ref());
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
            background_task: BackgroundHttpTask::new_axum(shutdown_tx, handle),
            is_running,
            health,
        })
    }

    /// Returns the local address the HTTP server is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }
}

fn protobuf_decode_task_limit() -> usize {
    std::thread::available_parallelism()
        .map_or(FALLBACK_PROTOBUF_DECODE_TASKS, |parallelism| {
            parallelism.get().saturating_mul(2)
        })
        .max(1)
}

impl Drop for OtlpReceiverInput {
    fn drop(&mut self) {
        self.health
            .store(ComponentHealth::Stopping.as_repr(), Ordering::Relaxed);
        self.is_running.store(false, Ordering::Relaxed);
        self.rx.take();
        self.health
            .store(ComponentHealth::Stopped.as_repr(), Ordering::Relaxed);
    }
}

impl InputSource for OtlpReceiverInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let Some(rx) = self.rx.as_ref() else {
            return Ok(vec![]);
        };
        Ok(drain_receiver_payloads(rx, MAX_DRAINED_PAYLOADS_PER_POLL))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        let stored = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        if self.background_task.is_finished() && self.is_running.load(Ordering::Relaxed) {
            ComponentHealth::Failed
        } else {
            stored
        }
    }
}

/// Decode OTLP protobuf bytes into a structured `RecordBatch`.
///
/// This performs receiver-side protobuf decode plus batch materialization
/// without HTTP transport overhead. Resource attributes are materialized with
/// [`logfwd_types::field_names::DEFAULT_RESOURCE_PREFIX`].
pub fn decode_protobuf_to_batch(body: &[u8]) -> Result<RecordBatch, InputError> {
    decode_otlp_logs_to_batch(body)
}

/// Decode OTLP protobuf bytes through the experimental projection path.
///
/// Supported primitive fields are projected directly into Arrow string views
/// backed by `body`; unsupported but valid OTLP value shapes fall back to the
/// prost reference decoder.
#[cfg(any(feature = "otlp-research", test))]
#[doc(hidden)]
pub fn decode_protobuf_bytes_to_batch_projected_experimental(
    body: Bytes,
) -> Result<RecordBatch, InputError> {
    decode_otlp_protobuf_bytes_with_mode(
        body,
        field_names::DEFAULT_RESOURCE_PREFIX,
        OtlpProtobufDecodeMode::ProjectedFallback,
    )
}

/// Decode OTLP protobuf bytes through the projection path without prost fallback.
#[cfg(any(feature = "otlp-research", test))]
#[doc(hidden)]
pub fn decode_protobuf_bytes_to_batch_projected_only_experimental(
    body: Bytes,
) -> Result<RecordBatch, InputError> {
    decode_otlp_protobuf_bytes_with_mode(
        body,
        field_names::DEFAULT_RESOURCE_PREFIX,
        OtlpProtobufDecodeMode::ProjectedOnly,
    )
}

/// Decode borrowed OTLP protobuf bytes through the detached projection path.
///
/// This is useful as a benchmark control for measuring wire projection without
/// Arrow views attached to the request body.
#[cfg(any(feature = "otlp-research", test))]
#[doc(hidden)]
pub fn decode_protobuf_to_batch_projected_detached_experimental(
    body: &[u8],
) -> Result<RecordBatch, InputError> {
    projection::decode_projected_otlp_logs(body, field_names::DEFAULT_RESOURCE_PREFIX)
        .map_err(ProjectionError::into_input_error)
}

/// Decode OTLP protobuf bytes through the prost reference path.
#[cfg(any(feature = "otlp-research", test))]
#[doc(hidden)]
pub fn decode_protobuf_to_batch_prost_reference(body: &[u8]) -> Result<RecordBatch, InputError> {
    decode_otlp_protobuf_with_prost(body, field_names::DEFAULT_RESOURCE_PREFIX)
}

/// Reusable OTLP projected decoder for benchmarks.
///
/// Holds a `ColumnarBatchBuilder` with recycled column capacity across batches.
/// Use [`ProjectedOtlpDecoder::decode_view_bytes`] in a loop to measure
/// steady-state allocation behaviour rather than first-batch overhead.
#[cfg(any(feature = "otlp-research", test))]
#[doc(hidden)]
pub use projection::ProjectedOtlpDecoder;

fn drain_receiver_payloads(
    rx: &mpsc::Receiver<ReceiverPayload>,
    max_drained_payloads: usize,
) -> Vec<InputEvent> {
    let mut events = Vec::with_capacity(max_drained_payloads);
    let mut drained_payloads = 0usize;

    while drained_payloads < max_drained_payloads {
        let Ok(data) = rx.try_recv() else {
            break;
        };
        drained_payloads += 1;
        events.push(InputEvent::Batch {
            batch: data.batch,
            source_id: None,
            accounted_bytes: data.accounted_bytes,
        });
    }

    events
}

#[cfg(test)]
mod poll_tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(value: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))])
            .expect("record batch should build")
    }

    #[test]
    fn drain_respects_limit_and_leaves_remainder_queued() {
        let (tx, rx) = mpsc::sync_channel(8);
        tx.send(ReceiverPayload {
            batch: make_batch(1),
            accounted_bytes: 10,
        })
        .expect("send payload 1");
        tx.send(ReceiverPayload {
            batch: make_batch(2),
            accounted_bytes: 20,
        })
        .expect("send payload 2");
        tx.send(ReceiverPayload {
            batch: make_batch(3),
            accounted_bytes: 30,
        })
        .expect("send payload 3");

        let events = drain_receiver_payloads(&rx, 2);
        assert_eq!(events.len(), 2);

        let remainder = drain_receiver_payloads(&rx, 2);
        assert_eq!(remainder.len(), 1, "one payload should remain queued");
    }

    #[test]
    fn drain_preserves_batch_order_and_accounted_bytes() {
        let (tx, rx) = mpsc::sync_channel(8);
        tx.send(ReceiverPayload {
            batch: make_batch(10),
            accounted_bytes: 100,
        })
        .expect("send payload 1");
        tx.send(ReceiverPayload {
            batch: make_batch(20),
            accounted_bytes: 200,
        })
        .expect("send payload 2");

        let events = drain_receiver_payloads(&rx, 8);
        assert_eq!(events.len(), 2);

        match &events[0] {
            InputEvent::Batch {
                batch,
                accounted_bytes,
                ..
            } => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(*accounted_bytes, 100);
            }
            _ => panic!("expected first batch event"),
        }
        match &events[1] {
            InputEvent::Batch {
                batch,
                accounted_bytes,
                ..
            } => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(*accounted_bytes, 200);
            }
            _ => panic!("expected second batch event"),
        }
    }
}
