//! OTLP HTTP receiver input source.
//!
//! Listens for OTLP ExportLogsServiceRequest via HTTP POST, decodes the
//! protobuf or JSON payload, and produces structured Arrow record batches.
//!
//! Endpoint: POST /v1/logs (protobuf or JSON)

mod convert;
mod decode;
mod server;
#[cfg(test)]
mod tests;
#[cfg(test)]
use convert::*;
use decode::decode_otlp_logs_to_batch;
#[cfg(test)]
use decode::*;

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};

use arrow::record_batch::RecordBatch;
use axum::routing::post;
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};
use logfwd_types::field_names;
use tokio::sync::oneshot;

use crate::InputError;
use crate::background_http_task::BackgroundHttpTask;
use crate::input::{InputEvent, InputSource};

const CHANNEL_BOUND: usize = 4096;

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
    stats: Option<Arc<ComponentStats>>,
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
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| io::Error::other(format!("OTLP receiver bind {addr}: {e}")))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!("OTLP receiver set_nonblocking {bound_addr}: {e}"))
        })?;

        let (tx, rx) = mpsc::sync_channel(capacity);
        let is_running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        let state = Arc::new(OtlpServerState {
            tx,
            is_running: Arc::clone(&is_running),
            health: Arc::clone(&health),
            resource_prefix,
            stats,
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
        let mut batches = Vec::new();

        while let Ok(data) = rx.try_recv() {
            batches.push((data.batch, data.accounted_bytes));
        }

        let events: Vec<InputEvent> = batches
            .into_iter()
            .map(|(batch, accounted_bytes)| InputEvent::Batch {
                batch,
                source_id: None,
                accounted_bytes,
            })
            .collect();
        Ok(events)
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
