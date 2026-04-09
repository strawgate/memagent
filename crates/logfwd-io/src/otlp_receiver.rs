//! OTLP HTTP receiver input source.
//!
//! Listens for OTLP ExportLogsServiceRequest via HTTP POST, decodes the
//! protobuf, and produces JSON lines that the scanner can process.
//!
//! Endpoint: POST /v1/logs (protobuf or JSON)
//!
//! This replaces the hand-rolled `--blackhole` with a proper pipeline input.

mod convert;
mod decode;
mod server;
#[cfg(test)]
mod tests;
#[cfg(test)]
use convert::*;
#[cfg(test)]
use decode::*;

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};

use arrow::record_batch::RecordBatch;
use axum::routing::post;
use tokio::sync::oneshot;

use crate::background_http_task::BackgroundHttpTask;
use crate::diagnostics::ComponentStats;
use crate::input::{InputEvent, InputSource};
use logfwd_types::diagnostics::ComponentHealth;

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

#[derive(Clone, Copy)]
enum ReceiverMode {
    JsonLines,
    StructuredBatch,
}

enum ReceiverPayload {
    JsonLines {
        lines: Vec<u8>,
        accounted_bytes: u64,
    },
    Batch {
        batch: RecordBatch,
        accounted_bytes: u64,
    },
}

struct OtlpServerState {
    tx: mpsc::SyncSender<ReceiverPayload>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    mode: ReceiverMode,
    stats: Option<Arc<ComponentStats>>,
}

impl OtlpReceiverInput {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4318").
    /// Spawns a background thread to handle requests.
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            CHANNEL_BOUND,
            ReceiverMode::JsonLines,
            None,
        )
    }

    /// Like [`Self::new`], but wires input diagnostics into receiver-side
    /// transport and decode failures.
    pub fn new_with_stats(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            CHANNEL_BOUND,
            ReceiverMode::JsonLines,
            Some(stats),
        )
    }

    /// Like [`Self::new`], but emits structured batches directly instead of
    /// JSON lines.
    pub fn new_structured(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            CHANNEL_BOUND,
            ReceiverMode::StructuredBatch,
            None,
        )
    }

    /// Like [`Self::new_structured`], but wires input diagnostics into
    /// receiver-side transport and decode failures.
    pub fn new_structured_with_stats(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            CHANNEL_BOUND,
            ReceiverMode::StructuredBatch,
            Some(stats),
        )
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    #[cfg(test)]
    fn new_with_capacity(name: impl Into<String>, addr: &str, capacity: usize) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(name, addr, capacity, ReceiverMode::JsonLines, None)
    }

    #[cfg(test)]
    fn new_with_capacity_and_stats(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            capacity,
            ReceiverMode::JsonLines,
            Some(stats),
        )
    }

    fn new_with_capacity_mode_and_stats(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        mode: ReceiverMode,
        stats: Option<Arc<ComponentStats>>,
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
            mode,
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
        let mut all = Vec::new();
        let mut all_accounted_bytes = 0_u64;
        let mut batches = Vec::new();

        while let Ok(data) = rx.try_recv() {
            match data {
                ReceiverPayload::JsonLines {
                    lines,
                    accounted_bytes,
                } => {
                    all.extend_from_slice(&lines);
                    all_accounted_bytes = all_accounted_bytes.saturating_add(accounted_bytes);
                }
                ReceiverPayload::Batch {
                    batch,
                    accounted_bytes,
                } => batches.push((batch, accounted_bytes)),
            }
        }

        let mut events = Vec::with_capacity((!all.is_empty()) as usize + batches.len());
        if !all.is_empty() {
            events.push(InputEvent::Data {
                bytes: all,
                source_id: None,
                accounted_bytes: all_accounted_bytes,
            });
        }
        events.extend(
            batches
                .into_iter()
                .map(|(batch, accounted_bytes)| InputEvent::Batch {
                    batch,
                    source_id: None,
                    accounted_bytes,
                }),
        );
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

impl ReceiverPayload {
    fn is_empty(&self) -> bool {
        match self {
            ReceiverPayload::JsonLines { lines, .. } => lines.is_empty(),
            ReceiverPayload::Batch { batch, .. } => batch.num_rows() == 0,
        }
    }
}
