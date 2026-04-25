//! Input pipeline: I/O → CPU worker separation.
//!
//! Every input gets two dedicated OS threads:
//! - **I/O worker**: polls the input source, accumulates bytes, sends chunks
//! - **CPU worker**: scans bytes into RecordBatch, runs SQL transform
//!
//! The two workers are connected by a bounded channel (capacity 4). Backpressure
//! cascades: output pool blocks → pipeline blocks → CPU worker blocks → I/O
//! worker blocks on its source.
//!
//! # Architecture
//!
//! ```text
//! Per input:
//! ┌──────────┐  bounded(4)  ┌──────────┐  ProcessedBatch
//! │ I/O      │─────────────▶│ CPU      │──────────────────────▶ pipeline rx
//! │ Worker   │              │ Worker   │
//! │ poll()   │              │ scan()   │
//! │ accum()  │              │ sql()    │
//! └──────────┘              └──────────┘
//!  OS thread                 OS thread
//! ```

#[cfg(not(feature = "turmoil"))]
use std::sync::Arc;
#[cfg(not(feature = "turmoil"))]
use std::time::Duration;

#[cfg(not(feature = "turmoil"))]
use tokio::sync::mpsc;
#[cfg(not(feature = "turmoil"))]
use tokio_util::sync::CancellationToken;

#[cfg(not(feature = "turmoil"))]
use ffwd_diagnostics::diagnostics::PipelineMetrics;

#[cfg(not(feature = "turmoil"))]
use super::cpu_worker::cpu_worker_loop;
#[cfg(not(feature = "turmoil"))]
use super::io_worker::{IoWorkItem, io_worker_loop};
#[cfg(not(feature = "turmoil"))]
use super::{IngestState, SourcePipeline};

/// Capacity of the bounded channel between I/O worker and CPU worker.
#[cfg(not(feature = "turmoil"))]
const IO_CPU_CHANNEL_CAPACITY: usize = 4;

// ---------------------------------------------------------------------------
// InputPipelineManager — spawns and joins I/O + CPU workers
// ---------------------------------------------------------------------------

/// Manages the I/O and CPU worker threads for all pipeline inputs.
///
/// Each input gets two OS threads connected by a bounded channel:
/// - I/O worker: polls source, accumulates bytes, sends `RawBatch`
/// - CPU worker: scans, SQL transforms, sends `ProcessedBatch`
///
/// Shutdown cascade: shutdown token → I/O workers exit → drop io_tx →
/// CPU workers drain remaining chunks → CPU workers exit → drop pipeline_tx →
/// pipeline's rx.recv() returns None.
#[cfg(not(feature = "turmoil"))]
pub(crate) struct InputPipelineManager {
    io_handles: Vec<std::thread::JoinHandle<()>>,
    cpu_handles: Vec<std::thread::JoinHandle<()>>,
}

#[cfg(not(feature = "turmoil"))]
impl InputPipelineManager {
    /// Spawn I/O + CPU worker pairs for each input.
    ///
    /// Consumes `inputs` and `transforms` — they're moved to their
    /// respective worker threads.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn(
        inputs: Vec<IngestState>,
        transforms: Vec<SourcePipeline>,
        pipeline_tx: mpsc::Sender<super::ProcessedBatch>,
        metrics: Arc<PipelineMetrics>,
        shutdown: CancellationToken,
        batch_target_bytes: usize,
        batch_timeout: Duration,
        poll_interval: Duration,
    ) -> Self {
        assert_eq!(
            inputs.len(),
            transforms.len(),
            "inputs ({}) and transforms ({}) must match",
            inputs.len(),
            transforms.len(),
        );

        let mut io_handles = Vec::with_capacity(inputs.len());
        let mut cpu_handles = Vec::with_capacity(inputs.len());

        for (idx, (input, transform)) in inputs.into_iter().zip(transforms).enumerate() {
            let (io_tx, io_rx) = mpsc::channel::<IoWorkItem>(IO_CPU_CHANNEL_CAPACITY);
            let source_metadata_plan = transform.source_metadata_plan;
            let input_name: Arc<str> = Arc::from(transform.input_name.as_str());

            // Spawn I/O worker.
            let shutdown_io = shutdown.clone();
            let metrics_io = Arc::clone(&metrics);
            io_handles.push(std::thread::spawn(move || {
                io_worker_loop(
                    input,
                    input_name,
                    io_tx,
                    metrics_io,
                    shutdown_io,
                    batch_target_bytes,
                    batch_timeout,
                    poll_interval,
                    idx,
                    source_metadata_plan,
                );
            }));

            // Spawn CPU worker.
            let pipeline_tx = pipeline_tx.clone();
            let metrics_cpu = Arc::clone(&metrics);
            cpu_handles.push(std::thread::spawn(move || {
                cpu_worker_loop(io_rx, pipeline_tx, transform, metrics_cpu);
            }));
        }

        Self {
            io_handles,
            cpu_handles,
        }
    }

    /// Join all worker threads. Call after the pipeline channel is fully drained.
    ///
    /// Joins CPU workers first (they exit when I/O workers drop their senders),
    /// then I/O workers.
    pub(super) fn join(self) {
        for h in self.cpu_handles {
            if let Err(e) = h.join() {
                tracing::error!(error = ?e, "pipeline: cpu worker panicked");
            }
        }
        for h in self.io_handles {
            if let Err(e) = h.join() {
                tracing::error!(error = ?e, "pipeline: io worker panicked");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(feature = "turmoil")))]
#[path = "input_pipeline_tests.rs"]
mod tests;
