//! High-performance S3 (and S3-compatible) object storage input.
//!
//! # Discovery modes
//!
//! - **SQS mode** (`sqs_queue_url` set): receives `ObjectCreated` event
//!   notifications via long-polling. Deletes messages after successful
//!   processing; on failure the message becomes visible again after the
//!   visibility timeout.
//! - **Prefix scan mode** (`sqs_queue_url` absent): periodically calls
//!   `ListObjectsV2` with an optional `prefix` and advances a `start_after`
//!   cursor to avoid re-processing objects.
//!
//! # Streaming pipeline
//!
//! Data flows through the pipeline without buffering entire objects:
//!
//! - **Uncompressed objects**: parallel range-GET fetches (default 1 MiB parts,
//!   16 concurrent). Each part is fully downloaded inside a
//!   `futures::stream::buffered` future, then chunked into ~256 KiB pieces for
//!   ordered delivery. Smaller parts + high concurrency hides S3 first-byte
//!   latency (~100-200ms).
//! - **Compressed objects**: single GET stream → streaming async decompression
//!   (gzip/zstd via `async-compression`, snappy via buffered bridge).
//!
//! All paths emit ~256 KiB chunks to the output channel as data arrives.
//! Peak memory per concurrent object: ~max_fetches × part_size (16 MiB default).
//!
//! # Integration
//!
//! `S3Input` implements [`crate::input::InputSource`] and is wired into the
//! pipeline via `build_input_state` in `logfwd-runtime`.

pub mod client;
pub mod decompress;
pub mod sqs;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};

use logfwd_types::diagnostics::ComponentHealth;
use logfwd_types::pipeline::SourceId;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, error, warn};

use crate::input::{InputEvent, InputSource};
use client::S3Client;
use decompress::{Compression, detect_compression};
use sqs::SqsClient;

// ── Constants ──────────────────────────────────────────────────────────────

/// Bounded output channel capacity.  Each slot holds a ~256 KiB chunk.
const CHANNEL_BOUND: usize = 1024;
/// Maximum chunks drained from the output channel per `poll()` call.
const MAX_DRAIN_PER_POLL: usize = 256;
/// Default range-GET part size: 1 MiB.
///
/// Smaller parts enable higher parallelism which hides S3 first-byte latency
/// (~100-200ms). Benchmarks show 1 MiB × 16 concurrent fetches dramatically
/// outperforms 8 MiB × 8 under realistic latency.
pub const DEFAULT_PART_SIZE: u64 = 1024 * 1024;
/// Default max concurrent range GETs *per object*.
pub const DEFAULT_MAX_CONCURRENT_FETCHES: usize = 16;
/// Default max objects fetched concurrently.
pub const DEFAULT_MAX_CONCURRENT_OBJECTS: usize = 4;
/// Default SQS visibility timeout in seconds.
pub const DEFAULT_VISIBILITY_TIMEOUT_SECS: u32 = 300;
/// Default `ListObjectsV2` polling interval in milliseconds.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 5_000;
/// Default AWS region.
const DEFAULT_REGION: &str = "us-east-1";
/// Output chunk size: ~256 KiB.
const OUTPUT_CHUNK_SIZE: usize = 256 * 1024;
/// Minimum SQS visibility timeout (seconds) enforced at config validation.
const MIN_SQS_VISIBILITY_TIMEOUT_SECS: u32 = 30;

/// Derive a stable `SourceId` from an S3 object key.
///
/// Uses FNV-1a 64-bit hash (no extra deps) for fast, deterministic hashing.
fn source_id_from_key(key: &str) -> SourceId {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    SourceId(hash)
}

// ── Settings struct ────────────────────────────────────────────────────────

/// Runtime settings for `S3Input`, translated from the YAML config by the
/// runtime crate. Avoids a hard dependency on `logfwd-config` inside `logfwd-io`.
#[derive(Clone)]
pub struct S3InputSettings {
    /// S3 bucket name.
    pub bucket: String,
    /// AWS region. Default: `"us-east-1"`.
    pub region: String,
    /// Override endpoint URL (path-style when set).
    pub endpoint: Option<String>,
    /// Key prefix filter.
    pub prefix: Option<String>,
    /// SQS queue URL for event-driven discovery.
    pub sqs_queue_url: Option<String>,
    /// `ListObjectsV2` start-after key for resumable prefix scanning.
    pub start_after: Option<String>,
    /// AWS access key ID.
    pub access_key_id: String,
    /// AWS secret access key.
    pub secret_access_key: String,
    /// AWS session token for temporary credentials.
    pub session_token: Option<String>,
    /// Range-GET part size in bytes.
    pub part_size_bytes: u64,
    /// Max concurrent range GET tasks per object.
    pub max_concurrent_fetches: usize,
    /// Max objects being fetched simultaneously.
    pub max_concurrent_objects: usize,
    /// SQS visibility timeout in seconds.
    pub visibility_timeout_secs: u32,
    /// Explicit compression override, or `None` for auto-detection.
    pub compression_override: Option<Compression>,
    /// `ListObjectsV2` polling interval in milliseconds.
    pub poll_interval_ms: u64,
    /// Whether to track current object keys for `InputSource::source_paths()` snapshots.
    pub should_expose_source_paths: bool,
}

impl std::fmt::Debug for S3InputSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3InputSettings")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("prefix", &self.prefix)
            .field("sqs_queue_url", &self.sqs_queue_url)
            .field("start_after", &self.start_after)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("part_size_bytes", &self.part_size_bytes)
            .field("max_concurrent_fetches", &self.max_concurrent_fetches)
            .field("max_concurrent_objects", &self.max_concurrent_objects)
            .field("visibility_timeout_secs", &self.visibility_timeout_secs)
            .field("compression_override", &self.compression_override)
            .field("poll_interval_ms", &self.poll_interval_ms)
            .field(
                "should_expose_source_paths",
                &self.should_expose_source_paths,
            )
            .finish()
    }
}

impl S3InputSettings {
    /// Resolve settings from optional config fields, falling back to env vars
    /// and built-in defaults.
    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        bucket: String,
        region: Option<String>,
        endpoint: Option<String>,
        prefix: Option<String>,
        sqs_queue_url: Option<String>,
        start_after: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        part_size_bytes: Option<u64>,
        max_concurrent_fetches: Option<usize>,
        max_concurrent_objects: Option<usize>,
        visibility_timeout_secs: Option<u32>,
        compression_override: Option<Compression>,
        poll_interval_ms: Option<u64>,
        should_expose_source_paths: bool,
    ) -> Result<Self, String> {
        let access_key_id = access_key_id
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
            .unwrap_or_default();
        let secret_access_key = secret_access_key
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
            .unwrap_or_default();

        if access_key_id.is_empty() {
            return Err(
                "s3.access_key_id is required (or set AWS_ACCESS_KEY_ID env var)".to_string(),
            );
        }
        if secret_access_key.is_empty() {
            return Err(
                "s3.secret_access_key is required (or set AWS_SECRET_ACCESS_KEY env var)"
                    .to_string(),
            );
        }

        Ok(Self {
            bucket,
            region: region
                .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
                .unwrap_or_else(|| DEFAULT_REGION.to_string()),
            endpoint,
            prefix,
            sqs_queue_url,
            start_after,
            access_key_id,
            secret_access_key,
            session_token: session_token.or_else(|| std::env::var("AWS_SESSION_TOKEN").ok()),
            part_size_bytes: part_size_bytes
                .unwrap_or(DEFAULT_PART_SIZE)
                .max(1024 * 1024),
            max_concurrent_fetches: max_concurrent_fetches
                .unwrap_or(DEFAULT_MAX_CONCURRENT_FETCHES)
                .max(1),
            max_concurrent_objects: max_concurrent_objects
                .unwrap_or(DEFAULT_MAX_CONCURRENT_OBJECTS)
                .max(1),
            visibility_timeout_secs: visibility_timeout_secs
                .unwrap_or(DEFAULT_VISIBILITY_TIMEOUT_SECS)
                .max(MIN_SQS_VISIBILITY_TIMEOUT_SECS),
            compression_override,
            poll_interval_ms: poll_interval_ms.unwrap_or(DEFAULT_POLL_INTERVAL_MS).max(1),
            should_expose_source_paths,
        })
    }
}

// ── Public types ───────────────────────────────────────────────────────────

/// S3 input source that tails objects from an S3-compatible bucket.
pub struct S3Input {
    name: String,
    rx: Option<mpsc::Receiver<ChunkPayload>>,
    health: Arc<AtomicU8>,
    is_running: Arc<AtomicBool>,
    thread_handle: Option<std::thread::JoinHandle<()>>,
    should_expose_source_paths: bool,
    active_source_paths: HashMap<SourceId, PathBuf>,
    poll_source_paths: HashMap<SourceId, PathBuf>,
}

// ── Internal types ─────────────────────────────────────────────────────────

struct ChunkPayload {
    bytes: Vec<u8>,
    accounted_bytes: u64,
    /// S3 key hashed to SourceId so FramedInput maintains per-object parser state.
    source_id: SourceId,
    /// S3 object key for source metadata. Present on the first chunk of an
    /// object when public source path snapshots are configured.
    source_path: Option<String>,
    /// When true, this signals end-of-file for the source_id so that
    /// FramedInput flushes any trailing partial line.
    is_eof: bool,
}

#[derive(Clone, Copy)]
struct FetchOptions {
    part_size: u64,
    max_fetches: usize,
    compression_override: Option<Compression>,
    should_expose_source_paths: bool,
}

/// A unit of work: one S3 object to fetch.
struct ObjectWork {
    key: String,
    size: u64,
    /// Shared tracker for the parent SQS message, if in SQS mode.
    /// Deletion happens when all records from the message are processed.
    message_tracker: Option<Arc<MessageTracker>>,
}

/// Tracks completion of all records within a single SQS message.
/// Only deletes the message when all records have been successfully processed.
struct MessageTracker {
    receipt_handle: String,
    /// Total number of records in this message (set once at creation).
    total: usize,
    /// Remaining records not yet completed (success or failure). Starts at `total`.
    remaining: std::sync::atomic::AtomicUsize,
    /// Number of records that completed successfully.
    successes: std::sync::atomic::AtomicUsize,
}

/// Completion signal for list-mode cursor tracking.
struct KeyCompletion {
    key: String,
    success: bool,
}

// ── Constructor ────────────────────────────────────────────────────────────

impl S3Input {
    /// Create a new `S3Input` from resolved settings.
    ///
    /// Spawns a background thread hosting a multi-thread Tokio runtime that
    /// runs discovery and fetch tasks.
    pub fn new(name: impl Into<String>, settings: S3InputSettings) -> io::Result<Self> {
        let name = name.into();

        let max_idle = settings.max_concurrent_fetches * settings.max_concurrent_objects;
        let s3_client = Arc::new(S3Client::new(
            &settings.bucket,
            &settings.region,
            settings.endpoint.as_deref(),
            settings.access_key_id.clone(),
            settings.secret_access_key.clone(),
            settings.session_token.clone(),
            max_idle,
        )?);

        let sqs_client: Option<Arc<SqsClient>> = if let Some(ref queue_url) = settings.sqs_queue_url
        {
            // Try to auto-detect the SQS region from the queue URL first (handles
            // cross-region setups where bucket and queue are in different regions).
            // Fall back to the configured S3 region for custom endpoints (e.g.,
            // LocalStack, MinIO) where URL parsing cannot determine the region.
            let sqs_region = sqs::extract_region_from_sqs_url(queue_url)
                .unwrap_or_else(|| settings.region.clone());
            Some(Arc::new(SqsClient::new(
                queue_url.clone(),
                Some(sqs_region),
                settings.access_key_id.clone(),
                settings.secret_access_key.clone(),
                settings.session_token.clone(),
            )?))
        } else {
            None
        };

        let (tx, rx) = mpsc::sync_channel(CHANNEL_BOUND);
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        let is_running = Arc::new(AtomicBool::new(true));

        let thread_name = format!("s3-input-{name}");
        let health_bg = Arc::clone(&health);
        let is_running_bg = Arc::clone(&is_running);
        let name_bg = name.clone();

        let part_size = settings.part_size_bytes;
        let max_fetches = settings.max_concurrent_fetches;
        let max_objects = settings.max_concurrent_objects;
        let visibility_timeout = settings.visibility_timeout_secs;
        let poll_interval_ms = settings.poll_interval_ms;
        let compression_override = settings.compression_override;
        let should_expose_source_paths = settings.should_expose_source_paths;
        let bucket = settings.bucket.clone();
        let prefix = settings.prefix.clone();
        let start_after_init = settings.start_after.clone();
        let sqs_queue_url_is_set = settings.sqs_queue_url.is_some();

        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        error!(
                            name = %name_bg,
                            error = %e,
                            "s3 input: failed to build tokio runtime"
                        );
                        health_bg.store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                        return;
                    }
                };

                rt.block_on(async move {
                    // Work channel: discovery → orchestrator.
                    let (work_tx, work_rx) =
                        tokio::sync::mpsc::channel::<ObjectWork>(max_objects * 4);

                    let semaphore = Arc::new(Semaphore::new(max_objects));

                    // Shared set of in-progress SQS receipt handles for heartbeats.
                    let in_progress: Arc<tokio::sync::Mutex<Vec<String>>> =
                        Arc::new(tokio::sync::Mutex::new(Vec::new()));

                    // Spawn discovery task.
                    // Completed-key channel for list-mode cursor advancement.
                    let (completed_tx, completed_rx) =
                        tokio::sync::mpsc::channel::<KeyCompletion>(max_objects * 4);

                    let (sqs_for_orch, completed_tx_for_orch, discovery_handle) =
                        if sqs_queue_url_is_set {
                            let sqs = sqs_client.expect("sqs_client set when queue_url set");
                            let sqs_orch = Arc::clone(&sqs);
                            let is_running_d = Arc::clone(&is_running_bg);
                            let health_d = Arc::clone(&health_bg);
                            let work_tx_d = work_tx.clone();
                            let name_d = name_bg.clone();
                            let in_progress_d = Arc::clone(&in_progress);
                            let handle = tokio::spawn(async move {
                                run_sqs_discovery(
                                    sqs,
                                    in_progress_d,
                                    work_tx_d,
                                    is_running_d,
                                    health_d,
                                    name_d,
                                    visibility_timeout,
                                )
                                .await;
                            });
                            // SQS mode doesn't need cursor advancement.
                            drop(completed_rx);
                            (Some(sqs_orch), None, handle)
                        } else {
                            let s3 = Arc::clone(&s3_client);
                            let is_running_d = Arc::clone(&is_running_bg);
                            let health_d = Arc::clone(&health_bg);
                            let work_tx_d = work_tx.clone();
                            let name_d = name_bg.clone();
                            let handle = tokio::spawn(async move {
                                run_list_discovery(
                                    s3,
                                    bucket,
                                    prefix,
                                    start_after_init,
                                    work_tx_d,
                                    completed_rx,
                                    is_running_d,
                                    health_d,
                                    name_d,
                                    poll_interval_ms,
                                )
                                .await;
                            });
                            (None, Some(completed_tx), handle)
                        };
                    // Drop our copy so the orchestrator exits when discovery stops.
                    drop(work_tx);

                    run_orchestrator(
                        s3_client,
                        sqs_for_orch,
                        in_progress,
                        completed_tx_for_orch,
                        work_rx,
                        tx,
                        semaphore,
                        part_size,
                        max_fetches,
                        compression_override,
                        should_expose_source_paths,
                        Arc::clone(&is_running_bg),
                        Arc::clone(&health_bg),
                        name_bg.clone(),
                    )
                    .await;

                    discovery_handle.abort();
                });

                // Bounded shutdown: abort any lingering fetch tasks (e.g.
                // stuck on a 300s reqwest timeout) instead of blocking
                // indefinitely when the runtime drops.
                rt.shutdown_timeout(std::time::Duration::from_secs(5));
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name,
            rx: Some(rx),
            health,
            is_running,
            thread_handle: Some(handle),
            should_expose_source_paths,
            active_source_paths: HashMap::new(),
            poll_source_paths: HashMap::new(),
        })
    }
}

// ── InputSource implementation ─────────────────────────────────────────────

impl InputSource for S3Input {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut events = Vec::with_capacity(MAX_DRAIN_PER_POLL);
        let mut drained = 0usize;
        self.poll_source_paths.clear();
        let Some(ref rx) = self.rx else {
            return Ok(events);
        };
        while drained < MAX_DRAIN_PER_POLL {
            let Ok(payload) = rx.try_recv() else {
                break;
            };
            drained += 1;
            if self.should_expose_source_paths {
                if let Some(source_path) = payload.source_path {
                    self.active_source_paths
                        .entry(payload.source_id)
                        .or_insert_with(|| PathBuf::from(source_path));
                }
                if (!payload.bytes.is_empty() || payload.is_eof)
                    && let Some(source_path) = self.active_source_paths.get(&payload.source_id)
                {
                    self.poll_source_paths
                        .entry(payload.source_id)
                        .or_insert_with(|| source_path.clone());
                }
            }
            // Only emit Data when there are actual bytes — EOF-only markers
            // carry empty bytes and sending them would cause the checkpoint
            // tracker to panic on `n_bytes == 0`.
            if !payload.bytes.is_empty() {
                events.push(InputEvent::Data {
                    bytes: payload.bytes,
                    source_id: Some(payload.source_id),
                    accounted_bytes: payload.accounted_bytes,
                });
            }
            if payload.is_eof {
                events.push(InputEvent::EndOfFile {
                    source_id: Some(payload.source_id),
                });
                self.active_source_paths.remove(&payload.source_id);
            }
        }
        Ok(events)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::from_repr(self.health.load(Ordering::Relaxed))
    }

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        self.poll_source_paths
            .iter()
            .map(|(source_id, path)| (*source_id, path.clone()))
            .collect()
    }
}

impl Drop for S3Input {
    fn drop(&mut self) {
        self.health
            .store(ComponentHealth::Stopping.as_repr(), Ordering::Relaxed);
        self.is_running.store(false, Ordering::Relaxed);
        // Drop the receiver first so that any background senders blocked on a
        // full channel will observe the disconnect and unblock, preventing a
        // deadlock when we join the worker thread below.
        drop(self.rx.take());
        if let Some(handle) = self.thread_handle.take() {
            // The background thread runs shutdown_timeout(5s) on its runtime,
            // so it should exit within ~6s. Use a timed join to avoid blocking
            // the pipeline shutdown indefinitely.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                if handle.is_finished() {
                    let _ = handle.join();
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    // Thread is stuck — leak it rather than block shutdown.
                    std::mem::forget(handle);
                    warn!("S3 input background thread did not exit within deadline, leaking");
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
}

// ── Background tasks ───────────────────────────────────────────────────────

/// SQS-driven discovery: long-polls for `ObjectCreated` events.
async fn run_sqs_discovery(
    sqs: Arc<SqsClient>,
    in_progress: Arc<tokio::sync::Mutex<Vec<String>>>,
    work_tx: tokio::sync::mpsc::Sender<ObjectWork>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    name: String,
    visibility_timeout: u32,
) {
    // Spawn heartbeat task.
    // Extend visibility at 40% of the timeout so we have a comfortable margin
    // before messages become visible again.
    let heartbeat_secs = (u64::from(visibility_timeout) * 2 / 5).max(5);
    let sqs_hb = Arc::clone(&sqs);
    let in_progress_hb = Arc::clone(&in_progress);
    let is_running_hb = Arc::clone(&is_running);
    let name_hb = name.clone();
    tokio::spawn(async move {
        let interval = std::time::Duration::from_secs(heartbeat_secs);
        loop {
            tokio::time::sleep(interval).await;
            if !is_running_hb.load(Ordering::Relaxed) {
                break;
            }
            let handles: Vec<String> = {
                let guard = in_progress_hb.lock().await;
                guard.clone()
            };
            for handle in &handles {
                if let Err(e) = sqs_hb
                    .change_message_visibility(handle, visibility_timeout)
                    .await
                {
                    warn!(name = %name_hb, error = %e, "SQS visibility extension failed");
                }
            }
        }
    });

    while is_running.load(Ordering::Relaxed) {
        let messages = match sqs.receive_messages(10, 20, visibility_timeout).await {
            Ok(msgs) => {
                health.store(ComponentHealth::Healthy.as_repr(), Ordering::Relaxed);
                msgs
            }
            Err(e) => {
                warn!(name = %name, error = %e, "SQS receive failed");
                health.store(ComponentHealth::Degraded.as_repr(), Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        for msg in messages {
            // If the message has no actionable S3 records (e.g. non-ObjectCreated
            // events, test notifications, or malformed payloads), delete it
            // immediately to prevent infinite redelivery.
            if msg.records.is_empty() {
                if let Err(e) = sqs.delete_message(&msg.receipt_handle).await {
                    warn!(name = %name, error = %e, "SQS delete empty message failed");
                }
                continue;
            }

            // Register receipt handle for heartbeats before dispatching.
            {
                let mut guard = in_progress.lock().await;
                guard.push(msg.receipt_handle.clone());
            }

            // Create a shared tracker for all records in this SQS message.
            // The message is only deleted when all records are successfully processed.
            let record_count = msg.records.len();
            let tracker = Arc::new(MessageTracker {
                receipt_handle: msg.receipt_handle.clone(),
                total: record_count,
                remaining: std::sync::atomic::AtomicUsize::new(record_count),
                successes: std::sync::atomic::AtomicUsize::new(0),
            });

            for record in msg.records {
                let work = ObjectWork {
                    key: record.key,
                    size: record.size,
                    message_tracker: Some(Arc::clone(&tracker)),
                };
                if work_tx.send(work).await.is_err() {
                    // Orchestrator shut down.
                    return;
                }
            }

            // Do not delete the SQS message here — the orchestrator will
            // delete it after the object is successfully fetched and dispatched.
            // If the fetch fails, the message reappears after visibility timeout.
        }
    }
}

/// `ListObjectsV2`-driven discovery: scans the bucket prefix periodically.
#[allow(clippy::too_many_arguments)]
async fn run_list_discovery(
    s3: Arc<S3Client>,
    _bucket: String,
    prefix: Option<String>,
    start_after_init: Option<String>,
    work_tx: tokio::sync::mpsc::Sender<ObjectWork>,
    mut completed_rx: tokio::sync::mpsc::Receiver<KeyCompletion>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    name: String,
    poll_interval_ms: u64,
) {
    /// Maximum times a key can fail before being skipped.
    const MAX_KEY_FAILURES: u32 = 5;

    let mut start_after: Option<String> = start_after_init;
    // Track dispatched keys in S3 sort order and completed keys.
    // Cursor only advances past the contiguous prefix of completed keys.
    let mut dispatched: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut dispatched_set: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut completed_set: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Deduplicate: skip keys that are still in flight from a previous poll cycle.
    // Keys stay in in_flight until the watermark cursor advances past them,
    // preventing re-discovery of completed-but-not-yet-cursored keys.
    let mut in_flight: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Per-key failure counter. Keys exceeding MAX_KEY_FAILURES are promoted
    // to completed_set so the watermark cursor can advance past them.
    let mut failure_counts: HashMap<String, u32> = HashMap::new();

    while is_running.load(Ordering::Relaxed) {
        let mut continuation: Option<String> = None;

        loop {
            if !is_running.load(Ordering::Relaxed) {
                return;
            }

            let result = s3
                .list_objects_v2(
                    prefix.as_deref(),
                    if continuation.is_none() {
                        start_after.as_deref()
                    } else {
                        None
                    },
                    continuation.as_deref(),
                    1000,
                )
                .await;

            match result {
                Err(e) => {
                    warn!(name = %name, error = %e, "S3 list objects failed");
                    health.store(ComponentHealth::Degraded.as_repr(), Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    break;
                }
                Ok((objects, next_token)) => {
                    for obj in &objects {
                        // Skip keys still being processed or awaiting cursor advancement.
                        if in_flight.contains(&obj.key) {
                            continue;
                        }

                        // A failed key remains in dispatched (for watermark tracking)
                        // but was removed from in_flight (to reach here). Re-add to
                        // in_flight and re-dispatch the work without duplicating the
                        // dispatched entry.
                        if dispatched_set.contains(&obj.key) {
                            in_flight.insert(obj.key.clone());
                        } else {
                            dispatched.push_back(obj.key.clone());
                            dispatched_set.insert(obj.key.clone());
                            in_flight.insert(obj.key.clone());
                        }

                        // Acquire a send permit, interleaving with completion
                        // draining to prevent deadlock when the work channel
                        // is full and completions are pending.
                        let permit = loop {
                            while let Ok(kc) = completed_rx.try_recv() {
                                if kc.success {
                                    completed_set.insert(kc.key);
                                } else {
                                    let count = failure_counts.entry(kc.key.clone()).or_insert(0);
                                    *count += 1;
                                    if *count >= MAX_KEY_FAILURES {
                                        warn!(name = %name, key = %kc.key, failures = *count,
                                              "S3 key exceeded max retries, skipping");
                                        completed_set.insert(kc.key.clone());
                                        // Leave in in_flight so re-discovery skips it
                                        // until the watermark advances past it.
                                    } else {
                                        in_flight.remove(&kc.key);
                                    }
                                }
                            }

                            tokio::select! {
                                result = work_tx.reserve() => {
                                    match result {
                                        Ok(p) => break p,
                                        Err(_) => return, // orchestrator shut down
                                    }
                                }
                                Some(kc) = completed_rx.recv() => {
                                    if kc.success {
                                        completed_set.insert(kc.key);
                                    } else {
                                        let count = failure_counts.entry(kc.key.clone()).or_insert(0);
                                        *count += 1;
                                        if *count >= MAX_KEY_FAILURES {
                                            warn!(name = %name, key = %kc.key, failures = *count,
                                                  "S3 key exceeded max retries, skipping");
                                            completed_set.insert(kc.key.clone());
                                        } else {
                                            in_flight.remove(&kc.key);
                                        }
                                    }
                                }
                            }
                        };

                        permit.send(ObjectWork {
                            key: obj.key.clone(),
                            size: obj.size,
                            message_tracker: None,
                        });
                    }

                    // Final drain + watermark advance.
                    while let Ok(kc) = completed_rx.try_recv() {
                        if kc.success {
                            completed_set.insert(kc.key);
                        } else {
                            let count = failure_counts.entry(kc.key.clone()).or_insert(0);
                            *count += 1;
                            if *count >= MAX_KEY_FAILURES {
                                warn!(name = %name, key = %kc.key, failures = *count,
                                      "S3 key exceeded max retries, skipping");
                                completed_set.insert(kc.key.clone());
                            } else {
                                in_flight.remove(&kc.key);
                            }
                        }
                    }
                    while let Some(front) = dispatched.front() {
                        if completed_set.contains(front) {
                            let key = dispatched.pop_front().expect("front existed");
                            completed_set.remove(&key);
                            dispatched_set.remove(&key);
                            in_flight.remove(&key);
                            failure_counts.remove(&key);
                            start_after = Some(key);
                        } else {
                            // Stop at the first key that hasn't completed
                            // (successfully or via max-retry skip).
                            break;
                        }
                    }

                    if let Some(token) = next_token {
                        continuation = Some(token);
                    } else {
                        break; // All pages consumed for this cycle.
                    }
                }
            }
        }

        // Wait before next poll cycle.
        if is_running.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_millis(poll_interval_ms)).await;
        }
    }
}

/// Orchestrator: receives work items, acquires semaphore, spawns fetch tasks.
#[allow(clippy::too_many_arguments)]
async fn run_orchestrator(
    s3: Arc<S3Client>,
    sqs: Option<Arc<SqsClient>>,
    in_progress: Arc<tokio::sync::Mutex<Vec<String>>>,
    completed_tx: Option<tokio::sync::mpsc::Sender<KeyCompletion>>,
    mut work_rx: tokio::sync::mpsc::Receiver<ObjectWork>,
    out_tx: mpsc::SyncSender<ChunkPayload>,
    semaphore: Arc<Semaphore>,
    part_size: u64,
    max_fetches: usize,
    compression_override: Option<Compression>,
    should_expose_source_paths: bool,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    name: String,
) {
    /// Maximum fetch attempts per object before giving up.
    const MAX_FETCH_ATTEMPTS: u32 = 3;

    // Deduplication: prevent concurrent fetches of the same key. SQS
    // at-least-once delivery can dispatch duplicates; concurrent fetches
    // share the same SourceId and corrupt FramedInput state.
    let fetching_keys: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> =
        Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));
    let fetch_options = FetchOptions {
        part_size,
        max_fetches,
        compression_override,
        should_expose_source_paths,
    };

    while is_running.load(Ordering::Relaxed) {
        let work = tokio::select! {
            biased;
            () = async {
                // Poll is_running every 500ms so shutdown doesn't block on a
                // long SQS long-poll in the discovery task.
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    if !is_running.load(Ordering::Relaxed) {
                        return;
                    }
                }
            } => break,
            item = work_rx.recv() => match item {
                Some(w) => w,
                None => break, // Discovery shut down.
            },
        };

        // Skip duplicate keys already being fetched concurrently.
        let is_duplicate = {
            let mut guard = fetching_keys.lock().await;
            !guard.insert(work.key.clone())
        };
        if is_duplicate {
            debug!(name = %name, key = %work.key, "skipping duplicate concurrent fetch");
            // Decrement the message tracker so the SQS message can still
            // be deleted when all non-duplicate records complete. Count
            // the skip as a success since the key IS being processed.
            if let Some(tracker) = &work.message_tracker {
                tracker.successes.fetch_add(1, Ordering::AcqRel);
                let prev = tracker.remaining.fetch_sub(1, Ordering::AcqRel);
                if prev == 1 {
                    let should_delete = {
                        let mut guard = in_progress.lock().await;
                        guard.retain(|h| h != &tracker.receipt_handle);
                        let success_count = tracker.successes.load(Ordering::Acquire);
                        success_count == tracker.total
                    };
                    if should_delete
                        && let Some(sqs) = &sqs
                        && let Err(e) = sqs.delete_message(&tracker.receipt_handle).await
                    {
                        warn!(name = %name, error = %e, "SQS delete message failed");
                    }
                }
            }
            continue;
        }

        // Acquire semaphore to limit concurrent object fetches.
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };

        let s3 = Arc::clone(&s3);
        let sqs = sqs.clone();
        let in_progress = Arc::clone(&in_progress);
        let completed_tx = completed_tx.clone();
        let out_tx = out_tx.clone();
        let health = Arc::clone(&health);
        let name = name.clone();
        let fetching_keys = Arc::clone(&fetching_keys);

        tokio::spawn(async move {
            let _permit = permit; // released when task completes

            // Retry with exponential backoff: 1s, 2s, 4s between attempts.
            let mut last_err = None;
            for attempt in 0..MAX_FETCH_ATTEMPTS {
                if attempt > 0 {
                    let delay_ms = 1000 * (1u64 << (attempt - 1));
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                match fetch_object(
                    Arc::clone(&s3),
                    &work.key,
                    work.size,
                    fetch_options,
                    out_tx.clone(),
                )
                .await
                {
                    Ok(()) => {
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            name = %name,
                            key = %work.key,
                            attempt = attempt + 1,
                            max_attempts = MAX_FETCH_ATTEMPTS,
                            error = %e,
                            "S3 fetch object failed"
                        );
                        last_err = Some(e);
                    }
                }
            }

            if last_err.is_some() {
                health.store(ComponentHealth::Degraded.as_repr(), Ordering::Relaxed);

                // Report failed key so list-mode can remove it from in_flight
                // and retry on the next poll cycle.
                if let Some(ref tx) = completed_tx {
                    let _ = tx
                        .send(KeyCompletion {
                            key: work.key.clone(),
                            success: false,
                        })
                        .await;
                }
            } else {
                // Restore health after a successful fetch.
                health.store(ComponentHealth::Healthy.as_repr(), Ordering::Relaxed);

                // Mark this record as successful for SQS tracker.
                if let Some(tracker) = &work.message_tracker {
                    tracker.successes.fetch_add(1, Ordering::AcqRel);
                }

                // Report successfully-processed key for list-mode cursor advancement.
                if let Some(ref tx) = completed_tx {
                    let _ = tx
                        .send(KeyCompletion {
                            key: work.key.clone(),
                            success: true,
                        })
                        .await;
                }
            }

            // Always decrement remaining so heartbeat cleanup happens once
            // all records are done (success or failure). Must happen after the
            // success increment above so the delete check sees the final count.
            if let Some(tracker) = &work.message_tracker {
                let prev = tracker.remaining.fetch_sub(1, Ordering::AcqRel);
                if prev == 1 {
                    // All records from this message have completed — stop heartbeating.
                    // Remove from heartbeat set first, then release lock
                    // before any network I/O to avoid blocking other heartbeats.
                    let should_delete = {
                        let mut guard = in_progress.lock().await;
                        guard.retain(|h| h != &tracker.receipt_handle);
                        // Check if all records succeeded.
                        let success_count = tracker.successes.load(Ordering::Acquire);
                        success_count == tracker.total
                    };
                    // guard dropped — lock is released before awaiting delete.

                    if should_delete
                        && let Some(sqs) = &sqs
                        && let Err(e) = sqs.delete_message(&tracker.receipt_handle).await
                    {
                        warn!(name = %name, error = %e, "SQS delete message failed");
                    }
                }
            }

            // Remove from dedup set so the key can be fetched again if
            // re-dispatched (e.g. SQS redelivery after failure).
            fetching_keys.lock().await.remove(&work.key);
        });
    }
}

/// Fetch a single S3 object via streaming download + streaming decompression,
/// sending ~256 KiB chunks to `out_tx` as they become available.
///
/// For **compressed** objects (or unknown size): single GET → streaming
/// decompression → 256 KiB chunks.
/// For **uncompressed** objects with known size: parallel range-GETs, each
/// part streamed directly as 256 KiB chunks (out-of-order for max throughput;
/// each part gets a unique sub-SourceId so the framer treats them independently).
async fn fetch_object(
    s3: Arc<S3Client>,
    key: &str,
    mut size: u64,
    options: FetchOptions,
    out_tx: mpsc::SyncSender<ChunkPayload>,
) -> io::Result<()> {
    use tokio::io::AsyncReadExt;

    // If size is unknown (0 from SQS notification) or no compression override,
    // issue a HEAD to discover metadata (content-encoding, content-type, size).
    let (content_encoding, content_type) = if options.compression_override.is_none() || size == 0 {
        match s3.head_object_metadata(key).await {
            Ok(meta) => {
                if size == 0 {
                    size = meta.content_length;
                }
                (meta.content_encoding, meta.content_type)
            }
            Err(e) => {
                warn!(key = %key, error = %e, "HEAD failed for compression detection; falling back to key extension");
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    let compression = options.compression_override.unwrap_or_else(|| {
        detect_compression(key, content_encoding.as_deref(), content_type.as_deref())
    });

    // Uncompressed objects with known size: parallel range-GET streaming.
    if compression == Compression::None && size > 0 {
        return fetch_parallel_stream(s3, key, size, options, out_tx).await;
    }

    // Compressed (or unknown-size): single GET → streaming decompress → chunks.
    let resp = s3.get_object_stream(key).await?;
    let async_read = decompress::response_to_async_read(resp);
    let mut reader = decompress::wrap_decompressor(async_read, compression);

    let source_id = source_id_from_key(key);
    let mut buf = vec![0u8; OUTPUT_CHUNK_SIZE];
    let mut first = true;
    let mut sent_eof = false;

    loop {
        let mut filled = 0;
        // Fill the buffer to OUTPUT_CHUNK_SIZE or until EOF.
        while filled < buf.len() {
            match reader.read(&mut buf[filled..]).await {
                Ok(0) => break, // EOF
                Ok(n) => filled += n,
                Err(e) => {
                    // Send an EOF marker so the framer flushes any partial
                    // remainder for this SourceId. Without this, a retry
                    // reuses the same SourceId and the stale bytes corrupt
                    // the first reconstructed line.
                    if !first {
                        let eof_payload = ChunkPayload {
                            bytes: Vec::new(),
                            accounted_bytes: 0,
                            source_id,
                            source_path: None,
                            is_eof: true,
                        };
                        let tx = out_tx.clone();
                        let _ = tokio::task::spawn_blocking(move || tx.send(eof_payload)).await;
                    }
                    return Err(io::Error::other(format!("streaming read: {e}")));
                }
            }
        }

        if filled == 0 {
            break; // EOF reached — no more data.
        }

        let eof_reached = filled < buf.len();

        // Only charge accounted_bytes on the first chunk to avoid inflation.
        // Use the S3 wire size (from HEAD / SQS notification) rather than
        // decompressed bytes so bandwidth metrics reflect actual transfer.
        let is_first_chunk = first;
        let ab = if is_first_chunk {
            first = false;
            size
        } else {
            0
        };

        if eof_reached {
            sent_eof = true;
        }

        let payload = ChunkPayload {
            bytes: buf[..filled].to_vec(),
            accounted_bytes: ab,
            source_id,
            source_path: if options.should_expose_source_paths && is_first_chunk {
                Some(key.to_string())
            } else {
                None
            },
            is_eof: eof_reached,
        };
        let tx = out_tx.clone();
        let send_result = tokio::task::spawn_blocking(move || tx.send(payload))
            .await
            .map_err(|e| io::Error::other(format!("spawn_blocking send: {e}")))?;
        if send_result.is_err() {
            return Err(io::Error::other("output channel closed"));
        }

        if eof_reached {
            break;
        }
    }

    // Send trailing EOF if we haven't already (empty object, or data was an
    // exact multiple of OUTPUT_CHUNK_SIZE so the last chunk wasn't short).
    if !sent_eof {
        let payload = ChunkPayload {
            bytes: Vec::new(),
            accounted_bytes: 0,
            source_id,
            source_path: None,
            is_eof: true,
        };
        let tx = out_tx.clone();
        let _ = tokio::task::spawn_blocking(move || tx.send(payload)).await;
    }

    Ok(())
}

/// Stream an uncompressed object via parallel range-GETs with ordered delivery.
///
/// Each part is downloaded fully (body included) inside a `buffered(N)` future,
/// so up to `max_fetches` parts download concurrently. Once a part resolves,
/// its bytes are chunked into ~256 KiB pieces and sent to `out_tx` in order.
///
/// Peak memory: `max_fetches × part_size` (default 16 × 1 MiB = 16 MiB).
async fn fetch_parallel_stream(
    s3: Arc<S3Client>,
    key: &str,
    size: u64,
    options: FetchOptions,
    out_tx: mpsc::SyncSender<ChunkPayload>,
) -> io::Result<()> {
    use futures_util::{StreamExt, stream};

    let source_id = source_id_from_key(key);

    // Build range list.
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    let mut offset: u64 = 0;
    while offset < size {
        let end = (offset + options.part_size - 1).min(size - 1);
        ranges.push((offset, end));
        offset = end + 1;
    }

    if ranges.is_empty() {
        let payload = ChunkPayload {
            bytes: Vec::new(),
            accounted_bytes: 0,
            source_id,
            source_path: None,
            is_eof: true,
        };
        let send_result = tokio::task::spawn_blocking(move || out_tx.send(payload))
            .await
            .map_err(|e| io::Error::other(format!("spawn_blocking send: {e}")))?;
        if send_result.is_err() {
            return Err(io::Error::other("output channel closed"));
        }
        return Ok(());
    }

    // Each future: fetch range + download full body → yield Bytes.
    // `buffered(N)` runs up to N downloads concurrently, yields in order.
    let key_arc: Arc<str> = Arc::from(key);
    let mut part_stream = stream::iter(ranges)
        .map(|(start, end)| {
            let s3 = Arc::clone(&s3);
            let key = Arc::clone(&key_arc);
            async move {
                let mut last_err = None;
                for attempt in 0..3u32 {
                    if attempt > 0 {
                        let delay_ms = 200 * (1u64 << (attempt - 1));
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                    match s3.get_object_range(&key, start, end).await {
                        Ok(data) => return Ok(data),
                        Err(e) => last_err = Some(e),
                    }
                }
                Err(last_err.expect("last_err is Some when all attempts fail"))
            }
        })
        .buffered(options.max_fetches.max(1));

    // Deliver pre-loaded parts in order, chunking into OUTPUT_CHUNK_SIZE.
    let mut first_chunk = true;
    while let Some(result) = part_stream.next().await {
        let data = match result {
            Ok(d) => d,
            Err(e) => {
                if !first_chunk {
                    let eof = ChunkPayload {
                        bytes: Vec::new(),
                        accounted_bytes: 0,
                        source_id,
                        source_path: None,
                        is_eof: true,
                    };
                    let tx = out_tx.clone();
                    let _ = tokio::task::spawn_blocking(move || tx.send(eof)).await;
                }
                return Err(e);
            }
        };

        // Chunk the downloaded body into OUTPUT_CHUNK_SIZE pieces.
        let mut pos = 0;
        while pos < data.len() {
            let end = (pos + OUTPUT_CHUNK_SIZE).min(data.len());
            let is_first_chunk = first_chunk;
            let ab = if is_first_chunk {
                first_chunk = false;
                size
            } else {
                0
            };
            let payload = ChunkPayload {
                bytes: data[pos..end].to_vec(),
                accounted_bytes: ab,
                source_id,
                source_path: if options.should_expose_source_paths && is_first_chunk {
                    Some(key.to_string())
                } else {
                    None
                },
                is_eof: false,
            };
            let tx = out_tx.clone();
            let send_result = tokio::task::spawn_blocking(move || tx.send(payload))
                .await
                .map_err(|e| io::Error::other(format!("spawn_blocking send: {e}")))?;
            if send_result.is_err() {
                return Err(io::Error::other("output channel closed"));
            }
            pos = end;
        }
    }

    // Send trailing EOF.
    let payload = ChunkPayload {
        bytes: Vec::new(),
        accounted_bytes: 0,
        source_id,
        source_path: None,
        is_eof: true,
    };
    let tx = out_tx.clone();
    let _ = tokio::task::spawn_blocking(move || tx.send(payload)).await;

    Ok(())
}

/// Download an object in parallel range-GET chunks and return the concatenated bytes.
///
/// Exposed as `pub` for benchmarks. Uses the old buffered path.
pub async fn fetch_parallel_bench(
    s3: Arc<S3Client>,
    key: &str,
    size: u64,
    part_size: u64,
    max_fetches: usize,
) -> io::Result<bytes::Bytes> {
    fetch_parallel_buffered(s3, key, size, part_size, max_fetches).await
}

/// Streaming parallel fetch benchmark entry point.
///
/// Returns the total bytes received. Exposed for benchmarking.
pub async fn fetch_parallel_stream_bench(
    s3: Arc<S3Client>,
    key: &str,
    size: u64,
    part_size: u64,
    max_fetches: usize,
) -> io::Result<usize> {
    let (tx, rx) = mpsc::sync_channel::<ChunkPayload>(16);
    let key_owned = key.to_string();
    let s3c = Arc::clone(&s3);
    let options = FetchOptions {
        part_size,
        max_fetches,
        compression_override: None,
        should_expose_source_paths: false,
    };
    let handle =
        tokio::spawn(
            async move { fetch_parallel_stream(s3c, &key_owned, size, options, tx).await },
        );
    let mut total = 0usize;
    while let Ok(chunk) = rx.recv() {
        total += chunk.bytes.len();
    }
    handle
        .await
        .map_err(|e| io::Error::other(format!("join: {e}")))??;
    Ok(total)
}

/// Buffered parallel fetch — kept for benchmarks that need the full Bytes result.
async fn fetch_parallel_buffered(
    s3: Arc<S3Client>,
    key: &str,
    size: u64,
    part_size: u64,
    max_fetches: usize,
) -> io::Result<bytes::Bytes> {
    // Build range list.
    let mut ranges: Vec<(usize, u64, u64)> = Vec::new();
    let mut offset: u64 = 0;
    let mut idx: usize = 0;
    while offset < size {
        let end = (offset + part_size - 1).min(size - 1);
        ranges.push((idx, offset, end));
        offset = end + 1;
        idx += 1;
    }

    if ranges.is_empty() {
        return Ok(bytes::Bytes::new());
    }

    let fetch_sem = Arc::new(Semaphore::new(max_fetches));
    let mut join_set: JoinSet<io::Result<(usize, bytes::Bytes)>> = JoinSet::new();

    for (range_idx, start, end) in ranges {
        let permit = fetch_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| io::Error::other(format!("semaphore acquire: {e}")))?;
        let s3 = Arc::clone(&s3);
        let key_owned = key.to_string();
        join_set.spawn(async move {
            let _permit = permit;
            let data = s3.get_object_range(&key_owned, start, end).await?;
            Ok((range_idx, data))
        });
    }

    // Collect results in order.
    let mut parts: Vec<(usize, bytes::Bytes)> = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(part)) => parts.push(part),
            Ok(Err(e)) => {
                join_set.abort_all();
                return Err(e);
            }
            Err(e) => {
                join_set.abort_all();
                return Err(io::Error::other(format!("range GET task panicked: {e}")));
            }
        }
    }

    parts.sort_by_key(|(i, _)| *i);
    let total: usize = parts.iter().map(|(_, b)| b.len()).sum();
    let mut out = bytes::BytesMut::with_capacity(total);
    for (_, part) in parts {
        out.extend_from_slice(&part);
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_input(rx: mpsc::Receiver<ChunkPayload>, should_expose_source_paths: bool) -> S3Input {
        S3Input {
            name: "s3-test".to_string(),
            rx: Some(rx),
            health: Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr())),
            is_running: Arc::new(AtomicBool::new(true)),
            thread_handle: None,
            should_expose_source_paths,
            active_source_paths: HashMap::new(),
            poll_source_paths: HashMap::new(),
        }
    }

    #[test]
    fn poll_exposes_current_object_key_as_source_path_when_enabled() {
        let (tx, rx) = mpsc::sync_channel(2);
        let key = "logs/app/2026-04-20.json";
        let source_id = source_id_from_key(key);
        tx.send(ChunkPayload {
            bytes: b"{\"msg\":\"hello\"}\n".to_vec(),
            accounted_bytes: 16,
            source_id,
            source_path: Some(key.to_string()),
            is_eof: false,
        })
        .expect("send chunk");

        let mut input = test_input(rx, true);
        let events = input.poll().expect("poll");
        assert_eq!(events.len(), 1);
        let InputEvent::Data {
            source_id: event_source_id,
            ..
        } = &events[0]
        else {
            panic!("expected data event");
        };
        assert_eq!(*event_source_id, Some(source_id));

        let paths = input.source_paths();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, source_id);
        assert_eq!(paths[0].1, PathBuf::from(key));
    }

    #[test]
    fn poll_keeps_s3_source_path_only_for_active_and_current_sources() {
        let (tx, rx) = mpsc::sync_channel(2);
        let key = "logs/app/split.json";
        let source_id = source_id_from_key(key);
        tx.send(ChunkPayload {
            bytes: b"{\"part\":1}\n".to_vec(),
            accounted_bytes: 11,
            source_id,
            source_path: Some(key.to_string()),
            is_eof: false,
        })
        .expect("send first chunk");

        let mut input = test_input(rx, true);
        input.poll().expect("poll first chunk");
        assert!(input.active_source_paths.contains_key(&source_id));

        tx.send(ChunkPayload {
            bytes: b"{\"part\":2}\n".to_vec(),
            accounted_bytes: 0,
            source_id,
            source_path: None,
            is_eof: true,
        })
        .expect("send final chunk");
        input.poll().expect("poll final chunk");

        let paths = input.source_paths();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, source_id);
        assert_eq!(paths[0].1, PathBuf::from(key));
        assert!(!input.active_source_paths.contains_key(&source_id));

        input.poll().expect("empty poll clears current snapshot");
        assert!(input.source_paths().is_empty());
    }

    #[test]
    fn poll_exposes_source_path_for_eof_only_flush_when_active() {
        let (tx, rx) = mpsc::sync_channel(1);
        let key = "logs/app/partial-without-newline.json";
        let source_id = source_id_from_key(key);
        tx.send(ChunkPayload {
            bytes: Vec::new(),
            accounted_bytes: 0,
            source_id,
            source_path: None,
            is_eof: true,
        })
        .expect("send eof");

        let mut input = test_input(rx, true);
        input
            .active_source_paths
            .insert(source_id, PathBuf::from(key));
        let events = input.poll().expect("poll eof");
        assert_eq!(events.len(), 1);
        let InputEvent::EndOfFile {
            source_id: eof_source_id,
        } = events[0]
        else {
            panic!("expected eof event");
        };
        assert_eq!(eof_source_id, Some(source_id));

        let paths = input.source_paths();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, source_id);
        assert_eq!(paths[0].1, PathBuf::from(key));
        assert!(!input.active_source_paths.contains_key(&source_id));
    }

    #[test]
    fn poll_does_not_track_s3_source_paths_when_disabled() {
        let (tx, rx) = mpsc::sync_channel(1);
        let key = "logs/app/no-metadata.json";
        let source_id = source_id_from_key(key);
        tx.send(ChunkPayload {
            bytes: b"{\"msg\":\"hello\"}\n".to_vec(),
            accounted_bytes: 16,
            source_id,
            source_path: Some(key.to_string()),
            is_eof: false,
        })
        .expect("send chunk");

        let mut input = test_input(rx, false);
        input.poll().expect("poll");
        assert!(input.source_paths().is_empty());
        assert!(input.active_source_paths.is_empty());
    }
}
