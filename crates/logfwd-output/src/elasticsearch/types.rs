//! Core types for the Elasticsearch sink.

use std::io;
use std::sync::Arc;
use std::time::Duration;

pub use logfwd_config::ElasticsearchRequestMode;
use logfwd_types::diagnostics::ComponentStats;

// ---------------------------------------------------------------------------
// ElasticsearchSink — reqwest-based async implementation of Sink
// ---------------------------------------------------------------------------

/// Configuration shared across all `ElasticsearchSink` instances from
/// the same factory.
pub(crate) struct ElasticsearchConfig {
    pub(super) endpoint: String,
    pub(super) headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
    pub(super) compress: bool,
    pub(super) request_mode: ElasticsearchRequestMode,
    /// Maximum uncompressed bulk payload size in bytes. Batches that serialize
    /// larger than this are split in half and sent as separate `_bulk` requests.
    /// Default: 5 MiB — safe for Elasticsearch Serverless and self-hosted.
    pub(super) max_bulk_bytes: usize,
    /// Target chunk size for experimental streaming bodies.
    pub(super) stream_chunk_bytes: usize,
    /// Precomputed `_bulk` URL with `filter_path` to avoid per-request allocation.
    pub(super) bulk_url: String,
    /// Precomputed `{"index":{"_index":"<name>"}}\n` bytes — avoids a `format!`
    /// allocation on every `serialize_batch` call.
    pub(super) action_bytes: Box<[u8]>,
}

/// Async Elasticsearch sink using reqwest.
///
/// Implements the [`super::super::sink::Sink`] trait for use with `OutputWorkerPool`.
/// All workers share the same `reqwest::Client` (cloned from the factory) to
/// reuse connection pools, TLS sessions, and DNS caches.
pub struct ElasticsearchSink {
    pub(super) config: Arc<ElasticsearchConfig>,
    pub(super) client: reqwest::Client,
    pub(super) name: String,
    pub(crate) batch_buf: Vec<u8>,
    pub(super) stats: Arc<ComponentStats>,
    pub(super) pending_retry_rows: Option<Vec<u32>>,
    pub(super) pending_rejections: Vec<String>,
}

impl ElasticsearchSink {
    pub(crate) fn new(
        name: String,
        config: Arc<ElasticsearchConfig>,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> Self {
        ElasticsearchSink {
            name,
            config,
            client,
            // Start with a modest initial capacity; the buffer will grow to the
            // right size after the first batch and stay there (via reserve in
            // send_batch_inner after each send).
            batch_buf: Vec::with_capacity(64 * 1024),
            stats,
            pending_retry_rows: None,
            pending_rejections: Vec::new(),
        }
    }

    /// Returns the number of bytes currently serialized in the internal buffer.
    /// Useful for benchmarks and diagnostics.
    pub fn serialized_len(&self) -> usize {
        self.batch_buf.len()
    }
}

#[derive(Default)]
pub(super) struct BulkItemResult {
    pub(super) retry_items: Vec<usize>,
    pub(super) permanent_errors: Vec<String>,
    pub(super) item_count: usize,
}

pub(super) enum SendAttempt {
    Ok,
    Rejected {
        rejections: Vec<String>,
        accepted_rows: usize,
    },
    RetryAfter {
        pending_rows: Vec<u32>,
        rejections: Vec<String>,
        accepted_rows: usize,
        delay: Duration,
    },
    IoError {
        pending_rows: Vec<u32>,
        rejections: Vec<String>,
        error: io::Error,
    },
}
