//! HTTP-cached live enrichment processor.
//!
//! Enriches batches by looking up per-row keys against a remote HTTP endpoint.
//! Uses a local in-process LRU-style cache to avoid repeated requests and to
//! tolerate transient failures.
//!
//! ## Processing model
//!
//! This processor is **eventually consistent**: the first time a key is seen,
//! the batch row receives `status = "pending"` and a background fetch is
//! started.  On subsequent batches, once the fetch has completed, the row will
//! receive the full enrichment data (`status = "hit"` or `status = "miss"`).
//!
//! This model avoids blocking the pipeline while external calls are in
//! flight — important for throughput on high-volume pipelines.
//!
//! ## Output columns
//!
//! Given `prefix = "acct"`:
//!
//! - `acct_json: Utf8` — raw JSON response body, or `NULL` on miss/pending
//! - `acct_status: Utf8` — one of `"hit"`, `"miss"`, `"pending"`, `"error"`
//!
//! Use DataFusion's `json()` / `json_str()` UDFs to extract specific fields
//! from `acct_json` in a subsequent SQL transform.
//!
//! ## Config (not yet wired — use `HttpEnrichProcessor::new` directly)
//!
//! ```yaml
//! # Planned config schema:
//! processors:
//!   - type: http_enrich
//!     source_column: customer_id
//!     url_template: "http://customer-svc.internal/api/customers/{key}"
//!     prefix: acct
//!     timeout_ms: 500
//!     max_cache_entries: 50000
//!     ttl_seconds: 300
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_output::BatchMetadata;
use smallvec::{SmallVec, smallvec};

use crate::processor::{Processor, ProcessorError};

// ---------------------------------------------------------------------------
// Cache types
// ---------------------------------------------------------------------------

/// Status of a cached enrichment entry.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CacheStatus {
    /// A fetch is currently in flight — return "pending" to the caller.
    Pending,
    /// Successful lookup; `data` contains the raw JSON body.
    Hit(String),
    /// The endpoint returned 404 or an empty/unrecognised response.
    Miss,
    /// The most recent fetch attempt failed with an error.
    Error(String),
}

#[derive(Debug, Clone)]
struct CacheEntry {
    status: CacheStatus,
    inserted_at: Instant,
}

// ---------------------------------------------------------------------------
// HttpEnrichProcessor
// ---------------------------------------------------------------------------

/// Configuration for `HttpEnrichProcessor`.
#[derive(Debug, Clone)]
pub struct HttpEnrichConfig {
    /// The batch column whose value is used as the lookup key.
    pub source_column: String,
    /// URL template.  Occurrences of `{key}` are replaced with the URL-encoded
    /// column value.
    pub url_template: String,
    /// Prefix for output columns (`{prefix}_json`, `{prefix}_status`).
    pub prefix: String,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Maximum number of entries in the in-process cache.
    pub max_entries: usize,
    /// How long a cache entry is considered fresh.
    pub ttl: Duration,
}

impl HttpEnrichConfig {
    /// Create with sensible defaults.
    pub fn new(
        source_column: impl Into<String>,
        url_template: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        HttpEnrichConfig {
            source_column: source_column.into(),
            url_template: url_template.into(),
            prefix: prefix.into(),
            timeout: Duration::from_millis(500),
            max_entries: 50_000,
            ttl: Duration::from_secs(300),
        }
    }
}

/// A stateless [`Processor`] that enriches batches via HTTP with local caching.
///
/// See the [module-level documentation](self) for design details.
#[derive(Debug)]
pub struct HttpEnrichProcessor {
    config: HttpEnrichConfig,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl HttpEnrichProcessor {
    /// Create a new processor.  No I/O happens at construction time.
    pub fn new(config: HttpEnrichConfig) -> Self {
        HttpEnrichProcessor {
            config,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Look up a key.  Returns the cached status, starting a background fetch
    /// for cache misses and expired entries.
    fn lookup_or_enqueue(&self, key: &str) -> CacheStatus {
        {
            let cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            if let Some(entry) = cache.get(key) {
                let is_pending = entry.status == CacheStatus::Pending;
                let is_expired = entry.inserted_at.elapsed() > self.config.ttl;

                if !is_expired || is_pending {
                    // Return cached value (even if stale while pending).
                    return entry.status.clone();
                }
                // Expired and not pending — fall through to re-fetch.
            }
        }

        // Mark as pending before spawning the fetch task to avoid duplicate
        // in-flight requests for the same key (thundering herd).
        {
            let mut cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            // Double-check after acquiring the write lock.
            if let Some(entry) = cache.get(key) {
                if entry.status == CacheStatus::Pending {
                    return CacheStatus::Pending;
                }
            }

            cache.insert(
                key.to_owned(),
                CacheEntry {
                    status: CacheStatus::Pending,
                    inserted_at: Instant::now(),
                },
            );
        }

        // Spawn a blocking HTTP call on the Tokio blocking thread pool.
        let url = self.config.url_template.replace("{key}", &urlencoded(key));
        let timeout = self.config.timeout;
        let cache = Arc::clone(&self.cache);
        let key_owned = key.to_owned();
        let max_entries = self.config.max_entries;

        // `handle.spawn_blocking` runs the closure on a dedicated thread,
        // leaving the async runtime free.  We detach it by dropping the
        // `JoinHandle` — the closure writes to the shared cache when done.
        let _ = tokio::runtime::Handle::current().spawn_blocking(move || {
            let result = ureq::Agent::config_builder()
                .timeout_global(Some(timeout))
                .build()
                .new_agent()
                .get(&url)
                .call();

            let new_status = match result {
                Ok(resp) => {
                    if resp.status() == 200 {
                        match resp.into_body().read_to_string() {
                            Ok(body) if !body.trim().is_empty() => CacheStatus::Hit(body),
                            _ => CacheStatus::Miss,
                        }
                    } else if resp.status() == 404 {
                        CacheStatus::Miss
                    } else {
                        CacheStatus::Error(format!("HTTP {}", resp.status()))
                    }
                }
                Err(e) => CacheStatus::Error(e.to_string()),
            };

            let mut cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            // Evict oldest entries if the cache is full.
            if cache.len() >= max_entries {
                // Simple strategy: evict the oldest 10% of entries.
                let evict_count = max_entries / 10;
                let mut entries: Vec<(Instant, String)> = cache
                    .iter()
                    .map(|(k, v)| (v.inserted_at, k.clone()))
                    .collect();
                entries.sort_by_key(|(t, _)| *t);
                for (_, k) in entries.into_iter().take(evict_count) {
                    cache.remove(&k);
                }
            }

            cache.insert(
                key_owned,
                CacheEntry {
                    status: new_status,
                    inserted_at: Instant::now(),
                },
            );
        });

        CacheStatus::Pending
    }
}

impl Processor for HttpEnrichProcessor {
    fn name(&self) -> &'static str {
        "http_enrich"
    }

    fn is_stateful(&self) -> bool {
        false // stateless from the pipeline's perspective (cache is internal)
    }

    fn process(
        &mut self,
        batch: RecordBatch,
        _meta: &BatchMetadata,
    ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
        if batch.num_rows() == 0 {
            return Ok(smallvec![batch]);
        }

        let num_rows = batch.num_rows();
        let json_col_name = format!("{}_json", self.config.prefix);
        let status_col_name = format!("{}_status", self.config.prefix);

        // Extract keys from the source column.  Missing or non-string columns
        // result in all-pending rows.
        let keys: Vec<Option<String>> = match batch.column_by_name(&self.config.source_column) {
            None => vec![None; num_rows],
            Some(col) => extract_strings(col, num_rows),
        };

        let mut json_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut status_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);

        for key_opt in &keys {
            match key_opt {
                None => {
                    json_builder.append_null();
                    status_builder.append_value("miss");
                }
                Some(key) => match self.lookup_or_enqueue(key) {
                    CacheStatus::Hit(json) => {
                        json_builder.append_value(&json);
                        status_builder.append_value("hit");
                    }
                    CacheStatus::Miss => {
                        json_builder.append_null();
                        status_builder.append_value("miss");
                    }
                    CacheStatus::Pending => {
                        json_builder.append_null();
                        status_builder.append_value("pending");
                    }
                    CacheStatus::Error(e) => {
                        json_builder.append_null();
                        status_builder.append_value(format!("error: {e}").as_str());
                    }
                },
            }
        }

        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(&json_col_name, DataType::Utf8, true)));
        fields.push(Arc::new(Field::new(
            &status_col_name,
            DataType::Utf8,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();
        columns.push(Arc::new(json_builder.finish()));
        columns.push(Arc::new(status_builder.finish()));

        RecordBatch::try_new(schema, columns)
            .map(|b| smallvec![b])
            .map_err(|e| ProcessorError::Permanent(format!("http_enrich: batch build error: {e}")))
    }

    fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
        SmallVec::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_strings(col: &Arc<dyn arrow::array::Array>, num_rows: usize) -> Vec<Option<String>> {
    use arrow::array::{Array, AsArray};
    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            (0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_owned())
                    }
                })
                .collect()
        }
        DataType::Utf8View => {
            let arr = col.as_string_view();
            (0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_owned())
                    }
                })
                .collect()
        }
        DataType::LargeUtf8 => {
            let arr = col.as_string::<i64>();
            (0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_owned())
                    }
                })
                .collect()
        }
        _ => vec![None; num_rows],
    }
}

/// Percent-encode a key for safe URL interpolation.
///
/// Only encodes characters outside `[A-Za-z0-9._~-]` (unreserved per RFC 3986).
fn urlencoded(s: &str) -> String {
    s.chars()
        .flat_map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '-') {
                vec![c]
            } else {
                // Encode as %XX for each UTF-8 byte.
                let mut buf = [0u8; 4];
                let bytes = c.encode_utf8(&mut buf);
                bytes
                    .bytes()
                    .flat_map(|b| {
                        vec![
                            '%',
                            char::from_digit(u32::from(b >> 4), 16)
                                .unwrap_or('0')
                                .to_ascii_uppercase(),
                            char::from_digit(u32::from(b & 0xF), 16)
                                .unwrap_or('0')
                                .to_ascii_uppercase(),
                        ]
                    })
                    .collect::<Vec<char>>()
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlencoded_safe_chars_unchanged() {
        assert_eq!(urlencoded("hello123"), "hello123");
        assert_eq!(urlencoded("user.name_v2"), "user.name_v2");
    }

    #[test]
    fn urlencoded_encodes_spaces_and_specials() {
        let encoded = urlencoded("hello world");
        assert!(encoded.contains("%20"));
        let encoded2 = urlencoded("a/b");
        assert!(encoded2.contains("%2F"));
    }

    #[test]
    fn new_processor_starts_with_empty_cache() {
        let cfg = HttpEnrichConfig::new("customer_id", "http://localhost/{key}", "acct");
        let proc = HttpEnrichProcessor::new(cfg);
        assert!(
            proc.cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
    }

    #[test]
    fn empty_batch_passed_through() {
        let cfg = HttpEnrichConfig::new("customer_id", "http://localhost/{key}", "acct");
        let mut proc = HttpEnrichProcessor::new(cfg);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "customer_id",
            DataType::Utf8,
            true,
        )]));
        let values: Vec<Option<&str>> = vec![];
        let arr: arrow::array::StringArray = values.into_iter().collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let meta = BatchMetadata {
            resource_attrs: Arc::new(vec![]),
            observed_time_ns: 0,
        };
        let out = proc.process(batch, &meta).unwrap();
        assert_eq!(out[0].num_rows(), 0);
    }
}
