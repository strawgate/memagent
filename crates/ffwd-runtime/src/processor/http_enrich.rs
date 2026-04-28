//! Batch-blocking HTTP enrichment processor.
//!
//! Enriches batches by looking up per-row keys against a remote HTTP endpoint.
//! Uses a local in-process cache to avoid repeated requests.
//!
//! ## Processing model
//!
//! This processor is **batch-blocking**: every row in the batch is fully
//! enriched before the batch is forwarded downstream. The pipeline never
//! sees "pending" rows.
//!
//! 1. Extract unique keys from the source column.
//! 2. Check the local cache — hits skip the network entirely.
//! 3. Fetch cache misses concurrently (bounded by `max_concurrency`).
//! 4. Wait for all lookups to complete (each subject to `timeout`).
//! 5. Build the enriched batch — every row gets `"hit"`, `"miss"`, or
//!    `"error"` status.
//!
//! ## Output columns
//!
//! Given `prefix = "acct"`:
//!
//! - `acct_json: Utf8` — raw JSON response body, or `NULL` on miss/error
//! - `acct_status: Utf8` — one of `"hit"`, `"miss"`, `"error: <reason>"`
//!
//! Use DataFusion's `json()` / `json_str()` UDFs to extract specific fields
//! from `acct_json` in a subsequent SQL transform.
//!
//! ## Config
//!
//! ```yaml
//! processors:
//!   - type: http_enrich
//!     source_column: customer_id
//!     url_template: "http://customer-svc.internal/api/customers/{key}"
//!     prefix: acct
//!     timeout_ms: 500
//!     max_concurrency: 8
//!     max_cache_entries: 50000
//!     ttl_seconds: 300
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ffwd_output::BatchMetadata;
use smallvec::{SmallVec, smallvec};
use std::sync::Arc;

use crate::processor::{Processor, ProcessorError};

// ---------------------------------------------------------------------------
// Cache types
// ---------------------------------------------------------------------------

/// Result of an HTTP enrichment lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LookupResult {
    /// Successful lookup; contains the raw JSON body.
    Hit(String),
    /// The endpoint returned 204, 404, or an empty/unrecognised response.
    Miss,
    /// The fetch failed (timeout, network error, non-200/204/404 status).
    Error(String),
}

#[derive(Debug, Clone)]
struct CacheEntry {
    result: LookupResult,
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
    /// Maximum number of concurrent HTTP lookups per batch (capped at 64).
    pub max_concurrency: usize,
    /// Maximum response body size in bytes. Responses larger than this are
    /// classified as errors to protect against unbounded memory growth.
    /// Defaults to 1 MiB.
    pub max_body_bytes: u64,
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
            max_concurrency: 8,
            max_body_bytes: 1_048_576, // 1 MiB
        }
    }
}

/// A stateless [`Processor`] that enriches batches via HTTP with local caching.
///
/// See the [module-level documentation](self) for design details.
#[derive(Debug)]
pub struct HttpEnrichProcessor {
    config: HttpEnrichConfig,
    cache: Mutex<HashMap<String, CacheEntry>>,
    agent: ureq::Agent,
}

impl HttpEnrichProcessor {
    /// Create a new processor.  No I/O happens at construction time.
    ///
    /// Returns an error if `url_template` does not contain `{key}`.
    pub fn new(config: HttpEnrichConfig) -> Result<Self, String> {
        if !config.url_template.contains("{key}") {
            return Err(format!(
                "http_enrich url_template must contain '{{key}}' placeholder, got: {}",
                config.url_template
            ));
        }
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(config.timeout))
            .build()
            .new_agent();
        Ok(HttpEnrichProcessor {
            config,
            cache: Mutex::new(HashMap::new()),
            agent,
        })
    }

    /// Look up a key in the cache.  Returns `Some` if the entry is fresh.
    fn cache_get(&self, key: &str) -> Option<LookupResult> {
        let cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.get(key).and_then(|entry| {
            (entry.inserted_at.elapsed() <= self.config.ttl).then(|| entry.result.clone())
        })
    }

    /// Insert results into the cache, evicting stale entries if necessary.
    fn cache_put_batch(&self, results: &[(String, LookupResult)]) {
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Filter to cacheable results first (errors are transient, not cached).
        let cacheable: Vec<&(String, LookupResult)> = results
            .iter()
            .filter(|(_, r)| !matches!(r, LookupResult::Error(_)))
            .collect();

        // If more cacheable results than capacity, only keep the newest ones.
        let cacheable = if cacheable.len() > self.config.max_entries {
            cacheable
                .get(cacheable.len() - self.config.max_entries..)
                .unwrap_or_default()
        } else {
            cacheable.as_slice()
        };

        // Count only net-new keys (not already in cache) for eviction sizing.
        let net_new = cacheable
            .iter()
            .filter(|(k, _)| !cache.contains_key(k))
            .count();
        if cache.len() + net_new > self.config.max_entries {
            let evict_target = (cache.len() + net_new).saturating_sub(self.config.max_entries)
                + self.config.max_entries / 10;
            let mut entries: Vec<(Instant, String)> = cache
                .iter()
                .map(|(k, v)| (v.inserted_at, k.clone()))
                .collect();
            entries.sort_by_key(|(t, _)| *t);
            for (_, k) in entries.into_iter().take(evict_target) {
                cache.remove(&k);
            }
        }

        let now = Instant::now();
        for (key, result) in cacheable {
            cache.insert(
                key.clone(),
                CacheEntry {
                    result: result.clone(),
                    inserted_at: now,
                },
            );
        }
    }

    /// Fetch a single key from the HTTP endpoint (blocking I/O).
    fn fetch_key(&self, key: &str) -> LookupResult {
        let url = self
            .config
            .url_template
            .replace("{key}", &encode_url_key(key));

        let result = self.agent.get(&url).call();

        match result {
            Ok(resp) => {
                let status = resp.status();
                if status == 200 {
                    use std::io::Read;
                    let limit = self.config.max_body_bytes;
                    let mut buf = String::new();
                    match resp
                        .into_body()
                        .as_reader()
                        .take(limit.saturating_add(1))
                        .read_to_string(&mut buf)
                    {
                        Ok(n) if n as u64 > limit => {
                            LookupResult::Error(format!("response body exceeds {limit} byte limit"))
                        }
                        Ok(_) if buf.trim().is_empty() => LookupResult::Miss,
                        Ok(_) => {
                            let trimmed = buf.trim_start();
                            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                                LookupResult::Hit(buf)
                            } else {
                                LookupResult::Error(
                                    "response is not JSON (expected '{' or '[')".to_owned(),
                                )
                            }
                        }
                        Err(e) => LookupResult::Error(format!("body read failed: {e}")),
                    }
                } else if status == 204 || status == 404 {
                    LookupResult::Miss
                } else {
                    LookupResult::Error(format!("HTTP {status}"))
                }
            }
            Err(e) => {
                // ureq turns 4xx/5xx into StatusCode errors by default.
                // Classify 204 and 404 as a miss.
                if let ureq::Error::StatusCode(204 | 404) = &e {
                    LookupResult::Miss
                } else if let ureq::Error::StatusCode(code) = &e {
                    LookupResult::Error(format!("HTTP {code}"))
                } else {
                    LookupResult::Error(e.to_string())
                }
            }
        }
    }

    /// Fetch all missing keys concurrently using scoped threads.
    ///
    /// Keys are chunked into groups of `max_concurrency` so we never have
    /// more than that many in-flight HTTP requests at once.
    fn fetch_missing(&self, keys: &[String]) -> Vec<(String, LookupResult)> {
        if keys.is_empty() {
            return Vec::new();
        }

        let mut all_results = Vec::with_capacity(keys.len());

        let concurrency = self.config.max_concurrency.clamp(1, 64);

        for chunk in keys.chunks(concurrency) {
            let chunk_keys: Vec<&str> = chunk.iter().map(String::as_str).collect();
            let results_before = all_results.len();

            std::thread::scope(|s| {
                let handles: Vec<_> = chunk_keys
                    .iter()
                    .enumerate()
                    .map(|(idx, &key)| {
                        let key_owned = key.to_owned();
                        s.spawn(move || {
                            let result = self.fetch_key(&key_owned);
                            (idx, key_owned, result)
                        })
                    })
                    .collect();

                for handle in handles {
                    match handle.join() {
                        Ok((_idx, key, result)) => all_results.push((key, result)),
                        Err(_) => {
                            // Thread panicked — shouldn't happen with ureq, but
                            // record an error. We can't recover the key from the
                            // panicked thread, but the idx lets us look it up.
                        }
                    }
                }

                // Check this chunk's results only for missing keys (panicked threads).
                let chunk_result_keys: HashSet<&str> = all_results
                    .get(results_before..)
                    .unwrap_or_default()
                    .iter()
                    .map(|(k, _)| k.as_str())
                    .collect();
                let missing: Vec<String> = chunk_keys
                    .iter()
                    .filter(|k| !chunk_result_keys.contains(**k))
                    .map(|k| (*k).to_owned())
                    .collect();
                for key in missing {
                    all_results.push((
                        key,
                        LookupResult::Error("internal: fetch thread panicked".to_owned()),
                    ));
                }
            });
        }

        all_results
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
        let num_rows = batch.num_rows();
        let json_col_name = format!("{}_json", self.config.prefix);
        let status_col_name = format!("{}_status", self.config.prefix);

        // Reject duplicate output column names early.
        let schema = batch.schema();
        if schema.column_with_name(&json_col_name).is_some() {
            return Err(ProcessorError::Permanent(format!(
                "http_enrich: output column '{json_col_name}' already exists in batch"
            )));
        }
        if schema.column_with_name(&status_col_name).is_some() {
            return Err(ProcessorError::Permanent(format!(
                "http_enrich: output column '{status_col_name}' already exists in batch"
            )));
        }

        if num_rows == 0 {
            // Return an empty batch with the correct enriched schema so
            // downstream consumers see a consistent column set.
            let mut fields = batch.schema().fields().to_vec();
            fields.push(Arc::new(Field::new(&json_col_name, DataType::Utf8, true)));
            fields.push(Arc::new(Field::new(
                &status_col_name,
                DataType::Utf8,
                false,
            )));
            let schema = Arc::new(Schema::new_with_metadata(
                fields,
                batch.schema().metadata().clone(),
            ));
            let empty = RecordBatch::new_empty(schema);
            return Ok(smallvec![empty]);
        }

        // 1. Extract keys from the source column.
        let keys: Vec<Option<String>> = match batch.column_by_name(&self.config.source_column) {
            None => vec![None; num_rows],
            Some(col) => extract_strings(col, num_rows, &self.config.source_column)?,
        };

        // 2. Deduplicate keys and check cache.
        let mut resolved: HashMap<String, LookupResult> = HashMap::new();
        let mut to_fetch_set: HashSet<String> = HashSet::new();
        let mut to_fetch: Vec<String> = Vec::new();

        for key in keys.iter().flatten() {
            if resolved.contains_key(key) || to_fetch_set.contains(key) {
                continue;
            }
            if let Some(cached) = self.cache_get(key) {
                resolved.insert(key.clone(), cached);
            } else {
                to_fetch_set.insert(key.clone());
                to_fetch.push(key.clone());
            }
        }

        // 3. Fetch cache misses (blocks until all complete).
        if !to_fetch.is_empty() {
            let fetched = self.fetch_missing(&to_fetch);
            self.cache_put_batch(&fetched);
            for (k, v) in fetched {
                resolved.insert(k, v);
            }
        }

        // 4. Build output columns.
        let mut json_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut status_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);

        for key_opt in &keys {
            match key_opt {
                None => {
                    json_builder.append_null();
                    status_builder.append_value("miss");
                }
                Some(key) => match resolved.get(key) {
                    Some(LookupResult::Hit(json)) => {
                        json_builder.append_value(json);
                        status_builder.append_value("hit");
                    }
                    Some(LookupResult::Miss) | None => {
                        json_builder.append_null();
                        status_builder.append_value("miss");
                    }
                    Some(LookupResult::Error(e)) => {
                        json_builder.append_null();
                        // Reuse a stack buffer to avoid per-row allocation.
                        let mut err_buf = String::with_capacity(7 + e.len());
                        err_buf.push_str("error: ");
                        err_buf.push_str(e);
                        status_builder.append_value(&err_buf);
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
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        ));

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

fn extract_strings(
    col: &Arc<dyn arrow::array::Array>,
    num_rows: usize,
    column_name: &str,
) -> Result<Vec<Option<String>>, ProcessorError> {
    use arrow::array::{Array, AsArray};
    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            Ok((0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_owned())
                    }
                })
                .collect())
        }
        DataType::Utf8View => {
            let arr = col.as_string_view();
            Ok((0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_owned())
                    }
                })
                .collect())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_string::<i64>();
            Ok((0..num_rows)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_owned())
                    }
                })
                .collect())
        }
        other => Err(ProcessorError::Permanent(format!(
            "http_enrich: source column '{column_name}' has unsupported type {other}; expected a string type"
        ))),
    }
}

/// Percent-encode a key for safe URL interpolation.
///
/// Only encodes characters outside `[A-Za-z0-9._~-]` (unreserved per RFC 3986).
fn encode_url_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '-') {
            out.push(c);
        } else {
            let mut buf = [0u8; 4];
            let encoded = c.encode_utf8(&mut buf);
            for b in encoded.bytes() {
                out.push('%');
                out.push(
                    char::from_digit(u32::from(b >> 4), 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit(u32::from(b & 0xF), 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_url_key_safe_chars_unchanged() {
        assert_eq!(encode_url_key("hello123"), "hello123");
        assert_eq!(encode_url_key("user.name_v2"), "user.name_v2");
    }

    #[test]
    fn encode_url_key_encodes_spaces_and_specials() {
        let encoded = encode_url_key("hello world");
        assert!(encoded.contains("%20"));
        let encoded2 = encode_url_key("a/b");
        assert!(encoded2.contains("%2F"));
    }

    #[test]
    fn new_processor_starts_with_empty_cache() {
        let cfg = HttpEnrichConfig::new("customer_id", "http://localhost/{key}", "acct");
        let proc = HttpEnrichProcessor::new(cfg).unwrap();
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
        let mut proc = HttpEnrichProcessor::new(cfg).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "customer_id",
            DataType::Utf8,
            true,
        )]));
        let values: Vec<Option<&str>> = vec![];
        let arr: arrow::array::StringArray = values.into_iter().collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch");

        let meta = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 0,
        };
        let out = proc.process(batch, &meta).expect("process should succeed");
        assert_eq!(out[0].num_rows(), 0);
    }

    fn make_batch(keys: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let arr: arrow::array::StringArray = keys.iter().copied().collect();
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch")
    }

    fn test_meta() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 0,
        }
    }

    #[test]
    fn cache_hit_skips_network() {
        let cfg = HttpEnrichConfig::new("id", "http://localhost/{key}", "e");
        let mut proc = HttpEnrichProcessor::new(cfg).unwrap();

        // Seed the cache.
        {
            let mut cache = proc.cache.lock().expect("lock");
            cache.insert(
                "abc".to_owned(),
                CacheEntry {
                    result: LookupResult::Hit(r#"{"name":"test"}"#.to_owned()),
                    inserted_at: Instant::now(),
                },
            );
        }

        let batch = make_batch(&[Some("abc")]);
        let out = proc.process(batch, &test_meta()).expect("process");
        assert_eq!(out[0].num_rows(), 1);

        use arrow::array::AsArray;
        let json_col = out[0].column_by_name("e_json").expect("e_json");
        let arr = json_col.as_string::<i32>();
        assert!(arr.value(0).contains("test"));

        let status_col = out[0].column_by_name("e_status").expect("e_status");
        let arr = status_col.as_string::<i32>();
        assert_eq!(arr.value(0), "hit");
    }

    #[test]
    fn cache_miss_returns_miss() {
        let cfg = HttpEnrichConfig::new("id", "http://localhost/{key}", "e");
        let mut proc = HttpEnrichProcessor::new(cfg).unwrap();

        // Seed with a Miss result.
        {
            let mut cache = proc.cache.lock().expect("lock");
            cache.insert(
                "gone".to_owned(),
                CacheEntry {
                    result: LookupResult::Miss,
                    inserted_at: Instant::now(),
                },
            );
        }

        let batch = make_batch(&[Some("gone")]);
        let out = proc.process(batch, &test_meta()).expect("process");

        use arrow::array::AsArray;
        let status_col = out[0].column_by_name("e_status").expect("e_status");
        let arr = status_col.as_string::<i32>();
        assert_eq!(arr.value(0), "miss");
    }

    #[test]
    fn expired_entry_triggers_refetch() {
        let mut cfg = HttpEnrichConfig::new("id", "http://invalid.test/{key}", "e");
        cfg.ttl = Duration::from_millis(1);
        cfg.timeout = Duration::from_millis(100);
        let mut proc = HttpEnrichProcessor::new(cfg).unwrap();

        // Seed with an expired entry.
        {
            let mut cache = proc.cache.lock().expect("lock");
            cache.insert(
                "old".to_owned(),
                CacheEntry {
                    result: LookupResult::Hit("stale".to_owned()),
                    inserted_at: Instant::now() - Duration::from_secs(10),
                },
            );
        }

        // Processing should re-fetch (and fail since host is invalid).
        let batch = make_batch(&[Some("old")]);
        let out = proc.process(batch, &test_meta()).expect("process");

        use arrow::array::AsArray;
        let status_col = out[0].column_by_name("e_status").expect("e_status");
        let arr = status_col.as_string::<i32>();
        assert!(
            arr.value(0).starts_with("error"),
            "expected error status, got: {}",
            arr.value(0)
        );
    }

    #[test]
    fn deduplicates_keys_within_batch() {
        let cfg = HttpEnrichConfig::new("id", "http://invalid.test/{key}", "e");
        let mut proc = HttpEnrichProcessor::new(cfg).unwrap();

        // Seed cache with "dup" so we can verify dedup without hitting network.
        {
            let mut cache = proc.cache.lock().expect("lock");
            cache.insert(
                "dup".to_owned(),
                CacheEntry {
                    result: LookupResult::Hit(r#"{"v":1}"#.to_owned()),
                    inserted_at: Instant::now(),
                },
            );
        }

        let batch = make_batch(&[Some("dup"), Some("dup"), Some("dup")]);
        let out = proc.process(batch, &test_meta()).expect("process");
        assert_eq!(out[0].num_rows(), 3);

        use arrow::array::AsArray;
        let json_col = out[0].column_by_name("e_json").expect("e_json");
        let arr = json_col.as_string::<i32>();
        for i in 0..3 {
            assert!(arr.value(i).contains("v"), "row {i} should have enrichment");
        }
    }

    #[test]
    fn null_keys_get_miss_status() {
        let cfg = HttpEnrichConfig::new("id", "http://localhost/{key}", "e");
        let mut proc = HttpEnrichProcessor::new(cfg).unwrap();

        let batch = make_batch(&[None, None]);
        let out = proc.process(batch, &test_meta()).expect("process");

        use arrow::array::AsArray;
        let status_col = out[0].column_by_name("e_status").expect("e_status");
        let arr = status_col.as_string::<i32>();
        assert_eq!(arr.value(0), "miss");
        assert_eq!(arr.value(1), "miss");
    }

    #[test]
    fn missing_source_column_yields_miss_status() {
        let cfg = HttpEnrichConfig::new("customer_id", "http://localhost/{key}", "e");
        let mut proc = HttpEnrichProcessor::new(cfg).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("other", DataType::Utf8, true)]));
        let arr: arrow::array::StringArray = vec![Some("val1")].into_iter().collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch");

        let out = proc.process(batch, &test_meta()).expect("process");
        assert_eq!(out[0].num_rows(), 1);

        use arrow::array::AsArray;
        let status_col = out[0].column_by_name("e_status").expect("e_status");
        let arr = status_col.as_string::<i32>();
        assert_eq!(arr.value(0), "miss");
    }

    #[test]
    fn eviction_makes_room_for_new_entries() {
        let mut cfg = HttpEnrichConfig::new("id", "http://localhost/{key}", "e");
        cfg.max_entries = 3;
        let proc = HttpEnrichProcessor::new(cfg).unwrap();

        // Fill cache to capacity.
        {
            let mut cache = proc.cache.lock().expect("lock");
            for (i, key) in ["a", "b", "c"].iter().enumerate() {
                cache.insert(
                    key.to_string(),
                    CacheEntry {
                        result: LookupResult::Hit(format!("val{i}")),
                        inserted_at: Instant::now() - Duration::from_secs(100 - i as u64),
                    },
                );
            }
        }

        // Insert a new batch of results — eviction should make room.
        proc.cache_put_batch(&[("d".to_owned(), LookupResult::Hit("new".to_owned()))]);

        let cache = proc.cache.lock().expect("lock");
        assert!(
            cache.contains_key("d"),
            "new entry must be in cache after eviction"
        );
        assert!(
            cache.len() <= 3,
            "cache should not exceed max_entries: {}",
            cache.len()
        );
    }
}
