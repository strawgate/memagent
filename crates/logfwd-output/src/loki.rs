//! Async Grafana Loki output sink.
//!
//! Implements the Loki HTTP push API (`POST /loki/api/v1/push`) using
//! reqwest. Log records are grouped into Loki streams by label set, with
//! entries sorted by timestamp and deduplicated to meet Loki's strict
//! monotonic-ordering requirement.
//!
//! # Timestamp handling
//!
//! Loki requires entries within each stream to be strictly monotonically
//! increasing by nanosecond timestamp. If two records share a timestamp,
//! the second is incremented by 1 ns. If the batch already has a `_timestamp`
//! or `@timestamp` column, those values are used; otherwise the batch's
//! `observed_time_ns` is used for all records.
//!
//! # Label extraction
//!
//! Label key-value pairs come from the factory's `static_labels` config and
//! from per-record columns whose names match the `label_columns` list in config.
//!
//! # Wire format
//!
//! Loki's push API accepts JSON:
//! ```json
//! {
//!   "streams": [
//!     {
//!       "stream": {"app":"logfwd","namespace":"default"},
//!       "values": [["1714000000000000000","log line"]]
//!     }
//!   ]
//! }
//! ```
//!
//! The `Content-Type` must be `application/json`. Loki 2.x also accepts
//! Protobuf (snappy-compressed), but JSON is used here for simplicity.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use logfwd_io::diagnostics::ComponentStats;

use super::{BatchMetadata, build_col_infos, write_row_json};

// ---------------------------------------------------------------------------
// LokiStream helpers
// ---------------------------------------------------------------------------

/// A single Loki log entry: (timestamp_ns, log_line).
type LokiEntry = (u64, String);

/// Collect entries per stream label set.
type StreamMap = HashMap<String, Vec<LokiEntry>>;

/// Sort entries by timestamp and deduplicate by incrementing conflicting timestamps.
///
/// Loki rejects any push where `entries[i].timestamp <= entries[i-1].timestamp`
/// within the same stream. We fix this by sorting then nudging duplicates up by 1 ns.
///
/// # Overflow behaviour
///
/// If `entries[i-1].0 == u64::MAX`, there is no valid strictly-larger timestamp to
/// assign. In that case all remaining entries from index `i` onward are truncated.
/// This is an extremely rare edge case (requires nanosecond-resolution timestamps at
/// or beyond the year 2554, or a malformed batch).
///
/// # Return value
///
/// Returns the number of entries retained. This will be less than the original length
/// only when an overflow truncation occurs. Callers must use this count for metrics
/// rather than the original entry count, so that dropped entries are not reported as
/// successfully delivered.
///
/// # Invariant (tested with proptest)
///
/// After calling this function, `entries[i].0 > entries[i-1].0` for all `i > 0`.
pub fn sort_and_dedup_timestamps(entries: &mut Vec<LokiEntry>) -> usize {
    if entries.len() <= 1 {
        return entries.len();
    }
    entries.sort_unstable_by_key(|(ts, _)| *ts);
    let mut i = 1;
    while i < entries.len() {
        if entries[i].0 <= entries[i - 1].0 {
            match entries[i - 1].0.checked_add(1) {
                Some(next) => entries[i].0 = next,
                None => {
                    // Overflow: cannot assign a timestamp beyond u64::MAX.
                    // Drop all remaining entries — they cannot be given unique timestamps.
                    entries.truncate(i);
                    break;
                }
            }
        }
        i += 1;
    }
    entries.len()
}

// ---------------------------------------------------------------------------
// LokiAsyncSink
// ---------------------------------------------------------------------------

struct LokiConfig {
    endpoint: String,
    tenant_id: Option<String>,
    static_labels: Vec<(String, String)>,
    label_columns: Vec<String>,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

/// Async Loki sink using reqwest.
pub struct LokiAsyncSink {
    config: Arc<LokiConfig>,
    client: Arc<reqwest::Client>,
    name: String,
    stats: Arc<ComponentStats>,
}

impl LokiAsyncSink {
    fn new(
        name: String,
        config: Arc<LokiConfig>,
        client: Arc<reqwest::Client>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        LokiAsyncSink {
            config,
            client,
            name,
            stats,
        }
    }

    /// Build the stream map from a RecordBatch.
    ///
    /// For each row:
    /// 1. Extract label values from `label_columns` (if they exist in the schema).
    /// 2. Build a label-set key string (sorted for determinism).
    /// 3. Serialize the row as JSON for the log line.
    /// 4. Append `(timestamp_ns, log_line)` to the correct stream bucket.
    fn build_stream_map(
        &self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
    ) -> io::Result<StreamMap> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        let cols = build_col_infos(batch);

        // Find timestamp column index (prefer `_timestamp`, fall back to `@timestamp`).
        let ts_col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "_timestamp" || f.name() == "@timestamp");

        // Find label column indices.
        let label_col_indices: Vec<(String, usize)> = self
            .config
            .label_columns
            .iter()
            .filter_map(|label_col| {
                schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == label_col.as_str())
                    .map(|idx| (label_col.clone(), idx))
            })
            .collect();

        let mut stream_map: StreamMap = HashMap::new();

        for row in 0..num_rows {
            // --- Timestamp ---
            let ts_ns = if let Some(ts_idx) = ts_col_idx {
                let col = batch.column(ts_idx);
                match col.data_type() {
                    DataType::Int64 => {
                        col.as_primitive::<arrow::datatypes::Int64Type>().value(row) as u64
                    }
                    DataType::UInt64 => col
                        .as_primitive::<arrow::datatypes::UInt64Type>()
                        .value(row),
                    _ => metadata.observed_time_ns,
                }
            } else {
                metadata.observed_time_ns
            };

            // --- Labels ---
            let mut labels: Vec<(String, String)> = self.config.static_labels.clone();
            for (label_name, col_idx) in &label_col_indices {
                let col = batch.column(*col_idx);
                if col.is_null(row) {
                    continue;
                }
                let val = match col.data_type() {
                    DataType::Utf8 => col.as_string::<i32>().value(row).to_string(),
                    DataType::LargeUtf8 => col.as_string::<i64>().value(row).to_string(),
                    DataType::Int64 => itoa::Buffer::new()
                        .format(col.as_primitive::<arrow::datatypes::Int64Type>().value(row))
                        .to_string(),
                    _ => continue,
                };
                labels.push((label_name.clone(), val));
            }
            labels.sort_unstable_by(|a, b| a.0.cmp(&b.0));

            // Build stream key as a JSON array of [key, value] pairs.
            // This is unambiguous even when label values contain commas or `=`,
            // which the previous `k=v,...` encoding could not represent losslessly.
            let stream_key = {
                let pairs: Vec<String> = labels
                    .iter()
                    .map(|(k, v)| format!("[\"{}\",\"{}\"]", escape_json(k), escape_json(v)))
                    .collect();
                format!("[{}]", pairs.join(","))
            };

            // --- Log line ---
            let mut log_line = Vec::new();
            write_row_json(batch, row, &cols, &mut log_line)?;
            let log_str = String::from_utf8(log_line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            stream_map
                .entry(stream_key)
                .or_default()
                .push((ts_ns, log_str));
        }

        Ok(stream_map)
    }

    /// Serialize `stream_map` to Loki push JSON payload.
    ///
    /// Returns `(payload, retained_row_count)`. The retained count may be less than
    /// the original row count if overflow truncation occurred in any stream
    /// (see [`sort_and_dedup_timestamps`]).
    fn serialize_loki_json(
        stream_map: &mut StreamMap,
        static_labels: &[(String, String)],
    ) -> (String, u64) {
        let mut streams_json = Vec::new();
        let mut retained: u64 = 0;

        for (stream_key, entries) in stream_map.iter_mut() {
            retained += sort_and_dedup_timestamps(entries) as u64;

            // Parse stream_key (JSON array of [key, value] pairs) back into label map.
            let mut labels_map: HashMap<String, String> = static_labels.iter().cloned().collect();
            if let Ok(pairs) = serde_json::from_str::<Vec<[String; 2]>>(stream_key.as_str()) {
                for [k, v] in pairs {
                    labels_map.insert(k, v);
                }
            }

            // Build stream JSON.
            let labels_str = labels_map
                .iter()
                .map(|(k, v)| format!("\"{}\":\"{}\"", escape_json(k), escape_json(v)))
                .collect::<Vec<_>>()
                .join(",");

            let values_str = entries
                .iter()
                .map(|(ts, line)| format!("[\"{ts}\",{}]", escape_json_raw(line)))
                .collect::<Vec<_>>()
                .join(",");

            streams_json.push(format!(
                "{{\"stream\":{{{labels_str}}},\"values\":[{values_str}]}}"
            ));
        }

        (
            format!("{{\"streams\":[{}]}}", streams_json.join(",")),
            retained,
        )
    }

    async fn do_send(
        &self,
        payload: String,
        row_count: u64,
    ) -> io::Result<super::sink::SendResult> {
        let url = format!("{}/loki/api/v1/push", self.config.endpoint);
        let byte_len = payload.len() as u64;

        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json");
        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }
        if let Some(tenant_id) = &self.config.tenant_id {
            req = req.header("X-Scope-OrgID", tenant_id.as_str());
        }
        req = req.body(payload);

        let response = req.send().await.map_err(io::Error::other)?;

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5);
            return Ok(super::sink::SendResult::RetryAfter(
                std::time::Duration::from_secs(retry_after),
            ));
        }

        if status.is_client_error() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            return Ok(super::sink::SendResult::Rejected(format!(
                "Loki rejected push with HTTP {status}: {body}"
            )));
        }

        if !status.is_success() {
            return Err(io::Error::other(format!("Loki returned HTTP {status}")));
        }

        self.stats.inc_lines(row_count);
        self.stats.inc_bytes(byte_len);
        Ok(super::sink::SendResult::Ok)
    }
}

impl super::sink::Sink for LokiAsyncSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<super::sink::SendResult>> + Send + 'a>,
    > {
        Box::pin(async move {
            if batch.num_rows() == 0 {
                return Ok(super::sink::SendResult::Ok);
            }
            let mut stream_map = self.build_stream_map(batch, metadata)?;
            let (payload, retained_rows) =
                Self::serialize_loki_json(&mut stream_map, &self.config.static_labels);
            self.do_send(payload, retained_rows).await
        })
    }

    fn flush(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// LokiSinkFactory
// ---------------------------------------------------------------------------

/// Creates `LokiAsyncSink` instances for the output worker pool.
pub struct LokiSinkFactory {
    name: String,
    config: Arc<LokiConfig>,
    client: Arc<reqwest::Client>,
    stats: Arc<ComponentStats>,
}

impl LokiSinkFactory {
    /// Create a new factory.
    ///
    /// - `endpoint`: Loki base URL (e.g. `http://localhost:3100`)
    /// - `tenant_id`: Optional X-Scope-OrgID header value for multi-tenant Loki
    /// - `static_labels`: Labels added to every stream
    /// - `label_columns`: Record columns to use as stream labels
    /// - `headers`: Additional HTTP headers (authentication etc.)
    pub fn new(
        name: String,
        endpoint: String,
        tenant_id: Option<String>,
        static_labels: Vec<(String, String)>,
        label_columns: Vec<String>,
        headers: Vec<(String, String)>,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(io::Error::other)?;

        let parsed_headers = headers
            .into_iter()
            .map(|(k, v)| {
                let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
                let value = reqwest::header::HeaderValue::from_str(&v)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
                Ok((name, value))
            })
            .collect::<io::Result<Vec<_>>>()?;

        Ok(LokiSinkFactory {
            name,
            config: Arc::new(LokiConfig {
                endpoint: endpoint.trim_end_matches('/').to_string(),
                tenant_id,
                static_labels,
                label_columns,
                headers: parsed_headers,
            }),
            client: Arc::new(client),
            stats,
        })
    }
}

impl super::sink::SinkFactory for LokiSinkFactory {
    fn create(&self) -> io::Result<Box<dyn super::sink::Sink>> {
        Ok(Box::new(LokiAsyncSink::new(
            self.name.clone(),
            Arc::clone(&self.config),
            Arc::clone(&self.client),
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Old placeholder (kept for backward compat with lib.rs exports if any)
// ---------------------------------------------------------------------------

/// Synchronous placeholder Loki sink.
///
/// Deprecated: use `LokiSinkFactory` with the async worker pool instead.
#[allow(dead_code)]
pub struct LokiSink {
    name: String,
}

#[allow(dead_code)]
impl LokiSink {
    pub fn new(name: String, _endpoint: String) -> Self {
        LokiSink { name }
    }
}

impl super::OutputSink for LokiSink {
    fn send_batch(&mut self, _batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// JSON escaping helpers
// ---------------------------------------------------------------------------

/// Escape a string for use as a JSON string value (without wrapping quotes).
fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// Wrap an already-valid JSON value (log line) as a JSON string.
/// If it's a JSON object/array, wrap it in quotes with escaping.
/// If it already starts with `"`, return as-is (already a JSON string).
fn escape_json_raw(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.starts_with('"') {
        // Already a JSON string.
        trimmed.to_string()
    } else {
        // A JSON object or array — escape it as a string value.
        format!("\"{}\"", escape_json(trimmed))
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_dedup_already_sorted_no_op() {
        let mut entries: Vec<LokiEntry> =
            vec![(100, "a".into()), (200, "b".into()), (300, "c".into())];
        sort_and_dedup_timestamps(&mut entries);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[1].0, 200);
        assert_eq!(entries[2].0, 300);
    }

    #[test]
    fn sort_dedup_unsorted_input() {
        let mut entries: Vec<LokiEntry> =
            vec![(300, "c".into()), (100, "a".into()), (200, "b".into())];
        sort_and_dedup_timestamps(&mut entries);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[1].0, 200);
        assert_eq!(entries[2].0, 300);
    }

    #[test]
    fn sort_dedup_duplicates_incremented() {
        let mut entries: Vec<LokiEntry> =
            vec![(100, "a".into()), (100, "b".into()), (100, "c".into())];
        sort_and_dedup_timestamps(&mut entries);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[1].0, 101);
        assert_eq!(entries[2].0, 102);
    }

    #[test]
    fn sort_dedup_monotonic_after_dedup() {
        let mut entries: Vec<LokiEntry> = vec![
            (5, "a".into()),
            (5, "b".into()),
            (6, "c".into()),
            (6, "d".into()),
        ];
        sort_and_dedup_timestamps(&mut entries);
        for i in 1..entries.len() {
            assert!(
                entries[i].0 > entries[i - 1].0,
                "timestamps not strictly monotonic at index {i}: {:?}",
                entries.iter().map(|(t, _)| t).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn sort_dedup_single_entry_unchanged() {
        let mut entries = vec![(42u64, "only".to_string())];
        sort_and_dedup_timestamps(&mut entries);
        assert_eq!(entries[0].0, 42);
    }

    #[test]
    fn sort_dedup_empty_no_panic() {
        let mut entries: Vec<LokiEntry> = vec![];
        sort_and_dedup_timestamps(&mut entries);
        assert!(entries.is_empty());
    }

    #[test]
    fn sort_dedup_overflow_truncates_at_u64_max() {
        // Entry at u64::MAX cannot be given a successor — subsequent entries must be dropped.
        let mut entries: Vec<LokiEntry> = vec![
            (u64::MAX, "at max".into()),
            (u64::MAX, "also at max".into()),
            (u64::MAX, "third at max".into()),
        ];
        sort_and_dedup_timestamps(&mut entries);
        // First entry stays; remaining entries cannot be assigned a valid timestamp.
        assert_eq!(
            entries.len(),
            1,
            "entries beyond u64::MAX must be truncated"
        );
        assert_eq!(entries[0].0, u64::MAX);
    }

    #[test]
    fn stream_key_encoding_roundtrip_with_special_chars() {
        // These characters were lossy in the old k=v,... format.
        let labels = vec![
            ("env".to_string(), "prod=us-east,eu-west".to_string()),
            ("app".to_string(), r#"my"app"#.to_string()),
            ("path".to_string(), r"C:\Users\log".to_string()),
            ("normal".to_string(), "value".to_string()),
        ];

        // Encode to stream key (same logic as build_stream_map).
        let stream_key = {
            let pairs: Vec<String> = labels
                .iter()
                .map(|(k, v)| format!("[\"{}\",\"{}\"]", escape_json(k), escape_json(v)))
                .collect();
            format!("[{}]", pairs.join(","))
        };

        // Parse back (same logic as serialize_loki_json).
        let parsed: Vec<[String; 2]> =
            serde_json::from_str(&stream_key).expect("stream_key must be valid JSON array");

        assert_eq!(parsed.len(), labels.len(), "label count must be preserved");
        for (i, [k, v]) in parsed.iter().enumerate() {
            assert_eq!(k, &labels[i].0, "key {i} must round-trip");
            assert_eq!(
                v, &labels[i].1,
                "value {i} must round-trip through JSON encoding"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Proptest: timestamp ordering invariant
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptest_loki {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// After sort_and_dedup_timestamps, entries must be strictly monotonically
        /// increasing. This holds for any input including duplicates and reverse order.
        #[test]
        fn prop_timestamps_strictly_monotonic_after_dedup(
            raw_timestamps in proptest::collection::vec(0u64..1_000_000_000u64, 0..50)
        ) {
            let mut entries: Vec<LokiEntry> = raw_timestamps
                .into_iter()
                .enumerate()
                .map(|(i, ts)| (ts, format!("line {i}")))
                .collect();
            sort_and_dedup_timestamps(&mut entries);
            for i in 1..entries.len() {
                prop_assert!(
                    entries[i].0 > entries[i - 1].0,
                    "not strictly monotonic at {i}: {:?}",
                    entries.iter().map(|(t, _)| t).collect::<Vec<_>>()
                );
            }
        }

        /// sort_and_dedup never changes the number of entries when all timestamps
        /// are far enough from u64::MAX that nudging cannot overflow.
        #[test]
        fn prop_entry_count_preserved(
            raw_timestamps in proptest::collection::vec(0u64..u64::MAX - 10000, 0..50)
        ) {
            let original_len = raw_timestamps.len();
            let mut entries: Vec<LokiEntry> = raw_timestamps
                .into_iter()
                .enumerate()
                .map(|(i, ts)| (ts, format!("line {i}")))
                .collect();
            sort_and_dedup_timestamps(&mut entries);
            prop_assert_eq!(entries.len(), original_len);
        }

        /// When all entries have the same timestamp, they must be assigned consecutive
        /// timestamps starting at the original value. This is the "minimal displacement"
        /// invariant: we perturb timestamps by the smallest amount that makes them unique.
        #[test]
        fn prop_minimal_displacement_on_duplicates(
            base_ts in 0u64..u64::MAX - 10000,
            count in 1usize..100,
        ) {
            let mut entries: Vec<LokiEntry> = (0..count)
                .map(|i| (base_ts, format!("line {i}")))
                .collect();

            sort_and_dedup_timestamps(&mut entries);

            // All entries must have been preserved (no overflow possible since
            // base_ts + count - 1 <= u64::MAX - 10000 + 99 < u64::MAX).
            prop_assert_eq!(entries.len(), count, "entries must not be dropped for non-overflow case");

            // Must be strictly monotonic starting at base_ts.
            for (i, (ts, _)) in entries.iter().enumerate() {
                prop_assert_eq!(
                    *ts,
                    base_ts + i as u64,
                    "Minimal displacement violated at index {}: expected {}, got {}",
                    i, base_ts + i as u64, ts
                );
            }
        }
    }
}
