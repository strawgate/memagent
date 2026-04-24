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
//! the second is incremented by 1 ns. The sink also tracks the last timestamp
//! sent per stream and bumps future batches forward when needed, so duplicate
//! or older timestamps in a later batch do not trigger out-of-order rejects.
//! If the batch already has a `_timestamp` or `@timestamp` column, those values
//! are used; otherwise the batch's `observed_time_ns` is used for all records.
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

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use logfwd_core::otlp::parse_timestamp_nanos;
use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;

use super::{BatchMetadata, build_col_infos, coalesce_as_str, write_row_json};

// ---------------------------------------------------------------------------
// LokiStream helpers
// ---------------------------------------------------------------------------

/// A single Loki log entry: (timestamp_ns, log_line).
type LokiEntry = (u64, String);

/// Sorted Loki stream labels used as a deterministic grouping key.
type StreamLabels = Vec<(String, String)>;

/// Collect entries per stream label set.
type StreamMap = BTreeMap<StreamLabels, Vec<LokiEntry>>;

type SharedTimestampState = Arc<Mutex<HashMap<StreamLabels, u64>>>;

fn sanitize_loki_label_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len().max(1));
    for (idx, ch) in name.chars().enumerate() {
        let valid = if idx == 0 {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        };
        out.push(if valid { ch } else { '_' });
    }
    if out.is_empty() { "_".to_string() } else { out }
}

fn sanitize_static_labels(static_labels: &[(String, String)]) -> io::Result<Vec<(String, String)>> {
    let mut sanitized = Vec::with_capacity(static_labels.len());
    let mut sanitized_sources: HashMap<String, String> =
        HashMap::with_capacity(static_labels.len());
    for (key, value) in static_labels {
        let sanitized_key = sanitize_loki_label_name(key);
        if let Some(existing) = sanitized_sources.get(&sanitized_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "duplicate Loki static label key after sanitization: '{key}' conflicts with {existing} as '{sanitized_key}'"
                ),
            ));
        }
        sanitized_sources.insert(sanitized_key.clone(), format!("static label '{key}'"));
        sanitized.push((sanitized_key, value.clone()));
    }
    Ok(sanitized)
}

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
/// If `min_timestamp_exclusive` is provided, the first entry is additionally
/// forced to be greater than that value.
pub fn sort_and_dedup_timestamps(
    entries: &mut Vec<LokiEntry>,
    min_timestamp_exclusive: Option<u64>,
) -> usize {
    if entries.is_empty() {
        return 0;
    }
    entries.sort_unstable_by_key(|(ts, _)| *ts);
    if let Some(min_ts) = min_timestamp_exclusive
        && entries[0].0 <= min_ts
    {
        match min_ts.checked_add(1) {
            Some(next) => entries[0].0 = next,
            None => {
                entries.clear();
                return 0;
            }
        }
    }
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
// LokiSink — reqwest-based async implementation of Sink
// ---------------------------------------------------------------------------

struct LokiConfig {
    endpoint: String,
    tenant_id: Option<String>,
    static_labels: Vec<(String, String)>,
    label_columns: Vec<String>,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

/// Maximum JSON payload size target for a single Loki push request.
const LOKI_MAX_PAYLOAD_BYTES: usize = 1_048_576;

#[derive(Debug)]
struct PreparedPayload {
    payload: String,
    row_count: u64,
    stream_labels: StreamLabels,
    last_timestamp_ns: u64,
}

/// Async Loki sink using reqwest.
pub struct LokiSink {
    config: Arc<LokiConfig>,
    client: Arc<reqwest::Client>,
    name: String,
    stats: Arc<ComponentStats>,
    last_timestamp_by_stream: SharedTimestampState,
}

impl LokiSink {
    #[cfg(test)]
    fn new(
        name: String,
        config: Arc<LokiConfig>,
        client: Arc<reqwest::Client>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self::with_timestamp_state(
            name,
            config,
            client,
            stats,
            Arc::new(Mutex::new(HashMap::new())),
        )
    }

    fn with_timestamp_state(
        name: String,
        config: Arc<LokiConfig>,
        client: Arc<reqwest::Client>,
        stats: Arc<ComponentStats>,
        last_timestamp_by_stream: SharedTimestampState,
    ) -> Self {
        LokiSink {
            config,
            client,
            name,
            stats,
            last_timestamp_by_stream,
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

        // Find timestamp column index.
        //
        // Priority (highest first):
        //   1. `_timestamp`  — logfwd canonical pipeline column
        //   2. `@timestamp`  — Elasticsearch convention
        //   3. Any other variant recognised by field_names::TIMESTAMP_VARIANTS
        //      (canonical "timestamp", "time", "ts") — covers OTLP receiver output
        //
        // Separate position() calls preserve priority regardless of schema order.
        // A single position() with `||` would return the first schema match, which
        // may not be the highest-priority name. (fixes #1661, #1670)
        let ts_col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == field_names::TIMESTAMP_UNDERSCORE)
            .or_else(|| {
                schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == field_names::TIMESTAMP_AT)
            })
            .or_else(|| {
                schema.fields().iter().position(|f| {
                    field_names::matches_any(
                        f.name(),
                        field_names::TIMESTAMP,
                        field_names::TIMESTAMP_VARIANTS,
                    )
                })
            });

        // Find label ColInfos for configured label columns.
        // Static labels are sanitized before collision detection (#1459, #1470).
        let static_labels = sanitize_static_labels(&self.config.static_labels)?;
        let mut sanitized_label_sources: HashMap<String, String> = self
            .config
            .static_labels
            .iter()
            .map(|(key, _)| {
                (
                    sanitize_loki_label_name(key),
                    format!("static label '{key}'"),
                )
            })
            .collect();
        let mut label_col_infos: Vec<(String, &super::ColInfo)> = Vec::new();
        for label_col in &self.config.label_columns {
            if let Some(ci) = cols.iter().find(|c| &c.field_name == label_col) {
                let sanitized = sanitize_loki_label_name(label_col);
                if let Some(existing) = sanitized_label_sources.get(&sanitized) {
                    // Collision: two sources sanitize to the same key. Keep the
                    // first one and warn — erroring would make config validity
                    // data-dependent since dynamic labels are schema-derived.
                    tracing::warn!(
                        label = label_col.as_str(),
                        sanitized = sanitized.as_str(),
                        conflicts_with = existing.as_str(),
                        "loki.label_collision_after_sanitization — keeping first, dropping duplicate"
                    );
                    continue;
                }
                sanitized_label_sources
                    .insert(sanitized.clone(), format!("label column '{label_col}'"));
                label_col_infos.push((sanitized, ci));
            }
        }

        let mut stream_map: StreamMap = BTreeMap::new();

        for row in 0..num_rows {
            // --- Timestamp ---
            let ts_ns = if let Some(ts_idx) = ts_col_idx {
                let col = batch.column(ts_idx);
                if col.is_null(row) {
                    metadata.observed_time_ns
                } else {
                    match col.data_type() {
                        DataType::Int64 => {
                            let val = col.as_primitive::<arrow::datatypes::Int64Type>().value(row);
                            // Negative nanosecond timestamps are invalid for Loki (u64 wire
                            // type). Fall back to observed time rather than sending epoch-zero
                            // or wrapping to a far-future value. (#1084)
                            if val >= 0 {
                                val as u64
                            } else {
                                metadata.observed_time_ns
                            }
                        }
                        DataType::UInt64 => col
                            .as_primitive::<arrow::datatypes::UInt64Type>()
                            .value(row),
                        // String timestamp columns (ISO 8601) — produced by the scanner
                        // when tailing log files, and by star_to_flat for `_timestamp`.
                        DataType::Utf8 => {
                            parse_timestamp_nanos(col.as_string::<i32>().value(row).as_bytes())
                                .unwrap_or(metadata.observed_time_ns)
                        }
                        DataType::Utf8View => {
                            parse_timestamp_nanos(col.as_string_view().value(row).as_bytes())
                                .unwrap_or(metadata.observed_time_ns)
                        }
                        DataType::LargeUtf8 => {
                            parse_timestamp_nanos(col.as_string::<i64>().value(row).as_bytes())
                                .unwrap_or(metadata.observed_time_ns)
                        }
                        DataType::Timestamp(unit, _) => {
                            use arrow::datatypes::{
                                TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
                                TimestampNanosecondType, TimestampSecondType,
                            };
                            let raw_ns = match unit {
                                TimeUnit::Nanosecond => {
                                    Some(col.as_primitive::<TimestampNanosecondType>().value(row))
                                }
                                TimeUnit::Microsecond => col
                                    .as_primitive::<TimestampMicrosecondType>()
                                    .value(row)
                                    .checked_mul(1_000),
                                TimeUnit::Millisecond => col
                                    .as_primitive::<TimestampMillisecondType>()
                                    .value(row)
                                    .checked_mul(1_000_000),
                                TimeUnit::Second => col
                                    .as_primitive::<TimestampSecondType>()
                                    .value(row)
                                    .checked_mul(1_000_000_000),
                            };
                            raw_ns
                                .and_then(|ns| u64::try_from(ns).ok())
                                .unwrap_or(metadata.observed_time_ns)
                        }
                        _ => metadata.observed_time_ns,
                    }
                }
            } else {
                metadata.observed_time_ns
            };

            // --- Labels ---
            // Use coalesce_as_str so that struct conflict columns (and plain Utf8
            // columns alike) always produce a string value for the label.
            // static_labels is pre-sanitized once before this loop.
            let mut labels = static_labels.clone();
            for (label_name, col_info) in &label_col_infos {
                if let Some(val) = coalesce_as_str(batch, row, col_info) {
                    // Skip empty label values — Loki API rejects them with HTTP 400.
                    if !val.is_empty() {
                        // label_name is already sanitized (pre-computed in label_col_infos).
                        labels.push((label_name.clone(), val));
                    }
                }
            }
            labels.sort_unstable_by(|a, b| a.0.cmp(&b.0));

            // --- Log line ---
            let mut log_line = Vec::new();
            write_row_json(batch, row, &cols, &mut log_line, false)?;
            let log_str = String::from_utf8(log_line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            stream_map.entry(labels).or_default().push((ts_ns, log_str));
        }

        Ok(stream_map)
    }

    fn serialize_stream_chunks(
        labels: &StreamLabels,
        entries: &[LokiEntry],
        max_payload_bytes: usize,
    ) -> Vec<PreparedPayload> {
        let labels_str = labels
            .iter()
            .map(|(k, v)| format!("\"{}\":\"{}\"", escape_json(k), escape_json(v)))
            .collect::<Vec<_>>()
            .join(",");
        let prefix = format!("{{\"streams\":[{{\"stream\":{{{labels_str}}},\"values\":[");
        let suffix = "]}]}";

        let mut payloads = Vec::new();
        let mut encoded_values = String::new();
        let mut row_count: u64 = 0;
        let mut last_timestamp_ns: u64 = 0;

        for (ts, line) in entries {
            let value = format!("[\"{ts}\",{}]", escape_json_raw(line));
            let sep_len = usize::from(row_count > 0);
            let projected_len =
                prefix.len() + encoded_values.len() + sep_len + value.len() + suffix.len();
            if row_count > 0 && projected_len > max_payload_bytes {
                let payload = format!("{prefix}{encoded_values}{suffix}");
                payloads.push(PreparedPayload {
                    payload,
                    row_count,
                    stream_labels: labels.clone(),
                    last_timestamp_ns,
                });
                encoded_values.clear();
                row_count = 0;
            }

            if row_count > 0 {
                encoded_values.push(',');
            }
            encoded_values.push_str(&value);
            row_count += 1;
            last_timestamp_ns = *ts;
        }

        if row_count > 0 {
            let payload = format!("{prefix}{encoded_values}{suffix}");
            payloads.push(PreparedPayload {
                payload,
                row_count,
                stream_labels: labels.clone(),
                last_timestamp_ns,
            });
        }

        payloads
    }

    fn prepare_payloads(
        stream_map: &mut StreamMap,
        last_timestamp_by_stream: &HashMap<StreamLabels, u64>,
        max_payload_bytes: usize,
    ) -> Vec<PreparedPayload> {
        let mut payloads = Vec::new();

        for (labels, entries) in stream_map.iter_mut() {
            let retained =
                sort_and_dedup_timestamps(entries, last_timestamp_by_stream.get(labels).copied());
            if retained == 0 {
                continue;
            }
            payloads.extend(Self::serialize_stream_chunks(
                labels,
                entries,
                max_payload_bytes,
            ));
        }

        payloads
    }

    fn prepare_and_reserve_payloads(
        &self,
        stream_map: &mut StreamMap,
    ) -> io::Result<(Vec<PreparedPayload>, HashMap<StreamLabels, u64>)> {
        let mut timestamp_state = self
            .last_timestamp_by_stream
            .lock()
            .map_err(|_e| io::Error::other("Loki timestamp state lock poisoned"))?;
        let payloads = Self::prepare_payloads(stream_map, &timestamp_state, LOKI_MAX_PAYLOAD_BYTES);

        // Save the previous timestamps so we can roll back if the send fails.
        let mut previous_timestamps = HashMap::new();
        for prepared in &payloads {
            if let Some(&prev) = timestamp_state.get(&prepared.stream_labels) {
                previous_timestamps.insert(prepared.stream_labels.clone(), prev);
            }
        }

        // Reserve before awaiting network IO so concurrent workers cannot prepare
        // overlapping timestamp ranges for the same Loki stream.
        for prepared in &payloads {
            timestamp_state.insert(prepared.stream_labels.clone(), prepared.last_timestamp_ns);
        }
        Ok((payloads, previous_timestamps))
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

        if !status.is_success() {
            let retry_after = response.headers().get("Retry-After").cloned();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            if let Some(send_result) = crate::http_classify::classify_http_status(
                status.as_u16(),
                retry_after.as_ref(),
                &format!("Loki push: {body}"),
            ) {
                return Ok(send_result);
            }
            // classify_http_status handles all non-2xx; unreachable in practice.
            return Err(io::Error::other(format!("Loki: HTTP {status}: {body}")));
        }

        self.stats.inc_lines(row_count);
        self.stats.inc_bytes(byte_len);
        Ok(super::sink::SendResult::Ok)
    }
}

impl super::sink::Sink for LokiSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> std::pin::Pin<Box<dyn Future<Output = super::sink::SendResult> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 {
                return super::sink::SendResult::Ok;
            }
            let mut stream_map = match self.build_stream_map(batch, metadata) {
                Ok(m) => m,
                Err(e) => return super::sink::SendResult::from_io_error(e),
            };
            let (mut payloads, previous_timestamps) =
                match self.prepare_and_reserve_payloads(&mut stream_map) {
                    Ok(result) => result,
                    Err(e) => return super::sink::SendResult::from_io_error(e),
                };
            let mut all_payloads = std::mem::take(&mut payloads).into_iter();
            while let Some(prepared) = all_payloads.next() {
                let labels_for_rollback = prepared.stream_labels.clone();
                let timestamp_for_rollback = prepared.last_timestamp_ns;
                match self
                    .do_send(prepared.payload, prepared.row_count)
                    .await
                {
                    Ok(super::sink::SendResult::Ok) => {}
                    outcome => {
                        // Rollback reserved timestamps if we failed, so that
                        // retries don't cause timestamp drift. Only revert if
                        // the state is still exactly what we set it to. We revert
                        // the failed payload and all remaining unsent payloads.
                        if let Ok(mut state) = self.last_timestamp_by_stream.lock() {
                            if state.get(&labels_for_rollback) == Some(&timestamp_for_rollback) {
                                if let Some(prev) = previous_timestamps.get(&labels_for_rollback) {
                                    state.insert(labels_for_rollback.clone(), *prev);
                                } else {
                                    state.remove(&labels_for_rollback);
                                }
                            }
                            for p in all_payloads {
                                if state.get(&p.stream_labels) == Some(&p.last_timestamp_ns) {
                                    if let Some(prev) = previous_timestamps.get(&p.stream_labels) {
                                        state.insert(p.stream_labels.clone(), *prev);
                                    } else {
                                        state.remove(&p.stream_labels);
                                    }
                                }
                            }
                        }
                        return match outcome {
                            Ok(other) => other,
                            Err(e) => super::sink::SendResult::from_io_error(e),
                        };
                    }
                }
            }
            super::sink::SendResult::Ok
        })
    }

    fn flush(&mut self) -> std::pin::Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> std::pin::Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// LokiSinkFactory
// ---------------------------------------------------------------------------

/// Creates `LokiSink` instances for the output worker pool.
pub struct LokiSinkFactory {
    name: String,
    config: Arc<LokiConfig>,
    client: Arc<reqwest::Client>,
    stats: Arc<ComponentStats>,
    last_timestamp_by_stream: SharedTimestampState,
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
        Self::new_with_client(
            name,
            endpoint,
            tenant_id,
            static_labels,
            label_columns,
            headers,
            reqwest::Client::builder().timeout(Duration::from_secs(30)),
            stats,
        )
    }

    #[allow(clippy::too_many_arguments)]
    /// Creates a Loki sink factory with a caller-provided HTTP client builder.
    ///
    /// The caller owns all cross-cutting HTTP client policy on `client_builder`, including
    /// request timeout, connection-pool sizing, TLS roots, mTLS identity, and certificate
    /// verification behavior. This constructor parses Loki labels and headers, then builds
    /// and reuses one `reqwest::Client` for every sink created by the factory.
    pub fn new_with_client(
        name: String,
        endpoint: String,
        tenant_id: Option<String>,
        static_labels: Vec<(String, String)>,
        label_columns: Vec<String>,
        headers: Vec<(String, String)>,
        client_builder: reqwest::ClientBuilder,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let client = client_builder.build().map_err(io::Error::other)?;

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
            config: Arc::new({
                LokiConfig {
                    endpoint: endpoint.trim_end_matches('/').to_string(),
                    tenant_id,
                    static_labels,
                    label_columns,
                    headers: parsed_headers,
                }
            }),
            client: Arc::new(client),
            stats,
            last_timestamp_by_stream: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

impl super::sink::SinkFactory for LokiSinkFactory {
    fn create(&self) -> io::Result<Box<dyn super::sink::Sink>> {
        Ok(Box::new(LokiSink::with_timestamp_state(
            self.name.clone(),
            Arc::clone(&self.config),
            Arc::clone(&self.client),
            Arc::clone(&self.stats),
            Arc::clone(&self.last_timestamp_by_stream),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl LokiSinkFactory {
    #[cfg(test)]
    fn create_sink(&self) -> LokiSink {
        LokiSink::with_timestamp_state(
            self.name.clone(),
            Arc::clone(&self.config),
            Arc::clone(&self.client),
            Arc::clone(&self.stats),
            Arc::clone(&self.last_timestamp_by_stream),
        )
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

/// Ensure the input becomes a valid JSON *string* value (with enclosing quotes).
///
/// Loki's push API requires the second element of each `values` entry to be a
/// JSON string — not an object, array, number, or other JSON value type.
///
/// - If the input is already a valid JSON string (starts with `"` and parses
///   as `Value::String`), return it as-is.
/// - If the input is any other valid JSON value (object, array, number, bool,
///   null), re-encode it as a JSON string (serialize the raw text as a string).
/// - Otherwise, escape it as a plain string value.
fn escape_json_raw(s: &str) -> String {
    let trimmed = s.trim();
    // Bug #1048: leading quote is not enough to guarantee valid JSON string.
    // Verify it actually parses as complete valid JSON before passthrough.
    if trimmed.starts_with('"') {
        if let Ok(serde_json::Value::String(_)) = serde_json::from_str::<serde_json::Value>(trimmed)
        {
            // Already a valid JSON string — pass through as-is.
            return trimmed.to_string();
        }
    } else if serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
        // Valid JSON but not a string (object, array, number, bool, null).
        // Loki requires the log line to be a JSON string, so re-encode it.
        return format!("\"{}\"", escape_json(trimmed));
    }
    // Not valid JSON. Escape it as a string value.
    format!("\"{}\"", escape_json(trimmed))
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
        sort_and_dedup_timestamps(&mut entries, None);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[1].0, 200);
        assert_eq!(entries[2].0, 300);
    }

    #[test]
    fn sort_dedup_unsorted_input() {
        let mut entries: Vec<LokiEntry> =
            vec![(300, "c".into()), (100, "a".into()), (200, "b".into())];
        sort_and_dedup_timestamps(&mut entries, None);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[1].0, 200);
        assert_eq!(entries[2].0, 300);
    }

    #[test]
    fn sort_dedup_duplicates_incremented() {
        let mut entries: Vec<LokiEntry> =
            vec![(100, "a".into()), (100, "b".into()), (100, "c".into())];
        sort_and_dedup_timestamps(&mut entries, None);
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
        sort_and_dedup_timestamps(&mut entries, None);
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
        sort_and_dedup_timestamps(&mut entries, None);
        assert_eq!(entries[0].0, 42);
    }

    #[test]
    fn sort_dedup_empty_no_panic() {
        let mut entries: Vec<LokiEntry> = vec![];
        sort_and_dedup_timestamps(&mut entries, None);
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
        sort_and_dedup_timestamps(&mut entries, None);
        // First entry stays; remaining entries cannot be assigned a valid timestamp.
        assert_eq!(
            entries.len(),
            1,
            "entries beyond u64::MAX must be truncated"
        );
        assert_eq!(entries[0].0, u64::MAX);
    }

    #[test]
    fn sort_dedup_respects_previous_batch_last_timestamp() {
        let mut first_batch: Vec<LokiEntry> = vec![(100, "a".into()), (100, "b".into())];
        sort_and_dedup_timestamps(&mut first_batch, None);
        assert_eq!(first_batch[0].0, 100);
        assert_eq!(first_batch[1].0, 101);

        let mut second_batch: Vec<LokiEntry> = vec![(100, "c".into()), (101, "d".into())];
        sort_and_dedup_timestamps(&mut second_batch, Some(101));
        assert_eq!(second_batch[0].0, 102);
        assert_eq!(second_batch[1].0, 103);
    }

    #[test]
    fn send_failure_rolls_back_timestamp_reservation() {
        use crate::sink::{SendResult, Sink};
        let mut server = mockito::Server::new();
        let mock = server.mock("POST", "/loki/api/v1/push")
            .with_status(500)
            .create();

        let factory = LokiSinkFactory::new(
            "loki".to_string(),
            server.url(),
            None,
            vec![("app".to_string(), "logfwd".to_string())],
            vec![],
            vec![],
            Arc::new(ComponentStats::new()),
        )
        .expect("factory");
        let mut sink = factory.create_sink();

        // Create a batch with timestamp 100
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(field_names::TIMESTAMP_UNDERSCORE, DataType::Int64, true),
            arrow::datatypes::Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![100i64])),
                Arc::new(arrow::array::StringArray::from(vec!["hello"])),
            ],
        ).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 99_999,
        };

        // First attempt (fails with 500)
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(sink.send_batch(&batch, &metadata));
        assert!(matches!(result, SendResult::IoError(_)));
        mock.assert();

        // Check state - timestamp should have been rolled back to None
        let labels: StreamLabels = vec![("app".to_string(), "logfwd".to_string())];
        {
            let state = sink.last_timestamp_by_stream.lock().unwrap();
            assert_eq!(state.get(&labels), None, "timestamp reservation should be rolled back on failure");
        }

        // Fix server to succeed
        let mock_success = server.mock("POST", "/loki/api/v1/push")
            .with_status(204)
            .create();

        // Retry same batch
        let result2 = rt.block_on(sink.send_batch(&batch, &metadata));
        assert!(matches!(result2, SendResult::Ok));
        mock_success.assert();

        // State should now have 100
        {
            let state = sink.last_timestamp_by_stream.lock().unwrap();
            assert_eq!(state.get(&labels), Some(&100), "successful send should keep timestamp");
        }
    }

    #[test]
    fn factory_created_sinks_share_timestamp_reservations() {
        let factory = LokiSinkFactory::new(
            "loki".to_string(),
            "http://127.0.0.1:3100".to_string(),
            None,
            vec![("app".to_string(), "logfwd".to_string())],
            vec![],
            vec![],
            Arc::new(ComponentStats::new()),
        )
        .expect("factory");
        let sink1 = factory.create_sink();
        let sink2 = factory.create_sink();
        let labels: StreamLabels = vec![("app".to_string(), "logfwd".to_string())];

        let mut first_stream_map = StreamMap::new();
        first_stream_map.insert(
            labels.clone(),
            vec![(100, "{\"message\":\"a\"}".to_string())],
        );
        let (first_payloads, _) = sink1
            .prepare_and_reserve_payloads(&mut first_stream_map)
            .expect("first payloads");
        assert_eq!(first_payloads[0].last_timestamp_ns, 100);

        let mut second_stream_map = StreamMap::new();
        second_stream_map.insert(labels, vec![(50, "{\"message\":\"b\"}".to_string())]);
        let (second_payloads, _) = sink2
            .prepare_and_reserve_payloads(&mut second_stream_map)
            .expect("second payloads");
        let parsed: serde_json::Value =
            serde_json::from_str(&second_payloads[0].payload).expect("payload json");
        assert_eq!(parsed["streams"][0]["values"][0][0], "101");
        assert_eq!(second_payloads[0].last_timestamp_ns, 101);
    }

    #[test]
    fn stream_labels_preserve_special_chars() {
        // These characters were lossy in the old k=v,... format.
        let labels: StreamLabels = vec![
            ("env".to_string(), "prod=us-east,eu-west".to_string()),
            ("app".to_string(), r#"my"app"#.to_string()),
            ("path".to_string(), r"C:\Users\log".to_string()),
            ("normal".to_string(), "value".to_string()),
        ];

        let mut stream_map = StreamMap::new();
        stream_map.insert(
            labels.clone(),
            vec![(1, "{\"message\":\"ok\"}".to_string())],
        );

        let payloads =
            LokiSink::prepare_payloads(&mut stream_map, &HashMap::new(), LOKI_MAX_PAYLOAD_BYTES);
        let retained: u64 = payloads.iter().map(|p| p.row_count).sum();
        let payload = &payloads[0].payload;
        let parsed: serde_json::Value =
            serde_json::from_str(payload).expect("payload must be valid JSON");
        let stream = &parsed["streams"][0]["stream"];

        assert_eq!(retained, 1);
        for (key, value) in labels {
            assert_eq!(stream[&key], value);
        }
    }

    #[test]
    fn prepare_payloads_splits_large_single_stream_payload() {
        let labels: StreamLabels = vec![("app".to_string(), "logfwd".to_string())];
        let mut stream_map = StreamMap::new();
        stream_map.insert(
            labels,
            vec![
                (1, "{\"message\":\"aaaaaaaaaaaaaaaaaaaaaaaa\"}".to_string()),
                (2, "{\"message\":\"bbbbbbbbbbbbbbbbbbbbbbbb\"}".to_string()),
                (3, "{\"message\":\"cccccccccccccccccccccccc\"}".to_string()),
            ],
        );

        let payloads = LokiSink::prepare_payloads(&mut stream_map, &HashMap::new(), 120);
        assert!(
            payloads.len() > 1,
            "payload should be split when max byte budget is small"
        );
        assert_eq!(payloads.iter().map(|p| p.row_count).sum::<u64>(), 3);
        for payload in &payloads {
            let parsed: serde_json::Value =
                serde_json::from_str(&payload.payload).expect("chunk must be valid JSON");
            assert_eq!(parsed["streams"].as_array().map_or(0, Vec::len), 1);
        }
    }

    #[test]
    fn prepare_payloads_handles_multi_stream_batches() {
        let mut stream_map = StreamMap::new();
        stream_map.insert(
            vec![("app".to_string(), "a".to_string())],
            vec![(10, "{\"message\":\"one\"}".to_string())],
        );
        stream_map.insert(
            vec![("app".to_string(), "b".to_string())],
            vec![(11, "{\"message\":\"two\"}".to_string())],
        );

        let payloads = LokiSink::prepare_payloads(&mut stream_map, &HashMap::new(), 1_024);
        assert_eq!(payloads.len(), 2);
        let row_total: u64 = payloads.iter().map(|p| p.row_count).sum();
        assert_eq!(row_total, 2);
    }

    #[test]
    fn factory_reuses_single_client_across_workers() {
        let factory = LokiSinkFactory::new(
            "loki".to_string(),
            "http://127.0.0.1:3100".to_string(),
            None,
            vec![("app".to_string(), "logfwd".to_string())],
            vec![],
            vec![],
            Arc::new(ComponentStats::new()),
        )
        .expect("factory");
        let sink1 = factory.create_sink();
        let sink2 = factory.create_sink();

        assert!(
            Arc::ptr_eq(&sink1.client, &sink2.client),
            "Loki workers should share factory client pool"
        );
    }

    // -----------------------------------------------------------------------
    // Struct conflict column label extraction tests
    // -----------------------------------------------------------------------

    /// Build a batch with `status: Struct { int: Int64, str: Utf8 }`.
    fn make_status_struct_batch(int_val: Option<i64>, str_val: Option<&str>) -> RecordBatch {
        use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{DataType, Field, Fields, Schema};

        let int_field = Arc::new(Field::new("int", DataType::Int64, true));
        let str_field = Arc::new(Field::new("str", DataType::Utf8, true));
        let int_arr: ArrayRef = Arc::new(Int64Array::from(vec![int_val]));
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![str_val]));

        let nulls = NullBuffer::new(arrow::buffer::BooleanBuffer::collect_bool(1, |_| {
            int_val.is_some() || str_val.is_some()
        }));

        let struct_field = Field::new(
            "status",
            DataType::Struct(Fields::from(vec![
                int_field.as_ref().clone(),
                str_field.as_ref().clone(),
            ])),
            true,
        );
        let struct_arr: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![int_field, str_field]),
            vec![int_arr, str_arr],
            Some(nulls),
        ));

        RecordBatch::try_new(Arc::new(Schema::new(vec![struct_field])), vec![struct_arr]).unwrap()
    }

    #[test]
    fn label_extraction_from_struct_conflict_column() {
        // status: Struct{int=200, str=null} — label should be "200" (int cast to str).
        let batch = make_status_struct_batch(Some(200), None);
        let cols = build_col_infos(&batch);
        let status_info = cols.iter().find(|c| c.field_name == "status").unwrap();
        let val = coalesce_as_str(&batch, 0, status_info);
        assert_eq!(val.as_deref(), Some("200"), "int label must be stringified");
    }

    #[test]
    fn label_extraction_prefers_str_over_int() {
        // status: Struct{int=200, str="OK"} — str wins in str_variants ordering.
        let batch = make_status_struct_batch(Some(200), Some("OK"));
        let cols = build_col_infos(&batch);
        let status_info = cols.iter().find(|c| c.field_name == "status").unwrap();
        let val = coalesce_as_str(&batch, 0, status_info);
        assert_eq!(
            val.as_deref(),
            Some("OK"),
            "str variant must win over int in str_variants ordering"
        );
    }

    #[test]
    fn test_escape_json_raw_malformed() {
        // Bug #1048: starts with quote but not valid JSON string
        let malformed = "\"not valid json";
        let escaped = escape_json_raw(malformed);

        // Should be properly escaped and wrapped.
        assert_ne!(
            escaped, malformed,
            "malformed JSON starting with quote should be escaped and wrapped"
        );
        assert_eq!(escaped, "\"\\\"not valid json\"", "should be fully escaped");

        // Object with trailing characters
        let malformed_obj = "{\"key\": \"value\"} trailing";
        let escaped_obj = escape_json_raw(malformed_obj);
        assert_eq!(escaped_obj, "\"{\\\"key\\\": \\\"value\\\"} trailing\"");

        let unterminated = "\"unterminated";
        let escaped_unterminated = escape_json_raw(unterminated);
        assert_eq!(
            escaped_unterminated, "\"\\\"unterminated\"",
            "unterminated string should be fully escaped"
        );

        let foo = "\"foo";
        let escaped_foo = escape_json_raw(foo);
        assert_eq!(
            escaped_foo, "\"\\\"foo\"",
            "foo without closing quote should be fully escaped"
        );
    }

    #[test]
    fn test_escape_json_raw_valid_string() {
        // Valid JSON string should be returned as-is
        let valid = "\"valid json string\"";
        let escaped = escape_json_raw(valid);
        assert_eq!(escaped, valid);
    }

    #[test]
    fn test_escape_json_raw_object() {
        // Valid JSON object must be re-encoded as a JSON string.
        // Loki requires the log-line element to be a JSON string, not an object.
        let obj = "{\"key\": \"value\"}";
        let escaped = escape_json_raw(obj);
        // The object text is wrapped in a JSON string.
        assert_eq!(escaped, "\"{\\\"key\\\": \\\"value\\\"}\"");
        assert_ne!(escaped, obj, "JSON object must not be passed through raw");
    }

    #[test]
    fn test_label_sanitization() {
        assert_eq!(sanitize_loki_label_name("valid_label_1"), "valid_label_1");
        assert_eq!(sanitize_loki_label_name("my.label.name"), "my_label_name");
        assert_eq!(sanitize_loki_label_name("1invalid_start"), "_invalid_start");
        assert_eq!(sanitize_loki_label_name("a-b*c/d"), "a_b_c_d");
        // Empty input must produce a valid label name, not an empty string which
        // Loki would reject (labels must match ^[a-zA-Z_][a-zA-Z0-9_]*$).
        assert_eq!(sanitize_loki_label_name(""), "_");
    }

    /// Regression test for #1670: canonical "timestamp" column must be used as
    /// the Loki entry timestamp when neither `_timestamp` nor `@timestamp` exist.
    #[test]
    fn test_canonical_timestamp_col_used_as_loki_ts() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        // Batch uses the canonical "timestamp" name (OTLP receiver output).
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![5_000i64])),
                Arc::new(arrow::array::StringArray::from(vec!["msg"])),
            ],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 99_999,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].0, 5_000,
            "canonical 'timestamp' column value (5000) must be used, not observed_time_ns (99999)"
        );
    }

    /// Regression test for #1661: when both `@timestamp` and `_timestamp` exist
    /// and `@timestamp` appears FIRST in the schema, the sink must still use
    /// `_timestamp` (the preferred field).
    #[test]
    fn test_timestamp_underscore_preferred_over_at_timestamp_regardless_of_schema_order() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        // @timestamp appears BEFORE _timestamp in the schema.
        // The sink must select _timestamp (1000) not @timestamp (100).
        let schema = Arc::new(Schema::new(vec![
            Field::new(field_names::TIMESTAMP_AT, DataType::Int64, true),
            Field::new(field_names::TIMESTAMP_UNDERSCORE, DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100i64])),
                Arc::new(Int64Array::from(vec![1_000i64])),
                Arc::new(arrow::array::StringArray::from(vec!["hi"])),
            ],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 99_999,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].0, 1_000,
            "_timestamp (1000) must be preferred over @timestamp (100)"
        );
    }

    /// Regression test for #1676: Loki sink must parse ISO 8601 Utf8 timestamp
    /// columns rather than falling back to observed_time_ns.
    ///
    /// The scanner produces Utf8 columns for string-typed timestamp fields in
    /// tailed log files. star_to_flat also produces `_timestamp` as Utf8. The
    /// previous `_ => metadata.observed_time_ns` default silently discarded the
    /// actual log timestamp and stamped all records with the batch ingestion time.
    #[test]
    fn test_utf8_timestamp_column_used_as_loki_ts() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        // _timestamp column is Utf8 (scanner output from tailed log files and
        // star_to_flat output).  The expected nanosecond value for
        // "2024-01-15T10:30:00Z" is 1_705_314_600 * 1_000_000_000.
        let expected_ns: u64 = 1_705_314_600_000_000_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new(field_names::TIMESTAMP_UNDERSCORE, DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["2024-01-15T10:30:00Z"])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 99_999_999_999,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].0, expected_ns,
            "Utf8 _timestamp '2024-01-15T10:30:00Z' must be parsed as {expected_ns} ns, \
             not observed_time_ns ({})",
            metadata.observed_time_ns
        );
    }

    #[test]
    fn sanitize_loki_label_name_rewrites_invalid_chars() {
        assert_eq!(sanitize_loki_label_name("service.name"), "service_name");
        assert_eq!(
            sanitize_loki_label_name("http.status_code"),
            "http_status_code"
        );
        assert_eq!(
            sanitize_loki_label_name("9invalid-prefix"),
            "_invalid_prefix"
        );
        assert_eq!(sanitize_loki_label_name(""), "_");
    }

    #[test]
    fn dynamic_label_columns_use_sanitized_loki_names() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec!["service.name".to_string()],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "service.name",
            DataType::Utf8,
            true,
        )]));
        let values = StringArray::from(vec![Some("checkout")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 1_000,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let key = stream_map.keys().next().unwrap();
        assert_eq!(
            key,
            &vec![("service_name".to_string(), "checkout".to_string())]
        );
    }

    #[test]
    fn static_labels_are_sanitized_in_streams() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![("service.name".to_string(), "frontend".to_string())],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let values = StringArray::from(vec![Some("ok")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 1_000,
        };

        let mut stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let payloads =
            LokiSink::prepare_payloads(&mut stream_map, &HashMap::new(), LOKI_MAX_PAYLOAD_BYTES);
        let retained: u64 = payloads.iter().map(|p| p.row_count).sum();
        let payload = &payloads[0].payload;
        assert_eq!(retained, 1);
        assert!(payload.contains("\"service_name\":\"frontend\""));
        assert!(!payload.contains("\"service.name\":\"frontend\""));
    }

    #[test]
    fn colliding_sanitized_label_columns_keeps_first() {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec!["service.name".to_string(), "service-name".to_string()],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("service.name", DataType::Utf8, true),
            Field::new("service-name", DataType::Utf8, true),
        ]));
        let left: ArrayRef = Arc::new(StringArray::from(vec![Some("checkout")]));
        let right: ArrayRef = Arc::new(StringArray::from(vec![Some("payments")]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 1_000,
        };

        // Collision is warned, not errored. First label wins.
        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        assert!(!stream_map.is_empty(), "should produce at least one stream");
    }

    #[test]
    fn test_negative_timestamp_handling() {
        // Bug #1084: negative i64 timestamps wrap to far-future u64 values
        use arrow::array::Int64Array;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            field_names::TIMESTAMP_UNDERSCORE,
            DataType::Int64,
            true,
        )]));
        let ts_arr = Int64Array::from(vec![Some(-100i64), Some(100i64)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 12345,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let mut entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        entries.sort_by_key(|(ts, _)| *ts);

        // After sorting, the valid timestamp (100) comes first, then the fallback observed_time_ns (12345).
        assert_eq!(entries[0].0, 100, "Positive timestamp should be preserved");
        assert_eq!(
            entries[1].0, metadata.observed_time_ns,
            "Negative timestamp should fall back to observed_time_ns"
        );
    }

    // Regression test for issue #1661: when both @timestamp and _timestamp are present,
    // _timestamp must be preferred regardless of schema column order.
    #[test]
    fn underscore_timestamp_preferred_over_at_timestamp_regardless_of_schema_order() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        // Schema: @timestamp (index 0) BEFORE _timestamp (index 1).
        // The old `position(|f| underscore || at)` would incorrectly pick @timestamp
        // because it appears first in schema order.
        let schema = Arc::new(Schema::new(vec![
            Field::new(field_names::TIMESTAMP_AT, DataType::Int64, true),
            Field::new(field_names::TIMESTAMP_UNDERSCORE, DataType::Int64, true),
        ]));
        let at_ts = 100i64; // @timestamp = 100 ns
        let under_ts = 9999i64; // _timestamp = 9999 ns — this must win
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![at_ts])),
                Arc::new(Int64Array::from(vec![under_ts])),
            ],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 1,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        assert_eq!(entries.len(), 1, "expected exactly one log entry");
        assert_eq!(
            entries[0].0, under_ts as u64,
            "_timestamp ({under_ts}) must be preferred over @timestamp ({at_ts}), \
             even when @timestamp appears first in the schema; got {}",
            entries[0].0
        );
    }

    #[test]
    fn dynamic_label_colliding_with_static_keeps_static() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![("env".to_string(), "prod".to_string())],
            label_columns: vec!["env".to_string()],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new("env", DataType::Utf8, true)]));
        let env_arr = StringArray::from(vec![Some("staging")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(env_arr)]).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 1_000,
        };

        // Collision is warned, dynamic "env" column is dropped. Static wins.
        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        assert!(!stream_map.is_empty());
    }

    #[test]
    fn sanitized_static_label_colliding_with_dynamic_keeps_static() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![("service.name".to_string(), "frontend".to_string())],
            label_columns: vec!["service_name".to_string()],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));
        let values = StringArray::from(vec![Some("checkout")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 1_000,
        };

        // service.name (static) sanitizes to service_name, colliding with
        // the dynamic column. Static wins, dynamic dropped with warning.
        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        assert!(!stream_map.is_empty());
    }

    #[test]
    fn empty_label_value_skipped() {
        // Bug #1385: empty label values were forwarded to Loki, causing HTTP 400.
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec!["namespace".to_string()],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "namespace",
            DataType::Utf8,
            true,
        )]));
        let ns_arr = StringArray::from(vec![Some("")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ns_arr)]).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 1_000,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        // There must be exactly one stream; its key must not include "namespace".
        assert_eq!(stream_map.len(), 1);
        let key = stream_map.keys().next().unwrap();
        assert!(
            !key.iter().any(|(label, _)| label == "namespace"),
            "empty label value must be excluded from stream key; key: {key:?}"
        );
    }

    #[test]
    fn test_null_timestamp_handling() {
        // Bug #1339: null timestamps should fall back to observed_time_ns
        use arrow::array::Int64Array;
        use arrow::datatypes::{Field, Schema};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            field_names::TIMESTAMP_UNDERSCORE,
            DataType::Int64,
            true,
        )]));
        // Array with one null and one valid timestamp
        let ts_arr = Int64Array::from(vec![None, Some(100i64)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 12345,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let mut entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        entries.sort_by_key(|(ts, _)| *ts);

        // Sorted by timestamp: the non-null row (100) comes before the
        // null row's fallback value (observed_time_ns = 12345).
        assert_eq!(entries[0].0, 100, "Valid timestamp should be preserved");
        assert_eq!(
            entries[1].0, metadata.observed_time_ns,
            "Null timestamp should fall back to observed_time_ns"
        );
    }
    /// Regression: Loki sink must read Arrow Timestamp(Nanosecond) columns
    /// and use the actual timestamp value, not fall back to observed_time_ns.
    /// Before the fix, the `DataType::Timestamp` arm was missing entirely and
    /// hit the `_ => metadata.observed_time_ns` default.
    #[test]
    fn test_arrow_timestamp_nanosecond_column_used_as_loki_ts() {
        use arrow::array::TimestampNanosecondArray;
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let expected_ns: i64 = 1_705_314_600_000_000_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                field_names::TIMESTAMP_UNDERSCORE,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![expected_ns])),
                Arc::new(arrow::array::StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 99_999,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].0, expected_ns as u64,
            "Timestamp(Nanosecond) value ({expected_ns}) must be used, not observed_time_ns ({})",
            metadata.observed_time_ns
        );
    }

    #[test]
    fn test_arrow_timestamp_microsecond_column_used_as_loki_ts() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let raw_us: i64 = 1_705_314_600_000_000;
        let expected_ns: u64 = (raw_us * 1_000) as u64;
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_names::TIMESTAMP_UNDERSCORE,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![raw_us]))],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 99_999,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, expected_ns);
    }

    #[test]
    fn test_arrow_timestamp_millisecond_column_used_as_loki_ts() {
        use arrow::array::TimestampMillisecondArray;
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let raw_ms: i64 = 1_705_314_600_000;
        let expected_ns: u64 = (raw_ms * 1_000_000) as u64;
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_names::TIMESTAMP_UNDERSCORE,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMillisecondArray::from(vec![raw_ms]))],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 99_999,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, expected_ns);
    }

    #[test]
    fn test_arrow_timestamp_second_column_used_as_loki_ts() {
        use arrow::array::TimestampSecondArray;
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let raw_s: i64 = 1_705_314_600;
        let expected_ns: u64 = (raw_s * 1_000_000_000) as u64;
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_names::TIMESTAMP_UNDERSCORE,
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampSecondArray::from(vec![raw_s]))],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 99_999,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, expected_ns);
    }

    #[test]
    fn test_arrow_timestamp_negative_or_overflow_falls_back_to_observed_time() {
        use arrow::array::TimestampSecondArray;
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let config = Arc::new(LokiConfig {
            endpoint: "http://localhost".to_string(),
            tenant_id: None,
            static_labels: vec![],
            label_columns: vec![],
            headers: vec![],
        });
        let sink = LokiSink::new(
            "test".to_string(),
            config,
            Arc::new(reqwest::Client::new()),
            Arc::new(ComponentStats::new()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            field_names::TIMESTAMP_UNDERSCORE,
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampSecondArray::from(vec![-1, i64::MAX]))],
        )
        .unwrap();
        let metadata = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 42,
        };

        let stream_map = sink.build_stream_map(&batch, &metadata).unwrap();
        let mut entries: Vec<LokiEntry> = stream_map.values().flatten().cloned().collect();
        entries.sort_by_key(|(ts, _)| *ts);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, metadata.observed_time_ns);
        assert_eq!(entries[1].0, metadata.observed_time_ns);
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
            sort_and_dedup_timestamps(&mut entries, None);
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
            sort_and_dedup_timestamps(&mut entries, None);
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

            sort_and_dedup_timestamps(&mut entries, None);

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
