use std::future::Future;
use std::io;
use std::io::Write as _;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, AsArray, BinaryArray, LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray,
    StringViewArray,
};
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt32Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use flate2::Compression as GzipLevel;
use flate2::write::GzEncoder;

use logfwd_arrow::conflict_schema::normalize_conflict_columns;
use logfwd_config::OtlpProtocol;
use logfwd_core::otlp::{
    self, Severity, bytes_field_size, encode_bytes_field, encode_fixed32, encode_fixed64,
    encode_tag, encode_varint, encode_varint_field, hex_decode, parse_severity,
    parse_timestamp_nanos, varint_len,
};
use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;
use zstd::bulk::Compressor as ZstdCompressor;

use super::{BatchMetadata, Compression};
use crate::http_classify::{self, DEFAULT_RETRY_AFTER_SECS};

mod generated {
    include!("generated/otlp_log_record_fast_v1.rs");
}

// ---------------------------------------------------------------------------
// InstrumentationScope constants
// ---------------------------------------------------------------------------

/// Name emitted in the OTLP `InstrumentationScope.name` field of every `ScopeLogs`.
const SCOPE_NAME: &[u8] = b"logfwd";
/// Version emitted in the OTLP `InstrumentationScope.version` field (from Cargo.toml).
const SCOPE_VERSION: &[u8] = env!("CARGO_PKG_VERSION").as_bytes();
/// Default gRPC outbound message size guardrail.
const DEFAULT_GRPC_MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// OtlpSink
// ---------------------------------------------------------------------------

/// Sends OTLP protobuf LogRecords over gRPC or HTTP.
pub struct OtlpSink {
    name: String,
    endpoint: String,
    protocol: OtlpProtocol,
    compression: Compression,
    headers: Vec<(String, String)>,
    message_field: String,
    pub(crate) encoder_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    grpc_buf: Vec<u8>,
    compressor: Option<ZstdCompressor<'static>>,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ResourceValueRef<'a> {
    Str(&'a str),
    Bytes(&'a [u8]),
    Int(i64),
    Float(u64),
    Bool(bool),
}

/// Per-row resource key: one optional borrowed/scalar value per resource column.
type ResourceKey<'a> = Vec<Option<ResourceValueRef<'a>>>;

/// Per-group scope key (name, version).
type ScopeKey<'a> = (Option<&'a str>, Option<&'a str>);

/// Per-group state: resource key + scope key + byte-range spans of encoded log records.
type ResourceGroup<'a> = (ResourceKey<'a>, ScopeKey<'a>, Vec<(usize, usize)>);

#[derive(Clone, Copy)]
enum FlagsArray<'a> {
    Int64(&'a PrimitiveArray<Int64Type>),
    UInt32(&'a PrimitiveArray<UInt32Type>),
    UInt64(&'a PrimitiveArray<UInt64Type>),
}

impl FlagsArray<'_> {
    #[inline]
    fn value_u32(self, row: usize) -> Option<u32> {
        match self {
            FlagsArray::Int64(arr) => (!arr.is_null(row))
                .then(|| arr.value(row))
                .and_then(|raw| u32::try_from(raw).ok()),
            FlagsArray::UInt32(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            FlagsArray::UInt64(arr) => (!arr.is_null(row))
                .then(|| arr.value(row))
                .and_then(|raw| u32::try_from(raw).ok()),
        }
    }
}

impl OtlpSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        endpoint: String,
        protocol: OtlpProtocol,
        compression: Compression,
        headers: Vec<(String, String)>,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        // For gRPC, ensure the endpoint has the correct service path.
        // Users typically configure just the host:port (e.g., "http://collector:4317").
        // The gRPC spec requires the full path: /package.Service/Method (#1059)
        let endpoint = if protocol == OtlpProtocol::Grpc {
            let trimmed = endpoint.trim_end_matches('/');
            if trimmed.ends_with("/Export") || trimmed.ends_with("/LogsService/Export") {
                endpoint
            } else {
                format!("{trimmed}/opentelemetry.proto.collector.logs.v1.LogsService/Export")
            }
        } else {
            endpoint
        };
        let compressor = match compression {
            Compression::Zstd => Some(ZstdCompressor::new(1).map_err(io::Error::other)?),
            _ => None,
        };
        Ok(OtlpSink {
            name,
            endpoint,
            protocol,
            compression,
            headers,
            message_field: field_names::BODY.to_string(),
            encoder_buf: Vec::with_capacity(64 * 1024),
            compress_buf: Vec::with_capacity(64 * 1024),
            grpc_buf: Vec::with_capacity(64 * 1024),
            compressor,
            client,
            stats,
        })
    }

    /// Override the source column used for OTLP `LogRecord.body`.
    ///
    /// The default is the canonical `body` field. Use this builder when a
    /// pipeline stores the primary log message under a different string column.
    #[inline]
    #[must_use]
    pub fn with_message_field(mut self, message_field: String) -> Self {
        self.message_field = message_field;
        self
    }

    /// Encode a full ExportLogsServiceRequest from a RecordBatch.
    /// Returns the raw protobuf bytes in `self.encoder_buf`.
    pub fn encode_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) {
        self.encode_batch_with_row_encoder(batch, metadata, encode_row_as_log_record);
    }

    /// Benchmark/reference path: encode using the generated fast-row encoder.
    pub fn encode_batch_generated_fast(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) {
        self.encode_batch_with_row_encoder(
            batch,
            metadata,
            generated::encode_row_as_log_record_fast_v1,
        );
    }

    /// Benchmark/reference path: encode only the concatenated LogRecord payloads using the
    /// handwritten row encoder.
    pub fn encode_rows_only_for_bench(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) {
        self.encode_rows_only_with_row_encoder(batch, metadata, encode_row_as_log_record);
    }

    /// Benchmark/reference path: encode only the concatenated LogRecord payloads using the
    /// generated fast row encoder.
    pub fn encode_rows_only_generated_fast_for_bench(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
    ) {
        self.encode_rows_only_with_row_encoder(
            batch,
            metadata,
            generated::encode_row_as_log_record_fast_v1,
        );
    }

    fn encode_batch_with_row_encoder(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        mut encode_row: impl FnMut(&BatchColumns<'_>, usize, &BatchMetadata, &mut Vec<u8>),
    ) {
        self.encoder_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        // Normalize any conflict struct columns (e.g. `status: Struct { int, str }`)
        // to flat Utf8 columns before encoding. Without this, struct columns would be
        // silently dropped, causing data loss when no SQL transform is applied upstream.
        let normalized;
        let batch = if batch
            .schema()
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::Struct(_)))
        {
            normalized = normalize_conflict_columns(batch.clone());
            &normalized
        } else {
            batch
        };

        // Resolve column roles and downcast arrays once for the whole batch.
        let columns = resolve_batch_columns(batch, self.message_field.as_str());

        // Phase 1: encode all LogRecords and assign each row to a resource group.
        let mut records_buf: Vec<u8> =
            Vec::with_capacity(estimate_records_buf_capacity(num_rows, &columns));
        let mut grouped_ranges: Vec<ResourceGroup<'_>> = Vec::new();

        // Fast path: when there are no resource or scope columns every row belongs to
        // the same single group.  This is the overwhelmingly common case for file / CRI
        // log forwarding and avoids a per-row HashMap lookup, key Vec allocation, and
        // scope-column null check.
        if columns.resource_cols.is_empty()
            && columns.scope_name_col.is_none()
            && columns.scope_version_col.is_none()
        {
            let mut record_ranges: Vec<(usize, usize)> = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let start = records_buf.len();
                encode_row(&columns, row, metadata, &mut records_buf);
                record_ranges.push((start, records_buf.len()));
            }
            grouped_ranges.push((Vec::new(), (None, None), record_ranges));
        } else {
            let mut group_index_by_key: std::collections::HashMap<
                (ResourceKey<'_>, ScopeKey<'_>),
                usize,
            > = std::collections::HashMap::new();

            for row in 0..num_rows {
                let mut key: ResourceKey<'_> = Vec::with_capacity(columns.resource_cols.len());
                for (field_name, attr) in &columns.resource_cols {
                    key.push(attr.value_ref(row, field_name));
                }
                let scope_name = columns.scope_name_col.as_ref().and_then(|(_, arr)| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    }
                });
                let scope_version = columns.scope_version_col.as_ref().and_then(|(_, arr)| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    }
                });
                let scope_key = (scope_name, scope_version);

                let group_idx = match group_index_by_key.entry((key, scope_key)) {
                    std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let idx = grouped_ranges.len();
                        let (group_key, group_scope) = entry.key();
                        grouped_ranges.push((group_key.clone(), *group_scope, Vec::new()));
                        entry.insert(idx);
                        idx
                    }
                };

                let start = records_buf.len();
                encode_row(&columns, row, metadata, &mut records_buf);
                grouped_ranges[group_idx].2.push((start, records_buf.len()));
            }
        }

        // Phase 2: compute sizes bottom-up per resource group.
        let mut grouped_resource_msgs: Vec<Vec<u8>> = Vec::with_capacity(grouped_ranges.len());
        let mut grouped_resource_inner_sizes: Vec<usize> = Vec::with_capacity(grouped_ranges.len());
        let mut grouped_scope_inner_sizes: Vec<usize> = Vec::with_capacity(grouped_ranges.len());
        let mut grouped_scope_values: Vec<(Vec<u8>, Vec<u8>)> =
            Vec::with_capacity(grouped_ranges.len());
        let mut request_size = 0usize;

        for (key, scope_key, record_ranges) in &grouped_ranges {
            let scope_name = scope_key
                .0
                .unwrap_or_else(|| std::str::from_utf8(SCOPE_NAME).unwrap_or("logfwd"));
            let scope_version = scope_key
                .1
                .unwrap_or_else(|| std::str::from_utf8(SCOPE_VERSION).unwrap_or(""));
            let scope_name_bytes = scope_name.as_bytes().to_vec();
            let scope_version_bytes = scope_version.as_bytes().to_vec();
            let instrumentation_scope_inner_size =
                bytes_field_size(otlp::INSTRUMENTATION_SCOPE_NAME, scope_name_bytes.len())
                    + bytes_field_size(
                        otlp::INSTRUMENTATION_SCOPE_VERSION,
                        scope_version_bytes.len(),
                    );
            let mut scope_logs_inner_size =
                bytes_field_size(otlp::SCOPE_LOGS_SCOPE, instrumentation_scope_inner_size);
            for &(start, end) in record_ranges {
                scope_logs_inner_size +=
                    bytes_field_size(otlp::SCOPE_LOGS_LOG_RECORDS, end - start);
            }

            let mut resource_msg: Vec<u8> = Vec::new();
            for (k, v) in metadata.resource_attrs.as_ref() {
                encode_key_value_string(
                    &mut resource_msg,
                    otlp::RESOURCE_ATTRIBUTES,
                    k.as_bytes(),
                    v.as_bytes(),
                );
            }
            for ((key_name, _), value) in columns.resource_cols.iter().zip(key.iter()) {
                if let Some(v) = value {
                    match v {
                        ResourceValueRef::Str(v) => encode_key_value_string(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            v.as_bytes(),
                        ),
                        ResourceValueRef::Bytes(v) => encode_key_value_bytes(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            v,
                        ),
                        ResourceValueRef::Int(v) => encode_key_value_int(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            *v,
                        ),
                        ResourceValueRef::Float(v) => encode_key_value_double(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            f64::from_bits(*v),
                        ),
                        ResourceValueRef::Bool(v) => encode_key_value_bool(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            *v,
                        ),
                    }
                }
            }

            let mut resource_inner_size =
                bytes_field_size(otlp::RESOURCE_LOGS_SCOPE_LOGS, scope_logs_inner_size);
            if !resource_msg.is_empty() {
                resource_inner_size +=
                    bytes_field_size(otlp::RESOURCE_LOGS_RESOURCE, resource_msg.len());
            }
            request_size +=
                bytes_field_size(otlp::EXPORT_LOGS_REQUEST_RESOURCE_LOGS, resource_inner_size);

            grouped_resource_msgs.push(resource_msg);
            grouped_resource_inner_sizes.push(resource_inner_size);
            grouped_scope_inner_sizes.push(scope_logs_inner_size);
            grouped_scope_values.push((scope_name_bytes, scope_version_bytes));
        }

        // Phase 3: write final protobuf with one ResourceLogs per group.
        self.encoder_buf.reserve(request_size + 16);
        for (group_idx, (_key, _scope_key, record_ranges)) in grouped_ranges.iter().enumerate() {
            let (scope_name, scope_version) = &grouped_scope_values[group_idx];
            let instrumentation_scope_inner_size =
                bytes_field_size(otlp::INSTRUMENTATION_SCOPE_NAME, scope_name.len())
                    + bytes_field_size(otlp::INSTRUMENTATION_SCOPE_VERSION, scope_version.len());
            encode_tag(
                &mut self.encoder_buf,
                otlp::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
                otlp::WIRE_TYPE_LEN,
            );
            encode_varint(
                &mut self.encoder_buf,
                grouped_resource_inner_sizes[group_idx] as u64,
            );

            let resource_msg = &grouped_resource_msgs[group_idx];
            if !resource_msg.is_empty() {
                encode_bytes_field(
                    &mut self.encoder_buf,
                    otlp::RESOURCE_LOGS_RESOURCE,
                    resource_msg,
                );
            }

            encode_tag(
                &mut self.encoder_buf,
                otlp::RESOURCE_LOGS_SCOPE_LOGS,
                otlp::WIRE_TYPE_LEN,
            );
            encode_varint(
                &mut self.encoder_buf,
                grouped_scope_inner_sizes[group_idx] as u64,
            );

            encode_tag(
                &mut self.encoder_buf,
                otlp::SCOPE_LOGS_SCOPE,
                otlp::WIRE_TYPE_LEN,
            );
            encode_varint(
                &mut self.encoder_buf,
                instrumentation_scope_inner_size as u64,
            );
            encode_bytes_field(
                &mut self.encoder_buf,
                otlp::INSTRUMENTATION_SCOPE_NAME,
                scope_name,
            );
            encode_bytes_field(
                &mut self.encoder_buf,
                otlp::INSTRUMENTATION_SCOPE_VERSION,
                scope_version,
            );

            for &(start, end) in record_ranges {
                encode_bytes_field(
                    &mut self.encoder_buf,
                    otlp::SCOPE_LOGS_LOG_RECORDS,
                    &records_buf[start..end],
                );
            }
        }
    }

    fn encode_rows_only_with_row_encoder(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        mut encode_row: impl FnMut(&BatchColumns<'_>, usize, &BatchMetadata, &mut Vec<u8>),
    ) {
        self.encoder_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        let normalized;
        let batch = if batch
            .schema()
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::Struct(_)))
        {
            normalized = normalize_conflict_columns(batch.clone());
            &normalized
        } else {
            batch
        };

        let columns = resolve_batch_columns(batch, self.message_field.as_str());
        self.encoder_buf.reserve(num_rows * 128);

        for row in 0..num_rows {
            encode_row(&columns, row, metadata, &mut self.encoder_buf);
        }
    }

    /// Returns the raw encoded OTLP protobuf payload produced by one of the
    /// encode methods on this sink.
    pub fn encoded_payload(&self) -> &[u8] {
        &self.encoder_buf
    }
}

fn estimate_records_buf_capacity(num_rows: usize, columns: &BatchColumns<'_>) -> usize {
    const MIN_RECORD_BYTES: usize = 128;
    // 64 bytes/attr covers typical string attribute values (~30 bytes) plus
    // protobuf framing overhead (~27 bytes key+tag+varint) with some margin.
    // Wide batches with 27 attrs need ~14.5 MB for 10K rows; at 48 bytes/attr
    // the estimate was 13.9 MB — slightly short, causing one realloc per batch.
    const ATTR_BYTES_HINT: usize = 64;
    const MAX_INITIAL_RECORDS_BUF: usize = 64 * 1024 * 1024;

    let hinted_attrs = columns.attribute_cols.len().min(64);
    let bytes_per_row = MIN_RECORD_BYTES.saturating_add(hinted_attrs * ATTR_BYTES_HINT);
    num_rows
        .saturating_mul(bytes_per_row)
        .min(MAX_INITIAL_RECORDS_BUF)
}

impl OtlpSink {
    /// Compress, frame, and send the encoded payload via reqwest.
    ///
    /// Returns `SendResult::RetryAfter` on 429, `SendResult::Ok` on success,
    /// and surfaces transient 5xx / network errors for worker-pool retry.
    async fn send_payload(&mut self, batch_rows: u64) -> io::Result<super::sink::SendResult> {
        if self.encoder_buf.is_empty() {
            return Ok(super::sink::SendResult::Ok);
        }

        let payload: &[u8] = match self.compression {
            Compression::Zstd => {
                if let Some(ref mut compressor) = self.compressor {
                    let bound = zstd::zstd_safe::compress_bound(self.encoder_buf.len());
                    self.compress_buf.clear();
                    self.compress_buf.reserve(bound);
                    let compressed_len = compressor
                        .compress_to_buffer(&self.encoder_buf, &mut self.compress_buf)
                        .map_err(io::Error::other)?;
                    self.compress_buf.truncate(compressed_len);
                    &self.compress_buf
                } else {
                    &self.encoder_buf
                }
            }
            Compression::Gzip => {
                let mut compress_buf = std::mem::take(&mut self.compress_buf);
                compress_buf.clear();
                let mut encoder = GzEncoder::new(compress_buf, GzipLevel::fast());
                encoder.write_all(&self.encoder_buf)?;
                self.compress_buf = encoder.finish()?;
                &self.compress_buf
            }
            Compression::None => &self.encoder_buf,
        };

        let content_type = match self.protocol {
            OtlpProtocol::Grpc => "application/grpc",
            OtlpProtocol::Http => "application/x-protobuf",
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported OTLP protocol '{other}'"),
                ));
            }
        };

        // For gRPC, prepend the 5-byte length-prefixed frame header required by the
        // gRPC wire protocol.
        //
        // The compressed flag reflects whether this specific payload is compressed
        // (i.e. Zstd was configured AND the compressor is present). If the compressor
        // was not initialized for some reason, the payload falls back to uncompressed
        // and the flag must be 0x00.
        let payload_is_compressed = match self.compression {
            Compression::Zstd => self.compressor.is_some(),
            Compression::Gzip => true,
            Compression::None => false,
        };
        let payload: &[u8] = if self.protocol == OtlpProtocol::Grpc {
            if payload.len() > DEFAULT_GRPC_MAX_MESSAGE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "OTLP gRPC payload too large: {} bytes exceeds max {} bytes",
                        payload.len(),
                        DEFAULT_GRPC_MAX_MESSAGE_BYTES
                    ),
                ));
            }
            write_grpc_frame(&mut self.grpc_buf, payload, payload_is_compressed)?;
            &self.grpc_buf
        } else {
            payload
        };

        let mut req = self.client.post(&self.endpoint);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", content_type);
        if payload_is_compressed {
            // gRPC compression is signaled via the wire-frame compressed flag byte
            // and the `grpc-encoding` header (per the gRPC-over-HTTP/2 spec).
            // Plain HTTP/protobuf uses `Content-Encoding` instead.
            if self.protocol == OtlpProtocol::Grpc {
                let encoding = match self.compression {
                    Compression::Zstd => "zstd",
                    Compression::Gzip => "gzip",
                    Compression::None => unreachable!("header only set when compressed"),
                };
                req = req.header("grpc-encoding", encoding);
            } else {
                let encoding = match self.compression {
                    Compression::Zstd => "zstd",
                    Compression::Gzip => "gzip",
                    Compression::None => unreachable!("header only set when compressed"),
                };
                req = req.header("Content-Encoding", encoding);
            }
        }

        let compressed_len = payload.len();
        match req.body(payload.to_vec()).send().await {
            Ok(response) => {
                let status = response.status();

                if status.is_success() {
                    if self.protocol == OtlpProtocol::Grpc
                        && let Some(send_result) = classify_grpc_status_headers(response.headers())
                    {
                        return Ok(send_result);
                    }
                    self.stats.inc_lines(batch_rows);
                    self.stats.inc_bytes(self.encoder_buf.len() as u64);
                    let span = tracing::Span::current();
                    span.record("req_bytes", self.encoder_buf.len() as u64);
                    span.record("cmp_bytes", compressed_len as u64);
                    return Ok(super::sink::SendResult::Ok);
                }

                // Non-success — read body for error detail, then classify.
                let retry_after = response.headers().get("Retry-After").cloned();
                let detail = response
                    .bytes()
                    .await
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default();
                if let Some(send_result) = http_classify::classify_http_status(
                    status.as_u16(),
                    retry_after.as_ref(),
                    &format!("OTLP: {detail}"),
                ) {
                    return Ok(send_result);
                }
                // classify_http_status handles all non-2xx; unreachable in practice.
                Err(io::Error::other(format!(
                    "OTLP request failed with status {status}: {detail}"
                )))
            }
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }
}

/// Classify gRPC application status from response headers.
///
/// OTLP/gRPC uses HTTP 200 for transport-level success and reports application errors
/// in `grpc-status`/`grpc-message`.
///
/// Behavior:
/// - Header-only or trailers-only responses where `grpc-status` is exposed in the initial
///   header map are classified and returned.
/// - Normal responses that send `grpc-status` only in the trailing headers are currently
///   treated as transport success because reqwest does not expose HTTP/2 trailing headers
///   via the response header map used here.
fn classify_grpc_status_headers(
    headers: &reqwest::header::HeaderMap,
) -> Option<super::sink::SendResult> {
    let grpc_status = headers.get("grpc-status")?;
    let code = grpc_status
        .to_str()
        .unwrap_or("unknown")
        .trim()
        .parse::<u32>()
        .unwrap_or(2); // default to UNKNOWN
    if code == 0 {
        return None;
    }
    let msg = headers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    Some(match code {
        1 | 4 | 10 | 14 => {
            super::sink::SendResult::IoError(io::Error::other(format!("gRPC error {code}: {msg}")))
        }
        8 => super::sink::SendResult::RetryAfter(Duration::from_secs(DEFAULT_RETRY_AFTER_SECS)),
        _ => super::sink::SendResult::Rejected(format!("gRPC error {code}: {msg}")),
    })
}

impl super::sink::Sink for OtlpSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = super::sink::SendResult> + Send + 'a>> {
        Box::pin(async move {
            self.encode_batch(batch, metadata);
            let rows = batch.num_rows() as u64;
            match self.send_payload(rows).await {
                Ok(r) => r,
                Err(e) => super::sink::SendResult::from_io_error(e),
            }
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// OtlpSinkFactory
// ---------------------------------------------------------------------------

/// Creates [`OtlpSink`] instances for the output worker pool.
///
/// All workers share a single `reqwest::Client` (which is internally
/// `Arc`-wrapped) so they reuse the same connection pool, TLS sessions,
/// and DNS cache.
pub struct OtlpSinkFactory {
    name: String,
    endpoint: String,
    protocol: OtlpProtocol,
    compression: Compression,
    headers: Vec<(String, String)>,
    message_field: String,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

impl OtlpSinkFactory {
    /// Create a new factory.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        endpoint: String,
        protocol: OtlpProtocol,
        compression: Compression,
        headers: Vec<(String, String)>,
        message_field: String,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Ok(OtlpSinkFactory {
            name,
            endpoint,
            protocol,
            compression,
            headers,
            message_field,
            client,
            stats,
        })
    }
}

impl super::sink::SinkFactory for OtlpSinkFactory {
    fn create(&self) -> io::Result<Box<dyn super::sink::Sink>> {
        let sink = OtlpSink::new(
            self.name.clone(),
            self.endpoint.clone(),
            self.protocol,
            self.compression,
            self.headers.clone(),
            self.client.clone(),
            Arc::clone(&self.stats),
        )?
        .with_message_field(self.message_field.clone());
        Ok(Box::new(sink))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Pre-downcast array variant for an attribute column.
#[derive(Clone, Copy)]
enum OtlpStrCol<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
    LargeUtf8(&'a LargeStringArray),
}

impl OtlpStrCol<'_> {
    #[inline(always)]
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(arr) => arr.is_null(row),
            Self::Utf8View(arr) => arr.is_null(row),
            Self::LargeUtf8(arr) => arr.is_null(row),
        }
    }

    #[inline(always)]
    fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(arr) => arr.value(row),
            Self::Utf8View(arr) => arr.value(row),
            Self::LargeUtf8(arr) => arr.value(row),
        }
    }
}

enum AttrArray<'a> {
    Str(OtlpStrCol<'a>),
    OtherStr(&'a dyn Array),
    PreformattedStr(Vec<Option<String>>),
    Bytes(&'a BinaryArray),
    LargeBytes(&'a LargeBinaryArray),
    Int(&'a PrimitiveArray<Int64Type>),
    UInt(&'a PrimitiveArray<UInt64Type>),
    Float(&'a PrimitiveArray<Float64Type>),
    Bool(&'a arrow::array::BooleanArray),
}

impl AttrArray<'_> {
    #[inline(always)]
    fn value_ref(&self, row: usize, field_name: &str) -> Option<ResourceValueRef<'_>> {
        match self {
            Self::Str(arr) => (!arr.is_null(row)).then(|| ResourceValueRef::Str(arr.value(row))),
            Self::OtherStr(arr) => {
                if !arr.is_null(row) {
                    tracing::warn!(
                        column = field_name,
                        row,
                        "skipping OTLP resource attribute: unsupported non-string column was not preformatted"
                    );
                }
                None
            }
            Self::PreformattedStr(values) => values
                .get(row)
                .and_then(|value| value.as_deref())
                .map(ResourceValueRef::Str),
            Self::Bytes(arr) => {
                (!arr.is_null(row)).then(|| ResourceValueRef::Bytes(arr.value(row)))
            }
            Self::LargeBytes(arr) => {
                (!arr.is_null(row)).then(|| ResourceValueRef::Bytes(arr.value(row)))
            }
            Self::Int(arr) => (!arr.is_null(row)).then(|| ResourceValueRef::Int(arr.value(row))),
            Self::UInt(arr) => {
                if arr.is_null(row) {
                    return None;
                }
                let value = arr.value(row);
                match i64::try_from(value) {
                    Ok(value) => Some(ResourceValueRef::Int(value)),
                    Err(_) => {
                        tracing::warn!(
                            column = field_name,
                            row,
                            value,
                            "skipping OTLP resource attribute: UInt64 value exceeds AnyValue.int_value range"
                        );
                        None
                    }
                }
            }
            Self::Float(arr) => {
                (!arr.is_null(row)).then(|| ResourceValueRef::Float(arr.value(row).to_bits()))
            }
            Self::Bool(arr) => (!arr.is_null(row)).then(|| ResourceValueRef::Bool(arr.value(row))),
        }
    }
}

/// Per-column attribute metadata, resolved once per batch.
///
/// Carries pre-encoded key bytes alongside the downcast array so the per-row
/// encoding loop can skip recomputing the key tag + varint + bytes on every
/// iteration.
struct ColAttr<'a> {
    /// Original field name used for error messages and fallback formatting.
    name: String,
    /// Pre-encoded `encode_bytes_field(KEY_VALUE_KEY, name_bytes)` — constant
    /// for every row in the batch.  Written with a single `extend_from_slice`
    /// instead of three separate encode_tag / encode_varint / extend calls.
    key_encoding: Vec<u8>,
    /// `bytes_field_size(KEY_VALUE_KEY, name.len())` — constant per column.
    /// Used directly when computing outer KV message size on each row.
    kv_key_cost: usize,
    /// Pre-computed `kv_key_cost + 4` used by the short-string fast path.
    ///
    /// When `string_value.len() <= 125` all intermediate length varints fit in
    /// one byte, so the outer KV length is simply `short_kv_inner_base +
    /// value.len()` — two additions replace four `varint_len` calls.
    short_kv_inner_base: usize,
    /// True when `array.null_count() > 0`.  When false the per-row `is_null`
    /// check is skipped entirely, eliminating a null-bitmap pointer fetch and
    /// branch on every row for columns that never contain nulls.
    has_nulls: bool,
    /// The downcast Arrow array.
    array: AttrArray<'a>,
}

/// Pre-resolved column roles and downcast arrays for one RecordBatch.
///
/// Built once in `encode_batch` before the per-row loop to avoid
/// re-scanning the schema and re-downcasting arrays on every row.
struct BatchColumns<'a> {
    /// Downcast array for the timestamp column (e.g. "2024-01-15T10:30:00Z").
    timestamp_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Raw array for Int64/UInt64/Timestamp timestamp columns (e.g. time_unix_nano from OTLP
    /// receiver). Used when the column is not a string type so `timestamp_col` cannot be set.
    timestamp_num_col: Option<(usize, &'a dyn Array)>,
    /// Downcast array for the level/severity column (e.g. "ERROR").
    level_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Downcast array for the primary message/body column.
    body_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Downcast array for the `trace_id` column (32 hex chars → 16-byte OTLP field 9).
    trace_id_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Downcast array for the `span_id` column (16 hex chars → 8-byte OTLP field 10).
    span_id_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Downcast array for the `flags` / `trace_flags` column (uint32, OTLP field 8).
    flags_col: Option<(usize, FlagsArray<'a>)>,
    /// Severity number column (Int64, OTLP severity_number).
    severity_num_col: Option<(usize, &'a PrimitiveArray<Int64Type>)>,
    /// Observed timestamp column (Int64, nanoseconds).
    observed_ts_col: Option<(usize, &'a dyn Array)>,
    /// Scope name column.
    scope_name_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Scope version column.
    scope_version_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Non-special attribute columns with pre-encoded key bytes.
    attribute_cols: Vec<ColAttr<'a>>,
    /// Resource attribute columns promoted to OTLP Resource attributes.
    resource_cols: Vec<(String, AttrArray<'a>)>,
}

fn resolve_otlp_str_col(col: &dyn Array) -> Option<OtlpStrCol<'_>> {
    match col.data_type() {
        DataType::Utf8 => Some(OtlpStrCol::Utf8(col.as_string::<i32>())),
        DataType::Utf8View => Some(OtlpStrCol::Utf8View(col.as_string_view())),
        DataType::LargeUtf8 => Some(OtlpStrCol::LargeUtf8(col.as_string::<i64>())),
        _ => None,
    }
}

/// Scan the batch schema once and resolve column roles and downcast arrays.
fn resolve_batch_columns<'a>(batch: &'a RecordBatch, message_field: &str) -> BatchColumns<'a> {
    let schema = batch.schema();
    let mut timestamp_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut timestamp_num_col: Option<(usize, &dyn Array)> = None;
    let mut level_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut body_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut trace_id_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut span_id_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut flags_col: Option<(usize, FlagsArray<'_>)> = None;
    // Column indices to exclude from attributes.
    let mut excluded = vec![false; schema.fields().len()];

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_name = field.name().as_str();
        if field_names::is_internal_column(col_name) {
            excluded[idx] = true;
            continue;
        }
        let field_name = col_name;
        match field_name {
            name if field_names::matches_any(
                name,
                field_names::TIMESTAMP,
                field_names::TIMESTAMP_VARIANTS,
            ) && timestamp_col.is_none()
                && timestamp_num_col.is_none() =>
            {
                if let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref()) {
                    timestamp_col = Some((idx, arr));
                    excluded[idx] = true;
                } else if matches!(
                    field.data_type(),
                    DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _)
                ) {
                    timestamp_num_col = Some((idx, batch.column(idx).as_ref()));
                    excluded[idx] = true;
                }
            }
            name if field_names::matches_any(
                name,
                field_names::SEVERITY,
                field_names::SEVERITY_VARIANTS,
            ) =>
            {
                if level_col.is_none()
                    && let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref())
                {
                    level_col = Some((idx, arr));
                    excluded[idx] = true;
                }
            }
            field_names::TRACE_ID => {
                if trace_id_col.is_none()
                    && let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref())
                {
                    trace_id_col = Some((idx, arr));
                    excluded[idx] = true;
                }
            }
            field_names::SPAN_ID => {
                if span_id_col.is_none()
                    && let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref())
                {
                    span_id_col = Some((idx, arr));
                    excluded[idx] = true;
                }
            }
            name if field_names::matches_any(
                name,
                field_names::TRACE_FLAGS,
                field_names::TRACE_FLAGS_VARIANTS,
            ) && flags_col.is_none() =>
            {
                let col = batch.column(idx);
                let resolved = match field.data_type() {
                    DataType::Int64 => Some(FlagsArray::Int64(col.as_primitive::<Int64Type>())),
                    DataType::UInt32 => Some(FlagsArray::UInt32(col.as_primitive::<UInt32Type>())),
                    DataType::UInt64 => Some(FlagsArray::UInt64(col.as_primitive::<UInt64Type>())),
                    _ => None,
                };
                if let Some(flags_arr) = resolved {
                    flags_col = Some((idx, flags_arr));
                    excluded[idx] = true;
                }
            }
            name if name == message_field
                || field_names::matches_any(
                    name,
                    field_names::BODY,
                    field_names::BODY_VARIANTS,
                ) =>
            {
                if let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref()) {
                    // Canonical "body" always wins; among aliases, first match wins.
                    if body_col.is_none() || name == field_names::BODY {
                        body_col = Some((idx, arr));
                    }
                }
            }
            _ => {}
        }
    }
    if let Some((idx, _)) = body_col {
        excluded[idx] = true;
    }

    // --- Second pass: new well-known columns and attribute/resource classification ---
    let mut severity_num_col: Option<(usize, &PrimitiveArray<Int64Type>)> = None;
    let mut observed_ts_col: Option<(usize, &dyn Array)> = None;
    let mut scope_name_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut scope_version_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut attribute_cols: Vec<ColAttr<'_>> = Vec::new();
    let mut resource_cols: Vec<(String, AttrArray<'_>)> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if excluded[idx] {
            continue;
        }
        let field_name = field.name().as_str();

        // Severity number — use directly instead of deriving from text.
        if field_name == field_names::SEVERITY_NUMBER
            && severity_num_col.is_none()
            && matches!(field.data_type(), DataType::Int64)
        {
            severity_num_col = Some((idx, batch.column(idx).as_primitive::<Int64Type>()));
            continue;
        }

        // Observed timestamp.
        if field_name == field_names::OBSERVED_TIMESTAMP
            && observed_ts_col.is_none()
            && matches!(
                field.data_type(),
                DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _)
            )
        {
            observed_ts_col = Some((idx, batch.column(idx).as_ref()));
            continue;
        }

        // Scope metadata.
        if field_name == field_names::SCOPE_NAME && scope_name_col.is_none() {
            scope_name_col = resolve_otlp_str_col(batch.column(idx).as_ref()).map(|arr| (idx, arr));
            if scope_name_col.is_some() {
                continue;
            }
        }
        if field_name == field_names::SCOPE_VERSION && scope_version_col.is_none() {
            scope_version_col =
                resolve_otlp_str_col(batch.column(idx).as_ref()).map(|arr| (idx, arr));
            if scope_version_col.is_some() {
                continue;
            }
        }

        let stripped_resource_key = field_name.strip_prefix(field_names::DEFAULT_RESOURCE_PREFIX);
        if matches!(stripped_resource_key, Some("")) {
            tracing::warn!(
                column = field_name,
                resource_prefix = field_names::DEFAULT_RESOURCE_PREFIX,
                "dropping resource column with empty OTLP key after prefix stripping"
            );
            continue;
        }
        if let Some(resource_key) = stripped_resource_key.map(str::to_string) {
            let resource_attr = match field.data_type() {
                DataType::Int64 => AttrArray::Int(batch.column(idx).as_primitive::<Int64Type>()),
                DataType::UInt64 => AttrArray::UInt(batch.column(idx).as_primitive::<UInt64Type>()),
                DataType::Float64 => {
                    AttrArray::Float(batch.column(idx).as_primitive::<Float64Type>())
                }
                DataType::Boolean => AttrArray::Bool(batch.column(idx).as_boolean()),
                DataType::Binary => {
                    let Some(arr) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() else {
                        continue;
                    };
                    AttrArray::Bytes(arr)
                }
                DataType::LargeBinary => {
                    let Some(arr) = batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                    else {
                        continue;
                    };
                    AttrArray::LargeBytes(arr)
                }
                DataType::Struct(_) => {
                    tracing::warn!(
                        column = field_name,
                        data_type = ?field.data_type(),
                        "dropping struct resource column; OTLP Resource attributes only support scalar values"
                    );
                    continue;
                }
                _ => resolve_otlp_str_col(batch.column(idx).as_ref()).map_or_else(
                    || {
                        AttrArray::PreformattedStr(preformat_non_string_attr_column(
                            batch.column(idx).as_ref(),
                            field_name,
                        ))
                    },
                    AttrArray::Str,
                ),
            };
            resource_cols.push((resource_key, resource_attr));
            continue;
        }
        // Dispatch on the actual Arrow DataType, not the column name suffix.
        // A SQL transform may produce a column whose name suffix disagrees with
        // its real type (e.g. `SELECT level_str AS count_int`); using
        // `field.data_type()` avoids an `as_primitive` panic in that case.
        let attr = match field.data_type() {
            DataType::Int64 => AttrArray::Int(batch.column(idx).as_primitive::<Int64Type>()),
            DataType::UInt64 => AttrArray::UInt(batch.column(idx).as_primitive::<UInt64Type>()),
            DataType::Float64 => AttrArray::Float(batch.column(idx).as_primitive::<Float64Type>()),
            DataType::Boolean => AttrArray::Bool(batch.column(idx).as_boolean()),
            DataType::Binary => {
                let Some(arr) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() else {
                    continue;
                };
                AttrArray::Bytes(arr)
            }
            DataType::LargeBinary => {
                let Some(arr) = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                else {
                    continue;
                };
                AttrArray::LargeBytes(arr)
            }
            // Non-conflict struct columns (e.g. nested objects not produced by the
            // type-conflict builder) cannot be encoded as a single typed OTLP attribute.
            // Conflict structs (Struct { int, str, float, bool }) are already normalized
            // to flat Utf8 by `encode_batch` before this function is called.
            DataType::Struct(_) => {
                tracing::warn!(
                    column = field.name().as_str(),
                    "non-conflict struct column cannot be encoded as OTLP attribute, skipping"
                );
                continue;
            }
            _ => resolve_otlp_str_col(batch.column(idx).as_ref()).map_or_else(
                || AttrArray::OtherStr(batch.column(idx).as_ref()),
                AttrArray::Str,
            ),
        };
        let name = field_name.to_string();
        let mut key_encoding = Vec::with_capacity(2 + name.len());
        encode_bytes_field(&mut key_encoding, otlp::KEY_VALUE_KEY, name.as_bytes());
        let kv_key_cost = bytes_field_size(otlp::KEY_VALUE_KEY, name.len());
        let short_kv_inner_base = kv_key_cost + 4;
        let has_nulls = batch.column(idx).null_count() > 0;
        attribute_cols.push(ColAttr {
            name,
            key_encoding,
            kv_key_cost,
            short_kv_inner_base,
            has_nulls,
            array: attr,
        });
    }

    BatchColumns {
        timestamp_col,
        timestamp_num_col,
        level_col,
        body_col,
        trace_id_col,
        span_id_col,
        flags_col,
        severity_num_col,
        observed_ts_col,
        scope_name_col,
        scope_version_col,
        attribute_cols,
        resource_cols,
    }
}

fn numeric_timestamp_ns(arr: &dyn Array, row: usize) -> Option<u64> {
    if arr.is_null(row) {
        return None;
    }

    match arr.data_type() {
        DataType::Int64 => {
            let v = arr.as_primitive::<Int64Type>().value(row);
            match u64::try_from(v) {
                Ok(ns) => Some(ns),
                Err(_) => {
                    tracing::debug!(
                        "timestamp parse fallback: negative i64 timestamp cannot convert to u64"
                    );
                    None
                }
            }
        }
        DataType::UInt64 => Some(arr.as_primitive::<UInt64Type>().value(row)),
        DataType::Timestamp(unit, _) => {
            use arrow::datatypes::{
                TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
                TimestampNanosecondType, TimestampSecondType,
            };
            let raw_ns = match unit {
                TimeUnit::Nanosecond => {
                    Some(arr.as_primitive::<TimestampNanosecondType>().value(row))
                }
                TimeUnit::Microsecond => arr
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row)
                    .checked_mul(1_000),
                TimeUnit::Millisecond => arr
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row)
                    .checked_mul(1_000_000),
                TimeUnit::Second => arr
                    .as_primitive::<TimestampSecondType>()
                    .value(row)
                    .checked_mul(1_000_000_000),
            };
            match raw_ns.and_then(|ns| u64::try_from(ns).ok()) {
                Some(ns) => Some(ns),
                None => {
                    tracing::debug!(
                        "timestamp parse fallback: numeric timestamp overflow or negative value"
                    );
                    None
                }
            }
        }
        _ => None,
    }
}

/// Encode a single RecordBatch row as an OTLP LogRecord using pre-resolved columns.
fn encode_row_as_log_record(
    columns: &BatchColumns<'_>,
    row: usize,
    metadata: &BatchMetadata,
    buf: &mut Vec<u8>,
) {
    // --- Read per-row values from pre-resolved columns ---

    let timestamp_ns: Option<u64> = if let Some((_, arr)) = columns.timestamp_num_col.as_ref() {
        numeric_timestamp_ns(*arr, row)
    } else if let Some((_, arr)) = columns.timestamp_col.as_ref() {
        if arr.is_null(row) {
            None
        } else {
            match parse_timestamp_nanos(arr.value(row).as_bytes()) {
                Some(ts) => Some(ts),
                None => {
                    tracing::debug!(
                        "timestamp parse fallback: event_time omitted for unparsable value"
                    );
                    None
                }
            }
        }
    } else {
        None
    };

    let (severity_num, severity_text): (Severity, &[u8]) =
        if let Some((_, arr)) = columns.level_col.as_ref() {
            if arr.is_null(row) {
                (Severity::Unspecified, b"")
            } else {
                parse_severity(arr.value(row).as_bytes())
            }
        } else {
            (Severity::Unspecified, b"")
        };
    let severity_number = if let Some((_, arr)) = columns.severity_num_col.as_ref() {
        if arr.is_null(row) {
            severity_num as u64
        } else {
            u64::try_from(arr.value(row)).unwrap_or(0)
        }
    } else {
        severity_num as u64
    };

    let body: &str = match columns.body_col.as_ref() {
        Some((_, body)) if !body.is_null(row) => body.value(row),
        _ => "",
    };
    let body_bytes = body.as_bytes();

    // --- Write protobuf fields ---

    // LogRecord.time_unix_nano (fixed64)
    if let Some(timestamp_ns) = timestamp_ns {
        encode_fixed64(buf, otlp::LOG_RECORD_TIME_UNIX_NANO, timestamp_ns);
    }

    // LogRecord.severity_number (varint)
    if severity_number > 0 {
        encode_varint_field(buf, otlp::LOG_RECORD_SEVERITY_NUMBER, severity_number);
    }

    // LogRecord.severity_text (string)
    if !severity_text.is_empty() {
        encode_bytes_field(buf, otlp::LOG_RECORD_SEVERITY_TEXT, severity_text);
    }

    // LogRecord.body (AnyValue { string_value })
    if !body_bytes.is_empty() {
        let anyvalue_inner_size = bytes_field_size(otlp::ANY_VALUE_STRING_VALUE, body_bytes.len());
        encode_tag(buf, otlp::LOG_RECORD_BODY, otlp::WIRE_TYPE_LEN);
        encode_varint(buf, anyvalue_inner_size as u64);
        encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, body_bytes);
    }

    // LogRecord.attributes — pre-resolved attribute columns
    for col in &columns.attribute_cols {
        encode_col_attr(buf, otlp::LOG_RECORD_ATTRIBUTES, col, row);
    }

    // LogRecord.flags (fixed32) — W3C trace flags.
    // Filter to u32 range: negative or >u32::MAX values are invalid per the
    // W3C Trace Context spec (only 8 bits are defined). (#1121)
    if let Some((_, arr)) = columns.flags_col
        && let Some(flags) = arr.value_u32(row)
    {
        encode_fixed32(buf, otlp::LOG_RECORD_FLAGS, flags);
    }

    // LogRecord.trace_id (bytes, 16 bytes) — hex-decoded from 32-char string column
    if let Some((_, arr)) = columns.trace_id_col
        && !arr.is_null(row)
    {
        let hex = arr.value(row);
        let mut decoded = [0u8; 16];
        if hex_decode(hex.as_bytes(), &mut decoded) {
            encode_bytes_field(buf, otlp::LOG_RECORD_TRACE_ID, &decoded);
        }
    }

    // LogRecord.span_id (bytes, 8 bytes) — hex-decoded from 16-char string column
    if let Some((_, arr)) = columns.span_id_col
        && !arr.is_null(row)
    {
        let hex = arr.value(row);
        let mut decoded = [0u8; 8];
        if hex_decode(hex.as_bytes(), &mut decoded) {
            encode_bytes_field(buf, otlp::LOG_RECORD_SPAN_ID, &decoded);
        }
    }

    // LogRecord.observed_time_unix_nano (fixed64)
    let observed_ns = columns
        .observed_ts_col
        .as_ref()
        .and_then(|(_, arr)| numeric_timestamp_ns(*arr, row))
        .unwrap_or(metadata.observed_time_ns);
    encode_fixed64(buf, otlp::LOG_RECORD_OBSERVED_TIME_UNIX_NANO, observed_ns);
}

/// Encode a KeyValue with string AnyValue as an attribute.
/// `field_number` is the protobuf field tag of the parent message's `attributes` repeated field
/// (e.g. `LOG_RECORD_ATTRIBUTES` for LogRecord, `RESOURCE_ATTRIBUTES` for Resource).
/// KeyValue: { key (string), value (AnyValue { string_value }) }
fn encode_key_value_string(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: &[u8]) {
    let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_STRING_VALUE, value.len());
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, value);
}

/// Encode a KeyValue with bytes AnyValue (`AnyValue.bytes_value`).
fn encode_key_value_bytes(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: &[u8]) {
    let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_BYTES_VALUE, value.len());
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_bytes_field(buf, otlp::ANY_VALUE_BYTES_VALUE, value);
}

/// Encode a KeyValue with int AnyValue (`AnyValue.int_value`).
fn encode_key_value_int(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: i64) {
    let anyvalue_inner = 1 + varint_len(value as u64); // tag(1 byte) + varint
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_varint_field(buf, otlp::ANY_VALUE_INT_VALUE, value as u64);
}

/// Encode one attribute column into `buf` using pre-computed key encoding.
///
/// Avoids recomputing the
/// key tag + varint + key bytes on every row by using `col.key_encoding` and
/// `col.kv_key_cost` which were computed once in `resolve_batch_columns`.
#[inline(always)]
fn encode_col_attr(buf: &mut Vec<u8>, field_number: u32, col: &ColAttr<'_>, row: usize) {
    // Shared helper for string-value KV pairs.
    //
    // When `value.len() <= 125` every intermediate length fits in a single
    // varint byte, so the outer KV byte-count reduces to two additions instead
    // of four `varint_len`/`bytes_field_size` calls.
    #[inline(always)]
    fn encode_str_value(buf: &mut Vec<u8>, field_number: u32, col: &ColAttr<'_>, value: &[u8]) {
        if value.len() <= 125 {
            // anyvalue_inner = tag(1) + len_varint(1) + data
            let anyvalue_inner = 2 + value.len();
            let kv_inner = col.short_kv_inner_base + value.len();
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, value);
        } else {
            let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_STRING_VALUE, value.len());
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, value);
        }
    }

    match &col.array {
        AttrArray::Int(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = 1 + varint_len(value as u64);
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_varint_field(buf, otlp::ANY_VALUE_INT_VALUE, value as u64);
        }
        AttrArray::UInt(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let Ok(value) = i64::try_from(value) else {
                tracing::warn!(
                    column = col.name.as_str(),
                    row,
                    value,
                    "skipping OTLP attribute: UInt64 value exceeds AnyValue.int_value range"
                );
                return;
            };
            let anyvalue_inner = 1 + varint_len(value as u64);
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_varint_field(buf, otlp::ANY_VALUE_INT_VALUE, value as u64);
        }
        AttrArray::Float(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = 1 + 8; // tag(1) + fixed64(8)
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_fixed64(buf, otlp::ANY_VALUE_DOUBLE_VALUE, value.to_bits());
        }
        AttrArray::Bool(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = 1 + 1; // tag(1) + varint(bool)
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_varint_field(buf, otlp::ANY_VALUE_BOOL_VALUE, u64::from(value));
        }
        AttrArray::Str(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            encode_str_value(buf, field_number, col, arr.value(row).as_bytes());
        }
        AttrArray::PreformattedStr(values) => {
            if let Some(Some(value)) = values.get(row) {
                encode_str_value(buf, field_number, col, value.as_bytes());
            }
        }
        AttrArray::Bytes(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_BYTES_VALUE, value.len());
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_bytes_field(buf, otlp::ANY_VALUE_BYTES_VALUE, value);
        }
        AttrArray::LargeBytes(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_BYTES_VALUE, value.len());
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_bytes_field(buf, otlp::ANY_VALUE_BYTES_VALUE, value);
        }
        AttrArray::OtherStr(arr) => {
            if let Some(value) = format_non_string_attr_value(*arr, row, &col.name) {
                encode_str_value(buf, field_number, col, value.as_bytes());
            }
        }
    }
}

fn format_non_string_attr_value(arr: &dyn Array, row: usize, field_name: &str) -> Option<String> {
    if arr.is_null(row) {
        return None;
    }
    match array_value_to_string(arr, row) {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::warn!(
                column = field_name,
                row,
                error = %err,
                "skipping OTLP attribute value: failed to format non-string Arrow value"
            );
            None
        }
    }
}

fn preformat_non_string_attr_column(arr: &dyn Array, field_name: &str) -> Vec<Option<String>> {
    (0..arr.len())
        .map(|row| format_non_string_attr_value(arr, row, field_name))
        .collect()
}

/// Encode a KeyValue with double AnyValue (`AnyValue.double_value`).
fn encode_key_value_double(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: f64) {
    let anyvalue_inner = 1 + 8; // tag(1 byte) + fixed64
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_fixed64(buf, otlp::ANY_VALUE_DOUBLE_VALUE, value.to_bits());
}

/// Encode a KeyValue with boolean AnyValue (`AnyValue.bool_value`).
fn encode_key_value_bool(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: bool) {
    let anyvalue_inner = 1 + 1; // tag(1 byte) + varint(1 byte)
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_varint_field(buf, otlp::ANY_VALUE_BOOL_VALUE, u64::from(value));
}

/// Write a gRPC length-prefixed message frame into `buf`.
///
/// gRPC wire format (per the [gRPC over HTTP/2 specification](https://grpc.io/docs/what-is-grpc/core-concepts/)):
/// ```text
/// [1 byte: compressed flag (0 = not compressed, 1 = compressed)]
/// [4 bytes: big-endian message length]
/// [N bytes: protobuf message]
/// ```
fn write_grpc_frame(buf: &mut Vec<u8>, payload: &[u8], compressed: bool) -> io::Result<()> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "gRPC message payload must be < 4 GiB",
        )
    })?;
    buf.clear();
    buf.push(u8::from(compressed));
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
    Ok(())
}

#[cfg(test)]
mod otlp_sink_correctness_tests;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;
    use proptest::string::string_regex;

    use super::*;

    #[tokio::test]
    async fn send_payload_returns_rejected_on_4xx() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/v1/logs")
            .with_status(400)
            .create_async()
            .await;

        let mut sink = OtlpSink::new(
            "test".into(),
            server.url() + "/v1/logs",
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::new()),
        )
        .unwrap();

        sink.encoder_buf.push(1); // Non-empty so it sends
        let result = sink.send_payload(1).await.unwrap();
        match result {
            crate::sink::SendResult::Rejected(_) => {} // Expected
            _ => panic!("Expected Rejected on 400 response, got: {:?}", result),
        }
    }

    #[tokio::test]
    async fn send_payload_returns_io_error_on_5xx_without_retry_after() {
        let mut server = mockito::Server::new_async().await;
        // Server responds with 500 and no Retry-After header. The shared
        // classifier returns IoError so the worker pool applies exponential
        // backoff (better than the old fixed-5s retry).
        let _mock = server
            .mock("POST", "/v1/logs")
            .with_status(500)
            .create_async()
            .await;

        let mut sink = OtlpSink::new(
            "test".into(),
            server.url() + "/v1/logs",
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::new()),
        )
        .unwrap();

        sink.encoder_buf.push(1);
        let result = sink.send_payload(1).await.unwrap();
        assert!(
            matches!(result, crate::sink::SendResult::IoError(_)),
            "Expected IoError on 500 without Retry-After, got: {result:?}",
        );
    }

    #[tokio::test]
    async fn send_payload_5xx_honours_retry_after_header() {
        let mut server = mockito::Server::new_async().await;
        // Server responds 503 with a Retry-After: 42 header.
        // send_payload should surface that duration rather than the default.
        let _mock = server
            .mock("POST", "/v1/logs")
            .with_status(503)
            .with_header("Retry-After", "42")
            .create_async()
            .await;

        let mut sink = OtlpSink::new(
            "test".into(),
            server.url() + "/v1/logs",
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::new()),
        )
        .unwrap();

        sink.encoder_buf.push(1);
        let result = sink.send_payload(1).await.unwrap();
        match result {
            crate::sink::SendResult::RetryAfter(d) => {
                assert_eq!(d.as_secs(), 42, "should honour Retry-After header value");
            }
            _ => panic!("Expected RetryAfter on 503 response, got: {:?}", result),
        }
    }

    #[tokio::test]
    async fn grpc_oversized_payload_is_rejected_before_send() {
        let mut sink = OtlpSink::new(
            "test".into(),
            "http://localhost:4317".into(),
            OtlpProtocol::Grpc,
            Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::new()),
        )
        .expect("sink must construct");

        sink.encoder_buf = vec![0u8; DEFAULT_GRPC_MAX_MESSAGE_BYTES + 1];
        let err = sink
            .send_payload(1)
            .await
            .expect_err("oversized gRPC payload must return InvalidInput");

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn invalid_struct_array_downcast_does_not_panic() {
        use crate::{ColVariant, get_array, is_null};

        // Create a non-struct array (e.g. StringArray)
        let str_arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["hello"]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "fake_struct",
            DataType::Utf8, // It's actually utf8
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![str_arr]).unwrap();

        // Simulate a variant that thinks the column is a StructArray
        let variant = ColVariant::StructField {
            struct_col_idx: 0,
            field_idx: 0,
            dt: DataType::Utf8,
        };

        // These should gracefully return true/None, not panic.
        assert!(is_null(&batch, &variant, 0));
        assert!(get_array(&batch, &variant).is_none());
    }

    /// Struct conflict columns (status: Struct { int, str }) must be normalized
    /// to flat Utf8 before OTLP encoding so values are not silently dropped.
    #[test]
    fn struct_conflict_column_is_normalized_not_dropped() {
        use arrow::array::{Int64Array as I64A, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field as F, Fields};

        let int_arr: Arc<dyn Array> = Arc::new(I64A::from(vec![Some(200i64), None]));
        let str_arr: Arc<dyn Array> = Arc::new(StringArray::from(vec![None::<&str>, Some("OK")]));
        let child_fields = Fields::from(vec![
            Arc::new(F::new("int", DataType::Int64, true)),
            Arc::new(F::new("str", DataType::Utf8, true)),
        ]);
        let validity = NullBuffer::from(vec![true, true]);
        let struct_arr: Arc<dyn Array> = Arc::new(StructArray::new(
            child_fields.clone(),
            vec![Arc::clone(&int_arr), Arc::clone(&str_arr)],
            Some(validity),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Struct(child_fields),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![struct_arr]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // After normalization the "status" key must appear in the encoded output
        // with its coalesced value ("200" from the int child, "OK" from str child).
        assert!(
            contains_bytes(&sink.encoder_buf, b"status"),
            "conflict struct column 'status' must be encoded as an OTLP attribute after normalization"
        );
        assert!(
            contains_bytes(&sink.encoder_buf, b"200"),
            "int value 200 must be encoded as the coalesced string '200'"
        );
        assert!(
            contains_bytes(&sink.encoder_buf, b"OK"),
            "str value 'OK' must be encoded as an OTLP attribute"
        );
    }

    fn make_sink() -> OtlpSink {
        OtlpSink::new(
            "test".into(),
            "http://localhost:4318".into(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            shared_test_client(),
            Arc::new(ComponentStats::new()),
        )
        .unwrap()
    }

    fn make_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 1_000_000_000,
        }
    }

    fn assert_timestamp_encoding_parity(
        schema: Arc<Schema>,
        columns: Vec<Arc<dyn Array>>,
        expected_ns: u64,
    ) {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let batch = RecordBatch::try_new(schema, columns).expect("valid timestamp test batch");
        let metadata = make_metadata();

        let mut handwritten = make_sink();
        handwritten.encode_batch(&batch, &metadata);
        let handwritten_request =
            ExportLogsServiceRequest::decode(handwritten.encoder_buf.as_slice())
                .expect("prost must decode handwritten output");

        let mut generated = make_sink();
        generated.encode_batch_generated_fast(&batch, &metadata);
        let generated_request = ExportLogsServiceRequest::decode(generated.encoder_buf.as_slice())
            .expect("prost must decode generated-fast output");

        let handwritten_ns =
            handwritten_request.resource_logs[0].scope_logs[0].log_records[0].time_unix_nano;
        let generated_ns =
            generated_request.resource_logs[0].scope_logs[0].log_records[0].time_unix_nano;

        assert_eq!(
            handwritten_ns, expected_ns,
            "handwritten path timestamp mismatch"
        );
        assert_eq!(
            generated_ns, expected_ns,
            "generated-fast path timestamp mismatch"
        );
        assert_eq!(
            generated.encoded_payload(),
            handwritten.encoded_payload(),
            "generated-fast payload drifted from handwritten payload"
        );
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    fn shared_test_client() -> reqwest::Client {
        static TEST_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
        TEST_CLIENT.get_or_init(reqwest::Client::new).clone()
    }

    fn unicode_string(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(any::<char>(), 0..=max_len)
            .prop_map(|chars| chars.into_iter().collect())
    }

    proptest! {
        #[test]
        fn proptest_generated_fast_matches_handwritten_random_utf8_rows(
            rows in proptest::collection::vec(
                (
                    prop::option::of(unicode_string(30)),
                    prop::option::of(unicode_string(8)),
                    prop::option::of(unicode_string(40)),
                    prop::option::of(prop_oneof![
                        string_regex("[0-9a-f]{32}").expect("valid regex"),
                        unicode_string(40),
                    ]),
                    prop::option::of(prop_oneof![
                        string_regex("[0-9a-f]{16}").expect("valid regex"),
                        unicode_string(24),
                    ]),
                    prop::option::of(any::<i64>()),
                    prop::option::of(unicode_string(20)),
                    prop::option::of(any::<i64>()),
                    prop::option::of((-1_000_000i64..1_000_000i64).prop_map(|n| n as f64 / 10.0)),
                    prop::option::of(any::<bool>()),
                ),
                0..24
            )
        ) {
            let timestamps: Vec<Option<String>> = rows.iter().map(|r| r.0.clone()).collect();
            let levels: Vec<Option<String>> = rows.iter().map(|r| r.1.clone()).collect();
            let messages: Vec<Option<String>> = rows.iter().map(|r| r.2.clone()).collect();
            let trace_ids: Vec<Option<String>> = rows.iter().map(|r| r.3.clone()).collect();
            let span_ids: Vec<Option<String>> = rows.iter().map(|r| r.4.clone()).collect();
            let flags: Vec<Option<i64>> = rows.iter().map(|r| r.5).collect();
            let hosts: Vec<Option<String>> = rows.iter().map(|r| r.6.clone()).collect();
            let counts: Vec<Option<i64>> = rows.iter().map(|r| r.7).collect();
            let latencies: Vec<Option<f64>> = rows.iter().map(|r| r.8).collect();
            let actives: Vec<Option<bool>> = rows.iter().map(|r| r.9).collect();

            let schema = Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Utf8, true),
                Field::new("level", DataType::Utf8, true),
                Field::new("message", DataType::Utf8, true),
                Field::new("trace_id", DataType::Utf8, true),
                Field::new("span_id", DataType::Utf8, true),
                Field::new("flags", DataType::Int64, true),
                Field::new("host", DataType::Utf8, true),
                Field::new("count", DataType::Int64, true),
                Field::new("latency", DataType::Float64, true),
                Field::new("active", DataType::Boolean, true),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(timestamps.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(levels.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(messages.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(trace_ids.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(span_ids.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                    Arc::new(Int64Array::from(flags)),
                    Arc::new(StringArray::from(hosts.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                    Arc::new(Int64Array::from(counts)),
                    Arc::new(arrow::array::Float64Array::from(latencies)),
                    Arc::new(arrow::array::BooleanArray::from(actives)),
                ],
            ).expect("valid random utf8 batch");

            let metadata = make_metadata();

            let mut handwritten = make_sink();
            handwritten.encode_batch(&batch, &metadata);

            let mut generated = make_sink();
            generated.encode_batch_generated_fast(&batch, &metadata);

            prop_assert_eq!(
                generated.encoded_payload(),
                handwritten.encoded_payload(),
                "generated-fast OTLP payload drifted from handwritten encoder on random Utf8 rows"
            );
        }
    }

    #[test]
    fn encode_trace_id_as_field_9() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // field 9, wire type 2: tag = (9 << 3) | 2 = 0x4A; length = 16 = 0x10
        let mut expected = vec![0x4Au8, 0x10u8];
        expected.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]);
        assert!(
            contains_bytes(&sink.encoder_buf, &expected),
            "trace_id field 9 not found in encoded output"
        );
    }

    #[test]
    fn encode_span_id_as_field_10() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "span_id",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["0102030405060708"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // field 10, wire type 2: tag = (10 << 3) | 2 = 0x52; length = 8 = 0x08
        let mut expected = vec![0x52u8, 0x08u8];
        expected.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        assert!(
            contains_bytes(&sink.encoder_buf, &expected),
            "span_id field 10 not found in encoded output"
        );
    }

    #[test]
    fn encode_flags_as_field_8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flags",
            DataType::Int64,
            true,
        )]));
        let arr = Int64Array::from(vec![1i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // field 8, wire type 5: tag = (8 << 3) | 5 = 0x45; then 4 bytes LE
        let expected = [0x45u8, 0x01, 0x00, 0x00, 0x00];
        assert!(
            contains_bytes(&sink.encoder_buf, &expected),
            "flags field 8 not found in encoded output"
        );
    }

    #[test]
    fn trace_id_not_encoded_as_attribute() {
        // A trace_id column must NOT appear as a KeyValue attribute (field 6).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // If trace_id were encoded as an attribute, its key bytes would appear.
        assert!(
            !contains_bytes(&sink.encoder_buf, b"trace_id"),
            "trace_id key must not appear as an attribute"
        );
    }

    #[test]
    fn span_id_not_encoded_as_attribute() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "span_id",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["0102030405060708"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        assert!(
            !contains_bytes(&sink.encoder_buf, b"span_id"),
            "span_id key must not appear as an attribute"
        );
    }

    #[test]
    fn configured_message_field_does_not_shadow_trace_id() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("canonical-body")])),
                Arc::new(StringArray::from(vec![Some(
                    "0102030405060708090a0b0c0d0e0f10",
                )])),
            ],
        )
        .unwrap();

        let mut sink = make_sink().with_message_field("trace_id".to_string());
        sink.encode_batch(&batch, &make_metadata());
        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode output");
        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];

        let body = lr.body.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(body, Some("canonical-body"));
        assert_eq!(
            lr.trace_id,
            vec![
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10
            ],
            "trace_id should remain encoded in dedicated OTLP field"
        );
    }

    #[test]
    fn configured_message_field_does_not_shadow_span_id() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("canonical-body")])),
                Arc::new(StringArray::from(vec![Some("0102030405060708")])),
            ],
        )
        .unwrap();

        let mut sink = make_sink().with_message_field("span_id".to_string());
        sink.encode_batch(&batch, &make_metadata());
        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode output");
        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];

        let body = lr.body.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(body, Some("canonical-body"));
        assert_eq!(
            lr.span_id,
            vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            "span_id should remain encoded in dedicated OTLP field"
        );
    }

    #[test]
    fn invalid_trace_id_hex_is_silently_ignored() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        )]));
        // Not a valid 32-char hex string.
        let arr = StringArray::from(vec!["not-a-valid-hex-string-here!!!!"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata()); // must not panic

        // Field 9 tag 0x4A must not appear.
        let mut probe = vec![0x4Au8, 0x10u8];
        probe.extend_from_slice(&[0u8; 16]);
        assert!(
            !contains_bytes(&sink.encoder_buf, &probe),
            "invalid trace_id should not produce field 9"
        );
    }

    #[test]
    fn grpc_frame_prepends_five_byte_header() {
        let proto_payload = [0x0a, 0x02, 0x08, 0x01];
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &proto_payload, false).unwrap();
        assert_eq!(framed.len(), 5 + proto_payload.len());
        assert_eq!(framed[0], 0x00, "compressed flag must be 0x00");
        let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
        assert_eq!(
            msg_len as usize,
            proto_payload.len(),
            "length field must match payload length"
        );
        assert_eq!(
            &framed[5..],
            &proto_payload,
            "payload bytes must follow header"
        );
    }

    #[test]
    fn grpc_frame_empty_payload() {
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &[], false).unwrap();
        assert_eq!(framed.len(), 5);
        assert_eq!(framed[0], 0x00, "compressed flag must be 0x00");
        let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
        assert_eq!(msg_len, 0, "length field must be zero for empty payload");
    }

    #[test]
    fn grpc_frame_compressed_flag() {
        let proto_payload = [0x0a, 0x02];
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &proto_payload, true).unwrap();
        assert_eq!(framed[0], 0x01, "compressed flag must be 0x01");
    }

    /// Verify that `encode_batch` + `write_grpc_frame` produce a valid 5-byte gRPC frame header
    /// followed by the exact protobuf payload. Tests the encode-and-frame path directly without
    /// making a real network call.
    #[test]
    fn encode_and_frame_payload() {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let arr = StringArray::from(vec!["hello"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        // Encode the batch the same way send_batch would, then frame it manually.
        let mut sink = OtlpSink::new(
            "test".into(),
            "http://localhost:4318".into(),
            OtlpProtocol::Grpc,
            Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::new()),
        )
        .unwrap();
        sink.encode_batch(&batch, &make_metadata());
        let proto_payload = sink.encoder_buf.clone();

        // Frame as send_batch would.
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &proto_payload, false).unwrap();

        // The frame header must be 5 bytes followed by the exact protobuf payload.
        assert_eq!(
            framed[0], 0x00,
            "compressed flag must be 0x00 for uncompressed gRPC"
        );
        let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
        assert_eq!(
            msg_len as usize,
            proto_payload.len(),
            "gRPC length field must match protobuf payload length"
        );
        assert_eq!(
            &framed[5..],
            proto_payload.as_slice(),
            "protobuf payload must follow the frame header"
        );
    }

    #[test]
    fn scope_logs_has_instrumentation_scope() {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let arr = StringArray::from(vec!["hello"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // The InstrumentationScope name "logfwd" must be present in the encoded bytes.
        assert!(
            contains_bytes(&sink.encoder_buf, b"logfwd"),
            "InstrumentationScope name 'logfwd' not found in encoded output"
        );

        // The InstrumentationScope version (from CARGO_PKG_VERSION) must also be present.
        let version = env!("CARGO_PKG_VERSION").as_bytes();
        assert!(
            contains_bytes(&sink.encoder_buf, version),
            "InstrumentationScope version not found in encoded output"
        );
    }

    #[test]
    fn encode_boolean_as_attribute() {
        use arrow::array::BooleanArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "active",
            DataType::Boolean,
            true,
        )]));
        let arr = BooleanArray::from(vec![Some(true)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // LogRecord field 6 tag: (6 << 3) | 2 = 0x32
        // KeyValue field 1 key tag: (1 << 3) | 2 = 0x0A, then "active"
        // KeyValue field 2 value AnyValue tag: (2 << 3) | 2 = 0x12
        // AnyValue field 2 bool_value tag: (2 << 3) | 0 = 0x10, then 0x01
        let expected = [0x10u8, 0x01];
        assert!(
            contains_bytes(&sink.encoder_buf, &expected),
            "boolean attribute not found in encoded output"
        );
        assert!(
            contains_bytes(&sink.encoder_buf, b"active"),
            "attribute key 'active' not found"
        );
    }

    /// Roundtrip oracle test: encode a RecordBatch with our hand-rolled encoder,
    /// decode with prost (the canonical protobuf library), and compare fields.
    ///
    /// This is the definitive test that our OTLP encoding is spec-compliant.
    /// If we encode a field incorrectly, prost::Message::decode will either
    /// fail or produce different values.
    #[test]
    fn roundtrip_encode_decode_via_prost() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        // Build a RecordBatch with all supported LogRecord field types.
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("flags", DataType::Int64, true),
            Field::new("host", DataType::Utf8, true), // string attribute
            Field::new("count", DataType::Int64, true), // int attribute
            Field::new("latency", DataType::Float64, true), // double attribute
            Field::new("active", DataType::Boolean, true), // bool attribute
        ]));

        let ts_arr = StringArray::from(vec!["2024-01-15T10:30:00Z"]);
        let level_arr = StringArray::from(vec!["ERROR"]);
        let msg_arr = StringArray::from(vec!["disk full"]);
        let trace_arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
        let span_arr = StringArray::from(vec!["0102030405060708"]);
        let flags_arr = Int64Array::from(vec![1i64]);
        let host_arr = StringArray::from(vec!["web-01"]);
        let count_arr = Int64Array::from(vec![42i64]);
        let latency_arr = arrow::array::Float64Array::from(vec![1.5f64]);
        let active_arr = arrow::array::BooleanArray::from(vec![Some(true)]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts_arr),
                Arc::new(level_arr),
                Arc::new(msg_arr),
                Arc::new(trace_arr),
                Arc::new(span_arr),
                Arc::new(flags_arr),
                Arc::new(host_arr),
                Arc::new(count_arr),
                Arc::new(latency_arr),
                Arc::new(active_arr),
            ],
        )
        .expect("valid batch");

        let observed_ns: u64 = 1_700_000_000_000_000_000;
        let resource_attrs = Arc::new(vec![("k8s.pod.name".to_string(), "my-pod".to_string())]);
        let metadata = BatchMetadata {
            resource_attrs,
            observed_time_ns: observed_ns,
        };

        // Encode with our hand-rolled encoder.
        let mut sink = make_sink();
        sink.encode_batch(&batch, &metadata);
        assert!(
            !sink.encoder_buf.is_empty(),
            "encoder must produce non-empty output"
        );

        // Decode with prost — the canonical protobuf decoder.
        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode our encoding without error");

        // --- Verify structure ---
        assert_eq!(request.resource_logs.len(), 1, "exactly one ResourceLogs");
        let rl = &request.resource_logs[0];

        // Resource attributes
        let resource = rl.resource.as_ref().expect("Resource must be present");
        let pod_attr = resource
            .attributes
            .iter()
            .find(|kv| kv.key == "k8s.pod.name");
        assert!(pod_attr.is_some(), "resource attr k8s.pod.name must exist");
        let pod_val = pod_attr
            .unwrap()
            .value
            .as_ref()
            .and_then(|v| match &v.value {
                Some(Value::StringValue(s)) => Some(s.as_str()),
                _ => None,
            });
        assert_eq!(pod_val, Some("my-pod"), "resource attr value mismatch");

        // ScopeLogs
        assert_eq!(rl.scope_logs.len(), 1, "exactly one ScopeLogs");
        let sl = &rl.scope_logs[0];
        let scope = sl
            .scope
            .as_ref()
            .expect("InstrumentationScope must be present");
        assert_eq!(scope.name, "logfwd", "scope name must be 'logfwd'");
        assert_eq!(
            scope.version,
            env!("CARGO_PKG_VERSION"),
            "scope version must match CARGO_PKG_VERSION"
        );

        // LogRecord
        assert_eq!(sl.log_records.len(), 1, "exactly one LogRecord");
        let lr = &sl.log_records[0];

        // time_unix_nano: 2024-01-15T10:30:00Z = 1705314600 seconds
        assert_eq!(
            lr.time_unix_nano, 1_705_314_600_000_000_000,
            "time_unix_nano mismatch"
        );

        // observed_time_unix_nano
        assert_eq!(
            lr.observed_time_unix_nano, observed_ns,
            "observed_time_unix_nano mismatch"
        );

        // severity
        assert_eq!(lr.severity_number, 17, "ERROR severity_number must be 17");
        assert_eq!(lr.severity_text, "ERROR", "severity_text mismatch");

        // body
        let body_str = lr.body.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(body_str, Some("disk full"), "body mismatch");

        // trace_id (16 bytes, decoded from hex)
        assert_eq!(
            lr.trace_id,
            vec![
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10
            ],
            "trace_id mismatch"
        );

        // span_id (8 bytes, decoded from hex)
        assert_eq!(
            lr.span_id,
            vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            "span_id mismatch"
        );

        // flags
        assert_eq!(lr.flags, 1, "flags mismatch");

        // --- Verify attributes ---
        let find_attr = |name: &str| lr.attributes.iter().find(|kv| kv.key == name);

        // String attribute: host
        let host_kv = find_attr("host").expect("host attribute must exist");
        let host_val = host_kv.value.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(host_val, Some("web-01"), "host attribute value mismatch");

        // Int attribute: count
        let count_kv = find_attr("count").expect("count attribute must exist");
        let count_val = count_kv.value.as_ref().and_then(|v| match &v.value {
            Some(Value::IntValue(i)) => Some(*i),
            _ => None,
        });
        assert_eq!(count_val, Some(42), "count attribute value mismatch");

        // Double attribute: latency
        let latency_kv = find_attr("latency").expect("latency attribute must exist");
        let latency_val = latency_kv.value.as_ref().and_then(|v| match &v.value {
            Some(Value::DoubleValue(d)) => Some(*d),
            _ => None,
        });
        assert!(
            (latency_val.unwrap() - 1.5).abs() < f64::EPSILON,
            "latency attribute value mismatch"
        );

        // Bool attribute: active
        let active_kv = find_attr("active").expect("active attribute must exist");
        let active_val = active_kv.value.as_ref().and_then(|v| match &v.value {
            Some(Value::BoolValue(b)) => Some(*b),
            _ => None,
        });
        assert_eq!(active_val, Some(true), "active attribute value mismatch");
    }

    #[test]
    fn binary_attributes_encode_as_otlp_bytes_values() {
        use arrow::array::BinaryArray;
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("log_payload", DataType::Binary, true),
            Field::new(
                "resource.attributes.resource_payload",
                DataType::Binary,
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("binary attrs")])),
                Arc::new(BinaryArray::from(vec![Some(&[0x00_u8, 0x0f, 0xff][..])])),
                Arc::new(BinaryArray::from(vec![Some(&[0xde_u8, 0xad][..])])),
            ],
        )
        .expect("valid batch");
        let metadata = make_metadata();

        let mut handwritten = make_sink();
        handwritten.encode_batch(&batch, &metadata);

        let mut generated = make_sink();
        generated.encode_batch_generated_fast(&batch, &metadata);
        assert_eq!(
            generated.encoded_payload(),
            handwritten.encoded_payload(),
            "generated-fast payload drifted from handwritten binary attribute encoding"
        );

        let request = ExportLogsServiceRequest::decode(handwritten.encoder_buf.as_slice())
            .expect("prost must decode binary attributes");
        let resource = request.resource_logs[0]
            .resource
            .as_ref()
            .expect("resource");
        let resource_payload = resource
            .attributes
            .iter()
            .find(|kv| kv.key == "resource_payload")
            .and_then(|kv| kv.value.as_ref())
            .and_then(|value| match &value.value {
                Some(Value::BytesValue(bytes)) => Some(bytes.as_slice()),
                _ => None,
            });
        assert_eq!(resource_payload, Some(&[0xde, 0xad][..]));

        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        let log_payload = record
            .attributes
            .iter()
            .find(|kv| kv.key == "log_payload")
            .and_then(|kv| kv.value.as_ref())
            .and_then(|value| match &value.value {
                Some(Value::BytesValue(bytes)) => Some(bytes.as_slice()),
                _ => None,
            });
        assert_eq!(log_payload, Some(&[0x00, 0x0f, 0xff][..]));
    }

    /// Roundtrip with minimal fields: only body, no timestamp, no severity,
    /// no trace context. Ensures sparse records encode correctly.
    #[test]
    fn roundtrip_minimal_record() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let msg_arr = StringArray::from(vec!["hello world"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg_arr)]).expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode minimal record");

        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(lr.time_unix_nano, 0, "no timestamp column means 0");
        assert_eq!(lr.severity_number, 0, "no severity means unspecified");
        let body_str = lr.body.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(body_str, Some("hello world"), "body mismatch");
        assert!(lr.trace_id.is_empty(), "no trace_id column means empty");
        assert!(lr.span_id.is_empty(), "no span_id column means empty");
    }

    #[test]
    fn severity_number_null_falls_back_to_level() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new(field_names::SEVERITY_NUMBER, DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("ERROR")])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Some("broken")])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode output");
        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(
            lr.severity_number, 17,
            "null severity_number column must fall back to parsed level"
        );
    }

    #[test]
    fn negative_severity_number_encodes_unspecified() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new(field_names::SEVERITY_NUMBER, DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("ERROR")])),
                Arc::new(Int64Array::from(vec![Some(-1)])),
                Arc::new(StringArray::from(vec![Some("broken")])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode output");
        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(
            lr.severity_number, 0,
            "negative severity_number must not wrap or inherit level severity"
        );
        assert_eq!(lr.severity_text, "ERROR");
    }

    #[test]
    fn unsupported_well_known_types_fall_back_to_attributes() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let metadata = BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 123_456_789,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new(field_names::SEVERITY_NUMBER, DataType::Utf8, true),
            Field::new(field_names::OBSERVED_TIMESTAMP, DataType::Utf8, true),
            Field::new(field_names::SCOPE_NAME, DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("ERROR")])),
                Arc::new(StringArray::from(vec![Some("17")])),
                Arc::new(StringArray::from(vec![Some("not-a-nanos-value")])),
                Arc::new(Int64Array::from(vec![Some(42)])),
                Arc::new(StringArray::from(vec![Some("hello")])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &metadata);

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode output");
        let rl = &request.resource_logs[0];
        let sl = &rl.scope_logs[0];
        let lr = &sl.log_records[0];

        assert_eq!(lr.severity_number, 17);
        assert_eq!(lr.observed_time_unix_nano, metadata.observed_time_ns);
        let scope = sl.scope.as_ref().expect("scope must exist");
        assert_eq!(scope.name, "logfwd");

        let find_attr = |name: &str| lr.attributes.iter().find(|kv| kv.key == name);

        let sev_attr = find_attr(field_names::SEVERITY_NUMBER)
            .expect("severity_number should fall back to regular attribute");
        let sev_text = sev_attr.value.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(sev_text, Some("17"));

        let observed_attr = find_attr(field_names::OBSERVED_TIMESTAMP)
            .expect("observed_timestamp should fall back to regular attribute");
        let observed_text = observed_attr.value.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(observed_text, Some("not-a-nanos-value"));

        let scope_name_attr = find_attr(field_names::SCOPE_NAME)
            .expect("scope.name should fall back to regular attribute");
        let scope_name_int = scope_name_attr.value.as_ref().and_then(|v| match &v.value {
            Some(Value::IntValue(i)) => Some(*i),
            _ => None,
        });
        assert_eq!(scope_name_int, Some(42));
    }

    #[test]
    fn otlp_encoder_keeps_unselected_body_alias_as_attribute() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("alias-body")])),
                Arc::new(StringArray::from(vec![Some("canonical-body")])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink().with_message_field("message".to_string());
        sink.encode_batch(&batch, &make_metadata());
        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode output");
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        let body = record.body.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        let message_attr = record.attributes.iter().find(|kv| kv.key == "message");
        let message_attr =
            message_attr
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                });

        assert_eq!(body, Some("canonical-body"));
        assert_eq!(message_attr, Some("alias-body"));
    }

    #[test]
    fn body_column_falls_back_when_message_is_null_for_row() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("message-body"), None])),
                Arc::new(StringArray::from(vec![
                    Some("raw-first"),
                    Some("raw-fallback"),
                ])),
            ],
        )
        .expect("valid batch");

        let metadata = make_metadata();

        let mut handwritten = OtlpSink::new(
            "test".to_string(),
            "http://localhost:4318".to_string(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::new()),
        )
        .expect("handwritten sink")
        .with_message_field("message".to_string());
        handwritten.encode_batch(&batch, &metadata);
        let handwritten_request =
            ExportLogsServiceRequest::decode(handwritten.encoder_buf.as_slice())
                .expect("prost must decode handwritten output");

        let mut generated = OtlpSink::new(
            "test".to_string(),
            "http://localhost:4318".to_string(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::new()),
        )
        .expect("generated sink")
        .with_message_field("message".to_string());
        generated.encode_batch_generated_fast(&batch, &metadata);
        let generated_request = ExportLogsServiceRequest::decode(generated.encoder_buf.as_slice())
            .expect("prost must decode generated-fast output");

        let handwritten_bodies: Vec<&str> = handwritten_request.resource_logs[0].scope_logs[0]
            .log_records
            .iter()
            .map(|lr| {
                lr.body
                    .as_ref()
                    .and_then(|v| match &v.value {
                        Some(Value::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("")
            })
            .collect();
        let generated_bodies: Vec<&str> = generated_request.resource_logs[0].scope_logs[0]
            .log_records
            .iter()
            .map(|lr| {
                lr.body
                    .as_ref()
                    .and_then(|v| match &v.value {
                        Some(Value::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("")
            })
            .collect();

        assert_eq!(handwritten_bodies, vec!["raw-first", "raw-fallback"]);
        assert_eq!(generated_bodies, vec!["raw-first", "raw-fallback"]);
    }

    /// Roundtrip with multiple rows to verify repeated LogRecord encoding.
    #[test]
    fn roundtrip_multiple_rows() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
        ]));
        let msg_arr = StringArray::from(vec!["first", "second", "third"]);
        let level_arr = StringArray::from(vec!["INFO", "WARN", "ERROR"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg_arr), Arc::new(level_arr)])
            .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode multi-row batch");

        let records = &request.resource_logs[0].scope_logs[0].log_records;
        assert_eq!(records.len(), 3, "must have 3 LogRecords");

        let bodies: Vec<&str> = records
            .iter()
            .filter_map(|lr| {
                lr.body.as_ref().and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
            })
            .collect();
        assert_eq!(bodies, vec!["first", "second", "third"]);

        let severities: Vec<i32> = records.iter().map(|lr| lr.severity_number).collect();
        assert_eq!(severities, vec![9, 13, 17], "INFO=9, WARN=13, ERROR=17");
    }

    #[test]
    fn resource_columns_group_rows_into_distinct_resource_logs() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("resource.attributes.service.name", DataType::Utf8, true),
            Field::new("resource.attributes.k8s.namespace", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a1"),
                    Some("b1"),
                    Some("a2"),
                    Some("b2"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("checkout"),
                    Some("payments"),
                    Some("checkout"),
                    Some("payments"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("prod"),
                    Some("prod"),
                    Some("prod"),
                    Some("prod"),
                ])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(
            &batch,
            &BatchMetadata {
                resource_attrs: Arc::new(vec![(
                    "deployment.environment".to_string(),
                    "test".to_string(),
                )]),
                observed_time_ns: 1,
            },
        );

        let request =
            ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
        assert_eq!(request.resource_logs.len(), 2, "two resource groups");

        let first_records = &request.resource_logs[0].scope_logs[0].log_records;
        let second_records = &request.resource_logs[1].scope_logs[0].log_records;
        let first_bodies: Vec<&str> = first_records
            .iter()
            .map(|lr| {
                lr.body
                    .as_ref()
                    .and_then(|v| match &v.value {
                        Some(Value::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("")
            })
            .collect();
        let second_bodies: Vec<&str> = second_records
            .iter()
            .map(|lr| {
                lr.body
                    .as_ref()
                    .and_then(|v| match &v.value {
                        Some(Value::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("")
            })
            .collect();
        assert_eq!(first_bodies, vec!["a1", "a2"], "group keeps row order");
        assert_eq!(second_bodies, vec!["b1", "b2"], "group keeps row order");

        let first_resource = request.resource_logs[0]
            .resource
            .as_ref()
            .expect("resource");
        let second_resource = request.resource_logs[1]
            .resource
            .as_ref()
            .expect("resource");
        assert!(
            first_resource
                .attributes
                .iter()
                .any(|kv| kv.key == "service.name"),
            "original dotted resource key preserved (not mangled to service_name)"
        );
        assert!(
            first_resource
                .attributes
                .iter()
                .any(|kv| kv.key == "deployment.environment"),
            "metadata resource attrs retained"
        );
        assert!(
            second_resource
                .attributes
                .iter()
                .any(|kv| kv.key == "service.name"),
            "second group carries service.name"
        );
    }

    #[test]
    fn resource_columns_not_emitted_as_log_record_attributes() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("resource.attributes.service.name", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("hello")])),
                Arc::new(StringArray::from(vec![Some("checkout")])),
                Arc::new(StringArray::from(vec![Some("host-a")])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request =
            ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert!(
            lr.attributes.iter().any(|kv| kv.key == "host"),
            "normal attrs remain log record attrs"
        );
        assert!(
            lr.attributes
                .iter()
                .all(|kv| kv.key != "resource.attributes.service.name"),
            "resource columns must not be log record attrs"
        );
    }

    #[test]
    fn empty_resource_attribute_key_is_dropped() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("resource.attributes.", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("hello")])),
                Arc::new(StringArray::from(vec![Some("bad-empty-key")])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request =
            ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
        if let Some(resource) = request.resource_logs[0].resource.as_ref() {
            assert!(
                resource.attributes.iter().all(|kv| !kv.key.is_empty()),
                "empty resource keys must not be emitted"
            );
        }
        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert!(
            lr.attributes
                .iter()
                .all(|kv| kv.key != "resource.attributes."),
            "malformed resource prefix column should not fall through to log attributes"
        );
    }

    #[test]
    fn typed_resource_columns_group_rows_and_encode_typed_attrs() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("resource.attributes.service.shard", DataType::Int64, true),
            Field::new(
                "resource.attributes.service.enabled",
                DataType::Boolean,
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a1"),
                    Some("b1"),
                    Some("a2"),
                    Some("b2"),
                ])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(1), Some(2)])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(false),
                ])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request =
            ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
        assert_eq!(request.resource_logs.len(), 2, "two typed resource groups");

        let first = &request.resource_logs[0];
        let second = &request.resource_logs[1];

        let first_bodies: Vec<&str> = first.scope_logs[0]
            .log_records
            .iter()
            .map(|lr| {
                lr.body
                    .as_ref()
                    .and_then(|v| match &v.value {
                        Some(Value::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("")
            })
            .collect();
        let second_bodies: Vec<&str> = second.scope_logs[0]
            .log_records
            .iter()
            .map(|lr| {
                lr.body
                    .as_ref()
                    .and_then(|v| match &v.value {
                        Some(Value::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("")
            })
            .collect();
        assert_eq!(first_bodies, vec!["a1", "a2"]);
        assert_eq!(second_bodies, vec!["b1", "b2"]);

        let first_attrs = &first.resource.as_ref().expect("resource").attributes;
        assert!(
            first_attrs.iter().any(|kv| {
                kv.key == "service.shard"
                    && matches!(
                        kv.value.as_ref().and_then(|v| v.value.as_ref()),
                        Some(Value::IntValue(1))
                    )
            }),
            "int resource attrs must be preserved and grouped"
        );
        assert!(
            first_attrs.iter().any(|kv| {
                kv.key == "service.enabled"
                    && matches!(
                        kv.value.as_ref().and_then(|v| v.value.as_ref()),
                        Some(Value::BoolValue(true))
                    )
            }),
            "bool resource attrs must be preserved and grouped"
        );
    }

    #[test]
    fn resource_grouping_treats_null_resource_values_as_distinct_combinations() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("resource.attributes.service.name", DataType::Utf8, true),
            Field::new("resource.attributes.service.shard", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a1"),
                    Some("n1"),
                    Some("a2"),
                    Some("n2"),
                    Some("b1"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("checkout"),
                    None,
                    Some("checkout"),
                    None,
                    Some("payments"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(1),
                    None,
                    Some(2),
                ])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());
        let request =
            ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
        assert_eq!(
            request.resource_logs.len(),
            4,
            "distinct null/non-null resource tuples should form separate groups"
        );

        let grouped_bodies: Vec<Vec<String>> = request
            .resource_logs
            .iter()
            .map(|rl| {
                rl.scope_logs[0]
                    .log_records
                    .iter()
                    .map(|lr| {
                        lr.body
                            .as_ref()
                            .and_then(|v| match &v.value {
                                Some(Value::StringValue(s)) => Some(s.clone()),
                                _ => None,
                            })
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            grouped_bodies,
            vec![
                vec!["a1".to_string(), "a2".to_string()], // checkout + shard=1
                vec!["n1".to_string()],                   // null service + shard=1
                vec!["n2".to_string()],                   // null service + null shard
                vec!["b1".to_string()],                   // payments + shard=2
            ],
            "row order must remain stable inside each resource group"
        );
    }

    #[test]
    fn generated_fast_otlp_matches_handwritten_encoder() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("flags", DataType::Int64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
            Field::new("latency", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("2024-01-15T10:30:00Z"),
                    Some("2024-01-15T10:30:01Z"),
                ])),
                Arc::new(StringArray::from(vec![Some("INFO"), Some("ERROR")])),
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
                Arc::new(StringArray::from(vec![
                    Some("0102030405060708090a0b0c0d0e0f10"),
                    Some("1112131415161718191a1b1c1d1e1f20"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("0102030405060708"),
                    Some("1112131415161718"),
                ])),
                Arc::new(Int64Array::from(vec![Some(1), Some(255)])),
                Arc::new(StringArray::from(vec![Some("web-01"), Some("web-02")])),
                Arc::new(Int64Array::from(vec![Some(42), Some(7)])),
                Arc::new(arrow::array::Float64Array::from(vec![Some(1.5), Some(2.5)])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                ])),
            ],
        )
        .expect("valid batch");

        let metadata = BatchMetadata {
            resource_attrs: Arc::new(vec![("service.name".to_string(), "otlp-test".to_string())]),
            observed_time_ns: 1_700_000_000_000_000_000,
        };

        let mut handwritten = make_sink();
        handwritten.encode_batch(&batch, &metadata);

        let mut generated = make_sink();
        generated.encode_batch_generated_fast(&batch, &metadata);

        assert_eq!(
            generated.encoded_payload(),
            handwritten.encoded_payload(),
            "generated-fast OTLP payload drifted from handwritten encoder",
        );
    }

    #[test]
    fn generated_fast_otlp_matches_handwritten_encoder_with_string_views() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8View, true),
            Field::new("level", DataType::Utf8View, true),
            Field::new("message", DataType::Utf8View, true),
            Field::new("trace_id", DataType::Utf8View, true),
            Field::new("span_id", DataType::Utf8View, true),
            Field::new("flags", DataType::Int64, true),
            Field::new("host", DataType::Utf8View, true),
            Field::new("count", DataType::Int64, true),
            Field::new("latency", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec![
                    Some("2024-01-15T10:30:00Z"),
                    Some("2024-01-15T10:30:01Z"),
                ])),
                Arc::new(StringViewArray::from(vec![Some("INFO"), Some("ERROR")])),
                Arc::new(StringViewArray::from(vec![Some("first"), Some("second")])),
                Arc::new(StringViewArray::from(vec![
                    Some("0102030405060708090a0b0c0d0e0f10"),
                    Some("1112131415161718191a1b1c1d1e1f20"),
                ])),
                Arc::new(StringViewArray::from(vec![
                    Some("0102030405060708"),
                    Some("1112131415161718"),
                ])),
                Arc::new(Int64Array::from(vec![Some(1), Some(255)])),
                Arc::new(StringViewArray::from(vec![Some("web-01"), Some("web-02")])),
                Arc::new(Int64Array::from(vec![Some(42), Some(7)])),
                Arc::new(arrow::array::Float64Array::from(vec![Some(1.5), Some(2.5)])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                ])),
            ],
        )
        .expect("valid batch");

        let metadata = BatchMetadata {
            resource_attrs: Arc::new(vec![("service.name".to_string(), "otlp-test".to_string())]),
            observed_time_ns: 1_700_000_000_000_000_000,
        };

        let mut handwritten = make_sink();
        handwritten.encode_batch(&batch, &metadata);

        let mut generated = make_sink();
        generated.encode_batch_generated_fast(&batch, &metadata);

        assert_eq!(
            generated.encoded_payload(),
            handwritten.encoded_payload(),
            "generated-fast OTLP payload drifted from handwritten encoder on Utf8View inputs",
        );
    }

    #[test]
    fn generated_fast_otlp_matches_handwritten_encoder_with_large_strings() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::LargeUtf8, true),
            Field::new("level", DataType::LargeUtf8, true),
            Field::new("message", DataType::LargeUtf8, true),
            Field::new("trace_id", DataType::LargeUtf8, true),
            Field::new("span_id", DataType::LargeUtf8, true),
            Field::new("flags", DataType::Int64, true),
            Field::new("host", DataType::LargeUtf8, true),
            Field::new("count", DataType::Int64, true),
            Field::new("latency", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(LargeStringArray::from(vec![
                    Some("2024-01-15T10:30:00Z"),
                    Some("2024-01-15T10:30:01Z"),
                ])),
                Arc::new(LargeStringArray::from(vec![Some("INFO"), Some("ERROR")])),
                Arc::new(LargeStringArray::from(vec![Some("first"), Some("second")])),
                Arc::new(LargeStringArray::from(vec![
                    Some("0102030405060708090a0b0c0d0e0f10"),
                    Some("1112131415161718191a1b1c1d1e1f20"),
                ])),
                Arc::new(LargeStringArray::from(vec![
                    Some("0102030405060708"),
                    Some("1112131415161718"),
                ])),
                Arc::new(Int64Array::from(vec![Some(1), Some(255)])),
                Arc::new(LargeStringArray::from(vec![Some("web-01"), Some("web-02")])),
                Arc::new(Int64Array::from(vec![Some(42), Some(7)])),
                Arc::new(arrow::array::Float64Array::from(vec![Some(1.5), Some(2.5)])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                ])),
            ],
        )
        .expect("valid batch");

        let metadata = BatchMetadata {
            resource_attrs: Arc::new(vec![("service.name".to_string(), "otlp-test".to_string())]),
            observed_time_ns: 1_700_000_000_000_000_000,
        };

        let mut handwritten = make_sink();
        handwritten.encode_batch(&batch, &metadata);

        let mut generated = make_sink();
        generated.encode_batch_generated_fast(&batch, &metadata);

        assert_eq!(
            generated.encoded_payload(),
            handwritten.encoded_payload(),
            "generated-fast OTLP payload drifted from handwritten encoder on LargeUtf8 inputs",
        );
    }

    /// An Int64 timestamp column (as produced by the OTLP receiver for `time_unix_nano`)
    /// must be recognised as the timestamp field and encoded into LogRecord.time_unix_nano.
    /// Before the fix, `resolve_batch_columns` rejected Int64 columns, so time_unix_nano
    /// was always 0 in OTLP→pipeline→OTLP pipelines.
    #[test]
    fn int64_timestamp_column_is_recognised() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        // 2024-01-15T10:30:00Z expressed as raw nanoseconds (as OTLP receiver produces).
        const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let ts_arr = Int64Array::from(vec![EXPECTED_NS as i64]);
        let body_arr = StringArray::from(vec!["hello"]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts_arr), Arc::new(body_arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode Int64-timestamp batch");

        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(
            lr.time_unix_nano, EXPECTED_NS,
            "Int64 time_unix_nano column must be encoded as LogRecord.time_unix_nano"
        );
    }

    #[test]
    fn epoch_zero_string_timestamp_is_encoded_as_valid_time() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("1970-01-01T00:00:00Z")])),
                Arc::new(StringArray::from(vec![Some("epoch")])),
            ],
        )
        .expect("valid batch");

        let mut handwritten = make_sink();
        handwritten.encode_batch(&batch, &make_metadata());

        let mut generated = make_sink();
        generated.encode_batch_generated_fast(&batch, &make_metadata());
        assert_eq!(generated.encoded_payload(), handwritten.encoded_payload());

        let request = ExportLogsServiceRequest::decode(handwritten.encoder_buf.as_slice())
            .expect("prost must decode epoch timestamp batch");
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(record.time_unix_nano, 0);
        assert!(
            contains_bytes(&handwritten.encoder_buf, &[0x09, 0, 0, 0, 0, 0, 0, 0, 0]),
            "valid epoch-zero timestamp should be emitted, not treated as parse failure"
        );
    }

    #[test]
    fn uint64_timestamp_column_is_recognised() {
        use arrow::array::UInt64Array;
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let ts_arr = UInt64Array::from(vec![EXPECTED_NS]);
        let body_arr = StringArray::from(vec!["hello"]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts_arr), Arc::new(body_arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode UInt64-timestamp batch");

        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(
            lr.time_unix_nano, EXPECTED_NS,
            "UInt64 time_unix_nano column must be encoded as LogRecord.time_unix_nano"
        );
    }

    #[test]
    fn uint64_attribute_encodes_or_drops_by_int64_range() {
        use arrow::array::UInt64Array;
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("count_u64", DataType::UInt64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("ok"), Some("too-big")])),
                Arc::new(UInt64Array::from(vec![Some(42u64), Some(u64::MAX)])),
            ],
        )
        .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode UInt64 attribute batch");
        let records = &request.resource_logs[0].scope_logs[0].log_records;
        let first = records[0]
            .attributes
            .iter()
            .find(|kv| kv.key == "count_u64")
            .expect("count_u64 attribute must be present");

        assert_eq!(
            first.value.as_ref().and_then(|v| match v.value.as_ref() {
                Some(Value::IntValue(v)) => Some(*v),
                _ => None,
            }),
            Some(42),
            "representable UInt64 must encode as AnyValue.int_value"
        );
        assert!(
            records[1].attributes.iter().all(|kv| kv.key != "count_u64"),
            "out-of-range UInt64 must not be stringified or wrapped into int_value"
        );
    }

    /// A Timestamp(Nanosecond) Arrow column must also be recognised as the timestamp field.
    #[test]
    fn timestamp_nanosecond_column_is_recognised() {
        use arrow::array::TimestampNanosecondArray;
        use arrow::datatypes::TimeUnit;
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let ts_arr = TimestampNanosecondArray::from(vec![EXPECTED_NS as i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode Timestamp(Nanosecond) batch");

        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(
            lr.time_unix_nano, EXPECTED_NS,
            "Timestamp(Nanosecond) column must be encoded as LogRecord.time_unix_nano"
        );
    }

    #[test]
    fn timestamp_second_column_is_recognised() {
        use arrow::array::TimestampSecondArray;
        use arrow::datatypes::TimeUnit;
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;
        const INPUT_S: i64 = 1_705_314_600; // EXPECTED_NS / 1_000_000_000

        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));
        let ts_arr = TimestampSecondArray::from(vec![INPUT_S]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode Timestamp(Second) batch");

        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(
            lr.time_unix_nano, EXPECTED_NS,
            "Timestamp(Second) column must be scaled to nanoseconds in LogRecord.time_unix_nano"
        );
    }

    #[test]
    fn timestamp_millisecond_column_is_recognised() {
        use arrow::array::TimestampMillisecondArray;
        use arrow::datatypes::TimeUnit;
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;
        const INPUT_MS: i64 = 1_705_314_600_000; // EXPECTED_NS / 1_000_000

        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let ts_arr = TimestampMillisecondArray::from(vec![INPUT_MS]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode Timestamp(Millisecond) batch");

        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(
            lr.time_unix_nano, EXPECTED_NS,
            "Timestamp(Millisecond) column must be scaled to nanoseconds in LogRecord.time_unix_nano"
        );
    }

    #[test]
    fn timestamp_microsecond_column_is_recognised() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;
        const INPUT_US: i64 = 1_705_314_600_000_000; // EXPECTED_NS / 1_000

        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let ts_arr = TimestampMicrosecondArray::from(vec![INPUT_US]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode Timestamp(Microsecond) batch");

        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(
            lr.time_unix_nano, EXPECTED_NS,
            "Timestamp(Microsecond) column must be scaled to nanoseconds in LogRecord.time_unix_nano"
        );
    }

    #[test]
    fn generated_fast_timestamp_numeric_columns_match_handwritten() {
        use arrow::array::{
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray, UInt64Array,
        };
        use arrow::datatypes::TimeUnit;

        const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;

        // Int64 nanos
        assert_timestamp_encoding_parity(
            Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::Int64, true),
                Field::new("body", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![EXPECTED_NS as i64])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
            EXPECTED_NS,
        );

        // UInt64 nanos
        assert_timestamp_encoding_parity(
            Arc::new(Schema::new(vec![
                Field::new("timestamp", DataType::UInt64, true),
                Field::new("body", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![EXPECTED_NS])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
            EXPECTED_NS,
        );

        // Timestamp(Nanosecond)
        assert_timestamp_encoding_parity(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )])),
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                EXPECTED_NS as i64,
            ]))],
            EXPECTED_NS,
        );

        // Timestamp(Microsecond)
        assert_timestamp_encoding_parity(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )])),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                1_705_314_600_000_000i64,
            ]))],
            EXPECTED_NS,
        );

        // Timestamp(Millisecond)
        assert_timestamp_encoding_parity(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )])),
            vec![Arc::new(TimestampMillisecondArray::from(vec![
                1_705_314_600_000i64,
            ]))],
            EXPECTED_NS,
        );

        // Timestamp(Second)
        assert_timestamp_encoding_parity(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            )])),
            vec![Arc::new(TimestampSecondArray::from(vec![1_705_314_600i64]))],
            EXPECTED_NS,
        );
    }
}
