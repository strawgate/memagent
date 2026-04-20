//! Experimental OTLP logs wire projection into Arrow.
//!
//! This module decodes the common OTLP logs protobuf shape directly into
//! a [`ColumnarBatchBuilder`], bypassing allocation of the full prost object
//! graph. It intentionally implements a projection, not a complete protobuf
//! object model: unsupported semantic cases return [`ProjectionError::Unsupported`]
//! so the receiver can fall back to the prost decoder.

use std::fmt;

use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use logfwd_arrow::columnar::builder::ColumnarBatchBuilder;
use logfwd_arrow::columnar::plan::FieldHandle;

use crate::InputError;

mod generated;
use generated::field_numbers as otlp_field;

#[derive(Debug)]
pub(super) enum ProjectionError {
    Invalid(&'static str),
    Unsupported(&'static str),
    Batch(String),
}

impl ProjectionError {
    /// Convert to `InputError`, preserving the `Unsupported` variant so
    /// callers can fall back to the prost reference decoder.
    pub(super) fn into_input_error(self) -> InputError {
        match self {
            ProjectionError::Unsupported(msg) => InputError::Unsupported(msg.to_string()),
            other => InputError::Receiver(other.to_string()),
        }
    }
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProjectionError::Invalid(msg) => write!(f, "invalid OTLP protobuf: {msg}"),
            ProjectionError::Unsupported(msg) => {
                write!(f, "unsupported OTLP projection case: {msg}")
            }
            ProjectionError::Batch(msg) => write!(f, "OTLP projection batch build error: {msg}"),
        }
    }
}

#[derive(Clone, Copy)]
enum WireField<'a> {
    Varint(u64),
    Fixed64(u64),
    Len(&'a [u8]),
    Fixed32(u32),
}

#[derive(Clone, Copy)]
enum WireAny<'a> {
    String(&'a [u8]),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(&'a [u8]),
    ArrayRaw(&'a [u8]),
    KvListRaw(&'a [u8]),
}

#[derive(Clone, Copy)]
enum StringStorage {
    Decoded,
    #[cfg(any(feature = "otlp-research", test))]
    InputView,
}

#[derive(Clone, Copy, Default)]
struct ScopeFields<'a> {
    name: Option<&'a [u8]>,
    version: Option<&'a [u8]>,
}

/// Decode an OTLP ExportLogsServiceRequest payload directly into an Arrow batch.
///
/// `body` contains the protobuf payload bytes and `resource_prefix` selects the
/// column prefix used for projected resource attributes. This detached variant
/// decodes strings into builder-owned storage before returning a `RecordBatch`.
pub(super) fn decode_projected_otlp_logs(
    body: &[u8],
    resource_prefix: &str,
) -> Result<RecordBatch, ProjectionError> {
    decode_projected_otlp_logs_inner(body, Bytes::new(), resource_prefix, StringStorage::Decoded)
}

#[cfg(any(feature = "otlp-research", test))]
/// Decode an OTLP ExportLogsServiceRequest payload into an Arrow batch that
/// views the request buffer where possible.
///
/// `body` is cloned as a cheap `Bytes` handle for batch backing storage, while
/// `resource_prefix` selects the projected resource-attribute column prefix.
/// This variant is available for `otlp-research` and tests.
pub(super) fn decode_projected_otlp_logs_view_bytes(
    body: Bytes,
    resource_prefix: &str,
) -> Result<RecordBatch, ProjectionError> {
    let backing = body.clone();
    decode_projected_otlp_logs_inner(
        body.as_ref(),
        backing,
        resource_prefix,
        StringStorage::InputView,
    )
}

/// Classify whether the payload is eligible for direct projection.
///
/// This generated preflight is used only by fallback mode. It allows
/// unsupported-but-valid OTLP shapes to skip builder mutation and go straight
/// to the prost fallback path while preserving malformed-wire rejection.
pub(super) fn classify_projected_fallback_support(body: &[u8]) -> Result<(), ProjectionError> {
    generated::classify_projection_support(body)
}

/// Reusable OTLP projected decoder.
///
/// Holds the `ColumnarBatchBuilder`, field handles, and scratch buffers so
/// successive batches reuse allocated capacity instead of re-allocating from
/// scratch.  This is the production-intended usage pattern: create once,
/// call [`decode_view_bytes`](Self::decode_view_bytes) per request.
#[cfg(any(feature = "otlp-research", test))]
pub struct ProjectedOtlpDecoder {
    builder: ColumnarBatchBuilder,
    handles: generated::OtlpFieldHandles,
    scratch: WireScratch,
    resource_prefix: String,
}

#[cfg(any(feature = "otlp-research", test))]
impl ProjectedOtlpDecoder {
    /// Create a reusable decoder with the given resource attribute prefix.
    pub fn new(resource_prefix: &str) -> Self {
        let (plan, handles) = generated::build_otlp_plan();
        Self {
            builder: ColumnarBatchBuilder::new(plan),
            handles,
            scratch: WireScratch::default(),
            resource_prefix: resource_prefix.to_owned(),
        }
    }

    /// Decode an OTLP payload using the view-bytes path, reusing builder capacity.
    pub fn decode_view_bytes(&mut self, body: Bytes) -> Result<RecordBatch, InputError> {
        self.try_decode_view_bytes(body)
            .map_err(ProjectionError::into_input_error)
    }

    pub(super) fn try_decode_view_bytes(
        &mut self,
        body: Bytes,
    ) -> Result<RecordBatch, ProjectionError> {
        let backing = body.clone();
        self.builder.begin_batch();
        if !backing.is_empty() {
            self.builder.set_original_buffer(Buffer::from(backing));
        }

        // Invalidate cached dynamic handles from the previous batch — they
        // reference columns that were drained by begin_batch().
        for entry in &mut self.scratch.attr_field_cache {
            entry.handle = None;
        }

        let string_storage = StringStorage::InputView;
        let decode_result = for_each_field(body.as_ref(), |field, value| {
            match (field, value) {
                (otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS, WireField::Len(resource_logs)) => {
                    decode_resource_logs_wire(
                        &mut self.builder,
                        &self.handles,
                        &mut self.scratch,
                        &self.resource_prefix,
                        resource_logs,
                        string_storage,
                    )?;
                }
                (otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS, _) => {
                    return Err(ProjectionError::Invalid(
                        "invalid wire type for ExportLogsServiceRequest.resource_logs",
                    ));
                }
                _ => {}
            }
            Ok(())
        });

        if let Err(e) = decode_result {
            // Reset builder to idle so the next begin_batch succeeds even
            // if we were mid-row when the error occurred.
            self.builder.discard_batch();
            return Err(e);
        }

        self.builder
            .finish_batch()
            .map_err(|e| ProjectionError::Batch(format!("batch build error: {e}")))
    }
}

fn decode_projected_otlp_logs_inner(
    body: &[u8],
    backing: Bytes,
    resource_prefix: &str,
    string_storage: StringStorage,
) -> Result<RecordBatch, ProjectionError> {
    if body.is_empty() {
        return Ok(RecordBatch::new_empty(
            arrow::datatypes::Schema::empty().into(),
        ));
    }

    let (plan, fields) = generated::build_otlp_plan();
    let mut builder = ColumnarBatchBuilder::new(plan);
    builder.begin_batch();
    if !backing.is_empty() {
        builder.set_original_buffer(Buffer::from(backing));
    }
    let mut scratch = WireScratch::default();

    for_each_field(body, |field, value| {
        match (field, value) {
            (otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS, WireField::Len(resource_logs)) => {
                decode_resource_logs_wire(
                    &mut builder,
                    &fields,
                    &mut scratch,
                    resource_prefix,
                    resource_logs,
                    string_storage,
                )?;
            }
            (otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ExportLogsServiceRequest.resource_logs",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;

    builder.finish_batch().map_err(|e| {
        ProjectionError::Batch(format!("structured OTLP projection batch build error: {e}"))
    })
}

#[derive(Default)]
struct WireScratch {
    hex: Vec<u8>,
    decimal: Vec<u8>,
    json: Vec<u8>,
    resource_key: Vec<u8>,
    attr_ranges: Vec<(usize, usize)>,
    attr_field_cache: Vec<AttrFieldCache>,
}

#[derive(Default)]
struct AttrFieldCache {
    key: Vec<u8>,
    handle: Option<FieldHandle>,
}

fn decode_resource_logs_wire(
    builder: &mut ColumnarBatchBuilder,
    fields: &generated::OtlpFieldHandles,
    scratch: &mut WireScratch,
    resource_prefix: &str,
    resource_logs: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    let mut resource_attrs = Vec::new();

    for_each_field(resource_logs, |field, value| {
        match (field, value) {
            (otlp_field::RESOURCE_LOGS_RESOURCE, WireField::Len(resource)) => {
                collect_resource_attrs(
                    builder,
                    scratch,
                    &mut resource_attrs,
                    resource_prefix,
                    resource,
                )?;
            }
            (otlp_field::RESOURCE_LOGS_RESOURCE, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ResourceLogs.resource",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;

    for_each_field(resource_logs, |field, value| {
        match (field, value) {
            (otlp_field::RESOURCE_LOGS_SCOPE_LOGS, WireField::Len(scope_logs)) => {
                decode_scope_logs_wire(
                    builder,
                    fields,
                    scratch,
                    &resource_attrs,
                    scope_logs,
                    string_storage,
                )?;
            }
            (otlp_field::RESOURCE_LOGS_SCOPE_LOGS, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ResourceLogs.scope_logs",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

fn collect_resource_attrs<'a>(
    builder: &mut ColumnarBatchBuilder,
    scratch: &mut WireScratch,
    resource_attrs: &mut Vec<(FieldHandle, WireAny<'a>)>,
    resource_prefix: &str,
    resource: &'a [u8],
) -> Result<(), ProjectionError> {
    for_each_field(resource, |field, value| {
        match (field, value) {
            (otlp_field::RESOURCE_ATTRIBUTES, WireField::Len(attr)) => {
                if let Some((key, value)) = decode_key_value_wire(attr)? {
                    scratch.resource_key.clear();
                    scratch
                        .resource_key
                        .reserve(resource_prefix.len() + key.len());
                    scratch
                        .resource_key
                        .extend_from_slice(resource_prefix.as_bytes());
                    scratch.resource_key.extend_from_slice(key);
                    let key_str = std::str::from_utf8(&scratch.resource_key)
                        .expect("resource prefix + validated UTF-8 key must be valid UTF-8");
                    let handle = builder
                        .resolve_dynamic(key_str, generated::wire_any_field_kind(&value))
                        .map_err(|e| {
                            ProjectionError::Batch(format!("resolve resource attr: {e}"))
                        })?;
                    resource_attrs.push((handle, value));
                }
            }
            (otlp_field::RESOURCE_ATTRIBUTES, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for Resource.attributes",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

fn decode_scope_logs_wire(
    builder: &mut ColumnarBatchBuilder,
    fields: &generated::OtlpFieldHandles,
    scratch: &mut WireScratch,
    resource_attrs: &[(FieldHandle, WireAny<'_>)],
    scope_logs: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    let mut scope_fields = ScopeFields::default();

    for_each_field(scope_logs, |field, value| {
        match (field, value) {
            (otlp_field::SCOPE_LOGS_SCOPE, WireField::Len(scope)) => {
                merge_scope_wire(scope, &mut scope_fields)?;
            }
            (otlp_field::SCOPE_LOGS_SCOPE, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ScopeLogs.scope",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;

    for_each_field(scope_logs, |field, value| {
        match (field, value) {
            (otlp_field::SCOPE_LOGS_LOG_RECORDS, WireField::Len(log_record)) => {
                decode_log_record_wire(
                    builder,
                    fields,
                    scratch,
                    resource_attrs,
                    scope_fields,
                    log_record,
                    string_storage,
                )?;
            }
            (otlp_field::SCOPE_LOGS_LOG_RECORDS, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ScopeLogs.log_records",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

fn merge_scope_wire<'a>(
    scope: &'a [u8],
    scope_fields: &mut ScopeFields<'a>,
) -> Result<(), ProjectionError> {
    for_each_field(scope, |field, value| {
        match (field, value) {
            (otlp_field::INSTRUMENTATION_SCOPE_NAME, WireField::Len(value)) => {
                scope_fields.name = Some(require_utf8(value, "invalid UTF-8 scope name")?);
            }
            (otlp_field::INSTRUMENTATION_SCOPE_NAME, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for InstrumentationScope.name",
                ));
            }
            (otlp_field::INSTRUMENTATION_SCOPE_VERSION, WireField::Len(value)) => {
                scope_fields.version = Some(require_utf8(value, "invalid UTF-8 scope version")?);
            }
            (otlp_field::INSTRUMENTATION_SCOPE_VERSION, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for InstrumentationScope.version",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;
    Ok(())
}

fn decode_log_record_wire(
    builder: &mut ColumnarBatchBuilder,
    fields: &generated::OtlpFieldHandles,
    scratch: &mut WireScratch,
    resource_attrs: &[(FieldHandle, WireAny<'_>)],
    scope_fields: ScopeFields<'_>,
    log_record: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    let mut time_unix_nano = 0;
    let mut observed_time_unix_nano = 0;
    let mut severity_number = 0;
    let mut severity_text = None;
    let mut body = None;
    let mut trace_id = None;
    let mut span_id = None;
    let mut flags = 0;
    scratch.attr_ranges.clear();

    for_each_field(log_record, |field, value| {
        match (field, value) {
            (otlp_field::LOG_RECORD_TIME_UNIX_NANO, WireField::Fixed64(value)) => {
                time_unix_nano = value;
            }
            (otlp_field::LOG_RECORD_TIME_UNIX_NANO, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.time_unix_nano",
                ));
            }
            (otlp_field::LOG_RECORD_OBSERVED_TIME_UNIX_NANO, WireField::Fixed64(value)) => {
                observed_time_unix_nano = value;
            }
            (otlp_field::LOG_RECORD_OBSERVED_TIME_UNIX_NANO, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.observed_time_unix_nano",
                ));
            }
            (otlp_field::LOG_RECORD_SEVERITY_NUMBER, WireField::Varint(value)) => {
                severity_number = i64::from(value as i32);
            }
            (otlp_field::LOG_RECORD_SEVERITY_NUMBER, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.severity_number",
                ));
            }
            (otlp_field::LOG_RECORD_SEVERITY_TEXT, WireField::Len(value)) => {
                severity_text = if value.is_empty() {
                    None
                } else {
                    Some(require_utf8(value, "invalid UTF-8 severity text")?)
                };
            }
            (otlp_field::LOG_RECORD_SEVERITY_TEXT, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.severity_text",
                ));
            }
            (otlp_field::LOG_RECORD_BODY, WireField::Len(value)) => {
                if let Some(value) = decode_any_value_wire(value)? {
                    body = Some(value);
                }
            }
            (otlp_field::LOG_RECORD_BODY, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.body",
                ));
            }
            (otlp_field::LOG_RECORD_TRACE_ID, WireField::Len(value)) => {
                trace_id = (!value.is_empty()).then_some(value);
            }
            (otlp_field::LOG_RECORD_TRACE_ID, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.trace_id",
                ));
            }
            (otlp_field::LOG_RECORD_SPAN_ID, WireField::Len(value)) => {
                span_id = (!value.is_empty()).then_some(value);
            }
            (otlp_field::LOG_RECORD_SPAN_ID, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.span_id",
                ));
            }
            (otlp_field::LOG_RECORD_FLAGS, WireField::Fixed32(value)) => flags = i64::from(value),
            (otlp_field::LOG_RECORD_FLAGS, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.flags",
                ));
            }
            (otlp_field::LOG_RECORD_ATTRIBUTES, WireField::Len(value)) => {
                scratch.attr_ranges.push(subslice_range(log_record, value)?);
            }
            (otlp_field::LOG_RECORD_ATTRIBUTES, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.attributes",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;

    builder.begin_row();

    let timestamp = if time_unix_nano > 0 {
        time_unix_nano
    } else {
        observed_time_unix_nano
    };
    if let Ok(value) = i64::try_from(timestamp)
        && value > 0
    {
        builder.write_i64(fields.timestamp, value);
    }
    if let Ok(value) = i64::try_from(observed_time_unix_nano)
        && value > 0
    {
        builder.write_i64(fields.observed_timestamp, value);
    }
    if let Some(value) = severity_text {
        write_wire_str(builder, fields.severity, value, string_storage)?;
    }
    if severity_number > 0 {
        builder.write_i64(fields.severity_number, severity_number);
    }
    if let Some(value) = body {
        generated::write_wire_any_as_string(builder, fields.body, value, scratch, string_storage)?;
    }
    if let Some(value) = trace_id {
        write_hex_field(builder, fields.trace_id, value, &mut scratch.hex)?;
    }
    if let Some(value) = span_id {
        write_hex_field(builder, fields.span_id, value, &mut scratch.hex)?;
    }
    if flags > 0 {
        builder.write_i64(fields.flags, flags);
    }
    if let Some(value) = scope_fields.name
        && !value.is_empty()
    {
        write_wire_str(builder, fields.scope_name, value, string_storage)?;
    }
    if let Some(value) = scope_fields.version
        && !value.is_empty()
    {
        write_wire_str(builder, fields.scope_version, value, string_storage)?;
    }

    // Decode and write record attributes inline to avoid a per-row Vec allocation.
    // `attr_ranges` entries are Copy so the immutable index doesn't conflict with
    // the mutable scratch borrows needed by resolve/write.
    for attr_idx in 0..scratch.attr_ranges.len() {
        let (start, len) = scratch.attr_ranges[attr_idx];
        let attr = &log_record[start..start + len];
        if let Some((key, value)) = decode_key_value_wire(attr)? {
            let handle = resolve_record_attr_field(
                builder,
                &mut scratch.attr_field_cache,
                attr_idx,
                key,
                &value,
            )?;
            generated::write_wire_any(builder, handle, value, scratch, string_storage)?;
        }
    }

    for &(handle, value) in resource_attrs {
        generated::write_wire_any(builder, handle, value, scratch, string_storage)?;
    }

    builder.end_row();
    Ok(())
}

fn decode_key_value_wire(kv: &[u8]) -> Result<Option<(&[u8], WireAny<'_>)>, ProjectionError> {
    generated::decode_key_value_wire(kv)
}

fn resolve_record_attr_field(
    builder: &mut ColumnarBatchBuilder,
    cache: &mut Vec<AttrFieldCache>,
    position: usize,
    key: &[u8],
    value: &WireAny<'_>,
) -> Result<FieldHandle, ProjectionError> {
    if let Some(cached) = cache.get(position)
        && cached.key.as_slice() == key
        && let Some(handle) = cached.handle
    {
        return Ok(handle);
    }

    let key_str = std::str::from_utf8(key).expect("attribute key already validated as UTF-8");
    let handle = builder
        .resolve_dynamic(key_str, generated::wire_any_field_kind(value))
        .map_err(|e| ProjectionError::Batch(format!("resolve attr field: {e}")))?;
    if let Some(cached) = cache.get_mut(position) {
        cached.key.clear();
        cached.key.extend_from_slice(key);
        cached.handle = Some(handle);
    } else {
        cache.push(AttrFieldCache {
            key: key.to_vec(),
            handle: Some(handle),
        });
    }
    Ok(handle)
}

fn decode_any_value_wire(value: &[u8]) -> Result<Option<WireAny<'_>>, ProjectionError> {
    generated::decode_any_value_wire(value)
}

fn require_utf8<'a>(bytes: &'a [u8], context: &'static str) -> Result<&'a [u8], ProjectionError> {
    if simdutf8::basic::from_utf8(bytes).is_err() {
        return Err(ProjectionError::Invalid(context));
    }
    Ok(bytes)
}

fn subslice_range(parent: &[u8], child: &[u8]) -> Result<(usize, usize), ProjectionError> {
    let parent_start = parent.as_ptr() as usize;
    let child_start = child.as_ptr() as usize;
    let Some(child_end) = child_start.checked_add(child.len()) else {
        return Err(ProjectionError::Invalid("invalid protobuf subslice"));
    };
    let Some(parent_end) = parent_start.checked_add(parent.len()) else {
        return Err(ProjectionError::Invalid("invalid protobuf parent slice"));
    };
    if child_start < parent_start || child_end > parent_end {
        return Err(ProjectionError::Invalid(
            "protobuf field outside parent slice",
        ));
    }
    Ok((child_start - parent_start, child.len()))
}

fn write_wire_any_complex_json(
    builder: &mut ColumnarBatchBuilder,
    handle: FieldHandle,
    value: WireAny<'_>,
    scratch: &mut WireScratch,
) -> Result<(), ProjectionError> {
    let mut json = std::mem::take(&mut scratch.json);
    json.clear();
    generated::write_wire_any_json(value, &mut json, scratch)?;
    builder
        .write_str_bytes(handle, &json)
        .map_err(|e| ProjectionError::Batch(e.to_string()))?;
    scratch.json = json;
    Ok(())
}

fn write_json_escaped_bytes(out: &mut Vec<u8>, value: &[u8]) {
    for &b in value {
        match b {
            b'"' => out.extend_from_slice(br#"\""#),
            b'\\' => out.extend_from_slice(br"\\"),
            b'\n' => out.extend_from_slice(br"\n"),
            b'\r' => out.extend_from_slice(br"\r"),
            b'\t' => out.extend_from_slice(br"\t"),
            0x08 => out.extend_from_slice(br"\b"),
            0x0c => out.extend_from_slice(br"\f"),
            0x00..=0x1f => {
                out.extend_from_slice(br"\u00");
                let hex = b"0123456789abcdef";
                out.push(hex[(b >> 4) as usize]);
                out.push(hex[(b & 0x0f) as usize]);
            }
            _ => out.push(b),
        }
    }
}

fn write_wire_str(
    builder: &mut ColumnarBatchBuilder,
    handle: FieldHandle,
    value: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    match string_storage {
        StringStorage::Decoded => builder
            .write_str_bytes(handle, value)
            .map_err(|e| ProjectionError::Batch(e.to_string()))?,
        #[cfg(any(feature = "otlp-research", test))]
        StringStorage::InputView => builder
            .write_input_ref(handle, value)
            .map_err(|e| ProjectionError::Batch(e.to_string()))?,
    }
    Ok(())
}

fn write_hex_field(
    builder: &mut ColumnarBatchBuilder,
    handle: FieldHandle,
    bytes: &[u8],
    hex_buf: &mut Vec<u8>,
) -> Result<(), ProjectionError> {
    hex_buf.clear();
    write_hex_to_buf(hex_buf, bytes);
    builder
        .write_str_bytes(handle, hex_buf)
        .map_err(|e| ProjectionError::Batch(e.to_string()))
}

fn write_hex_to_buf(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 0x0f) as usize]);
    }
}

fn for_each_field<'a>(
    mut input: &'a [u8],
    mut visit: impl FnMut(u32, WireField<'a>) -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    while !input.is_empty() {
        let key = read_varint(&mut input)?;
        let field = decode_field_number(key)?;
        let wire_type = (key & 0x07) as u8;
        match wire_type {
            0 => visit(field, WireField::Varint(read_varint(&mut input)?))?,
            1 => {
                if input.len() < 8 {
                    return Err(ProjectionError::Invalid("truncated fixed64 field"));
                }
                let (bytes, rest) = input.split_at(8);
                input = rest;
                visit(
                    field,
                    WireField::Fixed64(u64::from_le_bytes(
                        bytes.try_into().expect("fixed64 slice has 8 bytes"),
                    )),
                )?;
            }
            2 => {
                let len = usize::try_from(read_varint(&mut input)?)
                    .map_err(|_| ProjectionError::Invalid("protobuf length exceeds usize"))?;
                if input.len() < len {
                    return Err(ProjectionError::Invalid("truncated length-delimited field"));
                }
                let (bytes, rest) = input.split_at(len);
                input = rest;
                visit(field, WireField::Len(bytes))?;
            }
            5 => {
                if input.len() < 4 {
                    return Err(ProjectionError::Invalid("truncated fixed32 field"));
                }
                let (bytes, rest) = input.split_at(4);
                input = rest;
                visit(
                    field,
                    WireField::Fixed32(u32::from_le_bytes(
                        bytes.try_into().expect("fixed32 slice has 4 bytes"),
                    )),
                )?;
            }
            3 => skip_group(&mut input, field)?,
            4 => return Err(ProjectionError::Invalid("unexpected protobuf end group")),
            _ => return Err(ProjectionError::Invalid("invalid protobuf wire type")),
        }
    }
    Ok(())
}

fn decode_field_number(key: u64) -> Result<u32, ProjectionError> {
    const PROTOBUF_MAX_FIELD_NUMBER: u64 = 0x1FFF_FFFF;

    let field = key >> 3;
    if field == 0 {
        return Err(ProjectionError::Invalid("protobuf field number zero"));
    }
    if field > PROTOBUF_MAX_FIELD_NUMBER {
        return Err(ProjectionError::Invalid(
            "protobuf field number out of range",
        ));
    }
    u32::try_from(field).map_err(|_| ProjectionError::Invalid("protobuf field number overflow"))
}

const PROTOBUF_MAX_GROUP_DEPTH: usize = 64;

fn skip_group(input: &mut &[u8], start_field: u32) -> Result<(), ProjectionError> {
    let mut field_stack = [0u32; PROTOBUF_MAX_GROUP_DEPTH];
    let mut depth = 1usize;
    field_stack[0] = start_field;

    while !input.is_empty() {
        let key = read_varint(input)?;
        let field = decode_field_number(key)?;
        let wire_type = (key & 0x07) as u8;
        match wire_type {
            0 => {
                let _ = read_varint(input)?;
            }
            1 => {
                if input.len() < 8 {
                    return Err(ProjectionError::Invalid("truncated fixed64 field"));
                }
                *input = &input[8..];
            }
            2 => {
                let len = usize::try_from(read_varint(input)?)
                    .map_err(|_| ProjectionError::Invalid("protobuf length exceeds usize"))?;
                if input.len() < len {
                    return Err(ProjectionError::Invalid("truncated length-delimited field"));
                }
                *input = &input[len..];
            }
            3 => {
                if depth == PROTOBUF_MAX_GROUP_DEPTH {
                    return Err(ProjectionError::Invalid("protobuf group nesting too deep"));
                }
                field_stack[depth] = field;
                depth += 1;
            }
            4 => {
                if field != field_stack[depth - 1] {
                    return Err(ProjectionError::Invalid("mismatched protobuf end group"));
                }
                depth -= 1;
                if depth == 0 {
                    return Ok(());
                }
            }
            5 => {
                if input.len() < 4 {
                    return Err(ProjectionError::Invalid("truncated fixed32 field"));
                }
                *input = &input[4..];
            }
            _ => return Err(ProjectionError::Invalid("invalid protobuf wire type")),
        }
    }
    Err(ProjectionError::Invalid("unterminated protobuf group"))
}

fn read_varint(input: &mut &[u8]) -> Result<u64, ProjectionError> {
    let mut result = 0u64;
    for index in 0..10 {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(ProjectionError::Invalid("truncated varint"));
        };
        *input = rest;
        if index == 9 && byte > 0x01 {
            return Err(ProjectionError::Invalid("varint overflow"));
        }
        result |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(ProjectionError::Invalid("varint overflow"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use logfwd_types::field_names;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
    };
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use proptest::prelude::*;
    use prost::Message as _;

    use super::*;
    use crate::otlp_receiver::convert::convert_request_to_batch;

    fn primitive_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        kv_string("service.name", "checkout"),
                        kv_bool("resource.sampled", true),
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope-a".into(),
                        version: "1.2.3".into(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_712_509_200_123_456_789,
                        observed_time_unix_nano: 1_712_509_200_123_456_999,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(any_string("hello")),
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        flags: 1,
                        attributes: vec![
                            kv_i64("status", 200),
                            kv_f64("duration_ms", 12.5),
                            kv_bool("success", true),
                            kv_bytes("payload", &[0xde, 0xad, 0xbe, 0xef]),
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn generated_otlp_plan_declares_expected_planned_fields() {
        let (plan, handles) = generated::build_otlp_plan();

        assert_eq!(
            plan.lookup(field_names::TIMESTAMP)
                .expect("timestamp handle should exist"),
            handles.timestamp
        );
        assert_eq!(
            plan.lookup(field_names::OBSERVED_TIMESTAMP)
                .expect("observed timestamp handle should exist"),
            handles.observed_timestamp
        );
        assert_eq!(
            plan.lookup(field_names::SEVERITY)
                .expect("severity handle should exist"),
            handles.severity
        );
        assert_eq!(
            plan.lookup(field_names::SEVERITY_NUMBER)
                .expect("severity number handle should exist"),
            handles.severity_number
        );
        assert_eq!(
            plan.lookup(field_names::BODY)
                .expect("body handle should exist"),
            handles.body
        );
        assert_eq!(
            plan.lookup(field_names::TRACE_ID)
                .expect("trace id handle should exist"),
            handles.trace_id
        );
        assert_eq!(
            plan.lookup(field_names::SPAN_ID)
                .expect("span id handle should exist"),
            handles.span_id
        );
        assert_eq!(
            plan.lookup(field_names::FLAGS)
                .expect("flags handle should exist"),
            handles.flags
        );
        assert_eq!(
            plan.lookup(field_names::SCOPE_NAME)
                .expect("scope name handle should exist"),
            handles.scope_name
        );
        assert_eq!(
            plan.lookup(field_names::SCOPE_VERSION)
                .expect("scope version handle should exist"),
            handles.scope_version
        );
        assert_eq!(plan.num_planned(), 10);
    }

    #[test]
    fn projected_primitive_request_matches_prost_conversion() {
        let request = primitive_request();
        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_bytes_api_keeps_supported_strings_attached_to_request_body() {
        let body = Bytes::from(primitive_request().encode_to_vec());
        let batch = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
            body.clone(),
        )
        .expect("experimental projection should decode primitive request");

        assert!(
            logfwd_arrow::materialize::is_attached(&batch, &body),
            "projected string values should be Arrow views backed by the OTLP body"
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name(field_names::BODY)
                .expect("body field should exist")
                .data_type(),
            &arrow::datatypes::DataType::Utf8View
        );
    }

    #[test]
    fn projected_duplicate_names_match_prost_conversion() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv_string("service.name", "checkout")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(any_string("canonical-body")),
                        trace_id: vec![0xaa; 16],
                        flags: 1,
                        attributes: vec![
                            kv_string("body", "attr-body-shadow"),
                            kv_string("trace_id", "attr-trace-shadow"),
                            kv_string("flags", "attr-flags-shadow"),
                            kv_string("resource_shadow.service.name", "attr-resource-shadow"),
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_empty_key_attribute_matches_prost_conversion() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        attributes: vec![kv_string("", "empty-key")],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_repeated_scope_messages_merge_like_prost() {
        let first_scope = InstrumentationScope {
            name: "scope-a".into(),
            ..Default::default()
        }
        .encode_to_vec();
        let second_scope = InstrumentationScope {
            version: "1.2.3".into(),
            ..Default::default()
        }
        .encode_to_vec();
        let record = LogRecord {
            body: Some(any_string("hello")),
            ..Default::default()
        }
        .encode_to_vec();

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &first_scope);
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &second_scope);
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_repeated_empty_scope_name_clears_prior_scope_like_prost() {
        let first_scope = InstrumentationScope {
            name: "scope-a".into(),
            version: "1.2.3".into(),
            ..Default::default()
        }
        .encode_to_vec();
        let mut second_scope = Vec::new();
        encode_len_field(&mut second_scope, otlp_field::INSTRUMENTATION_SCOPE_NAME, b"");
        encode_len_field(&mut second_scope, otlp_field::INSTRUMENTATION_SCOPE_VERSION, b"");
        let record = LogRecord {
            body: Some(any_string("hello")),
            ..Default::default()
        }
        .encode_to_vec();

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &first_scope);
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &second_scope);
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_unknown_fields_match_prost_conversion() {
        let request = primitive_request();
        let mut payload = request.encode_to_vec();
        encode_len_field(&mut payload, 99, b"ignored-top-level-field");

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_unknown_fields_for_all_supported_wire_types_match_prost_conversion() {
        let request = primitive_request();
        let mut payload = request.encode_to_vec();
        encode_varint_field(&mut payload, 98, 123);
        encode_fixed64_field(&mut payload, 99, 0x0102_0304_0506_0708);
        encode_len_field(&mut payload, 100, b"ignored-top-level-field");
        encode_fixed32_field(&mut payload, 101, 0x0a0b_0c0d);

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_nested_unknown_field_interleavings_match_prost_conversion() {
        let mut any_value = Vec::new();
        encode_varint_field(&mut any_value, 98, 1);
        encode_len_field(&mut any_value, otlp_field::ANY_VALUE_STRING_VALUE, b"body");
        encode_fixed64_field(&mut any_value, 99, 0x0102_0304_0506_0708);

        let mut attr = Vec::new();
        encode_len_field(&mut attr, 77, b"ignored-kv-prefix");
        encode_len_field(&mut attr, otlp_field::KEY_VALUE_KEY, b"attr");
        encode_start_group(&mut attr, 78);
        encode_varint_field(&mut attr, 79, 123);
        encode_end_group(&mut attr, 78);
        encode_len_field(&mut attr, otlp_field::KEY_VALUE_VALUE, &any_value);

        let mut scope = Vec::new();
        encode_varint_field(&mut scope, 77, 7);
        encode_len_field(&mut scope, otlp_field::INSTRUMENTATION_SCOPE_NAME, b"scope");
        encode_start_group(&mut scope, 78);
        encode_len_field(&mut scope, 79, b"inside-scope-group");
        encode_end_group(&mut scope, 78);
        encode_len_field(&mut scope, otlp_field::INSTRUMENTATION_SCOPE_VERSION, b"1.0.0");

        let mut log_record = Vec::new();
        encode_fixed32_field(&mut log_record, 77, 0x0a0b_0c0d);
        encode_fixed64_field(&mut log_record, otlp_field::LOG_RECORD_TIME_UNIX_NANO, 123);
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any_value);
        encode_start_group(&mut log_record, 78);
        encode_varint_field(&mut log_record, 79, 456);
        encode_end_group(&mut log_record, 78);
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_ATTRIBUTES, &attr);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, 77, b"ignored-scope-logs-prefix");
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &scope);
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);
        encode_fixed32_field(&mut scope_logs, 78, 99);

        let mut resource = Vec::new();
        encode_varint_field(&mut resource, 77, 1);
        encode_len_field(&mut resource, otlp_field::RESOURCE_ATTRIBUTES, &attr);
        encode_start_group(&mut resource, 78);
        encode_end_group(&mut resource, 78);

        let mut resource_logs = Vec::new();
        encode_len_field(&mut resource_logs, 77, b"ignored-resource-logs-prefix");
        encode_len_field(&mut resource_logs, otlp_field::RESOURCE_LOGS_RESOURCE, &resource);
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        encode_fixed64_field(&mut resource_logs, 78, 0x1111_2222_3333_4444);

        let mut payload = Vec::new();
        encode_start_group(&mut payload, 99);
        encode_varint_field(&mut payload, 100, 1);
        encode_end_group(&mut payload, 99);
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );
        encode_fixed32_field(&mut payload, 101, 0x0102_0304);

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_oversized_field_number_is_invalid() {
        let mut payload = Vec::new();
        encode_varint(&mut payload, (u64::from(u32::MAX) + 1) << 3);

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("oversized protobuf field number should fail projection");

        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid projection reason, got {err:?}"
        );
    }

    #[test]
    fn projected_unknown_group_wire_type_matches_prost_conversion() {
        let mut payload = Vec::new();
        encode_start_group(&mut payload, 99);
        encode_varint_field(&mut payload, 99, 123);
        encode_end_group(&mut payload, 99);

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_nested_unknown_group_wire_type_matches_prost_conversion() {
        let mut payload = Vec::new();
        encode_start_group(&mut payload, 99);
        encode_start_group(&mut payload, 100);
        encode_varint_field(&mut payload, 101, 123);
        encode_end_group(&mut payload, 100);
        encode_end_group(&mut payload, 99);

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_repeated_empty_log_record_fields_match_prost_conversion() {
        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_SEVERITY_TEXT, b"INFO");
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_SEVERITY_TEXT, b"");
        encode_len_field(
            &mut log_record,
            otlp_field::LOG_RECORD_BODY,
            &any_string("first").encode_to_vec(),
        );
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, b"");
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_TRACE_ID, &[0xaa; 16]);
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_TRACE_ID, b"");
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_SPAN_ID, &[0xbb; 8]);
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_SPAN_ID, b"");

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_repeated_empty_key_value_value_matches_prost_conversion() {
        let mut attr = Vec::new();
        encode_len_field(&mut attr, otlp_field::KEY_VALUE_KEY, b"attr");
        encode_len_field(
            &mut attr,
            otlp_field::KEY_VALUE_VALUE,
            &any_string("kept").encode_to_vec(),
        );
        encode_len_field(&mut attr, otlp_field::KEY_VALUE_VALUE, b"");

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_ATTRIBUTES, &attr);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_anyvalue_unsupported_oneof_followed_by_primitive_matches_prost() {
        let mut any_value = Vec::new();
        encode_len_field(
            &mut any_value,
            otlp_field::ANY_VALUE_ARRAY_VALUE,
            &ArrayValue::default().encode_to_vec(),
        );
        encode_len_field(&mut any_value, otlp_field::ANY_VALUE_STRING_VALUE, b"kept");

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any_value);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_severity_number_uses_int32_truncation_like_prost() {
        let mut log_record = Vec::new();
        encode_varint_field(
            &mut log_record,
            otlp_field::LOG_RECORD_SEVERITY_NUMBER,
            u64::from(u32::MAX),
        );

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_field_zero_is_invalid() {
        let err = decode_projected_otlp_logs(&[0x00, 0x00], field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("protobuf field number zero should fail projection");

        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid projection reason, got {err:?}"
        );
    }

    #[test]
    fn invalid_utf8_string_field_is_rejected_by_projection() {
        let mut log_record = Vec::new();
        encode_len_field(
            &mut log_record,
            otlp_field::LOG_RECORD_SEVERITY_TEXT,
            &[0xff, 0xfe],
        );

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let projection_err =
            decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
                .expect_err("invalid UTF-8 should fail projection");
        assert!(
            matches!(projection_err, ProjectionError::Invalid(_)),
            "expected invalid projection reason, got {projection_err:?}"
        );
        assert!(
            crate::otlp_receiver::decode_protobuf_to_batch(&payload).is_err(),
            "production decoder should reject invalid UTF-8"
        );
    }

    #[test]
    fn projected_truncated_length_field_is_invalid() {
        let err =
            decode_projected_otlp_logs(&[0x0a, 0x05, 0x01], field_names::DEFAULT_RESOURCE_PREFIX)
                .expect_err("truncated top-level resource_logs field should fail projection");

        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid projection reason, got {err:?}"
        );
    }

    #[test]
    fn projected_overlong_ten_byte_varint_is_invalid() {
        let err = decode_projected_otlp_logs(
            &[
                0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
            ],
            field_names::DEFAULT_RESOURCE_PREFIX,
        )
        .expect_err("overlong varint should fail projection");

        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid projection reason, got {err:?}"
        );
    }

    #[test]
    fn projected_known_log_record_field_with_wrong_wire_type_is_invalid() {
        let mut log_record = Vec::new();
        encode_varint_field(&mut log_record, otlp_field::LOG_RECORD_SEVERITY_TEXT, 1);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let projection_err =
            decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
                .expect_err("wrong wire type should fail projection");
        assert!(
            matches!(projection_err, ProjectionError::Invalid(_)),
            "expected invalid projection reason, got {projection_err:?}"
        );
        assert!(
            crate::otlp_receiver::decode_protobuf_to_batch(&payload).is_err(),
            "production decoder should reject invalid known-field wire types"
        );
    }

    #[test]
    fn projected_top_level_known_field_with_wrong_wire_type_is_invalid() {
        let mut payload = Vec::new();
        encode_varint_field(&mut payload, otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS, 0);

        let projection_err =
            decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
                .expect_err("wrong top-level wire type should fail projection");
        assert!(
            matches!(projection_err, ProjectionError::Invalid(_)),
            "expected invalid projection reason, got {projection_err:?}"
        );
        assert!(
            crate::otlp_receiver::decode_protobuf_to_batch(&payload).is_err(),
            "production decoder should reject invalid top-level known-field wire types"
        );
    }

    #[test]
    fn projected_nested_kvlist_anyvalue_matches_prost_conversion() {
        let request = nested_kvlist_request();
        assert_projected_matches_prost(&request);
    }

    #[test]
    fn experimental_projection_fallback_matches_prost_for_unsupported_anyvalue() {
        let payload = nested_kvlist_request().encode_to_vec();
        let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost reference should decode unsupported projection fixture");
        let actual = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
            Bytes::from(payload),
        )
        .expect("experimental projection decoder should fall back to prost");

        assert_batches_match(&expected, &actual);
    }

    #[test]
    fn experimental_projection_fallback_matches_prost_for_array_anyvalue() {
        let payload = array_body_request().encode_to_vec();
        let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost reference should decode array AnyValue fixture");
        let actual = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
            Bytes::from(payload),
        )
        .expect("experimental projection decoder should fall back to prost");

        assert_batches_match(&expected, &actual);
    }

    fn array_body_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::ArrayValue(ArrayValue {
                                values: vec![any_string("nested")],
                            })),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn nested_kvlist_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        attributes: vec![KeyValue {
                            key: "complex".into(),
                            value: Some(AnyValue {
                                value: Some(Value::KvlistValue(KeyValueList {
                                    values: vec![kv_string("nested", "value")],
                                })),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn assert_projected_payload_matches_prost(payload: &[u8]) {
        let request = ExportLogsServiceRequest::decode(payload)
            .expect("prost reference should decode handcrafted payload");
        let expected =
            convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX).unwrap();
        let actual =
            decode_projected_otlp_logs(payload, field_names::DEFAULT_RESOURCE_PREFIX).unwrap();

        assert_batches_match(&expected, &actual);
    }

    fn assert_projected_matches_prost(request: &ExportLogsServiceRequest) {
        let payload = request.encode_to_vec();
        assert_projected_payload_matches_prost(&payload);
    }

    /// Compare prost (StreamingBuilder) and projected (ColumnarBatchBuilder) batches.
    ///
    /// ColumnarBatchBuilder produces `Utf8View` (including inside Struct
    /// conflict columns) while StreamingBuilder produces `Utf8`. This function
    /// normalizes Utf8View → Utf8 recursively, then compares by column name
    /// so column ordering differences don't cause false failures.
    fn assert_batches_match(expected: &RecordBatch, actual: &RecordBatch) {
        use arrow::array::StructArray;
        use arrow::compute::cast;
        assert_eq!(expected.num_rows(), actual.num_rows(), "row count mismatch");

        /// Recursively cast Utf8View → Utf8 inside an array (handles Struct children).
        fn normalize_utf8view(arr: &dyn arrow::array::Array) -> Arc<dyn arrow::array::Array> {
            use arrow::array::Array;
            use arrow::datatypes::DataType;
            match arr.data_type() {
                DataType::Utf8View => cast(arr, &DataType::Utf8).expect("cast Utf8View→Utf8"),
                DataType::Struct(fields) => {
                    let struct_arr = arr
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("struct downcast");
                    let new_fields: Vec<_> = fields
                        .iter()
                        .enumerate()
                        .map(|(i, f)| {
                            let child = normalize_utf8view(struct_arr.column(i).as_ref());
                            let new_field = if f.data_type() == &DataType::Utf8View {
                                Arc::new(arrow::datatypes::Field::new(
                                    f.name(),
                                    DataType::Utf8,
                                    f.is_nullable(),
                                ))
                            } else {
                                Arc::clone(f)
                            };
                            (new_field, child)
                        })
                        .collect();
                    let (new_field_refs, new_arrays): (Vec<_>, Vec<_>) =
                        new_fields.into_iter().unzip();
                    Arc::new(
                        StructArray::try_new(
                            new_field_refs.into(),
                            new_arrays,
                            struct_arr.nulls().cloned(),
                        )
                        .expect("struct rebuild"),
                    )
                }
                _ => arrow::array::make_array(arr.to_data()),
            }
        }

        // Match columns by name so ordering differences don't cause failures.
        let actual_schema = actual.schema();
        for (i, expected_field) in expected.schema().fields().iter().enumerate() {
            let name = expected_field.name();
            let actual_idx = actual_schema
                .index_of(name)
                .unwrap_or_else(|_| panic!("projected batch missing column '{name}'"));
            let expected_col = expected.column(i);
            let actual_col = actual.column(actual_idx);

            let actual_normalized = normalize_utf8view(actual_col.as_ref());

            assert_eq!(
                expected_col.to_data(),
                actual_normalized.to_data(),
                "column '{name}' data mismatch"
            );
        }

        // Both batches should now have the same column count since unwritten
        // planned fields are omitted (matching StreamingBuilder).
        assert_eq!(
            expected.num_columns(),
            actual.num_columns(),
            "column count mismatch: expected columns {:?}, actual columns {:?}",
            expected
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            actual
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
        );
    }

    fn encode_len_field(out: &mut Vec<u8>, field: u32, value: &[u8]) {
        encode_varint(out, u64::from(field << 3 | 2));
        encode_varint(out, value.len() as u64);
        out.extend_from_slice(value);
    }

    fn encode_varint_field(out: &mut Vec<u8>, field: u32, value: u64) {
        encode_varint(out, u64::from(field << 3));
        encode_varint(out, value);
    }

    fn encode_fixed64_field(out: &mut Vec<u8>, field: u32, value: u64) {
        encode_varint(out, u64::from(field << 3 | 1));
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn encode_fixed32_field(out: &mut Vec<u8>, field: u32, value: u32) {
        encode_varint(out, u64::from(field << 3 | 5));
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn encode_start_group(out: &mut Vec<u8>, field: u32) {
        encode_varint(out, u64::from(field << 3 | 3));
    }

    fn encode_end_group(out: &mut Vec<u8>, field: u32) {
        encode_varint(out, u64::from(field << 3 | 4));
    }

    fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    #[test]
    fn projected_complex_anyvalue_matches_prost_conversion() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::ArrayValue(ArrayValue {
                                values: vec![any_string("nested")],
                            })),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert_projected_matches_prost(&request);
    }

    // ── Multi-resource/scope container tests ──────────────────────────

    #[test]
    fn projected_multiple_resources_match_prost() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![kv_string("service.name", "frontend")],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "scope-fe".into(),
                            ..Default::default()
                        }),
                        log_records: vec![LogRecord {
                            body: Some(any_string("frontend-log")),
                            severity_text: "INFO".into(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![
                            kv_string("service.name", "backend"),
                            kv_i64("resource.pid", 42),
                        ],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "scope-be".into(),
                            version: "2.0.0".into(),
                            ..Default::default()
                        }),
                        log_records: vec![
                            LogRecord {
                                body: Some(any_string("backend-log-1")),
                                severity_text: "WARN".into(),
                                ..Default::default()
                            },
                            LogRecord {
                                body: Some(any_string("backend-log-2")),
                                attributes: vec![kv_bool("retried", true)],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        };
        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_multiple_scopes_per_resource_match_prost() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv_string("service.name", "multi-scope")],
                    ..Default::default()
                }),
                scope_logs: vec![
                    ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "http-handler".into(),
                            version: "1.0.0".into(),
                            ..Default::default()
                        }),
                        log_records: vec![LogRecord {
                            body: Some(any_string("request-received")),
                            severity_text: "INFO".into(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "db-client".into(),
                            version: "3.2.0".into(),
                            ..Default::default()
                        }),
                        log_records: vec![LogRecord {
                            body: Some(any_string("query-executed")),
                            severity_text: "DEBUG".into(),
                            attributes: vec![kv_string("db.system", "postgres")],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
        };
        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_resource_attrs_do_not_collide_with_log_attrs() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        kv_string("service.name", "collision-test"),
                        kv_string("host", "resource-host"),
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        attributes: vec![kv_string("host", "log-host")],
                        body: Some(any_string("collision")),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_empty_resource_and_scope_match_prost() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        body: Some(any_string("no-context")),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_empty_payload_matches_prost() {
        assert_projected_payload_matches_prost(&[]);
    }

    #[test]
    fn projected_ignored_metadata_fields_match_prost_conversion() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv_string("service.name", "metadata-test")],
                    dropped_attributes_count: 3,
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope-with-ignored-metadata".into(),
                        version: "2.0.0".into(),
                        attributes: vec![kv_string("scope.attr", "ignored")],
                        dropped_attributes_count: 5,
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_001,
                        observed_time_unix_nano: 1_700_000_000_000_000_002,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(any_string("metadata fields are ignored")),
                        attributes: vec![kv_string("kept", "log-attr")],
                        dropped_attributes_count: 7,
                        event_name: "ignored.event".into(),
                        ..Default::default()
                    }],
                    schema_url: "https://example.test/scope-schema/1.0.0".into(),
                    ..Default::default()
                }],
                schema_url: "https://example.test/resource-schema/1.0.0".into(),
                ..Default::default()
            }],
        };

        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_high_cardinality_dynamic_attrs_match_prost_conversion() {
        let attributes = (0..160)
            .map(|idx| {
                let key = format!("attr.high_cardinality.{idx}");
                match idx % 5 {
                    0 => kv_string(&key, &format!("value-{idx}")),
                    1 => kv_i64(&key, idx as i64),
                    2 => kv_f64(&key, idx as f64 + 0.25),
                    3 => kv_bool(&key, idx % 2 == 0),
                    _ => kv_bytes(&key, &[idx as u8, (idx >> 8) as u8]),
                }
            })
            .collect::<Vec<_>>();

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv_string("service.name", "high-cardinality")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(any_string("wide dynamic attrs")),
                        attributes,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_projected_matches_prost(&request);
    }

    // ── Malformed wire data tests ─────────────────────────────────────

    #[test]
    fn projected_truncated_varint_is_invalid() {
        // A varint that starts with a continuation bit but has no following byte.
        let payload = vec![0x80];
        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("truncated varint should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    #[test]
    fn projected_truncated_fixed64_is_invalid() {
        // A tag claiming fixed64 (wire type 1) followed by only 4 bytes instead of 8.
        let mut payload = Vec::new();
        // field=1, wire_type=1 (fixed64) => tag = (1 << 3) | 1 = 9
        // Wrap in a resource_logs container so field 1 is expected.
        let mut resource_logs_inner = Vec::new();
        encode_varint(&mut resource_logs_inner, (1_u64 << 3) | 1); // field 1, fixed64
        resource_logs_inner.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]); // only 4 bytes

        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs_inner,
        );

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("truncated fixed64 should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    #[test]
    fn projected_truncated_fixed32_is_invalid() {
        // A tag claiming fixed32 (wire type 5) followed by only 2 bytes instead of 4.
        let mut inner = Vec::new();
        encode_varint(&mut inner, (8_u64 << 3) | 5); // log_record field 8 (flags), fixed32
        inner.extend_from_slice(&[0x01, 0x02]); // only 2 bytes

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &inner);
        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("truncated fixed32 should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    #[test]
    fn projected_length_exceeding_buffer_is_invalid() {
        // A length-delimited field that claims more bytes than are available.
        let mut payload = Vec::new();
        encode_varint(&mut payload, (1_u64 << 3) | 2); // field 1, len
        encode_varint(&mut payload, 9999); // claims 9999 bytes
        payload.extend_from_slice(b"short"); // only 5 bytes

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("length exceeding buffer should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    #[test]
    fn projected_group_mismatch_is_invalid() {
        // Start group field 99, end group field 98 (mismatch).
        let mut payload = Vec::new();
        encode_start_group(&mut payload, 99);
        encode_end_group(&mut payload, 98);

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("mismatched group boundaries should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    #[test]
    fn projected_nested_group_mismatch_is_invalid() {
        let mut payload = Vec::new();
        encode_start_group(&mut payload, 99);
        encode_start_group(&mut payload, 100);
        encode_end_group(&mut payload, 99);

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("mismatched nested group boundaries should be invalid");
        assert!(
            matches!(
                err,
                ProjectionError::Invalid("mismatched protobuf end group")
            ),
            "expected nested group mismatch, got {err:?}"
        );
    }

    #[test]
    fn projected_group_depth_limit_is_invalid() {
        let mut payload = Vec::new();
        for _ in 0..=PROTOBUF_MAX_GROUP_DEPTH {
            encode_start_group(&mut payload, 99);
        }

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("overly deep protobuf groups should be invalid");
        assert!(
            matches!(
                err,
                ProjectionError::Invalid("protobuf group nesting too deep")
            ),
            "expected group depth failure, got {err:?}"
        );
    }

    #[test]
    fn projected_truncated_field_inside_group_is_invalid() {
        let mut payload = Vec::new();
        encode_start_group(&mut payload, 99);
        encode_fixed32_field(&mut payload, 100, 0x0102_0304);
        payload.truncate(payload.len() - 2);

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("truncated field inside group should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid("truncated fixed32 field")),
            "expected truncated field failure, got {err:?}"
        );
    }

    #[test]
    fn projected_nested_truncation_inside_log_record_is_invalid() {
        // Build a valid outer wrapper but truncate inside the log record body.
        let mut log_record = Vec::new();
        // severity_text field (3, len) with truncated length
        encode_varint(
            &mut log_record,
            (otlp_field::LOG_RECORD_SEVERITY_TEXT as u64) << 3 | 2,
        );
        encode_varint(&mut log_record, 100); // claims 100 bytes
        log_record.extend_from_slice(b"short"); // only 5 bytes

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);
        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("truncation inside log record should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    #[test]
    fn projected_invalid_utf8_in_attribute_key_is_invalid() {
        let mut kv_bytes = Vec::new();
        // key = field 1, len with invalid UTF-8
        encode_len_field(&mut kv_bytes, otlp_field::KEY_VALUE_KEY, &[0xff, 0xfe]);
        // value = field 2, len with a string AnyValue
        let mut any_val = Vec::new();
        encode_len_field(&mut any_val, otlp_field::ANY_VALUE_STRING_VALUE, b"valid");
        encode_len_field(&mut kv_bytes, otlp_field::KEY_VALUE_VALUE, &any_val);

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_ATTRIBUTES, &kv_bytes);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);
        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("invalid UTF-8 in attribute key should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    #[test]
    fn projected_invalid_utf8_in_attribute_string_value_is_invalid() {
        let mut any_val = Vec::new();
        encode_len_field(&mut any_val, otlp_field::ANY_VALUE_STRING_VALUE, &[0x80, 0x81]);
        let mut kv_bytes = Vec::new();
        encode_len_field(&mut kv_bytes, otlp_field::KEY_VALUE_KEY, b"bad-val");
        encode_len_field(&mut kv_bytes, otlp_field::KEY_VALUE_VALUE, &any_val);

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_ATTRIBUTES, &kv_bytes);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);
        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("invalid UTF-8 in attribute string value should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    #[test]
    fn projected_invalid_utf8_in_body_string_is_invalid() {
        let mut any_val = Vec::new();
        encode_len_field(&mut any_val, otlp_field::ANY_VALUE_STRING_VALUE, &[0xc0, 0xaf]);
        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any_val);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);
        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("invalid UTF-8 in body string should be invalid");
        assert!(
            matches!(err, ProjectionError::Invalid(_)),
            "expected invalid, got {err:?}"
        );
    }

    // ── Complex AnyValue projection tests ─────────────────────────────

    #[test]
    fn projected_kvlist_attribute_is_projected_as_json() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(any_string("normal body")),
                        attributes: vec![KeyValue {
                            key: "nested".into(),
                            value: Some(AnyValue {
                                value: Some(Value::KvlistValue(KeyValueList {
                                    values: vec![kv_string("inner", "value")],
                                })),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let payload = request.encode_to_vec();
        let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost should decode kvlist attribute fixture");
        let actual = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect("kvlist attribute should decode in projection");
        assert_batches_match(&expected, &actual);
    }

    #[test]
    fn projected_array_attribute_is_projected_as_json() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(any_string("normal body")),
                        attributes: vec![KeyValue {
                            key: "tags".into(),
                            value: Some(AnyValue {
                                value: Some(Value::ArrayValue(ArrayValue {
                                    values: vec![any_string("a"), any_string("b")],
                                })),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let payload = request.encode_to_vec();
        let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost should decode array attribute fixture");
        let actual = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect("array attribute should decode in projection");
        assert_batches_match(&expected, &actual);
    }

    #[test]
    fn projected_kvlist_body_is_projected_as_json() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![kv_string("key", "value")],
                            })),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let payload = request.encode_to_vec();
        let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost should decode kvlist body fixture");
        let actual = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect("kvlist body should decode in projection");
        assert_batches_match(&expected, &actual);
    }

    #[test]
    fn projected_body_anyvalue_shapes_match_prost_conversion() {
        let bodies = [
            any_string("body"),
            AnyValue {
                value: Some(Value::BoolValue(true)),
            },
            AnyValue {
                value: Some(Value::IntValue(-42)),
            },
            AnyValue {
                value: Some(Value::DoubleValue(0.0)),
            },
            AnyValue {
                value: Some(Value::DoubleValue(12.5)),
            },
            AnyValue {
                value: Some(Value::BytesValue(vec![0xde, 0xad, 0xbe, 0xef])),
            },
            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![any_string("a"), any_string("b")],
                })),
            },
            AnyValue {
                value: Some(Value::KvlistValue(KeyValueList {
                    values: vec![kv_string("k", "v")],
                })),
            },
        ];

        for body in bodies {
            let request = ExportLogsServiceRequest {
                resource_logs: vec![ResourceLogs {
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            body: Some(body),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            assert_projected_matches_prost(&request);
        }
    }

    #[test]
    fn projected_fallback_for_array_attr_matches_prost() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(any_string("fallback body")),
                        attributes: vec![KeyValue {
                            key: "tags".into(),
                            value: Some(AnyValue {
                                value: Some(Value::ArrayValue(ArrayValue {
                                    values: vec![any_string("x"), any_string("y")],
                                })),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let payload = request.encode_to_vec();
        let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost reference should decode array attribute fixture");
        let actual = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
            Bytes::from(payload),
        )
        .expect("projected fallback path should produce a batch for array attribute");
        assert_batches_match(&expected, &actual);
    }

    #[test]
    fn projected_mixed_primitive_and_complex_attrs_match_prost() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(any_string("mixed")),
                        attributes: vec![
                            kv_string("simple", "ok"),
                            kv_i64("count", 42),
                            KeyValue {
                                key: "nested_list".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::KvlistValue(KeyValueList {
                                        values: vec![kv_string("deep", "val")],
                                    })),
                                }),
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let payload = request.encode_to_vec();

        let direct = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect("mixed attrs with kvlist should decode in projection");

        let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost should decode mixed attrs");
        assert_batches_match(&expected, &direct);

        let actual = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
            Bytes::from(payload),
        )
        .expect("fallback should handle mixed attrs");
        assert_batches_match(&expected, &actual);
    }

    #[test]
    fn projected_malformed_wire_does_not_fall_back_as_valid_data() {
        // Malformed protobuf must return an error even in fallback mode,
        // because prost also rejects it.
        let mut payload = Vec::new();
        encode_varint(&mut payload, 0); // field number 0 — always invalid

        let err = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
            Bytes::from(payload),
        );
        assert!(
            err.is_err(),
            "malformed wire data must not silently produce a valid batch"
        );
    }

    #[test]
    fn projected_complex_anyvalue_plus_malformed_wire_remains_error() {
        // Complex AnyValue payloads are projected, but malformed trailing wire
        // must still fail instead of being treated as valid data.
        let mut any_val = Vec::new();
        encode_len_field(
            &mut any_val,
            otlp_field::ANY_VALUE_ARRAY_VALUE,
            &ArrayValue::default().encode_to_vec(),
        );

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any_val);
        encode_varint(&mut log_record, 0); // malformed field number zero

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);
        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let projection_err =
            decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
                .expect_err("projection should reject malformed trailing wire");
        assert!(matches!(projection_err, ProjectionError::Invalid(_)));

        let fallback_err =
            crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
                Bytes::from(payload),
            );
        assert!(
            fallback_err.is_err(),
            "unsupported projection fallback must preserve prost malformed-wire rejection"
        );
    }

    #[test]
    fn projected_complex_anyvalue_escapes_control_chars_quotes_and_backslashes_like_prost() {
        let weird = "quote=\" backslash=\\ newline=\n tab=\t nul=\u{0000}";
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::ArrayValue(ArrayValue {
                                values: vec![
                                    any_string(weird),
                                    AnyValue {
                                        value: Some(Value::KvlistValue(KeyValueList {
                                            values: vec![KeyValue {
                                                key: weird.into(),
                                                value: Some(any_string(weird)),
                                            }],
                                        })),
                                    },
                                ],
                            })),
                        }),
                        attributes: vec![KeyValue {
                            key: weird.into(),
                            value: Some(AnyValue {
                                value: Some(Value::KvlistValue(KeyValueList {
                                    values: vec![KeyValue {
                                        key: weird.into(),
                                        value: Some(any_string(weird)),
                                    }],
                                })),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_complex_anyvalue_non_finite_and_negative_zero_match_prost() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::ArrayValue(ArrayValue {
                                values: vec![
                                    AnyValue {
                                        value: Some(Value::DoubleValue(f64::NAN)),
                                    },
                                    AnyValue {
                                        value: Some(Value::DoubleValue(f64::INFINITY)),
                                    },
                                    AnyValue {
                                        value: Some(Value::DoubleValue(f64::NEG_INFINITY)),
                                    },
                                    AnyValue {
                                        value: Some(Value::DoubleValue(-0.0)),
                                    },
                                ],
                            })),
                        }),
                        attributes: vec![KeyValue {
                            key: "float.edge.cases".into(),
                            value: Some(AnyValue {
                                value: Some(Value::KvlistValue(KeyValueList {
                                    values: vec![KeyValue {
                                        key: "value".into(),
                                        value: Some(AnyValue {
                                            value: Some(Value::DoubleValue(-0.0)),
                                        }),
                                    }],
                                })),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_projected_matches_prost(&request);
    }

    #[test]
    fn projected_complex_anyvalue_duplicate_kv_fields_are_last_value_wins_like_prost() {
        let mut kv = Vec::new();
        encode_len_field(&mut kv, otlp_field::KEY_VALUE_KEY, b"first-key");
        encode_len_field(
            &mut kv,
            otlp_field::KEY_VALUE_VALUE,
            &any_string("first-value").encode_to_vec(),
        );
        encode_len_field(&mut kv, otlp_field::KEY_VALUE_KEY, b"last-key");
        encode_len_field(
            &mut kv,
            otlp_field::KEY_VALUE_VALUE,
            &any_string("last-value").encode_to_vec(),
        );

        let mut kvlist = Vec::new();
        encode_len_field(&mut kvlist, 1, &kv);

        let mut body = Vec::new();
        encode_len_field(&mut body, otlp_field::ANY_VALUE_KVLIST_VALUE, &kvlist);

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &body);
        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);
        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_complex_anyvalue_malformed_nested_length_is_invalid() {
        // AnyValue.array_value containing ArrayValue.values with truncated length.
        // array bytes: field=1(len), length=2, payload has only one byte.
        let malformed_array = [0x0a, 0x02, 0x01];
        let mut any = Vec::new();
        encode_len_field(&mut any, otlp_field::ANY_VALUE_ARRAY_VALUE, &malformed_array);

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any);
        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &log_record);
        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let projection_err =
            decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
                .expect_err("truncated nested complex AnyValue should fail projection");
        assert!(matches!(projection_err, ProjectionError::Invalid(_)));
        assert!(
            crate::otlp_receiver::decode_protobuf_to_batch(&payload).is_err(),
            "production decoder should reject malformed nested complex AnyValue wire"
        );
    }

    #[test]
    fn projected_deeply_nested_complex_anyvalue_matches_prost() {
        let mut nested = any_string("leaf");
        for _ in 0..48 {
            nested = AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![nested],
                })),
            };
        }

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(nested),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_projected_matches_prost(&request);
    }

    // ── Expanded proptest ─────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn projected_fallback_arbitrary_bytes_matches_prost_classification(
            data in proptest::collection::vec(any::<u8>(), 0..512),
        ) {
            let prost = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&data);
            let fallback = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
                Bytes::from(data.clone()),
            );

            match (prost, fallback) {
                (Ok(expected), Ok(actual)) => assert_batches_match(&expected, &actual),
                (Err(_), Err(_)) => {}
                (Ok(_), Err(err)) => {
                    prop_assert!(
                        false,
                        "projected fallback rejected protobuf bytes accepted by prost: {err}"
                    );
                }
                (Err(err), Ok(actual)) => {
                    prop_assert!(
                        false,
                        "projected fallback accepted malformed bytes rejected by prost ({err}); \
                         actual columns={:?}, rows={}",
                        actual.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                        actual.num_rows(),
                    );
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn projected_randomized_primitive_payload_matches_prost(
            rows in 1usize..24,
            attrs_per_row in 0usize..8,
            resource_attrs in 0usize..6,
            severity in proptest::option::of("[A-Z]{0,12}"),
            body in proptest::option::of("[ -~]{0,80}"),
            scope_name in proptest::option::of("[a-zA-Z0-9_.-]{0,24}"),
            scope_version in proptest::option::of("[0-9.]{0,12}"),
        ) {
            let request = randomized_primitive_request(
                rows,
                attrs_per_row,
                resource_attrs,
                severity,
                body,
                scope_name,
                scope_version,
            );
            let payload = request.encode_to_vec();
            let projected = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
                .expect("primitive randomized payload should decode in projection");
            let prost = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
                .expect("prost reference should decode randomized primitive payload");

            assert_batches_match(&prost, &projected);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn projected_multi_container_payload_matches_prost(
            num_resources in 1usize..5,
            scopes_per_resource in 1usize..4,
            rows_per_scope in 1usize..30,
            attrs_per_row in 0usize..12,
            resource_attrs in 0usize..8,
        ) {
            let request = randomized_multi_container_request(
                num_resources,
                scopes_per_resource,
                rows_per_scope,
                attrs_per_row,
                resource_attrs,
            );
            let payload = request.encode_to_vec();
            let projected = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
                .expect("multi-container randomized payload should decode in projection");
            let prost = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
                .expect("prost reference should decode multi-container payload");

            assert_batches_match(&prost, &projected);
        }
    }

    fn randomized_multi_container_request(
        num_resources: usize,
        scopes_per_resource: usize,
        rows_per_scope: usize,
        attrs_per_row: usize,
        resource_attrs: usize,
    ) -> ExportLogsServiceRequest {
        let mut global_row = 0usize;
        let resource_logs = (0..num_resources)
            .map(|res_idx| {
                let resource_attributes = (0..resource_attrs)
                    .map(|idx| {
                        let key = format!("resource.{res_idx}.attr.{idx}");
                        match idx % 4 {
                            0 => kv_string(&key, &format!("rv-{res_idx}-{idx}")),
                            1 => kv_i64(&key, (res_idx * 100 + idx) as i64),
                            2 => kv_f64(&key, res_idx as f64 + idx as f64 * 0.1),
                            _ => kv_bool(&key, idx % 2 == 0),
                        }
                    })
                    .collect::<Vec<_>>();

                let scope_logs = (0..scopes_per_resource)
                    .map(|scope_idx| {
                        let log_records = (0..rows_per_scope)
                            .map(|_| {
                                let row = global_row;
                                global_row += 1;
                                let attributes = (0..attrs_per_row)
                                    .map(|attr_idx| {
                                        let key = format!("attr.{attr_idx}");
                                        match (row + attr_idx) % 5 {
                                            0 => kv_string(&key, &format!("v-{row}-{attr_idx}")),
                                            1 => kv_i64(&key, row as i64 + attr_idx as i64),
                                            2 => kv_f64(&key, row as f64 + attr_idx as f64 / 10.0),
                                            3 => kv_bool(&key, (row + attr_idx) % 2 == 0),
                                            _ => kv_bytes(&key, &[row as u8, attr_idx as u8]),
                                        }
                                    })
                                    .collect::<Vec<_>>();

                                LogRecord {
                                    time_unix_nano: 1_700_000_000_000_000_000u64
                                        .saturating_add(row as u64),
                                    observed_time_unix_nano: 1_700_000_000_000_001_000u64
                                        .saturating_add(row as u64),
                                    severity_number: (row % 24) as i32,
                                    severity_text: ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"]
                                        [row % 5]
                                        .to_string(),
                                    body: Some(any_string(&format!("row-{row}"))),
                                    attributes,
                                    trace_id: vec![row as u8; 16],
                                    span_id: vec![row as u8; 8],
                                    flags: row as u32,
                                    ..Default::default()
                                }
                            })
                            .collect::<Vec<_>>();

                        ScopeLogs {
                            scope: Some(InstrumentationScope {
                                name: format!("scope-{res_idx}-{scope_idx}"),
                                version: format!("{res_idx}.{scope_idx}.0"),
                                ..Default::default()
                            }),
                            log_records,
                            ..Default::default()
                        }
                    })
                    .collect::<Vec<_>>();

                ResourceLogs {
                    resource: Some(Resource {
                        attributes: resource_attributes,
                        ..Default::default()
                    }),
                    scope_logs,
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();

        ExportLogsServiceRequest { resource_logs }
    }

    fn randomized_primitive_request(
        rows: usize,
        attrs_per_row: usize,
        resource_attrs: usize,
        severity: Option<String>,
        body: Option<String>,
        scope_name: Option<String>,
        scope_version: Option<String>,
    ) -> ExportLogsServiceRequest {
        let resource_attributes = (0..resource_attrs)
            .map(|index| {
                let key = if index % 2 == 0 {
                    format!("resource.key.{index}")
                } else {
                    "resource.key.dup".to_string()
                };
                match index % 4 {
                    0 => kv_string(&key, &format!("resource-value-{index}")),
                    1 => kv_i64(&key, -(index as i64) - 1),
                    2 => kv_f64(&key, index as f64 + 0.5),
                    _ => kv_bool(&key, index % 3 == 0),
                }
            })
            .collect::<Vec<_>>();

        let log_records = (0..rows)
            .map(|row_index| {
                let attributes = (0..attrs_per_row)
                    .map(|attr_index| {
                        let key = if attr_index % 3 == 0 {
                            format!("attr.{row_index}.{}", attr_index % 2)
                        } else {
                            format!("attr.{attr_index}")
                        };
                        match (row_index + attr_index) % 5 {
                            0 => kv_string(&key, &format!("value-{row_index}-{attr_index}")),
                            1 => {
                                let magnitude = (row_index * 10 + attr_index) as i64;
                                let value = if (row_index + attr_index) % 2 == 0 {
                                    magnitude
                                } else {
                                    -magnitude
                                };
                                kv_i64(&key, value)
                            }
                            2 => kv_f64(&key, row_index as f64 + attr_index as f64 / 10.0),
                            3 => kv_bool(&key, (row_index + attr_index) % 2 == 0),
                            _ => kv_bytes(&key, &[row_index as u8, attr_index as u8]),
                        }
                    })
                    .collect::<Vec<_>>();

                LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000u64.saturating_add(row_index as u64),
                    observed_time_unix_nano: 1_700_000_000_000_001_000u64
                        .saturating_add(row_index as u64),
                    severity_number: (row_index % 24) as i32,
                    severity_text: severity.clone().unwrap_or_else(|| "INFO".to_string()),
                    body: body.clone().as_deref().map(any_string),
                    attributes,
                    trace_id: vec![row_index as u8; 16],
                    span_id: vec![row_index as u8; 8],
                    flags: row_index as u32,
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();

        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: scope_name.unwrap_or_else(|| "scope-a".to_string()),
                        version: scope_version.unwrap_or_else(|| "1.0.0".to_string()),
                        ..Default::default()
                    }),
                    log_records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn any_string(value: &str) -> AnyValue {
        AnyValue {
            value: Some(Value::StringValue(value.into())),
        }
    }

    fn kv_string(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(any_string(value)),
        }
    }

    fn kv_bool(key: &str, value: bool) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::BoolValue(value)),
            }),
        }
    }

    fn kv_i64(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(value)),
            }),
        }
    }

    fn kv_f64(key: &str, value: f64) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::DoubleValue(value)),
            }),
        }
    }

    fn kv_bytes(key: &str, value: &[u8]) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::BytesValue(value.to_vec())),
            }),
        }
    }
}
