//! Experimental OTLP logs wire projection into Arrow.
//!
//! This module decodes the common OTLP logs protobuf shape directly into
//! `StreamingBuilder`, bypassing allocation of the full prost object graph.
//! It intentionally implements a projection, not a complete protobuf object
//! model: unsupported semantic cases return [`ProjectionError::Unsupported`]
//! so the receiver can fall back to the prost decoder.

use std::fmt;
use std::io::Write as _;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use logfwd_arrow::StreamingBuilder;
use logfwd_types::field_names;

#[derive(Debug)]
pub(super) enum ProjectionError {
    Invalid(&'static str),
    Unsupported(&'static str),
    Batch(String),
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
}

#[derive(Clone, Copy)]
enum StringStorage {
    Decoded,
    #[cfg(any(feature = "otlp-research", test))]
    InputView,
}

#[derive(Clone, Copy)]
enum BatchFinish {
    Detached,
    #[cfg(any(feature = "otlp-research", test))]
    View,
}

struct OtlpFieldIdx {
    timestamp: usize,
    observed_timestamp: usize,
    severity: usize,
    severity_number: usize,
    body: usize,
    trace_id: usize,
    span_id: usize,
    flags: usize,
    scope_name: usize,
    scope_version: usize,
}

#[derive(Clone, Copy, Default)]
struct ScopeFields<'a> {
    name: Option<&'a [u8]>,
    version: Option<&'a [u8]>,
}

mod spec {
    pub(super) mod export_logs_service_request {
        pub(crate) const RESOURCE_LOGS: u32 = 1;
    }

    pub(super) mod resource_logs {
        pub(crate) const RESOURCE: u32 = 1;
        pub(crate) const SCOPE_LOGS: u32 = 2;
    }

    pub(super) mod resource {
        pub(crate) const ATTRIBUTES: u32 = 1;
    }

    pub(super) mod scope_logs {
        pub(crate) const SCOPE: u32 = 1;
        pub(crate) const LOG_RECORDS: u32 = 2;
    }

    pub(super) mod instrumentation_scope {
        pub(crate) const NAME: u32 = 1;
        pub(crate) const VERSION: u32 = 2;
    }

    pub(super) mod log_record {
        pub(crate) const TIME_UNIX_NANO: u32 = 1;
        pub(crate) const SEVERITY_NUMBER: u32 = 2;
        pub(crate) const SEVERITY_TEXT: u32 = 3;
        pub(crate) const BODY: u32 = 5;
        pub(crate) const ATTRIBUTES: u32 = 6;
        pub(crate) const FLAGS: u32 = 8;
        pub(crate) const TRACE_ID: u32 = 9;
        pub(crate) const SPAN_ID: u32 = 10;
        pub(crate) const OBSERVED_TIME_UNIX_NANO: u32 = 11;
    }

    pub(super) mod key_value {
        pub(crate) const KEY: u32 = 1;
        pub(crate) const VALUE: u32 = 2;
    }

    pub(super) mod any_value {
        pub(crate) const STRING: u32 = 1;
        pub(crate) const BOOL: u32 = 2;
        pub(crate) const INT: u32 = 3;
        pub(crate) const DOUBLE: u32 = 4;
        pub(crate) const ARRAY: u32 = 5;
        pub(crate) const KVLIST: u32 = 6;
        pub(crate) const BYTES: u32 = 7;
    }
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
    decode_projected_otlp_logs_inner(
        body,
        Bytes::new(),
        resource_prefix,
        StringStorage::Decoded,
        BatchFinish::Detached,
    )
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
        BatchFinish::View,
    )
}

fn decode_projected_otlp_logs_inner(
    body: &[u8],
    backing: Bytes,
    resource_prefix: &str,
    string_storage: StringStorage,
    finish: BatchFinish,
) -> Result<RecordBatch, ProjectionError> {
    if body.is_empty() {
        return Ok(RecordBatch::new_empty(
            arrow::datatypes::Schema::empty().into(),
        ));
    }

    let mut builder = StreamingBuilder::new(None);
    builder.begin_batch(backing);
    let fields = OtlpFieldIdx {
        timestamp: builder.resolve_field(field_names::TIMESTAMP.as_bytes()),
        observed_timestamp: builder.resolve_field(field_names::OBSERVED_TIMESTAMP.as_bytes()),
        severity: builder.resolve_field(field_names::SEVERITY.as_bytes()),
        severity_number: builder.resolve_field(field_names::SEVERITY_NUMBER.as_bytes()),
        body: builder.resolve_field(field_names::BODY.as_bytes()),
        trace_id: builder.resolve_field(field_names::TRACE_ID.as_bytes()),
        span_id: builder.resolve_field(field_names::SPAN_ID.as_bytes()),
        flags: builder.resolve_field(field_names::FLAGS.as_bytes()),
        scope_name: builder.resolve_field(field_names::SCOPE_NAME.as_bytes()),
        scope_version: builder.resolve_field(field_names::SCOPE_VERSION.as_bytes()),
    };
    let mut scratch = WireScratch::default();

    for_each_field(body, |field, value| {
        match (field, value) {
            (spec::export_logs_service_request::RESOURCE_LOGS, WireField::Len(resource_logs)) => {
                decode_resource_logs_wire(
                    &mut builder,
                    &fields,
                    &mut scratch,
                    resource_prefix,
                    resource_logs,
                    string_storage,
                )?;
            }
            (spec::export_logs_service_request::RESOURCE_LOGS, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ExportLogsServiceRequest.resource_logs",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;

    let batch = match finish {
        BatchFinish::Detached => builder.finish_batch_detached(),
        #[cfg(any(feature = "otlp-research", test))]
        BatchFinish::View => builder.finish_batch(),
    };
    batch.map_err(|e| {
        ProjectionError::Batch(format!("structured OTLP projection batch build error: {e}"))
    })
}

#[derive(Default)]
struct WireScratch {
    hex: Vec<u8>,
    decimal: Vec<u8>,
    resource_key: Vec<u8>,
    attr_ranges: Vec<(usize, usize)>,
    attr_field_cache: Vec<AttrFieldCache>,
}

#[derive(Default)]
struct AttrFieldCache {
    key: Vec<u8>,
    idx: usize,
}

fn decode_resource_logs_wire(
    builder: &mut StreamingBuilder,
    fields: &OtlpFieldIdx,
    scratch: &mut WireScratch,
    resource_prefix: &str,
    resource_logs: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    let mut resource_attrs = Vec::new();

    for_each_field(resource_logs, |field, value| {
        match (field, value) {
            (spec::resource_logs::RESOURCE, WireField::Len(resource)) => {
                collect_resource_attrs(
                    builder,
                    scratch,
                    &mut resource_attrs,
                    resource_prefix,
                    resource,
                )?;
            }
            (spec::resource_logs::RESOURCE, _) => {
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
            (spec::resource_logs::SCOPE_LOGS, WireField::Len(scope_logs)) => {
                decode_scope_logs_wire(
                    builder,
                    fields,
                    scratch,
                    &resource_attrs,
                    scope_logs,
                    string_storage,
                )?;
            }
            (spec::resource_logs::SCOPE_LOGS, _) => {
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
    builder: &mut StreamingBuilder,
    scratch: &mut WireScratch,
    resource_attrs: &mut Vec<(usize, WireAny<'a>)>,
    resource_prefix: &str,
    resource: &'a [u8],
) -> Result<(), ProjectionError> {
    for_each_field(resource, |field, value| {
        match (field, value) {
            (spec::resource::ATTRIBUTES, WireField::Len(attr)) => {
                if let Some((key, value)) = decode_key_value_wire(attr)? {
                    scratch.resource_key.clear();
                    scratch
                        .resource_key
                        .reserve(resource_prefix.len() + key.len());
                    scratch
                        .resource_key
                        .extend_from_slice(resource_prefix.as_bytes());
                    scratch.resource_key.extend_from_slice(key);
                    let idx = builder.resolve_field(&scratch.resource_key);
                    resource_attrs.push((idx, value));
                }
            }
            (spec::resource::ATTRIBUTES, _) => {
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
    builder: &mut StreamingBuilder,
    fields: &OtlpFieldIdx,
    scratch: &mut WireScratch,
    resource_attrs: &[(usize, WireAny<'_>)],
    scope_logs: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    let mut scope_fields = ScopeFields::default();

    for_each_field(scope_logs, |field, value| {
        match (field, value) {
            (spec::scope_logs::SCOPE, WireField::Len(scope)) => {
                merge_scope_wire(scope, &mut scope_fields)?;
            }
            (spec::scope_logs::SCOPE, _) => {
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
            (spec::scope_logs::LOG_RECORDS, WireField::Len(log_record)) => {
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
            (spec::scope_logs::LOG_RECORDS, _) => {
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
            (spec::instrumentation_scope::NAME, WireField::Len(value)) => {
                scope_fields.name = Some(require_utf8(value, "invalid UTF-8 scope name")?);
            }
            (spec::instrumentation_scope::NAME, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for InstrumentationScope.name",
                ));
            }
            (spec::instrumentation_scope::VERSION, WireField::Len(value)) => {
                scope_fields.version = Some(require_utf8(value, "invalid UTF-8 scope version")?);
            }
            (spec::instrumentation_scope::VERSION, _) => {
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
    builder: &mut StreamingBuilder,
    fields: &OtlpFieldIdx,
    scratch: &mut WireScratch,
    resource_attrs: &[(usize, WireAny<'_>)],
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
            (spec::log_record::TIME_UNIX_NANO, WireField::Fixed64(value)) => {
                time_unix_nano = value;
            }
            (spec::log_record::TIME_UNIX_NANO, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.time_unix_nano",
                ));
            }
            (spec::log_record::OBSERVED_TIME_UNIX_NANO, WireField::Fixed64(value)) => {
                observed_time_unix_nano = value;
            }
            (spec::log_record::OBSERVED_TIME_UNIX_NANO, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.observed_time_unix_nano",
                ));
            }
            (spec::log_record::SEVERITY_NUMBER, WireField::Varint(value)) => {
                severity_number = i64::from(value as i32);
            }
            (spec::log_record::SEVERITY_NUMBER, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.severity_number",
                ));
            }
            (spec::log_record::SEVERITY_TEXT, WireField::Len(value)) => {
                severity_text = if value.is_empty() {
                    None
                } else {
                    Some(require_utf8(value, "invalid UTF-8 severity text")?)
                };
            }
            (spec::log_record::SEVERITY_TEXT, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.severity_text",
                ));
            }
            (spec::log_record::BODY, WireField::Len(value)) => {
                if let Some(value) = decode_any_value_wire(value)? {
                    body = Some(value);
                }
            }
            (spec::log_record::BODY, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.body",
                ));
            }
            (spec::log_record::TRACE_ID, WireField::Len(value)) => {
                trace_id = (!value.is_empty()).then_some(value);
            }
            (spec::log_record::TRACE_ID, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.trace_id",
                ));
            }
            (spec::log_record::SPAN_ID, WireField::Len(value)) => {
                span_id = (!value.is_empty()).then_some(value);
            }
            (spec::log_record::SPAN_ID, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.span_id",
                ));
            }
            (spec::log_record::FLAGS, WireField::Fixed32(value)) => flags = i64::from(value),
            (spec::log_record::FLAGS, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.flags",
                ));
            }
            (spec::log_record::ATTRIBUTES, WireField::Len(value)) => {
                scratch.attr_ranges.push(subslice_range(log_record, value)?);
            }
            (spec::log_record::ATTRIBUTES, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.attributes",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;

    let mut decoded_attrs = Vec::with_capacity(scratch.attr_ranges.len());
    for attr_idx in 0..scratch.attr_ranges.len() {
        let (start, len) = scratch.attr_ranges[attr_idx];
        let attr = &log_record[start..start + len];
        if let Some((key, value)) = decode_key_value_wire(attr)? {
            decoded_attrs.push((attr_idx, key, value));
        }
    }

    builder.begin_row();

    let timestamp = if time_unix_nano > 0 {
        time_unix_nano
    } else {
        observed_time_unix_nano
    };
    if let Ok(value) = i64::try_from(timestamp)
        && value > 0
    {
        builder.append_i64_value_by_idx(fields.timestamp, value);
    }
    if let Ok(value) = i64::try_from(observed_time_unix_nano)
        && value > 0
    {
        builder.append_i64_value_by_idx(fields.observed_timestamp, value);
    }
    if let Some(value) = severity_text {
        append_validated_wire_str(builder, fields.severity, value, string_storage);
    }
    if severity_number > 0 {
        builder.append_i64_value_by_idx(fields.severity_number, severity_number);
    }
    if let Some(value) = body {
        append_wire_any_as_string(builder, fields.body, value, scratch, string_storage);
    }
    if let Some(value) = trace_id {
        append_hex_field(builder, fields.trace_id, value, &mut scratch.hex);
    }
    if let Some(value) = span_id {
        append_hex_field(builder, fields.span_id, value, &mut scratch.hex);
    }
    if flags > 0 {
        builder.append_i64_value_by_idx(fields.flags, flags);
    }
    if let Some(value) = scope_fields.name
        && !value.is_empty()
    {
        append_validated_wire_str(builder, fields.scope_name, value, string_storage);
    }
    if let Some(value) = scope_fields.version
        && !value.is_empty()
    {
        append_validated_wire_str(builder, fields.scope_version, value, string_storage);
    }

    for (attr_idx, key, value) in decoded_attrs {
        let idx = resolve_record_attr_field(builder, &mut scratch.attr_field_cache, attr_idx, key);
        append_wire_any(builder, idx, value, scratch, string_storage);
    }

    for &(idx, value) in resource_attrs {
        append_wire_any(builder, idx, value, scratch, string_storage);
    }

    builder.end_row();
    Ok(())
}

fn decode_key_value_wire(kv: &[u8]) -> Result<Option<(&[u8], WireAny<'_>)>, ProjectionError> {
    let mut key = &[][..];
    let mut value = None;
    for_each_field(kv, |field, field_value| {
        match (field, field_value) {
            (spec::key_value::KEY, WireField::Len(bytes)) => {
                key = require_utf8(bytes, "invalid UTF-8 attribute key")?;
            }
            (spec::key_value::KEY, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for KeyValue.key",
                ));
            }
            (spec::key_value::VALUE, WireField::Len(bytes)) => {
                if let Some(decoded) = decode_any_value_wire(bytes)? {
                    value = Some(decoded);
                }
            }
            (spec::key_value::VALUE, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for KeyValue.value",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;
    Ok(value.map(|value| (key, value)))
}

fn resolve_record_attr_field(
    builder: &mut StreamingBuilder,
    cache: &mut Vec<AttrFieldCache>,
    position: usize,
    key: &[u8],
) -> usize {
    if let Some(cached) = cache.get(position)
        && cached.key.as_slice() == key
    {
        return cached.idx;
    }

    let idx = builder.resolve_field(key);
    if let Some(cached) = cache.get_mut(position) {
        cached.key.clear();
        cached.key.extend_from_slice(key);
        cached.idx = idx;
    } else {
        cache.push(AttrFieldCache {
            key: key.to_vec(),
            idx,
        });
    }
    idx
}

fn decode_any_value_wire(value: &[u8]) -> Result<Option<WireAny<'_>>, ProjectionError> {
    let mut out = None;
    let mut unsupported = None;
    for_each_field(value, |field, field_value| {
        match (field, field_value) {
            (spec::any_value::STRING, WireField::Len(bytes)) => {
                out = Some(WireAny::String(require_utf8(
                    bytes,
                    "invalid UTF-8 AnyValue string",
                )?));
            }
            (spec::any_value::STRING, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.string_value",
                ));
            }
            (spec::any_value::BOOL, WireField::Varint(value)) => {
                out = Some(WireAny::Bool(value != 0));
            }
            (spec::any_value::BOOL, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.bool_value",
                ));
            }
            (spec::any_value::INT, WireField::Varint(value)) => {
                out = Some(WireAny::Int(value as i64));
            }
            (spec::any_value::INT, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.int_value",
                ));
            }
            (spec::any_value::DOUBLE, WireField::Fixed64(value)) => {
                out = Some(WireAny::Double(f64::from_bits(value)));
            }
            (spec::any_value::DOUBLE, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.double_value",
                ));
            }
            (spec::any_value::BYTES, WireField::Len(bytes)) => out = Some(WireAny::Bytes(bytes)),
            (spec::any_value::BYTES, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.bytes_value",
                ));
            }
            (spec::any_value::ARRAY, WireField::Len(_)) => {
                out = None;
                unsupported = Some("AnyValue::ArrayValue");
            }
            (spec::any_value::ARRAY, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.array_value",
                ));
            }
            (spec::any_value::KVLIST, WireField::Len(_)) => {
                out = None;
                unsupported = Some("AnyValue::KvListValue");
            }
            (spec::any_value::KVLIST, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.kvlist_value",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;
    if let Some(reason) = unsupported
        && out.is_none()
    {
        return Err(ProjectionError::Unsupported(reason));
    }
    Ok(out)
}

fn require_utf8<'a>(bytes: &'a [u8], context: &'static str) -> Result<&'a [u8], ProjectionError> {
    if std::str::from_utf8(bytes).is_err() {
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

fn append_wire_any(
    builder: &mut StreamingBuilder,
    idx: usize,
    value: WireAny<'_>,
    scratch: &mut WireScratch,
    string_storage: StringStorage,
) {
    match value {
        WireAny::String(value) => append_validated_wire_str(builder, idx, value, string_storage),
        WireAny::Bool(value) => builder.append_bool_by_idx(idx, value),
        WireAny::Int(value) => builder.append_i64_value_by_idx(idx, value),
        WireAny::Double(value) => builder.append_f64_value_by_idx(idx, value),
        WireAny::Bytes(value) => append_hex_field(builder, idx, value, &mut scratch.hex),
    }
}

fn append_wire_any_as_string(
    builder: &mut StreamingBuilder,
    idx: usize,
    value: WireAny<'_>,
    scratch: &mut WireScratch,
    string_storage: StringStorage,
) {
    match value {
        WireAny::String(value) => append_validated_wire_str(builder, idx, value, string_storage),
        WireAny::Bool(true) => builder.append_validated_decoded_str_by_idx(idx, b"true"),
        WireAny::Bool(false) => builder.append_validated_decoded_str_by_idx(idx, b"false"),
        WireAny::Int(value) => {
            scratch.decimal.clear();
            write!(scratch.decimal, "{value}").expect("writing to Vec cannot fail");
            builder.append_validated_decoded_str_by_idx(idx, &scratch.decimal);
        }
        WireAny::Double(value) => {
            scratch.decimal.clear();
            write!(scratch.decimal, "{value}").expect("writing to Vec cannot fail");
            builder.append_validated_decoded_str_by_idx(idx, &scratch.decimal);
        }
        WireAny::Bytes(value) => append_hex_field(builder, idx, value, &mut scratch.hex),
    }
}

fn append_validated_wire_str(
    builder: &mut StreamingBuilder,
    idx: usize,
    value: &[u8],
    string_storage: StringStorage,
) {
    match string_storage {
        StringStorage::Decoded => builder.append_validated_decoded_str_by_idx(idx, value),
        #[cfg(any(feature = "otlp-research", test))]
        StringStorage::InputView => builder.append_validated_str_by_idx(idx, value),
    }
}

fn append_hex_field(
    builder: &mut StreamingBuilder,
    idx: usize,
    bytes: &[u8],
    hex_buf: &mut Vec<u8>,
) {
    hex_buf.clear();
    write_hex_to_buf(hex_buf, bytes);
    builder.append_validated_decoded_str_by_idx(idx, hex_buf);
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

fn skip_group(input: &mut &[u8], start_field: u32) -> Result<(), ProjectionError> {
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
            3 => skip_group(input, field)?,
            4 if field == start_field => return Ok(()),
            4 => return Err(ProjectionError::Invalid("mismatched protobuf end group")),
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
                            kv_string("_resource_service.name", "attr-resource-shadow"),
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
        encode_len_field(&mut scope_logs, spec::scope_logs::SCOPE, &first_scope);
        encode_len_field(&mut scope_logs, spec::scope_logs::SCOPE, &second_scope);
        encode_len_field(&mut scope_logs, spec::scope_logs::LOG_RECORDS, &record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            spec::resource_logs::SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
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
        encode_len_field(&mut second_scope, spec::instrumentation_scope::NAME, b"");
        encode_len_field(&mut second_scope, spec::instrumentation_scope::VERSION, b"");
        let record = LogRecord {
            body: Some(any_string("hello")),
            ..Default::default()
        }
        .encode_to_vec();

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, spec::scope_logs::SCOPE, &first_scope);
        encode_len_field(&mut scope_logs, spec::scope_logs::SCOPE, &second_scope);
        encode_len_field(&mut scope_logs, spec::scope_logs::LOG_RECORDS, &record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            spec::resource_logs::SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
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
        encode_varint(&mut payload, (99_u64 << 3) | 3);
        encode_varint_field(&mut payload, 99, 123);
        encode_varint(&mut payload, (99_u64 << 3) | 4);

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_repeated_empty_log_record_fields_match_prost_conversion() {
        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, spec::log_record::SEVERITY_TEXT, b"INFO");
        encode_len_field(&mut log_record, spec::log_record::SEVERITY_TEXT, b"");
        encode_len_field(
            &mut log_record,
            spec::log_record::BODY,
            &any_string("first").encode_to_vec(),
        );
        encode_len_field(&mut log_record, spec::log_record::BODY, b"");
        encode_len_field(&mut log_record, spec::log_record::TRACE_ID, &[0xaa; 16]);
        encode_len_field(&mut log_record, spec::log_record::TRACE_ID, b"");
        encode_len_field(&mut log_record, spec::log_record::SPAN_ID, &[0xbb; 8]);
        encode_len_field(&mut log_record, spec::log_record::SPAN_ID, b"");

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, spec::scope_logs::LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            spec::resource_logs::SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_repeated_empty_key_value_value_matches_prost_conversion() {
        let mut attr = Vec::new();
        encode_len_field(&mut attr, spec::key_value::KEY, b"attr");
        encode_len_field(
            &mut attr,
            spec::key_value::VALUE,
            &any_string("kept").encode_to_vec(),
        );
        encode_len_field(&mut attr, spec::key_value::VALUE, b"");

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, spec::log_record::ATTRIBUTES, &attr);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, spec::scope_logs::LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            spec::resource_logs::SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_anyvalue_unsupported_oneof_followed_by_primitive_matches_prost() {
        let mut any_value = Vec::new();
        encode_len_field(
            &mut any_value,
            spec::any_value::ARRAY,
            &ArrayValue::default().encode_to_vec(),
        );
        encode_len_field(&mut any_value, spec::any_value::STRING, b"kept");

        let mut log_record = Vec::new();
        encode_len_field(&mut log_record, spec::log_record::BODY, &any_value);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, spec::scope_logs::LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            spec::resource_logs::SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
            &resource_logs,
        );

        assert_projected_payload_matches_prost(&payload);
    }

    #[test]
    fn projected_severity_number_uses_int32_truncation_like_prost() {
        let mut log_record = Vec::new();
        encode_varint_field(
            &mut log_record,
            spec::log_record::SEVERITY_NUMBER,
            u64::from(u32::MAX),
        );

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, spec::scope_logs::LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            spec::resource_logs::SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
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
            spec::log_record::SEVERITY_TEXT,
            &[0xff, 0xfe],
        );

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, spec::scope_logs::LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            spec::resource_logs::SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
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
        encode_varint_field(&mut log_record, spec::log_record::SEVERITY_TEXT, 1);

        let mut scope_logs = Vec::new();
        encode_len_field(&mut scope_logs, spec::scope_logs::LOG_RECORDS, &log_record);

        let mut resource_logs = Vec::new();
        encode_len_field(
            &mut resource_logs,
            spec::resource_logs::SCOPE_LOGS,
            &scope_logs,
        );

        let mut payload = Vec::new();
        encode_len_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
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
        encode_varint_field(
            &mut payload,
            spec::export_logs_service_request::RESOURCE_LOGS,
            0,
        );

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
    fn projected_nested_kvlist_anyvalue_requests_fallback() {
        let request = nested_kvlist_request();
        let payload = request.encode_to_vec();

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("kvlist AnyValue should fall back to prost");
        assert!(
            matches!(err, ProjectionError::Unsupported(_)),
            "expected unsupported fallback reason, got {err:?}"
        );
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

    fn assert_batches_match(expected: &RecordBatch, actual: &RecordBatch) {
        assert_eq!(expected.schema(), actual.schema());
        assert_eq!(expected.num_rows(), actual.num_rows());
        for (expected_column, actual_column) in expected.columns().iter().zip(actual.columns()) {
            assert_eq!(expected_column.to_data(), actual_column.to_data());
        }
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

    fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    #[test]
    fn projected_complex_anyvalue_requests_fallback() {
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
        let payload = request.encode_to_vec();

        let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect_err("array AnyValue should fall back to prost");
        assert!(
            matches!(err, ProjectionError::Unsupported(_)),
            "expected unsupported fallback reason, got {err:?}"
        );
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
