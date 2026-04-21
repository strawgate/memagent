//! OTLP protobuf decode logic for projection.
//!
//! Walks the OTLP `ExportLogsServiceRequest` protobuf structure and
//! projects fields directly into a `ColumnarBatchBuilder`, bypassing
//! the full prost object graph.

use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use logfwd_arrow::columnar::builder::ColumnarBatchBuilder;
use logfwd_arrow::columnar::plan::FieldHandle;

use super::generated;
use super::generated::field_numbers as otlp_field;
use super::wire::{
    for_each_field, require_utf8, subslice_range, AttrFieldCache, ScopeFields, StringStorage,
    WireAny, WireField, WireScratch,
};
use super::write::{write_hex_field, write_wire_str};
use super::ProjectionError;

pub(super) fn decode_projected_otlp_logs_inner(
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

pub(super) fn decode_resource_logs_wire(
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
