pub(super) fn for_each_export_resource_logs<'a>(
    input: &'a [u8],
    mut visit: impl FnMut(&'a [u8]) -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    super::for_each_field(input, |field, value| {
        match (field, value) {
            (1, WireField::Len(bytes)) => visit(bytes)?,
            (1, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ExportLogsServiceRequest.resource_logs",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

pub(super) fn for_each_resource_logs_resource<'a>(
    input: &'a [u8],
    mut visit: impl FnMut(&'a [u8]) -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    super::for_each_field(input, |field, value| {
        match (field, value) {
            (1, WireField::Len(bytes)) => visit(bytes)?,
            (1, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ResourceLogs.resource",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

pub(super) fn for_each_resource_logs_scope_logs<'a>(
    input: &'a [u8],
    mut visit: impl FnMut(&'a [u8]) -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    super::for_each_field(input, |field, value| {
        match (field, value) {
            (2, WireField::Len(bytes)) => visit(bytes)?,
            (2, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ResourceLogs.scope_logs",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

pub(super) fn for_each_resource_attribute<'a>(
    input: &'a [u8],
    mut visit: impl FnMut(&'a [u8]) -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    super::for_each_field(input, |field, value| {
        match (field, value) {
            (1, WireField::Len(bytes)) => visit(bytes)?,
            (1, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for Resource.attributes",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

pub(super) fn for_each_scope_logs_scope<'a>(
    input: &'a [u8],
    mut visit: impl FnMut(&'a [u8]) -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    super::for_each_field(input, |field, value| {
        match (field, value) {
            (1, WireField::Len(bytes)) => visit(bytes)?,
            (1, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ScopeLogs.scope",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

pub(super) fn for_each_scope_logs_log_record<'a>(
    input: &'a [u8],
    mut visit: impl FnMut(&'a [u8]) -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    super::for_each_field(input, |field, value| {
        match (field, value) {
            (2, WireField::Len(bytes)) => visit(bytes)?,
            (2, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for ScopeLogs.log_records",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

pub(super) fn decode_any_value_wire(value: &[u8]) -> Result<Option<WireAny<'_>>, ProjectionError> {
    let mut out = None;
    super::for_each_field(value, |field, field_value| {
        match (field, field_value) {
            (1, WireField::Len(bytes)) => {
                out = Some(WireAny::String(super::require_utf8(
                    bytes,
                    "invalid UTF-8 AnyValue string",
                )?));
            }
            (1, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.string_value",
                ));
            }
            (2, WireField::Varint(value)) => {
                out = Some(WireAny::Bool(value != 0));
            }
            (2, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.bool_value",
                ));
            }
            (3, WireField::Varint(value)) => {
                out = Some(WireAny::Int(value as i64));
            }
            (3, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.int_value",
                ));
            }
            (4, WireField::Fixed64(value)) => {
                out = Some(WireAny::Double(f64::from_bits(value)));
            }
            (4, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.double_value",
                ));
            }
            (5, WireField::Len(bytes)) => {
                out = Some(WireAny::ArrayRaw(bytes));
            }
            (5, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.array_value",
                ));
            }
            (6, WireField::Len(bytes)) => {
                out = Some(WireAny::KvListRaw(bytes));
            }
            (6, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.kvlist_value",
                ));
            }
            (7, WireField::Len(bytes)) => {
                out = Some(WireAny::Bytes(bytes));
            }
            (7, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue.bytes_value",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;
    Ok(out)
}

pub(super) fn decode_key_value_wire(
    kv: &[u8],
) -> Result<Option<(&[u8], WireAny<'_>)>, ProjectionError> {
    let mut key = &[][..];
    let mut value = None;
    super::for_each_field(kv, |field, field_value| {
        match (field, field_value) {
            (1, WireField::Len(bytes)) => {
                key = super::require_utf8(bytes, "invalid UTF-8 attribute key")?;
            }
            (1, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for KeyValue.key",
                ));
            }
            (2, WireField::Len(bytes)) => {
                if let Some(decoded) = decode_any_value_wire(bytes)? {
                    value = Some(decoded);
                }
            }
            (2, _) => {
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

pub(super) fn wire_any_field_kind(value: &WireAny<'_>) -> FieldKind {
    match value {
        WireAny::String(_) => FieldKind::Utf8View,
        WireAny::Bool(_) => FieldKind::Bool,
        WireAny::Int(_) => FieldKind::Int64,
        WireAny::Double(_) => FieldKind::Float64,
        WireAny::ArrayRaw(_) => FieldKind::Utf8View,
        WireAny::KvListRaw(_) => FieldKind::Utf8View,
        WireAny::Bytes(_) => FieldKind::Utf8View,
    }
}

pub(super) fn write_wire_any(
    builder: &mut ColumnarBatchBuilder,
    handle: FieldHandle,
    value: WireAny<'_>,
    scratch: &mut WireScratch,
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    match value {
        WireAny::String(value) => super::write_wire_str(builder, handle, value, string_storage)?,
        WireAny::Bool(value) => builder.write_bool(handle, value),
        WireAny::Int(value) => builder.write_i64(handle, value),
        WireAny::Double(value) => builder.write_f64(handle, value),
        WireAny::Bytes(value) => super::write_hex_field(builder, handle, value)?,
        WireAny::ArrayRaw(value) => {
            super::write_wire_any_complex_json(builder, handle, WireAny::ArrayRaw(value), scratch)?;
        }
        WireAny::KvListRaw(value) => {
            super::write_wire_any_complex_json(
                builder,
                handle,
                WireAny::KvListRaw(value),
                scratch,
            )?;
        }
    }
    Ok(())
}
