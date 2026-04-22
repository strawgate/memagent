pub(super) fn write_wire_any_as_string(
    builder: &mut ColumnarBatchBuilder,
    handle: FieldHandle,
    value: WireAny<'_>,
    scratch: &mut WireScratch,
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    match value {
        WireAny::String(value) => super::write_wire_str(builder, handle, value, string_storage)?,
        WireAny::Bool(true) => {
            builder
                .write_str_bytes(handle, b"true")
                .map_err(|e| ProjectionError::Batch(e.to_string()))?;
        }
        WireAny::Bool(false) => {
            builder
                .write_str_bytes(handle, b"false")
                .map_err(|e| ProjectionError::Batch(e.to_string()))?;
        }
        WireAny::Int(value) => {
            scratch.decimal.clear();
            let mut buf = itoa::Buffer::new();
            scratch
                .decimal
                .extend_from_slice(buf.format(value).as_bytes());
            builder
                .write_str_bytes(handle, &scratch.decimal)
                .map_err(|e| ProjectionError::Batch(e.to_string()))?;
        }
        WireAny::Double(value) => {
            scratch.decimal.clear();
            let mut buf = ryu::Buffer::new();
            scratch
                .decimal
                .extend_from_slice(buf.format(value).as_bytes());
            builder
                .write_str_bytes(handle, &scratch.decimal)
                .map_err(|e| ProjectionError::Batch(e.to_string()))?;
        }
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

/// Maximum nesting depth for recursive AnyValue JSON serialization.
/// Protects against stack overflow from deeply nested ArrayValue/KvListValue payloads.
pub(super) const MAX_ANY_VALUE_DEPTH: usize = 64;

pub(super) fn write_wire_any_json(
    value: WireAny<'_>,
    out: &mut Vec<u8>,
    scratch: &mut WireScratch,
) -> Result<(), ProjectionError> {
    write_wire_any_json_depth(value, out, scratch, 0)
}

pub(super) fn write_wire_any_json_depth(
    value: WireAny<'_>,
    out: &mut Vec<u8>,
    scratch: &mut WireScratch,
    depth: usize,
) -> Result<(), ProjectionError> {
    if depth > MAX_ANY_VALUE_DEPTH {
        return Err(ProjectionError::Invalid(
            "AnyValue nesting depth exceeds limit",
        ));
    }
    match value {
        WireAny::String(value) => {
            out.push(b'"');
            super::write_json_escaped_bytes(out, value);
            out.push(b'"');
        }
        WireAny::Bool(value) => out.extend_from_slice(if value { b"true" } else { b"false" }),
        WireAny::Int(value) => {
            scratch.decimal.clear();
            let mut buf = itoa::Buffer::new();
            scratch
                .decimal
                .extend_from_slice(buf.format(value).as_bytes());
            out.extend_from_slice(&scratch.decimal);
        }
        WireAny::Double(value) => {
            if value.is_finite() {
                scratch.decimal.clear();
                let mut buf = ryu::Buffer::new();
                scratch
                    .decimal
                    .extend_from_slice(buf.format(value).as_bytes());
                out.extend_from_slice(&scratch.decimal);
            } else if value.is_nan() {
                out.extend_from_slice(b"\"NaN\"");
            } else if value.is_sign_positive() {
                out.extend_from_slice(b"\"inf\"");
            } else {
                out.extend_from_slice(b"\"-inf\"");
            }
        }
        WireAny::Bytes(value) => {
            out.push(b'"');
            super::write_hex_to_buf(out, value);
            out.push(b'"');
        }
        WireAny::ArrayRaw(value) => write_array_value_json(value, out, scratch, depth)?,
        WireAny::KvListRaw(value) => write_kvlist_value_json(value, out, scratch, depth)?,
    }
    Ok(())
}

pub(super) fn write_array_value_json(
    array_value: &[u8],
    out: &mut Vec<u8>,
    scratch: &mut WireScratch,
    depth: usize,
) -> Result<(), ProjectionError> {
    out.push(b'[');
    let mut first = true;
    super::for_each_field(array_value, |field, field_value| {
        if field != 1 {
            return Ok(());
        }
        let WireField::Len(any_value) = field_value else {
            return Err(ProjectionError::Invalid(
                "invalid wire type for ArrayValue.values",
            ));
        };
        if !first {
            out.push(b',');
        }
        first = false;
        if let Some(value) = decode_any_value_wire(any_value)? {
            write_wire_any_json_depth(value, out, scratch, depth + 1)?;
        } else {
            out.extend_from_slice(b"null");
        }
        Ok(())
    })?;
    out.push(b']');
    Ok(())
}

pub(super) fn write_kvlist_value_json(
    kvlist_value: &[u8],
    out: &mut Vec<u8>,
    scratch: &mut WireScratch,
    depth: usize,
) -> Result<(), ProjectionError> {
    out.push(b'[');
    let mut first = true;
    super::for_each_field(kvlist_value, |field, field_value| {
        if field != 1 {
            return Ok(());
        }
        let WireField::Len(kv) = field_value else {
            return Err(ProjectionError::Invalid(
                "invalid wire type for KeyValueList.values",
            ));
        };
        if !first {
            out.push(b',');
        }
        first = false;
        write_key_value_json(kv, out, scratch, depth)?;
        Ok(())
    })?;
    out.push(b']');
    Ok(())
}

pub(super) fn write_key_value_json(
    kv: &[u8],
    out: &mut Vec<u8>,
    scratch: &mut WireScratch,
    depth: usize,
) -> Result<(), ProjectionError> {
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
                value = decode_any_value_wire(bytes)?;
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

    out.extend_from_slice(b"{\"k\":\"");
    super::write_json_escaped_bytes(out, key);
    out.extend_from_slice(b"\",\"v\":");
    if let Some(value) = value {
        write_wire_any_json_depth(value, out, scratch, depth + 1)?;
    } else {
        out.extend_from_slice(b"null");
    }
    out.push(b'}');
    Ok(())
}
