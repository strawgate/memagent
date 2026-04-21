//! Builder write helpers for OTLP projection.
//!
//! Utility functions that write decoded wire values into the
//! `ColumnarBatchBuilder` using the appropriate storage strategy.

use logfwd_arrow::columnar::builder::ColumnarBatchBuilder;
use logfwd_arrow::columnar::plan::FieldHandle;

use super::ProjectionError;
use super::generated;
use super::wire::{StringStorage, WireAny, WireScratch};

pub(super) fn write_wire_any_complex_json(
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

pub(super) fn write_json_escaped_bytes(out: &mut Vec<u8>, value: &[u8]) {
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

pub(super) fn write_wire_str(
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

pub(super) fn write_hex_field(
    builder: &mut ColumnarBatchBuilder,
    handle: FieldHandle,
    bytes: &[u8],
    _hex_buf: &mut Vec<u8>,
) -> Result<(), ProjectionError> {
    builder
        .write_hex_bytes_lower(handle, bytes)
        .map_err(|e| ProjectionError::Batch(e.to_string()))
}

pub(super) fn write_hex_to_buf(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 0x0f) as usize]);
    }
}
