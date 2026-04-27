//! Wire-level protobuf primitives for OTLP projection decoding.
//!
//! Contains the low-level field iteration, varint decoding, and group
//! skipping logic that the higher-level OTLP decoder builds on.

use ffwd_arrow::columnar::plan::FieldHandle;

use super::ProjectionError;

#[derive(Clone, Copy)]
pub(super) enum WireField<'a> {
    Varint(u64),
    Fixed64(u64),
    Len(&'a [u8]),
    Fixed32(u32),
}

#[derive(Clone, Copy)]
pub(super) enum WireAny<'a> {
    String(&'a [u8]),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(&'a [u8]),
    ArrayRaw(&'a [u8]),
    KvListRaw(&'a [u8]),
}

#[derive(Clone, Copy)]
pub(super) enum StringStorage {
    Decoded,
    #[cfg(any(feature = "otlp-research", test))]
    InputView,
}

pub(super) fn for_each_field<'a>(
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
                    .map_err(|_e| ProjectionError::Invalid("protobuf length exceeds usize"))?;
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
    u32::try_from(field).map_err(|_e| ProjectionError::Invalid("protobuf field number overflow"))
}

pub(super) const PROTOBUF_MAX_GROUP_DEPTH: usize = 64;

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
                    .map_err(|_e| ProjectionError::Invalid("protobuf length exceeds usize"))?;
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

/// Decode a protobuf varint from `input`, advancing the slice.
///
/// Hot-path-optimized for the common case: most OTLP varints encode small
/// tag numbers (≤127, one byte) or short field lengths (≤127, one byte).
/// We try a one-byte fast path first, then fall back to the standard 10-byte
/// loop. samply showed `read_varint` was ~12% of wide-10k self-time before
/// this fast path; the one-byte branch handles ~95% of varints with a single
/// byte load and a single branch.
#[inline]
pub(super) fn read_varint(input: &mut &[u8]) -> Result<u64, ProjectionError> {
    // Fast path: 1-byte varint (top bit clear).
    if let Some((&first, rest)) = input.split_first()
        && first < 0x80
    {
        *input = rest;
        return Ok(u64::from(first));
    }
    read_varint_multibyte(input)
}

/// Slow path: 2-to-10 byte varints. Kept as a separate function so the
/// fast path inlines without bloating callers.
#[inline(never)]
#[cold]
fn read_varint_multibyte(input: &mut &[u8]) -> Result<u64, ProjectionError> {
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

/// Validate that `bytes` is valid UTF-8.
///
/// Hot-path-optimized for the common case: most OTLP attribute keys and
/// values are pure ASCII (a strict subset of UTF-8). We scan for any byte
/// with the high bit set using u64 chunks; if none are found, the input
/// is ASCII and trivially valid. We only fall through to `simdutf8` on the
/// rare non-ASCII case.
///
/// Why not just call simdutf8 directly: simdutf8's SIMD path activates at
/// 64 bytes; below that it falls back to the scalar `core::str::from_utf8`,
/// which dominated wide-10k self-time (~15%) before this fast path. OTLP
/// attribute strings are almost always shorter than 64 bytes.
#[inline]
pub(super) fn require_utf8<'a>(
    bytes: &'a [u8],
    context: &'static str,
) -> Result<&'a [u8], ProjectionError> {
    const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
    let mut i = 0;
    let n = bytes.len();
    // 8-byte chunks: a single u64 mask catches any non-ASCII byte.
    while i + 8 <= n {
        // SAFETY: bounds-checked by the loop condition.
        let chunk_bytes: [u8; 8] = bytes[i..i + 8].try_into().unwrap();
        let chunk = u64::from_ne_bytes(chunk_bytes);
        if chunk & HIGH_BITS != 0 {
            return require_utf8_full(bytes, context);
        }
        i += 8;
    }
    // Tail: ≤7 bytes. Byte-wise high-bit check.
    while i < n {
        if bytes[i] >= 0x80 {
            return require_utf8_full(bytes, context);
        }
        i += 1;
    }
    Ok(bytes)
}

/// Full UTF-8 validation. Used by `require_utf8` only when the ASCII fast
/// path detects a high-bit byte.
#[inline(never)]
#[cold]
fn require_utf8_full<'a>(
    bytes: &'a [u8],
    context: &'static str,
) -> Result<&'a [u8], ProjectionError> {
    if simdutf8::basic::from_utf8(bytes).is_err() {
        return Err(ProjectionError::Invalid(context));
    }
    Ok(bytes)
}

pub(super) fn subslice_range(
    parent: &[u8],
    child: &[u8],
) -> Result<(usize, usize), ProjectionError> {
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

#[derive(Default)]
pub(super) struct WireScratch {
    pub(super) decimal: Vec<u8>,
    pub(super) json: Vec<u8>,
    pub(super) resource_key: Vec<u8>,
    pub(super) attr_ranges: Vec<(usize, usize)>,
    pub(super) attr_field_cache: Vec<AttrFieldCache>,
}

#[derive(Default)]
pub(super) struct AttrFieldCache {
    pub(super) key: Vec<u8>,
    pub(super) handle: Option<FieldHandle>,
}
