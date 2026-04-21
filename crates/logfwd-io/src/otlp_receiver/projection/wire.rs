//! Wire-level protobuf primitives for OTLP projection decoding.
//!
//! Contains the low-level field iteration, varint decoding, and group
//! skipping logic that the higher-level OTLP decoder builds on.

use logfwd_arrow::columnar::plan::FieldHandle;

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

#[derive(Clone, Copy, Default)]
pub(super) struct ScopeFields<'a> {
    pub(super) name: Option<&'a [u8]>,
    pub(super) version: Option<&'a [u8]>,
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

pub(super) fn read_varint(input: &mut &[u8]) -> Result<u64, ProjectionError> {
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

pub(super) fn require_utf8<'a>(
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
    pub(super) hex: Vec<u8>,
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
