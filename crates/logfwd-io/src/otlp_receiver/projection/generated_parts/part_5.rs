fn scan_message(input: &[u8], message: MessageKind) -> Result<(), ProjectionError> {
    if message == MessageKind::AnyValue {
        return scan_any_value(input);
    }

    let mut input = input;
    while !input.is_empty() {
        let key = read_varint(&mut input)?;
        let field = decode_field_number(key)?;
        let wire = decode_wire_kind((key & 0x07) as u8)?;
        let rule = lookup_field(message, field);
        match wire {
            WireKind::Varint => {
                let _ = read_varint(&mut input)?;
            }
            WireKind::Fixed64 => consume_fixed(&mut input, 8, "truncated fixed64 field")?,
            WireKind::Len => {
                let bytes = consume_len(&mut input)?;
                if let Some(rule) = rule {
                    if rule.expected_wire != WireKind::Len {
                        return Err(ProjectionError::Invalid(
                            "invalid wire type for projected OTLP field",
                        ));
                    }
                    if let Some(child) = rule.child {
                        scan_message(bytes, child)?;
                    }
                }
            }
            WireKind::StartGroup => skip_group(&mut input, field)?,
            WireKind::EndGroup => {
                return Err(ProjectionError::Invalid("unexpected protobuf end group"));
            }
            WireKind::Fixed32 => consume_fixed(&mut input, 4, "truncated fixed32 field")?,
        }

        if let Some(rule) = rule
            && wire != rule.expected_wire
        {
            return Err(ProjectionError::Invalid(
                "invalid wire type for projected OTLP field",
            ));
        }
    }
    Ok(())
}

fn scan_any_value(input: &[u8]) -> Result<(), ProjectionError> {
    let mut input = input;
    let mut final_oneof: Option<ProjectionAction> = None;
    while !input.is_empty() {
        let key = read_varint(&mut input)?;
        let field = decode_field_number(key)?;
        let wire = decode_wire_kind((key & 0x07) as u8)?;
        match wire {
            WireKind::Varint => {
                let _ = read_varint(&mut input)?;
            }
            WireKind::Fixed64 => consume_fixed(&mut input, 8, "truncated fixed64 field")?,
            WireKind::Len => {
                let _ = consume_len(&mut input)?;
            }
            WireKind::StartGroup => skip_group(&mut input, field)?,
            WireKind::EndGroup => {
                return Err(ProjectionError::Invalid("unexpected protobuf end group"));
            }
            WireKind::Fixed32 => consume_fixed(&mut input, 4, "truncated fixed32 field")?,
        }

        if let Some(rule) = lookup_field(MessageKind::AnyValue, field) {
            if wire != rule.expected_wire {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for AnyValue field",
                ));
            }
            final_oneof = Some(rule.action);
        }
    }

    if final_oneof == Some(ProjectionAction::Unsupported) {
        return Err(ProjectionError::Unsupported(
            "generated AnyValue unsupported oneof",
        ));
    }
    Ok(())
}

fn lookup_field(message: MessageKind, number: u32) -> Option<&'static FieldRule> {
    match message {
        MessageKind::ExportLogsServiceRequest => match number {
            1 => Some(&EXPORTLOGSSERVICEREQUEST_FIELDS[0]),
            _ => None,
        },
        MessageKind::ResourceLogs => match number {
            1 => Some(&RESOURCELOGS_FIELDS[0]),
            2 => Some(&RESOURCELOGS_FIELDS[1]),
            3 => Some(&RESOURCELOGS_FIELDS[2]),
            _ => None,
        },
        MessageKind::Resource => match number {
            1 => Some(&RESOURCE_FIELDS[0]),
            2 => Some(&RESOURCE_FIELDS[1]),
            3 => Some(&RESOURCE_FIELDS[2]),
            _ => None,
        },
        MessageKind::ScopeLogs => match number {
            1 => Some(&SCOPELOGS_FIELDS[0]),
            2 => Some(&SCOPELOGS_FIELDS[1]),
            3 => Some(&SCOPELOGS_FIELDS[2]),
            _ => None,
        },
        MessageKind::InstrumentationScope => match number {
            1 => Some(&INSTRUMENTATIONSCOPE_FIELDS[0]),
            2 => Some(&INSTRUMENTATIONSCOPE_FIELDS[1]),
            3 => Some(&INSTRUMENTATIONSCOPE_FIELDS[2]),
            4 => Some(&INSTRUMENTATIONSCOPE_FIELDS[3]),
            _ => None,
        },
        MessageKind::LogRecord => match number {
            1 => Some(&LOGRECORD_FIELDS[0]),
            2 => Some(&LOGRECORD_FIELDS[1]),
            3 => Some(&LOGRECORD_FIELDS[2]),
            5 => Some(&LOGRECORD_FIELDS[3]),
            6 => Some(&LOGRECORD_FIELDS[4]),
            7 => Some(&LOGRECORD_FIELDS[5]),
            8 => Some(&LOGRECORD_FIELDS[6]),
            9 => Some(&LOGRECORD_FIELDS[7]),
            10 => Some(&LOGRECORD_FIELDS[8]),
            11 => Some(&LOGRECORD_FIELDS[9]),
            12 => Some(&LOGRECORD_FIELDS[10]),
            _ => None,
        },
        MessageKind::KeyValue => match number {
            1 => Some(&KEYVALUE_FIELDS[0]),
            2 => Some(&KEYVALUE_FIELDS[1]),
            _ => None,
        },
        MessageKind::AnyValue => match number {
            1 => Some(&ANYVALUE_FIELDS[0]),
            2 => Some(&ANYVALUE_FIELDS[1]),
            3 => Some(&ANYVALUE_FIELDS[2]),
            4 => Some(&ANYVALUE_FIELDS[3]),
            5 => Some(&ANYVALUE_FIELDS[4]),
            6 => Some(&ANYVALUE_FIELDS[5]),
            7 => Some(&ANYVALUE_FIELDS[6]),
            _ => None,
        },
        MessageKind::ArrayValue => match number {
            1 => Some(&ARRAYVALUE_FIELDS[0]),
            _ => None,
        },
        MessageKind::KeyValueList => match number {
            1 => Some(&KEYVALUELIST_FIELDS[0]),
            _ => None,
        },
    }
}

fn consume_fixed(input: &mut &[u8], len: usize, msg: &'static str) -> Result<(), ProjectionError> {
    if input.len() < len {
        return Err(ProjectionError::Invalid(msg));
    }
    *input = &input[len..];
    Ok(())
}

fn consume_len<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], ProjectionError> {
    let len = usize::try_from(read_varint(input)?)
        .map_err(|_err| ProjectionError::Invalid("protobuf length exceeds usize"))?;
    if input.len() < len {
        return Err(ProjectionError::Invalid("truncated length-delimited field"));
    }
    let (bytes, rest) = input.split_at(len);
    *input = rest;
    Ok(bytes)
}

fn decode_wire_kind(wire_type: u8) -> Result<WireKind, ProjectionError> {
    match wire_type {
        0 => Ok(WireKind::Varint),
        1 => Ok(WireKind::Fixed64),
        2 => Ok(WireKind::Len),
        3 => Ok(WireKind::StartGroup),
        4 => Ok(WireKind::EndGroup),
        5 => Ok(WireKind::Fixed32),
        _ => Err(ProjectionError::Invalid("invalid protobuf wire type")),
    }
}

const PROTOBUF_MAX_GROUP_DEPTH: usize = 64;

fn skip_group(input: &mut &[u8], start_field: u32) -> Result<(), ProjectionError> {
    let mut field_stack = [0u32; PROTOBUF_MAX_GROUP_DEPTH];
    let mut depth = 1usize;
    field_stack[0] = start_field;

    while !input.is_empty() {
        let key = read_varint(input)?;
        let field = decode_field_number(key)?;
        match decode_wire_kind((key & 0x07) as u8)? {
            WireKind::Varint => {
                let _ = read_varint(input)?;
            }
            WireKind::Fixed64 => consume_fixed(input, 8, "truncated fixed64 field")?,
            WireKind::Len => {
                let _ = consume_len(input)?;
            }
            WireKind::StartGroup => {
                if depth == PROTOBUF_MAX_GROUP_DEPTH {
                    return Err(ProjectionError::Invalid("protobuf group nesting too deep"));
                }
                field_stack[depth] = field;
                depth += 1;
            }
            WireKind::EndGroup => {
                if field != field_stack[depth - 1] {
                    return Err(ProjectionError::Invalid("mismatched protobuf end group"));
                }
                depth -= 1;
                if depth == 0 {
                    return Ok(());
                }
            }
            WireKind::Fixed32 => consume_fixed(input, 4, "truncated fixed32 field")?,
        }
    }
    Err(ProjectionError::Invalid("unterminated protobuf group"))
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
    u32::try_from(field).map_err(|_err| ProjectionError::Invalid("protobuf field number overflow"))
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
