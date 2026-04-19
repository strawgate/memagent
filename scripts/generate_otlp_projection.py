#!/usr/bin/env python3
"""Generate OTLP projection metadata and fallback preflight code."""

from __future__ import annotations

import argparse
import difflib
import json
import subprocess
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
SPEC = REPO / "crates" / "logfwd-io" / "codegen" / "otlp_projection_v1.schema.json"
OUT = REPO / "crates" / "logfwd-io" / "src" / "otlp_receiver" / "projection" / "generated.rs"

WIRE_TO_VARIANT = {
    "varint": "WireKind::Varint",
    "fixed64": "WireKind::Fixed64",
    "len": "WireKind::Len",
    "fixed32": "WireKind::Fixed32",
}

ACTION_TO_VARIANT = {
    "project": "ProjectionAction::Project",
    "descend": "ProjectionAction::Descend",
    "unsupported": "ProjectionAction::Unsupported",
}

ANY_VALUE_UNSUPPORTED_REASONS = {
    "array_value": "AnyValue::ArrayValue",
    "kvlist_value": "AnyValue::KvListValue",
}

MESSAGE_TO_OTLP_PREFIX = {
    "ExportLogsServiceRequest": "EXPORT_LOGS_REQUEST",
    "ResourceLogs": "RESOURCE_LOGS",
    "Resource": "RESOURCE",
    "ScopeLogs": "SCOPE_LOGS",
    "InstrumentationScope": "INSTRUMENTATION_SCOPE",
    "LogRecord": "LOG_RECORD",
    "KeyValue": "KEY_VALUE",
    "AnyValue": "ANY_VALUE",
}


def load_spec() -> dict:
    return json.loads(SPEC.read_text())


def validate_spec(spec: dict) -> None:
    messages = {message["name"]: message for message in spec["messages"]}
    for message in spec["messages"]:
        name = message["name"]
        if name not in MESSAGE_TO_OTLP_PREFIX:
            raise ValueError(f"OTLP projection schema has no core-constant prefix for {name}")

        seen_numbers: set[int] = set()
        seen_names: set[str] = set()
        for field in message["fields"]:
            field_name = field["name"]
            number = field["number"]
            if not isinstance(number, int) or number <= 0:
                raise ValueError(f"{name}.{field_name} has invalid field number {number!r}")
            if number in seen_numbers:
                raise ValueError(f"{name} duplicates field number {number}")
            if field_name in seen_names:
                raise ValueError(f"{name} duplicates field name {field_name}")
            seen_numbers.add(number)
            seen_names.add(field_name)

            if field["wire"] not in WIRE_TO_VARIANT:
                raise ValueError(f"{name}.{field_name} has unsupported wire kind {field['wire']!r}")
            if field["action"] not in ACTION_TO_VARIANT:
                raise ValueError(f"{name}.{field_name} has unsupported action {field['action']!r}")
            if field["action"] == "descend":
                child = field.get("message")
                if child not in messages:
                    raise ValueError(f"{name}.{field_name} descends into unknown message {child!r}")
            elif "message" in field:
                raise ValueError(f"{name}.{field_name} has message without descend action")

    any_value = messages.get("AnyValue")
    if any_value is None:
        raise ValueError("AnyValue message missing from OTLP projection schema")
    if any_value.get("oneof") != "value":
        raise ValueError("AnyValue schema must document the value oneof")


def rust_ident(name: str) -> str:
    return name.upper().replace(".", "_").replace("-", "_")


def render_field_tables(spec: dict) -> str:
    chunks: list[str] = []
    for message in spec["messages"]:
        fields = message["fields"]
        const_name = f"{rust_ident(message['name'])}_FIELDS"
        chunks.append(f"pub(super) const {const_name}: &[FieldRule] = &[")
        for field in fields:
            expected = WIRE_TO_VARIANT[field["wire"]]
            action = ACTION_TO_VARIANT[field["action"]]
            child = f'Some(MessageKind::{field["message"]})' if "message" in field else "None"
            chunks.append(
                "    FieldRule { "
                f"number: {field['number']}, "
                f"name: \"{field['name']}\", "
                f"expected_wire: {expected}, "
                f"action: {action}, "
                f"child: {child} "
                "},"
            )
        chunks.append("];\n")
    return "\n".join(chunks)


def any_value_fields(spec: dict) -> list[dict]:
    for message in spec["messages"]:
        if message["name"] == "AnyValue":
            return message["fields"]
    raise ValueError("AnyValue message missing from OTLP projection schema")


def render_any_value_decoder(spec: dict) -> str:
    arms: list[str] = []
    for field in any_value_fields(spec):
        number = field["number"]
        name = field["name"]
        kind = field.get("kind")
        invalid = f"invalid wire type for AnyValue.{name}"
        match field["action"], kind, field["wire"]:
            case "project", "string", "len":
                arms.append(
                    f"""            ({number}, WireField::Len(bytes)) => {{
                out = Some(WireAny::String(super::require_utf8(
                    bytes,
                    "invalid UTF-8 AnyValue string",
                )?));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
                )
            case "project", "bool", "varint":
                arms.append(
                    f"""            ({number}, WireField::Varint(value)) => {{
                out = Some(WireAny::Bool(value != 0));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
                )
            case "project", "int", "varint":
                arms.append(
                    f"""            ({number}, WireField::Varint(value)) => {{
                out = Some(WireAny::Int(value as i64));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
                )
            case "project", "double", "fixed64":
                arms.append(
                    f"""            ({number}, WireField::Fixed64(value)) => {{
                out = Some(WireAny::Double(f64::from_bits(value)));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
                )
            case "project", "bytes", "len":
                arms.append(
                    f"""            ({number}, WireField::Len(bytes)) => {{
                out = Some(WireAny::Bytes(bytes));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
                )
            case "unsupported", _, "len":
                reason = ANY_VALUE_UNSUPPORTED_REASONS.get(name, f"AnyValue::{name}")
                arms.append(
                    f"""            ({number}, WireField::Len(_)) => {{
                out = None;
                unsupported = Some("{reason}");
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
                )
            case _:
                raise ValueError(
                    f"unsupported AnyValue codegen mapping for field {name}: "
                    f"action={field['action']} kind={kind} wire={field['wire']}"
                )

    arms_text = "\n".join(arms)
    return f"""pub(super) fn decode_any_value_wire(value: &[u8]) -> Result<Option<WireAny<'_>>, ProjectionError> {{
    let mut out = None;
    let mut unsupported = None;
    super::for_each_field(value, |field, field_value| {{
        match (field, field_value) {{
{arms_text}
            _ => {{}}
        }}
        Ok(())
    }})?;
    if let Some(reason) = unsupported
        && out.is_none()
    {{
        return Err(ProjectionError::Unsupported(reason));
    }}
    Ok(out)
}}
"""


def message_fields(spec: dict, message_name: str) -> list[dict]:
    for message in spec["messages"]:
        if message["name"] == message_name:
            return message["fields"]
    raise ValueError(f"{message_name} message missing from OTLP projection schema")


def render_key_value_decoder(spec: dict) -> str:
    fields = {field["name"]: field for field in message_fields(spec, "KeyValue")}
    key_number = fields["key"]["number"]
    value_number = fields["value"]["number"]
    return f"""pub(super) fn decode_key_value_wire(kv: &[u8]) -> Result<Option<(&[u8], WireAny<'_>)>, ProjectionError> {{
    let mut key = &[][..];
    let mut value = None;
    super::for_each_field(kv, |field, field_value| {{
        match (field, field_value) {{
            ({key_number}, WireField::Len(bytes)) => {{
                key = super::require_utf8(bytes, "invalid UTF-8 attribute key")?;
            }}
            ({key_number}, _) => {{
                return Err(ProjectionError::Invalid("invalid wire type for KeyValue.key"));
            }}
            ({value_number}, WireField::Len(bytes)) => {{
                if let Some(decoded) = decode_any_value_wire(bytes)? {{
                    value = Some(decoded);
                }}
            }}
            ({value_number}, _) => {{
                return Err(ProjectionError::Invalid("invalid wire type for KeyValue.value"));
            }}
            _ => {{}}
        }}
        Ok(())
    }})?;
    Ok(value.map(|value| (key, value)))
}}
"""


def render_lookup_field(spec: dict) -> str:
    message_arms: list[str] = []
    for message in spec["messages"]:
        const_name = f"{rust_ident(message['name'])}_FIELDS"
        number_arms = [
            f"            {field['number']} => Some(&{const_name}[{index}]),"
            for index, field in enumerate(message["fields"])
        ]
        number_arms.append("            _ => None,")
        number_match = "\n".join(number_arms)
        message_arms.append(
            f"""        MessageKind::{message['name']} => match number {{
{number_match}
        }},"""
        )

    message_match = "\n".join(message_arms)
    return f"""fn lookup_field(message: MessageKind, number: u32) -> Option<&'static FieldRule> {{
    match message {{
{message_match}
    }}
}}
"""


def otlp_const_name(message_name: str, field_name: str) -> str:
    return f"{MESSAGE_TO_OTLP_PREFIX[message_name]}_{rust_ident(field_name)}"


def render_core_constant_drift_tests(spec: dict) -> str:
    lines = []
    for message in spec["messages"]:
        message_name = message["name"]
        for field in message["fields"]:
            lines.append(
                f"        assert_eq!({rust_ident(message_name)}_FIELDS"
                f".iter().find(|f| f.name == \"{field['name']}\").expect(\"schema field\").number, "
                f"otlp::{otlp_const_name(message_name, field['name'])});"
            )
    return "\n".join(lines)


def render(spec: dict) -> str:
    tables = render_field_tables(spec)
    any_value_decoder = render_any_value_decoder(spec)
    key_value_decoder = render_key_value_decoder(spec)
    lookup_field = render_lookup_field(spec)
    core_constant_drift_tests = render_core_constant_drift_tests(spec)
    return f"""// @generated by scripts/generate_otlp_projection.py; DO NOT EDIT.
// spec: {spec["name"]} v{spec["version"]}

use super::{{ProjectionError, WireAny, WireField}};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WireKind {{
    Varint,
    Fixed64,
    Len,
    StartGroup,
    EndGroup,
    Fixed32,
}}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProjectionAction {{
    Project,
    Descend,
    Unsupported,
}}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MessageKind {{
    ExportLogsServiceRequest,
    ResourceLogs,
    Resource,
    ScopeLogs,
    InstrumentationScope,
    LogRecord,
    KeyValue,
    AnyValue,
}}

#[derive(Clone, Copy, Debug)]
pub(super) struct FieldRule {{
    #[allow(dead_code)]
    pub(super) number: u32,
    #[allow(dead_code)]
    pub(super) name: &'static str,
    pub(super) expected_wire: WireKind,
    pub(super) action: ProjectionAction,
    pub(super) child: Option<MessageKind>,
}}

{tables}
#[allow(dead_code)]
pub(super) fn fields_for(message: MessageKind) -> &'static [FieldRule] {{
    match message {{
        MessageKind::ExportLogsServiceRequest => EXPORTLOGSSERVICEREQUEST_FIELDS,
        MessageKind::ResourceLogs => RESOURCELOGS_FIELDS,
        MessageKind::Resource => RESOURCE_FIELDS,
        MessageKind::ScopeLogs => SCOPELOGS_FIELDS,
        MessageKind::InstrumentationScope => INSTRUMENTATIONSCOPE_FIELDS,
        MessageKind::LogRecord => LOGRECORD_FIELDS,
        MessageKind::KeyValue => KEYVALUE_FIELDS,
        MessageKind::AnyValue => ANYVALUE_FIELDS,
    }}
}}

pub(super) fn classify_projection_support(input: &[u8]) -> Result<(), ProjectionError> {{
    scan_message(input, MessageKind::ExportLogsServiceRequest)
}}

{any_value_decoder}
{key_value_decoder}
fn scan_message(input: &[u8], message: MessageKind) -> Result<(), ProjectionError> {{
    if message == MessageKind::AnyValue {{
        return scan_any_value(input);
    }}

    let mut input = input;
    while !input.is_empty() {{
        let key = read_varint(&mut input)?;
        let field = decode_field_number(key)?;
        let wire = decode_wire_kind((key & 0x07) as u8)?;
        match wire {{
            WireKind::Varint => {{
                let _ = read_varint(&mut input)?;
            }}
            WireKind::Fixed64 => consume_fixed(&mut input, 8, "truncated fixed64 field")?,
            WireKind::Len => {{
                let bytes = consume_len(&mut input)?;
                if let Some(rule) = lookup_field(message, field) {{
                    if rule.expected_wire != WireKind::Len {{
                        return Err(ProjectionError::Invalid("invalid wire type for projected OTLP field"));
                    }}
                    if let Some(child) = rule.child {{
                        scan_message(bytes, child)?;
                    }}
                }}
            }}
            WireKind::StartGroup => skip_group(&mut input, field)?,
            WireKind::EndGroup => return Err(ProjectionError::Invalid("unexpected protobuf end group")),
            WireKind::Fixed32 => consume_fixed(&mut input, 4, "truncated fixed32 field")?,
        }}

        if let Some(rule) = lookup_field(message, field)
            && wire != rule.expected_wire
        {{
            return Err(ProjectionError::Invalid("invalid wire type for projected OTLP field"));
        }}
    }}
    Ok(())
}}

fn scan_any_value(input: &[u8]) -> Result<(), ProjectionError> {{
    let mut input = input;
    let mut final_oneof: Option<ProjectionAction> = None;
    while !input.is_empty() {{
        let key = read_varint(&mut input)?;
        let field = decode_field_number(key)?;
        let wire = decode_wire_kind((key & 0x07) as u8)?;
        match wire {{
            WireKind::Varint => {{
                let _ = read_varint(&mut input)?;
            }}
            WireKind::Fixed64 => consume_fixed(&mut input, 8, "truncated fixed64 field")?,
            WireKind::Len => {{
                let _ = consume_len(&mut input)?;
            }}
            WireKind::StartGroup => skip_group(&mut input, field)?,
            WireKind::EndGroup => return Err(ProjectionError::Invalid("unexpected protobuf end group")),
            WireKind::Fixed32 => consume_fixed(&mut input, 4, "truncated fixed32 field")?,
        }}

        if let Some(rule) = lookup_field(MessageKind::AnyValue, field) {{
            if wire != rule.expected_wire {{
                return Err(ProjectionError::Invalid("invalid wire type for AnyValue field"));
            }}
            final_oneof = Some(rule.action);
        }}
    }}

    if final_oneof == Some(ProjectionAction::Unsupported) {{
        return Err(ProjectionError::Unsupported("generated AnyValue unsupported oneof"));
    }}
    Ok(())
}}

{lookup_field}
fn consume_fixed(input: &mut &[u8], len: usize, msg: &'static str) -> Result<(), ProjectionError> {{
    if input.len() < len {{
        return Err(ProjectionError::Invalid(msg));
    }}
    *input = &input[len..];
    Ok(())
}}

fn consume_len<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], ProjectionError> {{
    let len = usize::try_from(read_varint(input)?)
        .map_err(|_| ProjectionError::Invalid("protobuf length exceeds usize"))?;
    if input.len() < len {{
        return Err(ProjectionError::Invalid("truncated length-delimited field"));
    }}
    let (bytes, rest) = input.split_at(len);
    *input = rest;
    Ok(bytes)
}}

fn decode_wire_kind(wire_type: u8) -> Result<WireKind, ProjectionError> {{
    match wire_type {{
        0 => Ok(WireKind::Varint),
        1 => Ok(WireKind::Fixed64),
        2 => Ok(WireKind::Len),
        3 => Ok(WireKind::StartGroup),
        4 => Ok(WireKind::EndGroup),
        5 => Ok(WireKind::Fixed32),
        _ => Err(ProjectionError::Invalid("invalid protobuf wire type")),
    }}
}}

const PROTOBUF_MAX_GROUP_DEPTH: usize = 64;

fn skip_group(input: &mut &[u8], start_field: u32) -> Result<(), ProjectionError> {{
    let mut field_stack = [0u32; PROTOBUF_MAX_GROUP_DEPTH];
    let mut depth = 1usize;
    field_stack[0] = start_field;

    while !input.is_empty() {{
        let key = read_varint(input)?;
        let field = decode_field_number(key)?;
        match decode_wire_kind((key & 0x07) as u8)? {{
            WireKind::Varint => {{
                let _ = read_varint(input)?;
            }}
            WireKind::Fixed64 => consume_fixed(input, 8, "truncated fixed64 field")?,
            WireKind::Len => {{
                let _ = consume_len(input)?;
            }}
            WireKind::StartGroup => {{
                if depth == PROTOBUF_MAX_GROUP_DEPTH {{
                    return Err(ProjectionError::Invalid("protobuf group nesting too deep"));
                }}
                field_stack[depth] = field;
                depth += 1;
            }}
            WireKind::EndGroup => {{
                if field != field_stack[depth - 1] {{
                    return Err(ProjectionError::Invalid("mismatched protobuf end group"));
                }}
                depth -= 1;
                if depth == 0 {{
                    return Ok(());
                }}
            }}
            WireKind::Fixed32 => consume_fixed(input, 4, "truncated fixed32 field")?,
        }}
    }}
    Err(ProjectionError::Invalid("unterminated protobuf group"))
}}

fn decode_field_number(key: u64) -> Result<u32, ProjectionError> {{
    const PROTOBUF_MAX_FIELD_NUMBER: u64 = 0x1FFF_FFFF;

    let field = key >> 3;
    if field == 0 {{
        return Err(ProjectionError::Invalid("protobuf field number zero"));
    }}
    if field > PROTOBUF_MAX_FIELD_NUMBER {{
        return Err(ProjectionError::Invalid("protobuf field number out of range"));
    }}
    u32::try_from(field).map_err(|_| ProjectionError::Invalid("protobuf field number overflow"))
}}

fn read_varint(input: &mut &[u8]) -> Result<u64, ProjectionError> {{
    let mut result = 0u64;
    for index in 0..10 {{
        let Some((&byte, rest)) = input.split_first() else {{
            return Err(ProjectionError::Invalid("truncated varint"));
        }};
        *input = rest;
        if index == 9 && byte > 0x01 {{
            return Err(ProjectionError::Invalid("varint overflow"));
        }}
        result |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {{
            return Ok(result);
        }}
    }}
    Err(ProjectionError::Invalid("varint overflow"))
}}

#[cfg(test)]
mod generated_tests {{
    use super::*;
    use super::super::otlp;

    #[test]
    fn generated_anyvalue_table_covers_all_current_oneof_fields() {{
        let fields = fields_for(MessageKind::AnyValue);
        assert_eq!(fields.len(), 7);
        assert!(fields.iter().any(|f| f.name == "array_value" && f.action == ProjectionAction::Unsupported));
        assert!(fields.iter().any(|f| f.name == "kvlist_value" && f.action == ProjectionAction::Unsupported));
        assert!(fields.iter().any(|f| f.name == "bytes_value" && f.action == ProjectionAction::Project));
    }}

    #[test]
    fn generated_logrecord_table_tracks_projected_protocol_fields() {{
        let fields = fields_for(MessageKind::LogRecord);
        assert!(fields.iter().any(|f| f.name == "body" && f.child == Some(MessageKind::AnyValue)));
        assert!(fields.iter().any(|f| f.name == "attributes" && f.child == Some(MessageKind::KeyValue)));
        assert!(fields.iter().any(|f| f.name == "flags" && f.expected_wire == WireKind::Fixed32));
    }}

    #[test]
    fn generated_schema_field_numbers_match_core_otlp_constants() {{
{core_constant_drift_tests}
    }}

    #[test]
    fn generated_decode_anyvalue_keeps_last_oneof_value() {{
        let bytes = [42, 0, 10, 2, b'o', b'k'];
        let value = decode_any_value_wire(&bytes).expect("generated AnyValue should decode");
        match value {{
            Some(WireAny::String(value)) => assert_eq!(value, b"ok"),
            _ => panic!("unexpected generated AnyValue variant"),
        }}
    }}

    #[test]
    fn generated_decode_anyvalue_terminal_unsupported_requests_fallback() {{
        let bytes = [10, 2, b'o', b'k', 42, 0];
        let err = match decode_any_value_wire(&bytes) {{
            Ok(_) => panic!("terminal unsupported oneof should request fallback"),
            Err(err) => err,
        }};
        assert!(matches!(err, ProjectionError::Unsupported(_)));
    }}

    #[test]
    fn generated_decode_anyvalue_wrong_wire_is_invalid() {{
        let bytes = [8, 1];
        let err = match decode_any_value_wire(&bytes) {{
            Ok(_) => panic!("wrong wire type should be invalid"),
            Err(err) => err,
        }};
        assert!(matches!(err, ProjectionError::Invalid(_)));
    }}

    #[test]
    fn generated_decode_keyvalue_keeps_last_key_and_value() {{
        let bytes = [10, 3, b'o', b'l', b'd', 10, 3, b'k', b'e', b'y', 18, 4, 10, 2, b'o', b'k'];
        let value = decode_key_value_wire(&bytes).expect("generated KeyValue should decode");
        match value {{
            Some((key, WireAny::String(value))) => {{
                assert_eq!(key, b"key");
                assert_eq!(value, b"ok");
            }}
            _ => panic!("unexpected generated KeyValue variant"),
        }}
    }}

    #[test]
    fn generated_decode_keyvalue_without_value_is_omitted() {{
        let bytes = [10, 3, b'k', b'e', b'y'];
        let value = decode_key_value_wire(&bytes).expect("generated KeyValue should decode");
        assert!(value.is_none());
    }}
}}
"""


def format_rust(rendered: str) -> str:
    try:
        proc = subprocess.run(
            ["rustfmt", "--edition", "2024", "--emit", "stdout"],
            input=rendered,
            text=True,
            capture_output=True,
            check=True,
        )
    except FileNotFoundError:
        sys.stderr.write("rustfmt not found; install Rust tooling before generating OTLP projection code\n")
        raise
    except subprocess.CalledProcessError as err:
        sys.stderr.write(err.stderr)
        raise
    return proc.stdout


def check_output(rendered: str) -> int:
    current = OUT.read_text() if OUT.exists() else ""
    if current == rendered:
        return 0

    diff = difflib.unified_diff(
        current.splitlines(keepends=True),
        rendered.splitlines(keepends=True),
        fromfile=str(OUT),
        tofile=f"{OUT} (generated)",
    )
    sys.stderr.writelines(diff)
    sys.stderr.write(
        "\nGenerated OTLP projection code is out of date. "
        "Run `python3 scripts/generate_otlp_projection.py`.\n"
    )
    return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the checked-in generated file matches the generator output",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    spec = load_spec()
    validate_spec(spec)
    rendered = format_rust(render(spec))
    if args.check:
        return check_output(rendered)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
