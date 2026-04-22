#!/usr/bin/env python3
"""Generate OTLP projection metadata and fallback preflight code."""

from __future__ import annotations

import argparse
import copy
import difflib
import subprocess
import sys
from pathlib import Path

from otlp_proto import parse_proto_fields, vendored_proto_root


REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "crates" / "logfwd-io" / "src" / "otlp_receiver" / "projection" / "generated.rs"
PROTO_BASE = REPO / "crates" / "logfwd-io" / "codegen" / "opentelemetry-proto"
PROTO_ROOT = vendored_proto_root(PROTO_BASE)
PROTO_VERSION = PROTO_ROOT.name
PROTO_FILES = [
    PROTO_ROOT / "opentelemetry" / "proto" / "collector" / "logs" / "v1" / "logs_service.proto",
    PROTO_ROOT / "opentelemetry" / "proto" / "logs" / "v1" / "logs.proto",
    PROTO_ROOT / "opentelemetry" / "proto" / "resource" / "v1" / "resource.proto",
    PROTO_ROOT / "opentelemetry" / "proto" / "common" / "v1" / "common.proto",
]

WIRE_TO_VARIANT = {
    "varint": "WireKind::Varint",
    "fixed64": "WireKind::Fixed64",
    "len": "WireKind::Len",
    "fixed32": "WireKind::Fixed32",
}

ACTION_TO_VARIANT = {
    "project": "ProjectionAction::Project",
    "descend": "ProjectionAction::Descend",
    "ignore": "ProjectionAction::Ignore",
    "unsupported": "ProjectionAction::Unsupported",
}

OTLP_PLANNED_FIELDS = [
    ("timestamp", "TIMESTAMP", "Int64", "int64"),
    ("observed_timestamp", "OBSERVED_TIMESTAMP", "Int64", "int64"),
    ("severity", "SEVERITY", "Utf8View", "string"),
    ("severity_number", "SEVERITY_NUMBER", "Int64", "int64"),
    ("body", "BODY", "Utf8View", "any_string"),
    ("trace_id", "TRACE_ID", "Utf8View", "hex"),
    ("span_id", "SPAN_ID", "Utf8View", "hex"),
    ("flags", "FLAGS", "Int64", "int64"),
    ("scope_name", "SCOPE_NAME", "Utf8View", "string"),
    ("scope_version", "SCOPE_VERSION", "Utf8View", "string"),
]

PROTO_TYPE_TO_ANY_KIND = {
    "string": "string",
    "bool": "bool",
    "int64": "int",
    "double": "double",
    "ArrayValue": "array",
    "KeyValueList": "kvlist",
    "bytes": "bytes",
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

PROJECTION_SPEC = {
    "name": "otlp_projection",
    "version": 1,
    "proto_version": PROTO_VERSION,
    "messages": [
        {
            "name": "ExportLogsServiceRequest",
            "fields": [
                {"name": "resource_logs", "action": "descend", "message": "ResourceLogs"},
            ],
        },
        {
            "name": "ResourceLogs",
            "fields": [
                {"name": "resource", "action": "descend", "message": "Resource"},
                {"name": "scope_logs", "action": "descend", "message": "ScopeLogs"},
                {"name": "schema_url", "action": "ignore"},
            ],
        },
        {
            "name": "Resource",
            "fields": [
                {"name": "attributes", "action": "descend", "message": "KeyValue"},
                {"name": "dropped_attributes_count", "action": "ignore"},
                {"name": "entity_refs", "action": "ignore"},
            ],
        },
        {
            "name": "ScopeLogs",
            "fields": [
                {"name": "scope", "action": "descend", "message": "InstrumentationScope"},
                {"name": "log_records", "action": "descend", "message": "LogRecord"},
                {"name": "schema_url", "action": "ignore"},
            ],
        },
        {
            "name": "InstrumentationScope",
            "fields": [
                {"name": "name", "action": "project"},
                {"name": "version", "action": "project"},
                {"name": "attributes", "action": "ignore"},
                {"name": "dropped_attributes_count", "action": "ignore"},
            ],
        },
        {
            "name": "LogRecord",
            "fields": [
                {"name": "time_unix_nano", "action": "project"},
                {"name": "severity_number", "action": "project"},
                {"name": "severity_text", "action": "project"},
                {"name": "body", "action": "descend", "message": "AnyValue"},
                {"name": "attributes", "action": "descend", "message": "KeyValue"},
                {"name": "dropped_attributes_count", "action": "ignore"},
                {"name": "flags", "action": "project"},
                {"name": "trace_id", "action": "project"},
                {"name": "span_id", "action": "project"},
                {"name": "observed_time_unix_nano", "action": "project"},
                {"name": "event_name", "action": "ignore"},
            ],
        },
        {
            "name": "KeyValue",
            "fields": [
                {"name": "key", "action": "project"},
                {"name": "value", "action": "descend", "message": "AnyValue"},
            ],
        },
        {
            "name": "AnyValue",
            "oneof": "value",
            "fields": [
                {"name": "string_value", "action": "project", "kind": "string"},
                {"name": "bool_value", "action": "project", "kind": "bool"},
                {"name": "int_value", "action": "project", "kind": "int"},
                {"name": "double_value", "action": "project", "kind": "double"},
                {"name": "array_value", "action": "project", "kind": "array"},
                {"name": "kvlist_value", "action": "project", "kind": "kvlist"},
                {"name": "bytes_value", "action": "project", "kind": "bytes"},
            ],
        },
        {
            "name": "ArrayValue",
            "fields": [
                {"name": "values", "action": "descend", "message": "AnyValue"},
            ],
        },
        {
            "name": "KeyValueList",
            "fields": [
                {"name": "values", "action": "descend", "message": "KeyValue"},
            ],
        },
    ],
}

def enrich_spec_from_proto(spec: dict) -> dict:
    proto_messages = parse_proto_fields(PROTO_FILES)
    enriched = copy.deepcopy(spec)
    for message in enriched["messages"]:
        name = message["name"]
        proto_fields = proto_messages.get(name)
        if proto_fields is None:
            raise ValueError(f"{name} missing from vendored OTLP proto files")

        for field in message["fields"]:
            field_name = field["name"]
            proto_field = proto_fields.get(field_name)
            if proto_field is None:
                raise ValueError(f"{name}.{field_name} missing from vendored OTLP proto files")

            policy_number = field.get("number")
            if policy_number is not None and policy_number != proto_field["number"]:
                raise ValueError(
                    f"{name}.{field_name} number mismatch: policy={policy_number} "
                    f"proto={proto_field['number']}"
                )

            policy_wire = field.get("wire")
            if policy_wire is not None and policy_wire != proto_field["wire"]:
                raise ValueError(
                    f"{name}.{field_name} wire mismatch: policy={policy_wire} "
                    f"proto={proto_field['wire']}"
                )

            expected_message = field.get("message")
            if expected_message is not None and expected_message != proto_field["proto_type"]:
                raise ValueError(
                    f"{name}.{field_name} child mismatch: policy={expected_message} "
                    f"proto={proto_field['proto_type']}"
                )

            expected_kind = field.get("kind")
            if expected_kind is not None:
                proto_kind = PROTO_TYPE_TO_ANY_KIND.get(proto_field["proto_type"])
                if expected_kind != proto_kind:
                    raise ValueError(
                        f"{name}.{field_name} kind mismatch: policy={expected_kind} "
                        f"proto={proto_kind}"
                    )

            field["number"] = proto_field["number"]
            field["wire"] = proto_field["wire"]
            field["proto_type"] = proto_field["proto_type"]
            if proto_field["oneof"] is not None:
                field["oneof"] = proto_field["oneof"]

        policy_fields = {field["name"] for field in message["fields"]}
        for field_name in sorted(set(proto_fields) - policy_fields):
            raise ValueError(
                f"{name}.{field_name} missing from projection policy; "
                "classify it as project, descend, ignore, or unsupported"
            )
    return enriched


def validate_spec(spec: dict) -> None:
    messages = {message["name"]: message for message in spec["messages"]}
    for message in spec["messages"]:
        name = message["name"]
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
        raise ValueError("AnyValue message missing from OTLP projection policy")
    if any_value.get("oneof") != "value":
        raise ValueError("AnyValue policy must document the value oneof")

    proto_messages = parse_proto_fields(PROTO_FILES)
    any_value_fields = proto_messages.get("AnyValue", {})
    policy_any_value_fields = {field["name"] for field in any_value["fields"]}
    for field_name, field in any_value_fields.items():
        if field.get("oneof") == "value" and field_name not in policy_any_value_fields:
            raise ValueError(f"AnyValue oneof field {field_name} missing from projection policy")


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


def render_field_number_constants(spec: dict) -> str:
    chunks = ["#[allow(dead_code)]", "pub(super) mod field_numbers {"]
    for message in spec["messages"]:
        message_name = message["name"]
        if message_name not in MESSAGE_TO_OTLP_PREFIX:
            continue
        for field in message["fields"]:
            chunks.append(
                f"    pub(crate) const {otlp_const_name(message_name, field['name'])}: u32 = "
                f"{field['number']};"
            )
    chunks.append("}")
    return "\n".join(chunks)


def any_value_fields(spec: dict) -> list[dict]:
    for message in spec["messages"]:
        if message["name"] == "AnyValue":
            return message["fields"]
    raise ValueError("AnyValue message missing from OTLP projection policy")


def render_any_value_decoder(spec: dict) -> str:
    arms: list[str] = []
    for field in any_value_fields(spec):
        number = field["number"]
        name = field["name"]
        kind = field.get("kind")
        invalid = f"invalid wire type for AnyValue.{name}"
        action = field["action"]
        wire = field["wire"]
        if action == "project" and kind == "string" and wire == "len":
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
        elif action == "project" and kind == "bool" and wire == "varint":
            arms.append(
                f"""            ({number}, WireField::Varint(value)) => {{
                out = Some(WireAny::Bool(value != 0));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
            )
        elif action == "project" and kind == "int" and wire == "varint":
            arms.append(
                f"""            ({number}, WireField::Varint(value)) => {{
                out = Some(WireAny::Int(value as i64));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
            )
        elif action == "project" and kind == "double" and wire == "fixed64":
            arms.append(
                f"""            ({number}, WireField::Fixed64(value)) => {{
                out = Some(WireAny::Double(f64::from_bits(value)));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
            )
        elif action == "project" and kind == "bytes" and wire == "len":
            arms.append(
                f"""            ({number}, WireField::Len(bytes)) => {{
                out = Some(WireAny::Bytes(bytes));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
            )
        elif action == "project" and kind == "array" and wire == "len":
            arms.append(
                f"""            ({number}, WireField::Len(bytes)) => {{
                out = Some(WireAny::ArrayRaw(bytes));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
            )
        elif action == "project" and kind == "kvlist" and wire == "len":
            arms.append(
                f"""            ({number}, WireField::Len(bytes)) => {{
                out = Some(WireAny::KvListRaw(bytes));
            }}
            ({number}, _) => {{
                return Err(ProjectionError::Invalid("{invalid}"));
            }}"""
            )
        else:
            raise ValueError(
                f"unsupported AnyValue codegen mapping for field {name}: "
                f"action={field['action']} kind={kind} wire={field['wire']}"
            )

    arms_text = "\n".join(arms)
    return f"""pub(super) fn decode_any_value_wire(value: &[u8]) -> Result<Option<WireAny<'_>>, ProjectionError> {{
    let mut out = None;
    super::for_each_field(value, |field, field_value| {{
        match (field, field_value) {{
{arms_text}
            _ => {{}}
        }}
        Ok(())
    }})?;
    Ok(out)
}}
"""


def message_fields(spec: dict, message_name: str) -> list[dict]:
    for message in spec["messages"]:
        if message["name"] == message_name:
            return message["fields"]
    raise ValueError(f"{message_name} message missing from OTLP projection policy")


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
            if field["action"] == "ignore" or message_name not in MESSAGE_TO_OTLP_PREFIX:
                continue
            lines.append(
                f"        assert_eq!({rust_ident(message_name)}_FIELDS"
                f".iter().find(|f| f.name == \"{field['name']}\").expect(\"projection field\").number, "
                f"otlp::{otlp_const_name(message_name, field['name'])});"
            )
    return "\n".join(lines)


def render_generated_test_vectors(spec: dict) -> str:
    message_kinds = ", ".join(f"MessageKind::{message['name']}" for message in spec["messages"])
    complex_anyvalue_numbers = ", ".join(
        str(field["number"]) for field in any_value_fields(spec) if field["kind"] in {"array", "kvlist"}
    )
    return f"""fn sample_field_for_wire(number: u32, wire: WireKind) -> Vec<u8> {{
    let mut out = Vec::new();
    push_varint(&mut out, (u64::from(number) << 3) | u64::from(wire_id(wire)));
    match wire {{
        WireKind::Varint => push_varint(&mut out, 1),
        WireKind::Fixed64 => out.extend_from_slice(&1u64.to_le_bytes()),
        WireKind::Len => {{
            push_varint(&mut out, 1);
            out.push(0);
        }}
        WireKind::StartGroup => {{
            push_varint(
                &mut out,
                (u64::from(number) << 3) | u64::from(wire_id(WireKind::EndGroup)),
            );
        }}
        WireKind::EndGroup => {{}}
        WireKind::Fixed32 => out.extend_from_slice(&1u32.to_le_bytes()),
    }}
    out
}}

fn push_varint(out: &mut Vec<u8>, mut value: u64) {{
    while value >= 0x80 {{
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }}
    out.push(value as u8);
}}

fn push_len_field(out: &mut Vec<u8>, number: u32, bytes: &[u8]) {{
    push_varint(out, (u64::from(number) << 3) | u64::from(wire_id(WireKind::Len)));
    push_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}}

fn push_fixed64_field(out: &mut Vec<u8>, number: u32, value: u64) {{
    push_varint(
        out,
        (u64::from(number) << 3) | u64::from(wire_id(WireKind::Fixed64)),
    );
    out.extend_from_slice(&value.to_le_bytes());
}}

fn push_fixed32_field(out: &mut Vec<u8>, number: u32, value: u32) {{
    push_varint(
        out,
        (u64::from(number) << 3) | u64::from(wire_id(WireKind::Fixed32)),
    );
    out.extend_from_slice(&value.to_le_bytes());
}}

fn push_varint_field(out: &mut Vec<u8>, number: u32, value: u64) {{
    push_varint(
        out,
        (u64::from(number) << 3) | u64::from(wire_id(WireKind::Varint)),
    );
    push_varint(out, value);
}}

fn wire_id(wire: WireKind) -> u8 {{
    match wire {{
        WireKind::Varint => 0,
        WireKind::Fixed64 => 1,
        WireKind::Len => 2,
        WireKind::StartGroup => 3,
        WireKind::EndGroup => 4,
        WireKind::Fixed32 => 5,
    }}
}}

fn all_messages() -> &'static [MessageKind] {{
    &[{message_kinds}]
}}

fn wrong_wires(expected: WireKind) -> [WireKind; 5] {{
    match expected {{
        WireKind::Varint => [
            WireKind::Fixed64,
            WireKind::Len,
            WireKind::StartGroup,
            WireKind::EndGroup,
            WireKind::Fixed32,
        ],
        WireKind::Fixed64 => [
            WireKind::Varint,
            WireKind::Len,
            WireKind::StartGroup,
            WireKind::EndGroup,
            WireKind::Fixed32,
        ],
        WireKind::Len => [
            WireKind::Varint,
            WireKind::Fixed64,
            WireKind::StartGroup,
            WireKind::EndGroup,
            WireKind::Fixed32,
        ],
        WireKind::StartGroup => [
            WireKind::Varint,
            WireKind::Fixed64,
            WireKind::Len,
            WireKind::EndGroup,
            WireKind::Fixed32,
        ],
        WireKind::EndGroup => [
            WireKind::Varint,
            WireKind::Fixed64,
            WireKind::Len,
            WireKind::StartGroup,
            WireKind::Fixed32,
        ],
        WireKind::Fixed32 => [
            WireKind::Varint,
            WireKind::Fixed64,
            WireKind::Len,
            WireKind::StartGroup,
            WireKind::EndGroup,
        ],
    }}
}}

fn complex_anyvalue_field_numbers() -> &'static [u32] {{
    &[{complex_anyvalue_numbers}]
}}
"""


def render_planned_handle_builder() -> str:
    handle_fields = "\n".join(
        f"    pub(super) {field_name}: FieldHandle,"
        for field_name, _field_const, _kind, _writer in OTLP_PLANNED_FIELDS
    )
    handle_bindings = "\n".join(
        "        "
        f"{field_name}: plan"
        f".declare_planned(field_names::{field_const}, FieldKind::{kind})"
        '.expect("duplicate planned field"),'
        for field_name, field_const, kind, _writer in OTLP_PLANNED_FIELDS
    )
    appender_methods = []
    for field_name, _field_const, _kind, writer in OTLP_PLANNED_FIELDS:
        if writer == "int64":
            appender_methods.append(
                f"""    pub(super) fn write_{field_name}(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: i64,
    ) {{
        builder.write_i64(self.{field_name}, value);
    }}"""
            )
        elif writer == "string":
            appender_methods.append(
                f"""    pub(super) fn write_{field_name}(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: &[u8],
        string_storage: StringStorage,
    ) -> Result<(), ProjectionError> {{
        super::write_wire_str(builder, self.{field_name}, value, string_storage)
    }}"""
            )
        elif writer == "hex":
            appender_methods.append(
                f"""    pub(super) fn write_{field_name}(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: &[u8],
    ) -> Result<(), ProjectionError> {{
        super::write_hex_field(builder, self.{field_name}, value)
    }}"""
            )
        elif writer == "any_string":
            appender_methods.append(
                f"""    pub(super) fn write_{field_name}(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: WireAny<'_>,
        scratch: &mut WireScratch,
        string_storage: StringStorage,
    ) -> Result<(), ProjectionError> {{
        write_wire_any_as_string(builder, self.{field_name}, value, scratch, string_storage)
    }}"""
            )
        else:
            raise ValueError(f"unknown OTLP planned field writer {writer!r}")

    appender_impl = "\n\n".join(appender_methods)
    return f"""pub(super) struct OtlpFieldHandles {{
{handle_fields}
}}

impl OtlpFieldHandles {{
{appender_impl}
}}

pub(super) fn build_otlp_plan() -> (BatchPlan, OtlpFieldHandles) {{
    let mut plan = BatchPlan::new();
    let handles = OtlpFieldHandles {{
{handle_bindings}
    }};
    (plan, handles)
}}
"""


def render_scope_log_decoders(spec: dict) -> str:
    scope = {field["name"]: field for field in message_fields(spec, "InstrumentationScope")}
    log_record = {field["name"]: field for field in message_fields(spec, "LogRecord")}

    return """#[derive(Clone, Copy, Default)]
pub(super) struct ScopeFields<'a> {
    pub(super) name: Option<&'a [u8]>,
    pub(super) version: Option<&'a [u8]>,
}

pub(super) fn merge_scope_wire<'a>(
    scope: &'a [u8],
    scope_fields: &mut ScopeFields<'a>,
) -> Result<(), ProjectionError> {
    super::for_each_field(scope, |field, value| {
        match (field, value) {
            (__SCOPE_NAME_FIELD__, WireField::Len(value)) => {
                scope_fields.name = Some(super::require_utf8(
                    value,
                    "invalid UTF-8 scope name",
                )?);
            }
            (__SCOPE_NAME_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for InstrumentationScope.name",
                ));
            }
            (__SCOPE_VERSION_FIELD__, WireField::Len(value)) => {
                scope_fields.version = Some(super::require_utf8(
                    value,
                    "invalid UTF-8 scope version",
                )?);
            }
            (__SCOPE_VERSION_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for InstrumentationScope.version",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

#[derive(Clone, Copy, Default)]
pub(super) struct LogRecordFields<'a> {
    pub(super) time_unix_nano: u64,
    pub(super) observed_time_unix_nano: u64,
    pub(super) severity_number: i64,
    pub(super) severity_text: Option<&'a [u8]>,
    pub(super) body: Option<WireAny<'a>>,
    pub(super) trace_id: Option<&'a [u8]>,
    pub(super) span_id: Option<&'a [u8]>,
    pub(super) flags: i64,
}

pub(super) fn decode_log_record_fields<'a>(
    log_record: &'a [u8],
    attr_ranges: &mut Vec<(usize, usize)>,
) -> Result<LogRecordFields<'a>, ProjectionError> {
    let mut out = LogRecordFields::default();
    super::for_each_field(log_record, |field, value| {
        match (field, value) {
            (__LOG_TIME_FIELD__, WireField::Fixed64(value)) => {
                out.time_unix_nano = value;
            }
            (__LOG_TIME_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.time_unix_nano",
                ));
            }
            (__LOG_OBSERVED_TIME_FIELD__, WireField::Fixed64(value)) => {
                out.observed_time_unix_nano = value;
            }
            (__LOG_OBSERVED_TIME_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.observed_time_unix_nano",
                ));
            }
            (__LOG_SEVERITY_NUMBER_FIELD__, WireField::Varint(value)) => {
                out.severity_number = i64::from(value as i32);
            }
            (__LOG_SEVERITY_NUMBER_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.severity_number",
                ));
            }
            (__LOG_SEVERITY_TEXT_FIELD__, WireField::Len(value)) => {
                out.severity_text = if value.is_empty() {
                    None
                } else {
                    Some(super::require_utf8(
                        value,
                        "invalid UTF-8 severity text",
                    )?)
                };
            }
            (__LOG_SEVERITY_TEXT_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.severity_text",
                ));
            }
            (__LOG_BODY_FIELD__, WireField::Len(value)) => {
                if let Some(value) = decode_any_value_wire(value)? {
                    out.body = Some(value);
                }
            }
            (__LOG_BODY_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.body",
                ));
            }
            (__LOG_TRACE_ID_FIELD__, WireField::Len(value)) => {
                out.trace_id = (!value.is_empty()).then_some(value);
            }
            (__LOG_TRACE_ID_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.trace_id",
                ));
            }
            (__LOG_SPAN_ID_FIELD__, WireField::Len(value)) => {
                out.span_id = (!value.is_empty()).then_some(value);
            }
            (__LOG_SPAN_ID_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.span_id",
                ));
            }
            (__LOG_FLAGS_FIELD__, WireField::Fixed32(value)) => {
                out.flags = i64::from(value);
            }
            (__LOG_FLAGS_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.flags",
                ));
            }
            (__LOG_ATTRS_FIELD__, WireField::Len(value)) => {
                attr_ranges.push(super::subslice_range(log_record, value)?);
            }
            (__LOG_ATTRS_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.attributes",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;
    Ok(out)
}
""".replace("__SCOPE_NAME_FIELD__", str(scope["name"]["number"])).replace(
        "__SCOPE_VERSION_FIELD__", str(scope["version"]["number"])
    ).replace("__LOG_TIME_FIELD__", str(log_record["time_unix_nano"]["number"])).replace(
        "__LOG_OBSERVED_TIME_FIELD__", str(log_record["observed_time_unix_nano"]["number"])
    ).replace("__LOG_SEVERITY_NUMBER_FIELD__", str(log_record["severity_number"]["number"])).replace(
        "__LOG_SEVERITY_TEXT_FIELD__", str(log_record["severity_text"]["number"])
    ).replace("__LOG_BODY_FIELD__", str(log_record["body"]["number"])).replace(
        "__LOG_TRACE_ID_FIELD__", str(log_record["trace_id"]["number"])
    ).replace("__LOG_SPAN_ID_FIELD__", str(log_record["span_id"]["number"])).replace(
        "__LOG_FLAGS_FIELD__", str(log_record["flags"]["number"])
    ).replace("__LOG_ATTRS_FIELD__", str(log_record["attributes"]["number"]))


def render_len_field_visitors(spec: dict) -> str:
    messages = {message["name"]: {field["name"]: field for field in message["fields"]} for message in spec["messages"]}
    visitors = [
        (
            "for_each_export_resource_logs",
            "ExportLogsServiceRequest",
            "resource_logs",
            "ExportLogsServiceRequest.resource_logs",
        ),
        (
            "for_each_resource_logs_resource",
            "ResourceLogs",
            "resource",
            "ResourceLogs.resource",
        ),
        (
            "for_each_resource_logs_scope_logs",
            "ResourceLogs",
            "scope_logs",
            "ResourceLogs.scope_logs",
        ),
        (
            "for_each_resource_attribute",
            "Resource",
            "attributes",
            "Resource.attributes",
        ),
        (
            "for_each_scope_logs_scope",
            "ScopeLogs",
            "scope",
            "ScopeLogs.scope",
        ),
        (
            "for_each_scope_logs_log_record",
            "ScopeLogs",
            "log_records",
            "ScopeLogs.log_records",
        ),
    ]
    blocks = []
    for fn_name, message_name, field_name, display_name in visitors:
        field = messages[message_name][field_name]
        if field["wire"] != "len":
            raise ValueError(f"{message_name}.{field_name} visitor requires len wire")
        blocks.append(
            f"""pub(super) fn {fn_name}<'a>(
    input: &'a [u8],
    mut visit: impl FnMut(&'a [u8]) -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {{
    super::for_each_field(input, |field, value| {{
        match (field, value) {{
            ({field["number"]}, WireField::Len(bytes)) => visit(bytes)?,
            ({field["number"]}, _) => {{
                return Err(ProjectionError::Invalid(
                    "invalid wire type for {display_name}",
                ));
            }}
            _ => {{}}
        }}
        Ok(())
    }})
}}"""
        )
    return "\n\n".join(blocks) + "\n"


def render_wire_any_field_kind(spec: dict) -> str:
    kind_to_field_kind = {
        "string": "FieldKind::Utf8View",
        "bool": "FieldKind::Bool",
        "int": "FieldKind::Int64",
        "double": "FieldKind::Float64",
        "bytes": "FieldKind::Utf8View",
        "array": "FieldKind::Utf8View",
        "kvlist": "FieldKind::Utf8View",
    }
    variant_by_kind = {
        "string": "String",
        "bool": "Bool",
        "int": "Int",
        "double": "Double",
        "bytes": "Bytes",
        "array": "ArrayRaw",
        "kvlist": "KvListRaw",
    }
    arms = []
    for field in any_value_fields(spec):
        if field["action"] != "project":
            continue
        kind = field["kind"]
        variant = variant_by_kind[kind]
        field_kind = kind_to_field_kind[kind]
        arms.append(f"        WireAny::{variant}(_) => {field_kind},")
    return f"""pub(super) fn wire_any_field_kind(value: &WireAny<'_>) -> FieldKind {{
    match value {{
{chr(10).join(arms)}
    }}
}}
"""


def render_wire_any_appenders(spec: dict) -> str:
    kinds = {
        field["kind"]
        for field in any_value_fields(spec)
        if field["action"] == "project"
    }
    expected_kinds = {"string", "bool", "int", "double", "bytes", "array", "kvlist"}
    if kinds != expected_kinds:
        raise ValueError(
            "AnyValue projection kinds drifted; update generated appender mapping: "
            f"expected={sorted(expected_kinds)} got={sorted(kinds)}"
        )

    return """pub(super) fn write_wire_any(
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
            super::write_wire_any_complex_json(builder, handle, WireAny::KvListRaw(value), scratch)?;
        }
    }
    Ok(())
}

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
                .write_str_bytes(handle, b\"true\")
                .map_err(|e| ProjectionError::Batch(e.to_string()))?;
        }
        WireAny::Bool(false) => {
            builder
                .write_str_bytes(handle, b\"false\")
                .map_err(|e| ProjectionError::Batch(e.to_string()))?;
        }
        WireAny::Int(value) => {
            scratch.decimal.clear();
            let mut buf = itoa::Buffer::new();
            scratch.decimal.extend_from_slice(buf.format(value).as_bytes());
            builder
                .write_str_bytes(handle, &scratch.decimal)
                .map_err(|e| ProjectionError::Batch(e.to_string()))?;
        }
        WireAny::Double(value) => {
            scratch.decimal.clear();
            let mut buf = ryu::Buffer::new();
            scratch.decimal.extend_from_slice(buf.format(value).as_bytes());
            builder
                .write_str_bytes(handle, &scratch.decimal)
                .map_err(|e| ProjectionError::Batch(e.to_string()))?;
        }
        WireAny::Bytes(value) => super::write_hex_field(builder, handle, value)?,
        WireAny::ArrayRaw(value) => {
            super::write_wire_any_complex_json(builder, handle, WireAny::ArrayRaw(value), scratch)?;
        }
        WireAny::KvListRaw(value) => {
            super::write_wire_any_complex_json(builder, handle, WireAny::KvListRaw(value), scratch)?;
        }
    }
    Ok(())
}
"""


def render_wire_any_json_writer(spec: dict) -> str:
    kinds = {
        field["kind"]
        for field in any_value_fields(spec)
        if field["action"] == "project"
    }
    expected_kinds = {"string", "bool", "int", "double", "bytes", "array", "kvlist"}
    if kinds != expected_kinds:
        raise ValueError(
            "AnyValue projection kinds drifted; update generated JSON writer mapping: "
            f"expected={sorted(expected_kinds)} got={sorted(kinds)}"
        )

    array_value_fields = {field["name"]: field for field in message_fields(spec, "ArrayValue")}
    kvlist_value_fields = {field["name"]: field for field in message_fields(spec, "KeyValueList")}
    key_value_fields = {field["name"]: field for field in message_fields(spec, "KeyValue")}
    array_values_number = array_value_fields["values"]["number"]
    kvlist_values_number = kvlist_value_fields["values"]["number"]
    key_number = key_value_fields["key"]["number"]
    value_number = key_value_fields["value"]["number"]

    return """/// Maximum nesting depth for recursive AnyValue JSON serialization.
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
            out.push(b'\"');
            super::write_json_escaped_bytes(out, value);
            out.push(b'\"');
        }
        WireAny::Bool(value) => out.extend_from_slice(if value { b\"true\" } else { b\"false\" }),
        WireAny::Int(value) => {
            scratch.decimal.clear();
            let mut buf = itoa::Buffer::new();
            scratch.decimal.extend_from_slice(buf.format(value).as_bytes());
            out.extend_from_slice(&scratch.decimal);
        }
        WireAny::Double(value) => {
            if value.is_finite() {
                scratch.decimal.clear();
                let mut buf = ryu::Buffer::new();
                scratch.decimal.extend_from_slice(buf.format(value).as_bytes());
                out.extend_from_slice(&scratch.decimal);
            } else if value.is_nan() {
                out.extend_from_slice(b\"\\\"NaN\\\"\");
            } else if value.is_sign_positive() {
                out.extend_from_slice(b\"\\\"inf\\\"\");
            } else {
                out.extend_from_slice(b\"\\\"-inf\\\"\");
            }
        }
        WireAny::Bytes(value) => {
            out.push(b'\"');
            super::write_hex_to_buf(out, value);
            out.push(b'\"');
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
        if field != __ARRAY_VALUES_FIELD__ {
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
        if field != __KVLIST_VALUES_FIELD__ {
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
            (__KEY_VALUE_KEY_FIELD__, WireField::Len(bytes)) => {
                key = super::require_utf8(bytes, "invalid UTF-8 attribute key")?;
            }
            (__KEY_VALUE_KEY_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for KeyValue.key",
                ));
            }
            (__KEY_VALUE_VALUE_FIELD__, WireField::Len(bytes)) => {
                value = decode_any_value_wire(bytes)?;
            }
            (__KEY_VALUE_VALUE_FIELD__, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for KeyValue.value",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;

    out.extend_from_slice(b"{\\"k\\":\\"");
    super::write_json_escaped_bytes(out, key);
    out.extend_from_slice(b"\\",\\"v\\":");
    if let Some(value) = value {
        write_wire_any_json_depth(value, out, scratch, depth + 1)?;
    } else {
        out.extend_from_slice(b"null");
    }
    out.push(b'}');
    Ok(())
}
""".replace("__ARRAY_VALUES_FIELD__", str(array_values_number)).replace(
        "__KVLIST_VALUES_FIELD__", str(kvlist_values_number)
    ).replace("__KEY_VALUE_KEY_FIELD__", str(key_number)).replace(
        "__KEY_VALUE_VALUE_FIELD__", str(value_number)
    )


def render(spec: dict) -> str:
    field_number_constants = render_field_number_constants(spec)
    tables = render_field_tables(spec)
    any_value_decoder = render_any_value_decoder(spec)
    key_value_decoder = render_key_value_decoder(spec)
    lookup_field = render_lookup_field(spec)
    core_constant_drift_tests = render_core_constant_drift_tests(spec)
    generated_test_vectors = render_generated_test_vectors(spec)
    planned_handle_builder = render_planned_handle_builder()
    scope_log_decoders = render_scope_log_decoders(spec)
    len_field_visitors = render_len_field_visitors(spec)
    wire_any_field_kind = render_wire_any_field_kind(spec)
    wire_any_appenders = render_wire_any_appenders(spec)
    wire_any_json_writer = render_wire_any_json_writer(spec)
    message_variants = "\n    ".join(message["name"] + "," for message in spec["messages"])
    fields_for_arms = "\n        ".join(
        f"MessageKind::{message['name']} => {rust_ident(message['name'])}_FIELDS,"
        for message in spec["messages"]
    )
    return f"""// @generated by scripts/generate_otlp_projection.py; DO NOT EDIT.
// spec: {spec["name"]} v{spec["version"]}; opentelemetry-proto {spec["proto_version"]}

use super::{{ProjectionError, StringStorage, WireAny, WireField, WireScratch}};
use logfwd_arrow::columnar::builder::ColumnarBatchBuilder;
use logfwd_arrow::columnar::plan::{{BatchPlan, FieldHandle, FieldKind}};
use logfwd_types::field_names;

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
    Ignore,
    Unsupported,
}}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MessageKind {{
    {message_variants}
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

{field_number_constants}

{tables}
#[allow(dead_code)]
pub(super) fn fields_for(message: MessageKind) -> &'static [FieldRule] {{
    match message {{
        {fields_for_arms}
    }}
}}

pub(super) fn classify_projection_support(input: &[u8]) -> Result<(), ProjectionError> {{
    scan_message(input, MessageKind::ExportLogsServiceRequest)
}}

{planned_handle_builder}
{scope_log_decoders}
{len_field_visitors}
{any_value_decoder}
{key_value_decoder}
{wire_any_field_kind}
{wire_any_appenders}
{wire_any_json_writer}
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
        .map_err(|_err| ProjectionError::Invalid("protobuf length exceeds usize"))?;
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
    u32::try_from(field).map_err(|_err| ProjectionError::Invalid("protobuf field number overflow"))
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
    use logfwd_core::otlp;

{generated_test_vectors}

    #[test]
    fn generated_anyvalue_table_covers_all_current_oneof_fields() {{
        let fields = fields_for(MessageKind::AnyValue);
        assert_eq!(fields.len(), 7);
        assert!(fields.iter().any(|f| f.name == "array_value" && f.action == ProjectionAction::Project));
        assert!(fields.iter().any(|f| f.name == "kvlist_value" && f.action == ProjectionAction::Project));
        assert!(fields.iter().any(|f| f.name == "bytes_value" && f.action == ProjectionAction::Project));
    }}

    #[test]
    fn generated_wire_any_field_kind_matches_variant_policy() {{
        assert_eq!(wire_any_field_kind(&WireAny::String(b"s")), FieldKind::Utf8View);
        assert_eq!(wire_any_field_kind(&WireAny::Bool(true)), FieldKind::Bool);
        assert_eq!(wire_any_field_kind(&WireAny::Int(42)), FieldKind::Int64);
        assert_eq!(wire_any_field_kind(&WireAny::Double(42.5)), FieldKind::Float64);
        assert_eq!(wire_any_field_kind(&WireAny::Bytes(b"b")), FieldKind::Utf8View);
        assert_eq!(wire_any_field_kind(&WireAny::ArrayRaw(&[])), FieldKind::Utf8View);
        assert_eq!(wire_any_field_kind(&WireAny::KvListRaw(&[])), FieldKind::Utf8View);
    }}

    #[test]
    fn generated_wire_any_json_handles_scalar_and_complex_variants() {{
        let mut scratch = WireScratch::default();
        let cases: &[(WireAny<'_>, &[u8])] = &[
            (WireAny::String(br#"a"b"#), br#""a\\"b""#),
            (WireAny::Bool(true), b"true"),
            (WireAny::Int(-42), b"-42"),
            (WireAny::Double(12.5), b"12.5"),
            (WireAny::Bytes(&[0xde, 0xad]), br#""dead""#),
            (WireAny::ArrayRaw(&[]), b"[]"),
            (WireAny::KvListRaw(&[]), b"[]"),
        ];

        for &(value, expected) in cases {{
            let mut out = Vec::new();
            write_wire_any_json(value, &mut out, &mut scratch)
                .expect("generated JSON writer should handle projected variant");
            assert_eq!(out, expected);
        }}
    }}

    #[test]
    fn generated_complex_json_writers_handle_nested_messages() {{
        let mut scratch = WireScratch::default();
        let any_string = [10, 1, b'x'];
        let array = [10, 3, 10, 1, b'x'];
        let kv = [10, 1, b'k', 18, 3, 10, 1, b'v'];
        let kvlist = [10, 8, 10, 1, b'k', 18, 3, 10, 1, b'v'];

        let mut out = Vec::new();
        write_array_value_json(&array, &mut out, &mut scratch, 0)
            .expect("generated array JSON writer should decode repeated AnyValue");
        assert_eq!(out, br#"["x"]"#);

        out.clear();
        write_key_value_json(&kv, &mut out, &mut scratch, 0)
            .expect("generated key value JSON writer should decode KeyValue");
        assert_eq!(out, br#"{{"k":"k","v":"v"}}"#);

        out.clear();
        write_kvlist_value_json(&kvlist, &mut out, &mut scratch, 0)
            .expect("generated kvlist JSON writer should decode repeated KeyValue");
        assert_eq!(out, br#"[{{"k":"k","v":"v"}}]"#);

        out.clear();
        write_wire_any_json(WireAny::ArrayRaw(&array), &mut out, &mut scratch)
            .expect("generated AnyValue JSON writer should dispatch arrays");
        assert_eq!(out, br#"["x"]"#);

        out.clear();
        write_wire_any_json(WireAny::String(&any_string[2..]), &mut out, &mut scratch)
            .expect("generated AnyValue JSON writer should keep scalar dispatch");
        assert_eq!(out, br#""x""#);
    }}

    #[test]
    fn generated_planned_appenders_write_expected_columns() {{
        use arrow::array::{{Int64Array, StringViewArray}};

        let (plan, handles) = build_otlp_plan();
        let mut builder = ColumnarBatchBuilder::new(plan);
        let mut scratch = WireScratch::default();

        builder.begin_batch();
        builder.begin_row();
        handles.write_timestamp(&mut builder, 11);
        handles.write_observed_timestamp(&mut builder, 12);
        handles
            .write_severity(&mut builder, b"INFO", StringStorage::Decoded)
            .expect("severity appender should write");
        handles.write_severity_number(&mut builder, 9);
        handles
            .write_body(
                &mut builder,
                WireAny::String(b"body"),
                &mut scratch,
                StringStorage::Decoded,
            )
            .expect("body appender should write");
        handles
            .write_trace_id(&mut builder, &[0xab, 0xcd])
            .expect("trace id appender should write");
        handles
            .write_span_id(&mut builder, &[0x12, 0x34])
            .expect("span id appender should write");
        handles.write_flags(&mut builder, 1);
        handles
            .write_scope_name(&mut builder, b"scope", StringStorage::Decoded)
            .expect("scope name appender should write");
        handles
            .write_scope_version(&mut builder, b"1.2.3", StringStorage::Decoded)
            .expect("scope version appender should write");
        builder.end_row();

        let batch = builder
            .finish_batch()
            .expect("generated appenders should produce a batch");

        let int_value = |name: &str| -> i64 {{
            let column = batch
                .column(batch.schema().index_of(name).expect("planned int field"));
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("planned field should be Int64")
                .value(0)
        }};
        let str_value = |name: &str| -> String {{
            let column = batch
                .column(batch.schema().index_of(name).expect("planned string field"));
            column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("planned field should be StringViewArray")
                .value(0)
                .to_owned()
        }};

        assert_eq!(int_value(field_names::TIMESTAMP), 11);
        assert_eq!(int_value(field_names::OBSERVED_TIMESTAMP), 12);
        assert_eq!(str_value(field_names::SEVERITY), "INFO");
        assert_eq!(int_value(field_names::SEVERITY_NUMBER), 9);
        assert_eq!(str_value(field_names::BODY), "body");
        assert_eq!(str_value(field_names::TRACE_ID), "abcd");
        assert_eq!(str_value(field_names::SPAN_ID), "1234");
        assert_eq!(int_value(field_names::FLAGS), 1);
        assert_eq!(str_value(field_names::SCOPE_NAME), "scope");
        assert_eq!(str_value(field_names::SCOPE_VERSION), "1.2.3");
    }}

    #[test]
    fn generated_scope_decoder_merges_partial_scope_messages() {{
        let mut scope_fields = ScopeFields::default();
        let mut name = Vec::new();
        let mut version = Vec::new();
        push_len_field(
            &mut name,
            field_numbers::INSTRUMENTATION_SCOPE_NAME,
            b"scope-a",
        );
        push_len_field(
            &mut version,
            field_numbers::INSTRUMENTATION_SCOPE_VERSION,
            b"1.2.3",
        );

        merge_scope_wire(&name, &mut scope_fields).expect("scope name should decode");
        merge_scope_wire(&version, &mut scope_fields).expect("scope version should decode");

        assert_eq!(scope_fields.name, Some(b"scope-a".as_slice()));
        assert_eq!(scope_fields.version, Some(b"1.2.3".as_slice()));
    }}

    #[test]
    fn generated_log_record_decoder_extracts_projected_fields() {{
        let mut body = Vec::new();
        push_len_field(&mut body, field_numbers::ANY_VALUE_STRING_VALUE, b"body");

        let mut attr_value = Vec::new();
        push_len_field(&mut attr_value, field_numbers::ANY_VALUE_STRING_VALUE, b"value");

        let mut attr = Vec::new();
        push_len_field(&mut attr, field_numbers::KEY_VALUE_KEY, b"key");
        push_len_field(&mut attr, field_numbers::KEY_VALUE_VALUE, &attr_value);

        let trace_id = [0x11; 16];
        let span_id = [0x22; 8];
        let mut log_record = Vec::new();
        push_fixed64_field(
            &mut log_record,
            field_numbers::LOG_RECORD_TIME_UNIX_NANO,
            11,
        );
        push_fixed64_field(
            &mut log_record,
            field_numbers::LOG_RECORD_OBSERVED_TIME_UNIX_NANO,
            12,
        );
        push_varint_field(
            &mut log_record,
            field_numbers::LOG_RECORD_SEVERITY_NUMBER,
            9,
        );
        push_len_field(
            &mut log_record,
            field_numbers::LOG_RECORD_SEVERITY_TEXT,
            b"INFO",
        );
        push_len_field(&mut log_record, field_numbers::LOG_RECORD_BODY, &body);
        push_len_field(&mut log_record, field_numbers::LOG_RECORD_TRACE_ID, &trace_id);
        push_len_field(&mut log_record, field_numbers::LOG_RECORD_SPAN_ID, &span_id);
        push_fixed32_field(&mut log_record, field_numbers::LOG_RECORD_FLAGS, 1);
        push_len_field(&mut log_record, field_numbers::LOG_RECORD_ATTRIBUTES, &attr);

        let mut attr_ranges = Vec::new();
        let decoded = decode_log_record_fields(&log_record, &mut attr_ranges)
            .expect("log record fields should decode");

        assert_eq!(decoded.time_unix_nano, 11);
        assert_eq!(decoded.observed_time_unix_nano, 12);
        assert_eq!(decoded.severity_number, 9);
        assert_eq!(decoded.severity_text, Some(b"INFO".as_slice()));
        assert_eq!(decoded.trace_id, Some(trace_id.as_slice()));
        assert_eq!(decoded.span_id, Some(span_id.as_slice()));
        assert_eq!(decoded.flags, 1);
        match decoded.body {{
            Some(WireAny::String(value)) => assert_eq!(value, b"body"),
            _ => panic!("body should decode as string AnyValue"),
        }}
        assert_eq!(attr_ranges.len(), 1);
        let (start, len) = attr_ranges[0];
        assert_eq!(&log_record[start..start + len], attr.as_slice());
    }}

    #[test]
    fn generated_len_visitors_yield_expected_payloads() {{
        let mut export = Vec::new();
        let mut resource_logs = Vec::new();
        let mut resource = Vec::new();
        let mut scope_logs = Vec::new();

        push_len_field(&mut resource, field_numbers::RESOURCE_ATTRIBUTES, b"attr");
        push_len_field(&mut scope_logs, field_numbers::SCOPE_LOGS_SCOPE, b"scope");
        push_len_field(
            &mut scope_logs,
            field_numbers::SCOPE_LOGS_LOG_RECORDS,
            b"log",
        );
        push_len_field(
            &mut resource_logs,
            field_numbers::RESOURCE_LOGS_RESOURCE,
            &resource,
        );
        push_len_field(
            &mut resource_logs,
            field_numbers::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        push_len_field(
            &mut export,
            field_numbers::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let mut seen_resource_logs = Vec::new();
        for_each_export_resource_logs(&export, |payload| {{
            seen_resource_logs.push(payload.to_vec());
            Ok(())
        }})
        .expect("export visitor should yield resource logs");
        assert_eq!(seen_resource_logs, vec![resource_logs.clone()]);

        let mut seen_resources = Vec::new();
        for_each_resource_logs_resource(&resource_logs, |payload| {{
            seen_resources.push(payload.to_vec());
            Ok(())
        }})
        .expect("resource visitor should yield resource payload");
        assert_eq!(seen_resources, vec![resource.clone()]);

        let mut seen_scope_logs = Vec::new();
        for_each_resource_logs_scope_logs(&resource_logs, |payload| {{
            seen_scope_logs.push(payload.to_vec());
            Ok(())
        }})
        .expect("scope logs visitor should yield scope logs payload");
        assert_eq!(seen_scope_logs, vec![scope_logs.clone()]);

        let mut seen_attrs = Vec::new();
        for_each_resource_attribute(&resource, |payload| {{
            seen_attrs.push(payload.to_vec());
            Ok(())
        }})
        .expect("resource attribute visitor should yield attribute payload");
        assert_eq!(seen_attrs, vec![b"attr".to_vec()]);

        let mut seen_scopes = Vec::new();
        for_each_scope_logs_scope(&scope_logs, |payload| {{
            seen_scopes.push(payload.to_vec());
            Ok(())
        }})
        .expect("scope visitor should yield scope payload");
        assert_eq!(seen_scopes, vec![b"scope".to_vec()]);

        let mut seen_logs = Vec::new();
        for_each_scope_logs_log_record(&scope_logs, |payload| {{
            seen_logs.push(payload.to_vec());
            Ok(())
        }})
        .expect("log visitor should yield log payload");
        assert_eq!(seen_logs, vec![b"log".to_vec()]);
    }}

    #[test]
    fn generated_len_visitors_reject_wrong_wire() {{
        let payload = sample_field_for_wire(
            field_numbers::RESOURCE_LOGS_SCOPE_LOGS,
            WireKind::Varint,
        );
        let err = for_each_resource_logs_scope_logs(&payload, |_| Ok(()))
            .expect_err("wrong wire should be invalid");
        assert!(matches!(err, ProjectionError::Invalid(_)));
    }}

    #[test]
    fn generated_logrecord_table_tracks_projected_protocol_fields() {{
        let fields = fields_for(MessageKind::LogRecord);
        assert!(fields.iter().any(|f| f.name == "body" && f.child == Some(MessageKind::AnyValue)));
        assert!(fields.iter().any(|f| f.name == "attributes" && f.child == Some(MessageKind::KeyValue)));
        assert!(fields.iter().any(|f| f.name == "flags" && f.expected_wire == WireKind::Fixed32));
    }}

    #[test]
    fn generated_projection_field_numbers_match_core_otlp_constants() {{
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
    fn generated_decode_anyvalue_terminal_complex_keeps_last_oneof_value() {{
        let bytes = [10, 2, b'o', b'k', 42, 0];
        let value = decode_any_value_wire(&bytes).expect("terminal oneof should decode");
        assert!(matches!(value, Some(WireAny::ArrayRaw(_))));
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

    #[test]
    fn generated_known_fields_reject_wrong_wire_types() {{
        for &message in all_messages() {{
            for rule in fields_for(message) {{
                for wrong_wire in wrong_wires(rule.expected_wire) {{
                    let payload = sample_field_for_wire(rule.number, wrong_wire);
                    let err = scan_message(&payload, message)
                        .expect_err("known field wrong wire should be invalid");
                    assert!(
                        matches!(err, ProjectionError::Invalid(_)),
                        "message={{message:?}} field={{}} wrong_wire={{wrong_wire:?}} err={{err:?}}",
                        rule.name
                    );
                }}
            }}
        }}
    }}

    #[test]
    fn generated_ignored_fields_accept_expected_wire() {{
        for &message in all_messages() {{
            for rule in fields_for(message) {{
                if rule.action != ProjectionAction::Ignore {{
                    continue;
                }}
                let payload = sample_field_for_wire(rule.number, rule.expected_wire);
                scan_message(&payload, message)
                    .expect("ignored field with expected wire should be accepted");
            }}
        }}
    }}

    #[test]
    fn generated_ignored_fields_reject_wrong_wire() {{
        for &message in all_messages() {{
            for rule in fields_for(message) {{
                if rule.action != ProjectionAction::Ignore {{
                    continue;
                }}
                for wrong_wire in wrong_wires(rule.expected_wire) {{
                    let payload = sample_field_for_wire(rule.number, wrong_wire);
                    let err = scan_message(&payload, message)
                        .expect_err("ignored field wrong wire should be invalid");
                    assert!(
                        matches!(err, ProjectionError::Invalid(_)),
                        "message={{message:?}} field={{}} wrong_wire={{wrong_wire:?}} err={{err:?}}",
                        rule.name
                    );
                }}
            }}
        }}
    }}

    #[test]
    fn generated_anyvalue_complex_shapes_are_projected() {{
        for &field_number in complex_anyvalue_field_numbers() {{
            let payload = sample_field_for_wire(field_number, WireKind::Len);
            scan_message(&payload, MessageKind::AnyValue)
                .expect("complex AnyValue shape should classify for projection");
            let value = decode_any_value_wire(&payload)
                .expect("complex AnyValue should decode");
            assert!(matches!(
                value,
                Some(WireAny::ArrayRaw(_)) | Some(WireAny::KvListRaw(_))
            ));
        }}
    }}

    #[test]
    fn generated_anyvalue_oneof_last_value_wins_for_complex_and_primitive() {{
        let projected = sample_field_for_wire(otlp::ANY_VALUE_STRING_VALUE, WireKind::Len);
        for &field_number in complex_anyvalue_field_numbers() {{
            let complex = sample_field_for_wire(field_number, WireKind::Len);

            let mut complex_then_projected = complex.clone();
            complex_then_projected.extend_from_slice(&projected);
            let value = decode_any_value_wire(&complex_then_projected)
                .expect("terminal projected oneof should decode");
            assert!(matches!(value, Some(WireAny::String(_))));

            let mut projected_then_complex = projected.clone();
            projected_then_complex.extend_from_slice(&complex);
            let value = decode_any_value_wire(&projected_then_complex)
                .expect("terminal complex oneof should decode");
            assert!(matches!(
                value,
                Some(WireAny::ArrayRaw(_)) | Some(WireAny::KvListRaw(_))
            ));
        }}
    }}

    #[test]
    fn generated_security_malformed_wire_corpus_is_rejected() {{
        let malformed_cases: &[&[u8]] = &[
            &[0x0a, 0x02, 0x01],
            &[0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02],
            &[0x53, 0x64],
            &[0x0b, 0x13],
            &[0x0c],
        ];
        for bytes in malformed_cases {{
            let err = classify_projection_support(bytes)
                .expect_err("malformed protobuf bytes must be rejected");
            assert!(matches!(err, ProjectionError::Invalid(_)));
        }}
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
    spec = enrich_spec_from_proto(copy.deepcopy(PROJECTION_SPEC))
    validate_spec(spec)
    rendered = format_rust(render(spec))
    if args.check:
        return check_output(rendered)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
