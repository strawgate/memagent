"""Small proto metadata parser shared by OTLP code generators."""

from __future__ import annotations

import re


FIELD_RE = re.compile(
    r"^\s*(?:(repeated)\s+)?([.\w]+)\s+([A-Za-z_]\w*)\s*=\s*(\d+)\s*(?:\[[^\]]*\])?\s*;"
)
MESSAGE_RE = re.compile(r"^\s*message\s+([A-Za-z_]\w*)\s*\{")
ENUM_RE = re.compile(r"^\s*enum\s+([A-Za-z_]\w*)\s*\{")
ONEOF_RE = re.compile(r"^\s*oneof\s+([A-Za-z_]\w*)\s*\{")
PROTO_VERSION_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")

PROTO_TYPE_TO_WIRE = {
    "bool": "varint",
    "int32": "varint",
    "int64": "varint",
    "uint32": "varint",
    "uint64": "varint",
    "sint32": "varint",
    "sint64": "varint",
    "fixed32": "fixed32",
    "sfixed32": "fixed32",
    "fixed64": "fixed64",
    "sfixed64": "fixed64",
    "float": "fixed32",
    "double": "fixed64",
    "string": "len",
    "bytes": "len",
    "SeverityNumber": "varint",
}


def short_proto_type(proto_type: str) -> str:
    return proto_type.rsplit(".", 1)[-1]


def proto_version_key(path) -> tuple:
    match = PROTO_VERSION_RE.match(path.name)
    if match is None:
        return (-1, -1, -1)
    return tuple(int(part) for part in match.groups())


def vendored_proto_root(base_path):
    roots = [path for path in base_path.iterdir() if path.is_dir() and proto_version_key(path)[0] >= 0]
    if not roots:
        raise FileNotFoundError(f"no vendored OTLP proto versions found under: {base_path}")
    return max(roots, key=proto_version_key)


def strip_quoted_text(line: str) -> str:
    out = []
    in_string = False
    escaped = False
    for char in line:
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            out.append(" ")
        elif char == '"':
            in_string = True
            out.append(" ")
        else:
            out.append(char)
    return "".join(out)


def brace_delta(line: str) -> int:
    return line.count("{") - line.count("}")


def parse_proto_symbols(proto_files: list) -> tuple:
    message_types = set()
    enum_types = set()
    for path in proto_files:
        if not path.exists():
            raise FileNotFoundError(f"vendored OTLP proto file missing: {path}")
        for raw_line in path.read_text().splitlines():
            line = strip_quoted_text(raw_line.split("//", 1)[0]).strip()
            if not line:
                continue
            message_match = MESSAGE_RE.match(line)
            if message_match:
                message_types.add(message_match.group(1))
                continue
            enum_match = ENUM_RE.match(line)
            if enum_match:
                enum_types.add(enum_match.group(1))
    return message_types, enum_types


def proto_wire_for(proto_type: str, message_types: set, enum_types: set) -> str:
    short_type = short_proto_type(proto_type)
    if short_type in PROTO_TYPE_TO_WIRE:
        return PROTO_TYPE_TO_WIRE[short_type]
    if short_type in enum_types:
        return "varint"
    if short_type in message_types:
        return "len"
    raise ValueError(f"unknown proto type for wire inference: {proto_type}")


def parse_proto_fields(proto_files: list) -> dict:
    message_types, enum_types = parse_proto_symbols(proto_files)
    messages = {}
    for path in proto_files:
        if not path.exists():
            raise FileNotFoundError(f"vendored OTLP proto file missing: {path}")

        current_message = None
        in_oneof = None
        message_depth = 0
        oneof_depth = 0
        for raw_line in path.read_text().splitlines():
            line = strip_quoted_text(raw_line.split("//", 1)[0]).strip()
            if not line:
                continue

            if current_message is None:
                message_match = MESSAGE_RE.match(line)
                if message_match:
                    current_message = message_match.group(1)
                    message_depth = brace_delta(line)
                    messages.setdefault(current_message, {})
                continue

            oneof_match = ONEOF_RE.match(line)
            if oneof_match:
                in_oneof = oneof_match.group(1)
                oneof_depth = brace_delta(line)
                message_depth += brace_delta(line)
                continue

            field_match = FIELD_RE.match(line)
            if field_match:
                repeated, proto_type, field_name, number = field_match.groups()
                messages[current_message][field_name] = {
                    "name": field_name,
                    "number": int(number),
                    "wire": proto_wire_for(proto_type, message_types, enum_types),
                    "proto_type": short_proto_type(proto_type),
                    "repeated": repeated is not None,
                    "oneof": in_oneof,
                }
                continue

            delta = brace_delta(line)
            message_depth += delta
            if in_oneof is not None:
                oneof_depth += delta
                if oneof_depth <= 0:
                    in_oneof = None
                    oneof_depth = 0
            if message_depth <= 0:
                current_message = None
                message_depth = 0

    return messages
