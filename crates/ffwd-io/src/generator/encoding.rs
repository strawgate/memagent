// JSON encoding helpers for static attributes in record profile.

fn write_json_u64_field(out: &mut Vec<u8>, key: &str, value: u64, first: &mut bool) {
    if !*first {
        out.push(b',');
    }
    *first = false;
    write_json_key(out, key);
    let _ = write!(out, ":{value}");
}

fn write_json_u128_field(out: &mut Vec<u8>, key: &str, value: u128, first: &mut bool) {
    if !*first {
        out.push(b',');
    }
    *first = false;
    write_json_key(out, key);
    let _ = write!(out, ":{value}");
}

fn write_json_key(out: &mut Vec<u8>, key: &str) {
    write_json_quoted_string(out, key);
}

fn write_json_quoted_string(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    write_json_escaped_string_contents(out, value);
    out.push(b'"');
}

fn write_json_escaped_string_contents(out: &mut Vec<u8>, value: &str) {
    for ch in value.chars() {
        match ch {
            '"' => out.extend_from_slice(br#"\""#),
            '\\' => out.extend_from_slice(br"\\"),
            '\n' => out.extend_from_slice(br"\n"),
            '\r' => out.extend_from_slice(br"\r"),
            '\t' => out.extend_from_slice(br"\t"),
            c if c <= '\u{1F}' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
}

fn encode_static_field(key: &str, value: &GeneratorAttributeValue) -> Vec<u8> {
    let mut out = Vec::new();
    write_json_key(&mut out, key);
    out.push(b':');
    match value {
        GeneratorAttributeValue::String(value) => {
            out.push(b'"');
            write_json_escaped_string_contents(&mut out, value);
            out.push(b'"');
        }
        GeneratorAttributeValue::Integer(value) => {
            let _ = write!(&mut out, "{value}");
        }
        GeneratorAttributeValue::Float(value) => {
            if value.is_finite() {
                let rendered = serde_json::to_string(value).expect("finite float serializes");
                out.extend_from_slice(rendered.as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        GeneratorAttributeValue::Bool(value) => {
            out.extend_from_slice(if *value { b"true" } else { b"false" });
        }
        GeneratorAttributeValue::Null => {
            out.extend_from_slice(b"null");
        }
    }
    out
}
