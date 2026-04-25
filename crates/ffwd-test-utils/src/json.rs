//! Proptest strategies for generating valid JSON data.
//!
//! These strategies produce syntactically valid JSON strings, values, objects,
//! and NDJSON buffers suitable for property-based testing of the scanner,
//! builders, and output encoders.

use proptest::prelude::*;

/// Generate a safe JSON string value (ASCII, with optional escapes).
pub fn arb_json_string() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            // Plain ASCII (most common)
            "[a-zA-Z0-9_ /:.-]".prop_map(|s| s),
            // Escape sequences
            Just("\\\"".to_string()),
            Just("\\\\".to_string()),
            Just("\\n".to_string()),
            Just("\\t".to_string()),
            Just("\\r".to_string()),
            Just("\\/".to_string()),
            Just("\\b".to_string()),
            Just("\\f".to_string()),
            // Unicode escape sequences (\uXXXX) — BMP code points
            (0x0020u16..0xD800).prop_map(|cp| format!("\\u{cp:04X}")),
        ],
        0..20,
    )
    .prop_map(|parts| parts.join(""))
}

/// Generate a simple JSON value (no nested objects — avoids recursive types).
pub fn arb_json_value_simple() -> impl Strategy<Value = String> {
    prop_oneof![
        40 => arb_json_string().prop_map(|s| format!("\"{s}\"")),
        20 => (-1_000_000i64..1_000_000).prop_map(|n| n.to_string()),
        10 => (-1.0e6f64..1.0e6).prop_filter("finite", |f| f.is_finite())
            .prop_map(|f| format!("{f}")),
        10 => prop::bool::ANY.prop_map(|b| b.to_string()),
        5 => Just("null".to_string()),
        15 => prop::collection::vec(
            prop_oneof![
                arb_json_string().prop_map(|s| format!("\"{s}\"")),
                (-100i64..100).prop_map(|n| n.to_string()),
                Just("null".to_string()),
            ],
            0..5,
        ).prop_map(|elems| format!("[{}]", elems.join(","))),
    ]
}

/// Generate a flat JSON object with N fields.
pub fn arb_flat_object(
    field_count: impl Into<prop::collection::SizeRange>,
) -> impl Strategy<Value = String> {
    prop::collection::vec(
        ("[a-zA-Z_][a-zA-Z0-9_]{0,12}", arb_json_value_simple()),
        field_count,
    )
    .prop_map(|fields| {
        let pairs: Vec<String> = fields
            .into_iter()
            .map(|(k, v)| format!("\"{k}\":{v}"))
            .collect();
        format!("{{{}}}", pairs.join(","))
    })
}

/// Generate a multi-line NDJSON buffer.
pub fn arb_ndjson_buffer() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(arb_flat_object(1..15), 1..20).prop_map(|lines| {
        let mut buf = Vec::new();
        for line in lines {
            buf.extend_from_slice(line.as_bytes());
            buf.push(b'\n');
        }
        buf
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::test_runner::{Config, TestRunner};

    #[test]
    fn arb_json_string_produces_valid_content() {
        let mut runner = TestRunner::new(Config::with_cases(64));
        runner
            .run(&arb_json_string(), |s| {
                // The string should not contain unescaped control chars
                // (our generator only produces printable ASCII and valid escapes)
                for ch in s.chars() {
                    prop_assert!(ch.is_ascii_graphic() || ch == ' ' || ch == '\\');
                }
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn arb_flat_object_produces_valid_json_braces() {
        let mut runner = TestRunner::new(Config::with_cases(64));
        runner
            .run(&arb_flat_object(1..10), |obj| {
                let starts_ok = obj.starts_with('{');
                let ends_ok = obj.ends_with('}');
                prop_assert!(starts_ok, "expected object to start with open brace");
                prop_assert!(ends_ok, "expected object to end with close brace");
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn arb_ndjson_buffer_ends_with_newline() {
        let mut runner = TestRunner::new(Config::with_cases(32));
        runner
            .run(&arb_ndjson_buffer(), |buf| {
                prop_assert!(!buf.is_empty());
                prop_assert_eq!(*buf.last().unwrap(), b'\n');
                Ok(())
            })
            .unwrap();
    }
}
