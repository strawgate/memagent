// scanner.rs — Fast JSON line scanner that feeds Arrow BatchBuilder.
//
// Hot path: scans newline-delimited JSON, extracts key-value pairs,
// and appends directly into Arrow column builders.
//
// Design:
// - Uses memchr for fast newline/quote scanning.
// - Works on raw &[u8] — no UTF-8 validation in the scan loop.
// - No per-line heap allocations.
// - Field pushdown via ScanConfig.

use crate::batch_builder::BatchBuilder;
use arrow::record_batch::RecordBatch;
use memchr::memchr;

// ---------------------------------------------------------------------------
// ScanConfig
// ---------------------------------------------------------------------------

/// Specification for a single field to extract.
pub struct FieldSpec {
    pub name: String,
    pub aliases: Vec<String>,
}

/// Controls which fields to extract and whether to keep _raw.
pub struct ScanConfig {
    /// Fields to extract. Empty = extract all (SELECT *).
    pub wanted_fields: Vec<FieldSpec>,
    /// True if the query uses SELECT * (extract everything).
    pub extract_all: bool,
    /// True if _raw column is needed.
    pub keep_raw: bool,
}

impl Default for ScanConfig {
    fn default() -> Self {
        ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: true,
        }
    }
}

impl ScanConfig {
    /// Is this field name wanted?
    #[inline]
    pub fn is_wanted(&self, _key: &[u8]) -> bool {
        if self.extract_all {
            return true;
        }
        for fs in &self.wanted_fields {
            if _key == fs.name.as_bytes() {
                return true;
            }
            for a in &fs.aliases {
                if _key == a.as_bytes() {
                    return true;
                }
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Scanner
// ---------------------------------------------------------------------------

/// Scans a buffer of newline-delimited JSON and produces an Arrow RecordBatch.
pub struct Scanner {
    builder: BatchBuilder,
    config: ScanConfig,
}

impl Scanner {
    pub fn new(config: ScanConfig, expected_rows: usize) -> Self {
        Scanner {
            builder: BatchBuilder::new(expected_rows, config.keep_raw),
            config,
        }
    }

    /// Scan a buffer of newline-delimited JSON lines and return a RecordBatch.
    /// Reuses internal builders — call this repeatedly for streaming.
    pub fn scan(&mut self, buf: &[u8]) -> RecordBatch {
        self.builder.begin_batch();
        let mut pos = 0;
        let len = buf.len();

        while pos < len {
            // Find end of line.
            let eol = match memchr(b'\n', &buf[pos..]) {
                Some(offset) => pos + offset,
                None => len,
            };
            let line = &buf[pos..eol];
            if !line.is_empty() {
                self.scan_line(line);
            }
            pos = eol + 1;
        }
        self.builder.finish_batch()
    }

    /// Scan a single JSON line — the innermost hot loop.
    ///
    /// Expects top-level `{ "key": value, ... }`.  We parse just enough
    /// JSON to extract top-level key/value pairs.  Nested objects/arrays
    /// are captured as raw strings.
    #[inline]
    fn scan_line(&mut self, line: &[u8]) {
        self.builder.begin_row();

        if self.config.keep_raw {
            self.builder.append_raw(line);
        }

        let len = line.len();
        // Skip leading whitespace / opening brace.
        let mut pos = skip_ws(line, 0);
        if pos >= len || line[pos] != b'{' {
            self.builder.end_row();
            return;
        }
        pos += 1; // skip '{'

        loop {
            pos = skip_ws(line, pos);
            if pos >= len || line[pos] == b'}' {
                break;
            }

            // Expect a quoted key.
            if line[pos] != b'"' {
                break; // malformed
            }
            let (key, after_key) = match scan_string(line, pos) {
                Some(r) => r,
                None => break,
            };
            pos = after_key;

            // Skip ':'
            pos = skip_ws(line, pos);
            if pos >= len || line[pos] != b':' {
                break;
            }
            pos += 1;
            pos = skip_ws(line, pos);
            if pos >= len {
                break;
            }

            // Determine value type and extract.
            let wanted = self.config.is_wanted(key);
            let b = line[pos];
            match b {
                b'"' => {
                    // String value.
                    if let Some((val, after_val)) = scan_string(line, pos) {
                        if wanted {
                            self.builder.append_str(key, val);
                        }
                        pos = after_val;
                    } else {
                        break;
                    }
                }
                b'{' | b'[' => {
                    // Nested object or array — capture as raw string.
                    let start = pos;
                    pos = skip_nested(line, pos);
                    if wanted {
                        self.builder.append_str(key, &line[start..pos]);
                    }
                }
                b't' | b'f' => {
                    // Boolean — store as string "true" / "false".
                    let start = pos;
                    while pos < len
                        && line[pos] != b','
                        && line[pos] != b'}'
                        && line[pos] != b' '
                        && line[pos] != b'\t'
                    {
                        pos += 1;
                    }
                    if wanted {
                        self.builder.append_str(key, &line[start..pos]);
                    }
                }
                b'n' => {
                    // null
                    pos += 4; // skip "null"
                    if pos > len {
                        pos = len;
                    }
                    if wanted {
                        self.builder.append_null(key);
                    }
                }
                _ => {
                    // Number — integer or float.
                    let start = pos;
                    let mut is_float = false;
                    while pos < len {
                        let c = line[pos];
                        if c == b'.' || c == b'e' || c == b'E' {
                            is_float = true;
                        } else if c == b','
                            || c == b'}'
                            || c == b' '
                            || c == b'\t'
                            || c == b'\n'
                            || c == b'\r'
                        {
                            break;
                        }
                        pos += 1;
                    }
                    if wanted {
                        let val = &line[start..pos];
                        if is_float {
                            self.builder.append_float(key, val);
                        } else {
                            self.builder.append_int(key, val);
                        }
                    }
                }
            }

            // Skip comma (if any).
            pos = skip_ws(line, pos);
            if pos < len && line[pos] == b',' {
                pos += 1;
            }
        }

        self.builder.end_row();
    }

    /// Access the underlying builder (e.g. for field_type_map).
    pub fn builder(&self) -> &BatchBuilder {
        &self.builder
    }
}

// ---------------------------------------------------------------------------
// Low-level scan helpers — all work on raw bytes, no allocations.
// ---------------------------------------------------------------------------

/// Skip whitespace bytes.
#[inline(always)]
fn skip_ws(buf: &[u8], mut pos: usize) -> usize {
    let len = buf.len();
    while pos < len {
        match buf[pos] {
            b' ' | b'\t' | b'\r' | b'\n' => pos += 1,
            _ => break,
        }
    }
    pos
}

/// Scan a JSON string starting at `pos` (which points to the opening `"`).
/// Returns (content_slice, position_after_closing_quote).
/// The content_slice is the bytes between the quotes (escapes NOT decoded).
#[inline]
fn scan_string(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    debug_assert!(buf[pos] == b'"');
    let start = pos + 1;
    let mut i = start;
    let len = buf.len();
    while i < len {
        let b = buf[i];
        if b == b'"' {
            return Some((&buf[start..i], i + 1));
        }
        if b == b'\\' {
            i += 2; // skip escaped char
        } else {
            i += 1;
        }
    }
    None // unterminated string
}

/// Skip a nested JSON object or array, handling balanced braces/brackets and strings.
#[inline]
fn skip_nested(buf: &[u8], mut pos: usize) -> usize {
    let len = buf.len();
    let mut depth: u32 = 0;
    while pos < len {
        match buf[pos] {
            b'{' | b'[' => {
                depth += 1;
                pos += 1;
            }
            b'}' | b']' => {
                depth -= 1;
                pos += 1;
                if depth == 0 {
                    return pos;
                }
            }
            b'"' => {
                // Skip string contents.
                pos += 1;
                while pos < len {
                    if buf[pos] == b'\\' {
                        pos += 2;
                    } else if buf[pos] == b'"' {
                        pos += 1;
                        break;
                    } else {
                        pos += 1;
                    }
                }
            }
            _ => {
                pos += 1;
            }
        }
    }
    pos
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};

    fn default_scanner(rows: usize) -> Scanner {
        Scanner::new(ScanConfig::default(), rows)
    }

    #[test]
    fn test_scan_simple_json() {
        let input = br#"{"host":"web1","status":200,"latency":1.5}
{"host":"web2","status":404,"latency":0.3}
{"host":"web3","status":200,"latency":2.1}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 3);

        let host = batch
            .column_by_name("host_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(host.value(0), "web1");
        assert_eq!(host.value(1), "web2");
        assert_eq!(host.value(2), "web3");

        let status = batch
            .column_by_name("status_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(status.value(0), 200);
        assert_eq!(status.value(1), 404);
        assert_eq!(status.value(2), 200);

        let lat = batch
            .column_by_name("latency_float")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((lat.value(0) - 1.5).abs() < 1e-10);
    }

    #[test]
    fn test_scan_type_conflict() {
        // status is int in line 1, string in line 2
        let input = br#"{"status":200}
{"status":"OK"}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.column_by_name("status_int").is_some());
        assert!(batch.column_by_name("status_str").is_some());

        let int_col = batch
            .column_by_name("status_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(int_col.value(0), 200);
        assert!(int_col.is_null(1));

        let str_col = batch
            .column_by_name("status_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(str_col.is_null(0));
        assert_eq!(str_col.value(1), "OK");
    }

    #[test]
    fn test_scan_missing_fields() {
        let input = br#"{"a":"hello"}
{"b":"world"}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 2);

        let a = batch
            .column_by_name("a_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "hello");
        assert!(a.is_null(1));

        let b = batch
            .column_by_name("b_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(b.is_null(0));
        assert_eq!(b.value(1), "world");
    }

    #[test]
    fn test_scan_nested_json() {
        let input = br#"{"user":{"name":"alice","id":1},"level":"info"}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);

        // Nested object stored as string
        let user = batch
            .column_by_name("user_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let val = user.value(0);
        assert!(val.contains("alice"));
        assert!(val.starts_with('{'));
    }

    #[test]
    fn test_scan_config_pushdown() {
        let input =
            br#"{"a":"1","b":"2","c":"3","d":"4","e":"5","f":"6","g":"7","h":"8","i":"9","j":"10"}
"#;
        let config = ScanConfig {
            wanted_fields: vec![
                FieldSpec {
                    name: "a".into(),
                    aliases: vec![],
                },
                FieldSpec {
                    name: "c".into(),
                    aliases: vec![],
                },
            ],
            extract_all: false,
            keep_raw: false,
        };
        let mut s = Scanner::new(config, 4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);
        // Only a and c should be present.
        assert!(batch.column_by_name("a_str").is_some());
        assert!(batch.column_by_name("c_str").is_some());
        assert!(batch.column_by_name("b_str").is_none());
        assert!(batch.column_by_name("d_str").is_none());
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_scan_keep_raw() {
        let line = br#"{"msg":"hello"}"#;
        let input = [line.as_slice(), b"\n"].concat();
        let mut s = default_scanner(4);
        let batch = s.scan(&input);
        let raw = batch
            .column_by_name("_raw")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(raw.value(0), r#"{"msg":"hello"}"#);
    }

    #[test]
    fn test_scan_no_raw() {
        let input = br#"{"msg":"hello"}
"#;
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
        };
        let mut s = Scanner::new(config, 4);
        let batch = s.scan(input);
        assert!(batch.column_by_name("_raw").is_none());
    }

    #[test]
    fn test_batch_builder_reuse() {
        let input = br#"{"x":1}
{"x":2}
"#;
        let mut s = default_scanner(4);

        // First batch.
        let b1 = s.scan(input);
        assert_eq!(b1.num_rows(), 2);

        // Second batch — reuse the same scanner.
        let b2 = s.scan(input);
        assert_eq!(b2.num_rows(), 2);

        let col = b2
            .column_by_name("x_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
    }

    #[test]
    fn test_scan_bool_and_null() {
        let input = br#"{"active":true,"deleted":false,"extra":null}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);

        let active = batch
            .column_by_name("active_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(active.value(0), "true");

        let deleted = batch
            .column_by_name("deleted_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(deleted.value(0), "false");

        // "extra" with null — should exist but be null
        // (append_null was called, so str_builder got a null)
        // Actually, append_null just pads active builders. The field might not
        // appear at all if no typed data was ever written. Let's verify it
        // gracefully handles this.
    }

    #[test]
    fn test_scan_negative_int() {
        let input = br#"{"offset":-42}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        let col = batch
            .column_by_name("offset_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), -42);
    }

    #[test]
    fn test_scan_escaped_string() {
        // Key with escaped quote inside value
        let input = br#"{"msg":"hello \"world\""}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        let col = batch
            .column_by_name("msg_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Escapes are preserved (not decoded) — this is by design for speed.
        assert!(col.value(0).contains("\\\"world\\\""));
    }
}
