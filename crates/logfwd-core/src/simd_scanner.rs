// simd_scanner.rs — Chunk-level SIMD JSON-to-Arrow scanner.
//
// Stage 1: Classify the entire NDJSON buffer in one SIMD pass (ChunkIndex).
//          Produces pre-computed bitmasks of quote and string positions.
//          Runs at ~21 GiB/s — essentially free.
//
// Stage 2: Walk the buffer line-by-line. For each line, extract top-level
//          key-value pairs using the pre-computed index. String scanning
//          is O(1) bit-scan instead of per-byte comparison.
//
// No DOM. No tape. No intermediate representation. Direct to Arrow.

use crate::batch_builder::{BatchBuilder, parse_int_fast};
use crate::chunk_classify::ChunkIndex;
use crate::scanner::ScanConfig;
use arrow::record_batch::RecordBatch;
use memchr::memchr;

/// Chunk-level SIMD scanner — drop-in replacement for `Scanner`.
pub struct SimdScanner {
    builder: BatchBuilder,
    config: ScanConfig,
}

impl SimdScanner {
    pub fn new(config: ScanConfig, expected_rows: usize) -> Self {
        SimdScanner {
            builder: BatchBuilder::new(expected_rows, config.keep_raw),
            config,
        }
    }

    /// Scan a buffer of newline-delimited JSON lines and return a RecordBatch.
    ///
    /// 1. One SIMD pass classifies the entire buffer (~21 GiB/s)
    /// 2. Per-line extraction uses pre-computed bitmasks for O(1) string scanning
    pub fn scan(&mut self, buf: &[u8]) -> RecordBatch {
        // Stage 1: Classify entire buffer
        let index = ChunkIndex::new(buf);

        // Stage 2: Walk lines
        self.builder.begin_batch();
        let mut pos = 0;
        let len = buf.len();

        while pos < len {
            let eol = match memchr(b'\n', &buf[pos..]) {
                Some(offset) => pos + offset,
                None => len,
            };
            let line_start = pos;
            let line_end = eol;
            if line_start < line_end {
                self.scan_line(buf, line_start, line_end, &index);
            }
            pos = eol + 1;
        }
        self.builder.finish_batch()
    }

    /// Scan a single JSON line using the pre-computed chunk index.
    /// `buf` is the full buffer, `start..end` is the line within it.
    #[inline]
    fn scan_line(&mut self, buf: &[u8], start: usize, end: usize, index: &ChunkIndex) {
        self.builder.begin_row();

        if self.config.keep_raw {
            self.builder.append_raw(&buf[start..end]);
        }

        let mut pos = skip_ws(buf, start, end);
        if pos >= end || buf[pos] != b'{' {
            self.builder.end_row();
            return;
        }
        pos += 1;

        loop {
            pos = skip_ws(buf, pos, end);
            if pos >= end || buf[pos] == b'}' {
                break;
            }

            // --- Key ---
            if buf[pos] != b'"' {
                break;
            }
            let (key, after_key) = match index.scan_string(buf, pos) {
                Some(r) => r,
                None => break,
            };
            pos = after_key;

            // --- Colon ---
            pos = skip_ws(buf, pos, end);
            if pos >= end || buf[pos] != b':' {
                break;
            }
            pos += 1;
            pos = skip_ws(buf, pos, end);
            if pos >= end {
                break;
            }

            // --- Value ---
            let wanted = self.config.is_wanted(key);

            let b = buf[pos];
            match b {
                b'"' => {
                    let (val, after) = match index.scan_string(buf, pos) {
                        Some(r) => r,
                        None => break,
                    };
                    if wanted {
                        self.builder.append_str(key, val);
                    }
                    pos = after;
                }
                b'{' | b'[' => {
                    let val_start = pos;
                    pos = index.skip_nested(buf, pos).min(end);
                    if wanted {
                        self.builder.append_str(key, &buf[val_start..pos]);
                    }
                }
                b't' | b'f' => {
                    let val_start = pos;
                    while pos < end
                        && buf[pos] != b','
                        && buf[pos] != b'}'
                        && buf[pos] != b' '
                        && buf[pos] != b'\t'
                    {
                        pos += 1;
                    }
                    if wanted {
                        self.builder.append_str(key, &buf[val_start..pos]);
                    }
                }
                b'n' => {
                    pos += 4;
                    if pos > end {
                        pos = end;
                    }
                    if wanted {
                        self.builder.append_null(key);
                    }
                }
                _ => {
                    // Number
                    let val_start = pos;
                    let mut is_float = false;
                    while pos < end {
                        let c = buf[pos];
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
                        let val = &buf[val_start..pos];
                        if is_float {
                            self.builder.append_float(key, val);
                        } else if parse_int_fast(val).is_some() {
                            self.builder.append_int(key, val);
                        } else {
                            self.builder.append_float(key, val);
                        }
                    }
                }
            }

            // --- Comma ---
            pos = skip_ws(buf, pos, end);
            if pos < end && buf[pos] == b',' {
                pos += 1;
            }
        }

        self.builder.end_row();
    }

    pub fn builder(&self) -> &BatchBuilder {
        &self.builder
    }
}

#[inline(always)]
fn skip_ws(buf: &[u8], mut pos: usize, end: usize) -> usize {
    while pos < end {
        match buf[pos] {
            b' ' | b'\t' | b'\r' | b'\n' => pos += 1,
            _ => break,
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
    use crate::scanner::FieldSpec;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};

    fn default_scanner(rows: usize) -> SimdScanner {
        SimdScanner::new(ScanConfig::default(), rows)
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
        let input = br#"{"status":200}
{"status":"OK"}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.column_by_name("status_int").is_some());
        assert!(batch.column_by_name("status_str").is_some());
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
    }

    #[test]
    fn test_scan_nested_json() {
        let input = br#"{"user":{"name":"alice","id":1},"level":"info"}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);
        let user = batch
            .column_by_name("user_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let val = user.value(0);
        assert!(val.contains("alice"), "got: {val}");
        assert!(val.starts_with('{'), "got: {val}");
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
        let mut s = SimdScanner::new(config, 4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("a_str").is_some());
        assert!(batch.column_by_name("c_str").is_some());
        assert!(batch.column_by_name("b_str").is_none());
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
        let mut s = SimdScanner::new(config, 4);
        let batch = s.scan(input);
        assert!(batch.column_by_name("_raw").is_none());
    }

    #[test]
    fn test_batch_reuse() {
        let input = br#"{"x":1}
{"x":2}
"#;
        let mut s = default_scanner(4);
        let _b1 = s.scan(input);
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
        // Escapes preserved as raw bytes — value contains literal backslash + quote.
        let v = col.value(0);
        assert!(v.contains("world"), "got: {v}");
    }

    #[test]
    fn test_scan_large_batch() {
        let mut input = Vec::with_capacity(100 * 1024);
        for i in 0..1000 {
            let line = format!(
                r#"{{"level":"INFO","msg":"request handled path=/api/v1/users/{}","status":{},"duration_ms":{:.2}}}"#,
                10000 + i,
                if i % 10 == 0 { 500 } else { 200 },
                0.5 + (i as f64) * 0.1
            );
            input.extend_from_slice(line.as_bytes());
            input.push(b'\n');
        }
        let mut s = default_scanner(1024);
        let batch = s.scan(&input);
        assert_eq!(batch.num_rows(), 1000);
        let status = batch
            .column_by_name("status_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(status.value(0), 500);
        assert_eq!(status.value(1), 200);
    }

    #[test]
    fn test_duplicate_keys_no_panic() {
        let input = br#"{"a":1,"a":2}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);
        let col = batch
            .column_by_name("a_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
    }

    #[test]
    fn test_i64_overflow_falls_to_float() {
        let input = br#"{"big":99999999999999999999}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("big_float").is_some());
    }

    #[test]
    fn test_array_value() {
        let input = br#"{"tags":["a","b","c"],"n":1}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        let tags = batch
            .column_by_name("tags_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tags.value(0), r#"["a","b","c"]"#);
    }

    #[test]
    fn test_empty_object() {
        let input = b"{}\n";
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_braces_in_nested_string() {
        let input = br#"{"data":{"msg":"has } and { inside"},"ok":true}
"#;
        let mut s = default_scanner(4);
        let batch = s.scan(input);
        assert_eq!(batch.num_rows(), 1);
        let ok = batch
            .column_by_name("ok_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ok.value(0), "true");
    }
}
