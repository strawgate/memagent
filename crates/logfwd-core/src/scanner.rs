// simd_scanner.rs — Chunk-level SIMD JSON-to-Arrow scanner.
//
// Two scanner variants sharing a generic scan loop:
//   SimdScanner          — StorageBuilder (self-contained, compressible)
//   StreamingSimdScanner — StreamingBuilder (zero-copy StringViewArrays)

use std::sync::Arc;

use crate::chunk_classify::ChunkIndex;
use crate::scan_config::ScanConfig;
use crate::scan_config::parse_int_fast;
use crate::storage_builder::StorageBuilder;
use crate::streaming_builder::StreamingBuilder;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use memchr::memchr;

// ---------------------------------------------------------------------------
// ScanBuilder trait — shared interface for both builders
// ---------------------------------------------------------------------------

pub(crate) trait ScanBuilder {
    fn begin_batch(&mut self);
    fn begin_row(&mut self);
    fn end_row(&mut self);
    fn resolve_field(&mut self, key: &[u8]) -> usize;
    fn append_str_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_int_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_float_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_null_by_idx(&mut self, idx: usize);
    fn append_raw(&mut self, line: &[u8]);
}

impl ScanBuilder for StorageBuilder {
    #[inline(always)]
    fn begin_batch(&mut self) {
        self.begin_batch();
    }
    #[inline(always)]
    fn begin_row(&mut self) {
        self.begin_row();
    }
    #[inline(always)]
    fn end_row(&mut self) {
        self.end_row();
    }
    #[inline(always)]
    fn resolve_field(&mut self, key: &[u8]) -> usize {
        self.resolve_field(key)
    }
    #[inline(always)]
    fn append_str_by_idx(&mut self, idx: usize, v: &[u8]) {
        self.append_str_by_idx(idx, v);
    }
    #[inline(always)]
    fn append_int_by_idx(&mut self, idx: usize, v: &[u8]) {
        self.append_int_by_idx(idx, v);
    }
    #[inline(always)]
    fn append_float_by_idx(&mut self, idx: usize, v: &[u8]) {
        self.append_float_by_idx(idx, v);
    }
    #[inline(always)]
    fn append_null_by_idx(&mut self, idx: usize) {
        self.append_null_by_idx(idx);
    }
    #[inline(always)]
    fn append_raw(&mut self, line: &[u8]) {
        self.append_raw(line);
    }
}

impl ScanBuilder for StreamingBuilder {
    #[inline(always)]
    fn begin_batch(&mut self) { /* no-op: begin_batch(Bytes) called by StreamingSimdScanner */
    }
    #[inline(always)]
    fn begin_row(&mut self) {
        self.begin_row();
    }
    #[inline(always)]
    fn end_row(&mut self) {
        self.end_row();
    }
    #[inline(always)]
    fn resolve_field(&mut self, key: &[u8]) -> usize {
        self.resolve_field(key)
    }
    #[inline(always)]
    fn append_str_by_idx(&mut self, idx: usize, v: &[u8]) {
        self.append_str_by_idx(idx, v);
    }
    #[inline(always)]
    fn append_int_by_idx(&mut self, idx: usize, v: &[u8]) {
        self.append_int_by_idx(idx, v);
    }
    #[inline(always)]
    fn append_float_by_idx(&mut self, idx: usize, v: &[u8]) {
        self.append_float_by_idx(idx, v);
    }
    #[inline(always)]
    fn append_null_by_idx(&mut self, idx: usize) {
        self.append_null_by_idx(idx);
    }
    #[inline(always)]
    fn append_raw(&mut self, _line: &[u8]) { /* not supported */
    }
}

// ---------------------------------------------------------------------------
// Generic scan loop
// ---------------------------------------------------------------------------

#[inline(never)]
fn scan_into<B: ScanBuilder>(buf: &[u8], config: &ScanConfig, builder: &mut B) {
    debug_assert!(
        std::str::from_utf8(buf).is_ok(),
        "Scanner input must be valid UTF-8"
    );
    let index = ChunkIndex::new(buf);
    builder.begin_batch();
    let mut pos = 0;
    let len = buf.len();
    while pos < len {
        let eol = match memchr(b'\n', &buf[pos..]) {
            Some(o) => pos + o,
            None => len,
        };
        if pos < eol {
            scan_line(buf, pos, eol, &index, config, builder);
        }
        pos = eol + 1;
    }
}

#[inline]
fn scan_line<B: ScanBuilder>(
    buf: &[u8],
    start: usize,
    end: usize,
    index: &ChunkIndex,
    config: &ScanConfig,
    builder: &mut B,
) {
    builder.begin_row();
    if config.keep_raw {
        builder.append_raw(&buf[start..end]);
    }

    let mut pos = skip_ws(buf, start, end);
    if pos >= end || buf[pos] != b'{' {
        builder.end_row();
        return;
    }
    pos += 1;

    loop {
        pos = skip_ws(buf, pos, end);
        if pos >= end || buf[pos] == b'}' {
            break;
        }
        if buf[pos] != b'"' {
            break;
        }
        let (key, after_key) = match index.scan_string(buf, pos) {
            Some(r) => r,
            None => break,
        };
        pos = after_key;
        pos = skip_ws(buf, pos, end);
        if pos >= end || buf[pos] != b':' {
            break;
        }
        pos += 1;
        pos = skip_ws(buf, pos, end);
        if pos >= end {
            break;
        }

        let wanted = config.is_wanted(key);
        match buf[pos] {
            b'"' => {
                let (val, after) = match index.scan_string(buf, pos) {
                    Some(r) => r,
                    None => break,
                };
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, val);
                }
                pos = after;
            }
            b'{' | b'[' => {
                let s = pos;
                pos = index.skip_nested(buf, pos).min(end);
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, &buf[s..pos]);
                }
            }
            b't' | b'f' => {
                let s = pos;
                while pos < end
                    && buf[pos] != b','
                    && buf[pos] != b'}'
                    && buf[pos] != b' '
                    && buf[pos] != b'\t'
                    && buf[pos] != b'\r'
                {
                    pos += 1;
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, &buf[s..pos]);
                }
            }
            b'n' => {
                // Scan past the null/identifier token to the next delimiter.
                while pos < end
                    && buf[pos] != b','
                    && buf[pos] != b'}'
                    && buf[pos] != b' '
                    && buf[pos] != b'\t'
                    && buf[pos] != b'\r'
                {
                    pos += 1;
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_null_by_idx(idx);
                }
            }
            _ => {
                let s = pos;
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
                    let idx = builder.resolve_field(key);
                    let val = &buf[s..pos];
                    if is_float {
                        builder.append_float_by_idx(idx, val);
                    } else if parse_int_fast(val).is_some() {
                        builder.append_int_by_idx(idx, val);
                    } else {
                        builder.append_float_by_idx(idx, val);
                    }
                }
            }
        }
        pos = skip_ws(buf, pos, end);
        if pos < end && buf[pos] == b',' {
            pos += 1;
        }
    }
    builder.end_row();
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
// SimdScanner — StorageBuilder (self-contained, compressible)
// ---------------------------------------------------------------------------

/// SIMD scanner producing self-contained `RecordBatch` via `StorageBuilder`.
///
/// Output owns all its data — the input buffer can be freed after `scan()`.
/// Suitable for: pipeline hot path, or compress → disk.
pub struct SimdScanner {
    builder: StorageBuilder,
    config: ScanConfig,
}

impl SimdScanner {
    pub fn new(config: ScanConfig) -> Self {
        SimdScanner {
            builder: StorageBuilder::new(config.keep_raw),
            config,
        }
    }
    pub fn scan(&mut self, buf: &[u8]) -> Result<RecordBatch, ArrowError> {
        if self.config.validate_utf8 {
            std::str::from_utf8(buf).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "SimdScanner: input is not valid UTF-8: {e}"
                ))
            })?;
        }
        scan_into(buf, &self.config, &mut self.builder);
        let batch = self.builder.finish_batch()?;
        inject_resource_columns(batch, &self.config.resource_columns)
    }
}

// ---------------------------------------------------------------------------
// StreamingSimdScanner — StreamingBuilder (zero-copy hot path)
// ---------------------------------------------------------------------------

/// SIMD scanner producing zero-copy `RecordBatch` via `StreamingBuilder`.
///
/// String values are `StringViewArray` views into the reference-counted input
/// buffer (`bytes::Bytes`). 20% faster scan, no string copies.
/// The input buffer must stay alive while the `RecordBatch` is in use.
pub struct StreamingSimdScanner {
    builder: StreamingBuilder,
    config: ScanConfig,
}

impl StreamingSimdScanner {
    pub fn new(config: ScanConfig) -> Self {
        StreamingSimdScanner {
            builder: StreamingBuilder::new(),
            config,
        }
    }
    pub fn scan(&mut self, buf: bytes::Bytes) -> Result<RecordBatch, ArrowError> {
        if self.config.validate_utf8 {
            std::str::from_utf8(&buf).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "StreamingSimdScanner: input is not valid UTF-8: {e}"
                ))
            })?;
        }
        self.builder.begin_batch(buf.clone());
        scan_into(&buf, &self.config, &mut self.builder);
        let batch = self.builder.finish_batch()?;
        inject_resource_columns(batch, &self.config.resource_columns)
    }
}

// ---------------------------------------------------------------------------
// Resource column injection
// ---------------------------------------------------------------------------

/// Append constant `_resource_*` columns to a `RecordBatch`.
///
/// Each entry in `resource_columns` produces a non-nullable `Utf8` column
/// where every row holds the same value. Columns are appended after all
/// scanned fields, preserving their original order.
///
/// This is the injection point described in the architecture: source identity
/// (K8s pod name, namespace, service name, …) becomes a first-class column
/// that SQL transforms can filter/group on and output sinks can use for
/// resource grouping (e.g. OTLP `ResourceLogs`).
pub fn inject_resource_columns(
    batch: RecordBatch,
    resource_columns: &[(String, String)],
) -> Result<RecordBatch, ArrowError> {
    if resource_columns.is_empty() {
        return Ok(batch);
    }
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    for (name, value) in resource_columns {
        // Constant column: every row holds the same value.
        let arr = StringArray::from(vec![value.as_str(); num_rows]);
        fields.push(Field::new(name, DataType::Utf8, false));
        columns.push(Arc::new(arr) as ArrayRef);
    }

    let new_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(new_schema, columns)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan_config::FieldSpec;
    use arrow::array::{Array, Int64Array, StringArray};

    fn default_scanner(_rows: usize) -> SimdScanner {
        SimdScanner::new(ScanConfig::default())
    }

    #[test]
    fn test_simple() {
        let input = b"{\"host\":\"web1\",\"status\":200,\"lat\":1.5}\n{\"host\":\"web2\",\"status\":404,\"lat\":0.3}\n";
        let batch = default_scanner(4).scan(input).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column_by_name("host_str")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "web1"
        );
        assert_eq!(
            batch
                .column_by_name("status_int")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            404
        );
    }
    #[test]
    fn test_type_conflict() {
        let batch = default_scanner(4).scan(b"{\"s\":200}\n{\"s\":\"OK\"}\n").unwrap();
        assert!(batch.column_by_name("s_int").is_some());
        assert!(batch.column_by_name("s_str").is_some());
    }
    #[test]
    fn test_missing_fields() {
        let batch = default_scanner(4).scan(b"{\"a\":\"hello\"}\n{\"b\":\"world\"}\n").unwrap();
        assert_eq!(batch.num_rows(), 2);
        let a = batch.column_by_name("a_str").unwrap();
        assert!(!a.is_null(0));
        assert!(a.is_null(1));
    }
    #[test]
    fn test_nested() {
        let batch = default_scanner(4)
            .scan(
                br#"{"u":{"name":"alice"},"level":"info"}
"#,
            )
            .unwrap();
        assert!(
            batch
                .column_by_name("u_str")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .contains("alice")
        );
    }
    #[test]
    fn test_pushdown() {
        let config = ScanConfig {
            wanted_fields: vec![FieldSpec {
                name: "a".into(),
                aliases: vec![],
            }],
            extract_all: false,
            keep_raw: false,
            validate_utf8: false,
            resource_columns: vec![],
        };
        let batch = SimdScanner::new(config)
            .scan(
                br#"{"a":"1","b":"2","c":"3"}
"#,
            )
            .unwrap();
        assert!(batch.column_by_name("a_str").is_some());
        assert!(batch.column_by_name("b_str").is_none());
    }
    #[test]
    fn test_keep_raw() {
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: true,
            validate_utf8: false,
            resource_columns: vec![],
        };
        let batch = SimdScanner::new(config).scan(b"{\"msg\":\"hi\"}\n").unwrap();
        assert!(batch.column_by_name("_raw").is_some());
    }
    #[test]
    fn test_batch_reuse() {
        let mut s = default_scanner(4);
        let _ = s.scan(b"{\"x\":1}\n").unwrap();
        let b = s.scan(b"{\"x\":2}\n").unwrap();
        assert_eq!(b.num_rows(), 1);
    }
    #[test]
    fn test_bool_null() {
        let batch = default_scanner(4).scan(b"{\"a\":true,\"b\":false,\"c\":null}\n").unwrap();
        assert_eq!(
            batch
                .column_by_name("a_str")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "true"
        );
    }
    #[test]
    fn test_duplicate_keys() {
        let batch = default_scanner(4).scan(b"{\"a\":1,\"a\":2}\n").unwrap();
        assert_eq!(
            batch
                .column_by_name("a_int")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
    }
    #[test]
    fn test_i64_overflow() {
        let batch = default_scanner(4).scan(b"{\"big\":99999999999999999999}\n").unwrap();
        assert!(batch.column_by_name("big_float").is_some());
    }
    #[test]
    fn test_empty_object() {
        assert_eq!(default_scanner(4).scan(b"{}\n").unwrap().num_rows(), 1);
    }
    #[test]
    fn test_array_value() {
        let batch = default_scanner(4)
            .scan(
                br#"{"tags":["a","b"],"n":1}
"#,
            )
            .unwrap();
        assert_eq!(
            batch
                .column_by_name("tags_str")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            r#"["a","b"]"#
        );
    }
    #[test]
    fn test_braces_in_string() {
        let batch = default_scanner(4)
            .scan(
                br#"{"d":{"m":"has } and {"},"ok":true}
"#,
            )
            .unwrap();
        assert_eq!(
            batch
                .column_by_name("ok_str")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "true"
        );
    }
    #[test]
    fn test_escaped_string() {
        let batch = default_scanner(4)
            .scan(
                br#"{"msg":"hello \"world\""}
"#,
            )
            .unwrap();
        assert!(
            batch
                .column_by_name("msg_str")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .contains("world")
        );
    }
    #[test]
    fn test_large_batch() {
        let mut input = Vec::new();
        for i in 0..1000 {
            input.extend_from_slice(format!("{{\"n\":{i}}}\n").as_bytes());
        }
        assert_eq!(default_scanner(1024).scan(&input).unwrap().num_rows(), 1000);
    }

    // --- StreamingSimdScanner ---
    #[test]
    fn test_streaming_simple() {
        let mut s = StreamingSimdScanner::new(ScanConfig::default());
        let batch = s
            .scan(bytes::Bytes::from_static(
                b"{\"host\":\"web1\",\"status\":200}\n{\"host\":\"web2\"}\n",
            ))
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.column_by_name("host_str").is_some());
    }
    #[test]
    fn test_streaming_reuse() {
        let mut s = StreamingSimdScanner::new(ScanConfig::default());
        let _ = s.scan(bytes::Bytes::from_static(b"{\"x\":\"a\"}\n")).unwrap();
        let b = s.scan(bytes::Bytes::from_static(b"{\"x\":\"b\"}\n")).unwrap();
        assert_eq!(b.num_rows(), 1);
    }

    // --- validate_utf8 option ---
    #[test]
    fn test_validate_utf8_accepts_valid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            ..ScanConfig::default()
        };
        let batch = SimdScanner::new(config).scan(b"{\"msg\":\"hello\"}\n").unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_validate_utf8_returns_error_on_invalid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            ..ScanConfig::default()
        };
        // 0xFF is not valid UTF-8
        let result = SimdScanner::new(config).scan(b"{\"msg\":\"\xFF\"}\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_streaming_validate_utf8_accepts_valid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            ..ScanConfig::default()
        };
        let batch = StreamingSimdScanner::new(config)
            .scan(bytes::Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_streaming_validate_utf8_returns_error_on_invalid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            ..ScanConfig::default()
        };
        let result =
            StreamingSimdScanner::new(config).scan(bytes::Bytes::from_static(b"{\"msg\":\"\xFF\"}\n"));
        assert!(result.is_err());
    }

    // --- resource column injection ---

    #[test]
    fn test_resource_columns_injected_by_simd_scanner() {
        let config = ScanConfig {
            resource_columns: vec![
                ("_resource_k8s_pod_name".to_string(), "my-pod".to_string()),
                (
                    "_resource_k8s_namespace".to_string(),
                    "prod".to_string(),
                ),
            ],
            ..ScanConfig::default()
        };
        let batch = SimdScanner::new(config)
            .scan(b"{\"msg\":\"hello\"}\n{\"msg\":\"world\"}\n")
            .unwrap();
        assert_eq!(batch.num_rows(), 2);

        let pod = batch.column_by_name("_resource_k8s_pod_name").unwrap();
        let ns = batch.column_by_name("_resource_k8s_namespace").unwrap();

        // All rows hold the constant resource value.
        let pod_arr = pod.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(pod_arr.value(0), "my-pod");
        assert_eq!(pod_arr.value(1), "my-pod");

        let ns_arr = ns.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ns_arr.value(0), "prod");
        assert_eq!(ns_arr.value(1), "prod");
    }

    #[test]
    fn test_resource_columns_injected_by_streaming_scanner() {
        let config = ScanConfig {
            resource_columns: vec![(
                "_resource_service_name".to_string(),
                "nginx".to_string(),
            )],
            ..ScanConfig::default()
        };
        let batch = StreamingSimdScanner::new(config)
            .scan(bytes::Bytes::from_static(b"{\"level\":\"INFO\"}\n"))
            .unwrap();
        assert_eq!(batch.num_rows(), 1);

        let svc = batch.column_by_name("_resource_service_name").unwrap();
        let svc_arr = svc.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(svc_arr.value(0), "nginx");
    }

    #[test]
    fn test_no_resource_columns_noop() {
        // With no resource_columns, inject_resource_columns must be a no-op.
        let config = ScanConfig::default();
        let batch = StreamingSimdScanner::new(config)
            .scan(bytes::Bytes::from_static(b"{\"x\":\"1\"}\n"))
            .unwrap();
        assert!(
            !batch
                .schema()
                .fields()
                .iter()
                .any(|f| f.name().starts_with("_resource_")),
            "expected no _resource_ columns"
        );
    }
}
