//! Scanner wrapper type that produces Arrow `RecordBatch` from raw bytes.
//!
//! Composes ffwd-core's generic `scan_streaming` with Arrow builders
//! to produce `RecordBatch`. The core scan logic lives in ffwd-core;
//! this crate provides the Arrow-specific builder implementations.

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use ffwd_core::json_scanner::scan_streaming;
use ffwd_core::scan_config::ScanConfig;
use ffwd_core::scanner::ScanBuilder;

use crate::streaming_builder::StreamingBuilder;

// ---------------------------------------------------------------------------
// ScanBuilder impl for StreamingBuilder
// ---------------------------------------------------------------------------

impl ScanBuilder for StreamingBuilder {
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
    fn append_decoded_str_by_idx(&mut self, idx: usize, v: &[u8]) {
        self.append_decoded_str_by_idx(idx, v);
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
    fn append_bool_by_idx(&mut self, idx: usize, v: bool) {
        self.append_bool_by_idx(idx, v);
    }
    #[inline(always)]
    fn append_null_by_idx(&mut self, idx: usize) {
        self.append_null_by_idx(idx);
    }
    #[inline(always)]
    fn append_line(&mut self, line: &[u8]) {
        // Explicitly call the inherent method to avoid any ambiguity
        // with this trait method of the same name.
        StreamingBuilder::append_line(self, line);
    }
}

// ---------------------------------------------------------------------------
// Scanner — StreamingBuilder (zero-copy hot path)
// ---------------------------------------------------------------------------

/// Zero-copy scanner producing `RecordBatch` via `StreamingBuilder`.
///
/// String values are `StringViewArray` views into the reference-counted input
/// buffer (`bytes::Bytes`). The input buffer must stay alive while the
/// `RecordBatch` is in use.
pub struct Scanner {
    builder: StreamingBuilder,
    config: ScanConfig,
    resource_attrs: Vec<(String, String)>,
}

impl Scanner {
    /// Create a new streaming scanner with the given configuration.
    pub fn new(config: ScanConfig) -> Self {
        Scanner {
            builder: StreamingBuilder::new(config.line_field_name.clone()),
            config,
            resource_attrs: Vec::new(),
        }
    }

    /// Create a scanner that injects constant resource attribute columns per row.
    pub fn with_resource_attrs(config: ScanConfig, resource_attrs: Vec<(String, String)>) -> Self {
        Scanner {
            builder: StreamingBuilder::new(config.line_field_name.clone()),
            config,
            resource_attrs,
        }
    }
    /// Scan an NDJSON buffer into a zero-copy `RecordBatch`.
    ///
    /// String columns are `StringViewArray` views into the input `Bytes`
    /// buffer. The buffer's refcount keeps it alive as long as the
    /// `RecordBatch` exists.
    pub fn scan(&mut self, buf: bytes::Bytes) -> Result<RecordBatch, ArrowError> {
        if self.config.validate_utf8 {
            std::str::from_utf8(&buf).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("Scanner: input is not valid UTF-8: {e}"))
            })?;
        }
        self.builder.begin_batch(buf.clone());
        self.builder
            .set_resource_attributes(self.resource_attrs.as_slice());
        scan_streaming(&buf, &self.config, &mut self.builder);
        self.builder.finish_batch()
    }

    /// Scan an NDJSON buffer into a self-contained `RecordBatch`.
    ///
    /// Uses the same zero-copy scan as [`scan`](Self::scan) but produces
    /// owned `StringArray` columns instead of `StringViewArray` views.
    /// The input buffer can be freed immediately after this call.
    ///
    /// This is the optimal persistence path: zero-copy scan speed with
    /// a single bulk copy at finalization.
    pub fn scan_detached(&mut self, buf: bytes::Bytes) -> Result<RecordBatch, ArrowError> {
        if self.config.validate_utf8 {
            std::str::from_utf8(&buf).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("Scanner: input is not valid UTF-8: {e}"))
            })?;
        }
        self.builder.begin_batch(buf.clone());
        self.builder
            .set_resource_attributes(self.resource_attrs.as_slice());
        scan_streaming(&buf, &self.config, &mut self.builder);
        self.builder.finish_batch_detached()
    }
}

#[cfg(test)]
mod tests {
    use crate::scanner::Scanner;
    use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
    use bytes::Bytes;
    use ffwd_core::scan_config::{FieldSpec, ScanConfig};

    fn default_scanner(_rows: usize) -> Scanner {
        Scanner::new(ScanConfig::default())
    }

    #[test]
    fn test_simple() {
        let input = b"{\"host\":\"web1\",\"status\":200,\"lat\":1.5}\n{\"host\":\"web2\",\"status\":404,\"lat\":0.3}\n";
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(input.to_vec()))
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type fields: bare names
        assert_eq!(
            batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "web1"
        );
        assert_eq!(
            batch
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            404
        );
        // Single-type fields use bare names; type suffixes are only for conflicts.
        assert!(
            batch.column_by_name("host_str").is_none(),
            "single-type string fields must not emit suffixed columns"
        );
        assert!(
            batch.column_by_name("status_int").is_none(),
            "single-type int fields must not emit suffixed columns"
        );
    }
    #[test]
    fn test_type_conflict() {
        use arrow::array::StructArray;
        use arrow::datatypes::DataType;
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(
                b"{\"status\":200}\n{\"status\":\"OK\"}\n".to_vec(),
            ))
            .unwrap();
        // Old suffixed columns must not exist
        assert!(
            batch.column_by_name("status__int").is_none(),
            "status__int must not exist"
        );
        assert!(
            batch.column_by_name("status__str").is_none(),
            "status__str must not exist"
        );
        // New struct column
        let status_col = batch
            .column_by_name("status")
            .expect("status struct column");
        assert!(
            matches!(status_col.data_type(), DataType::Struct(_)),
            "status must be Struct, got {:?}",
            status_col.data_type()
        );
        let sa = status_col.as_any().downcast_ref::<StructArray>().unwrap();
        let child_names: Vec<&str> = sa.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(child_names.contains(&"int"), "missing int child");
        assert!(child_names.contains(&"str"), "missing str child");
    }
    #[test]
    fn test_missing_fields() {
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(
                b"{\"a\":\"hello\"}\n{\"b\":\"world\"}\n".to_vec(),
            ))
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string field: bare name
        let a = batch.column_by_name("a").unwrap();
        assert!(!a.is_null(0));
        assert!(a.is_null(1));
    }
    #[test]
    fn test_nested() {
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(
                br#"{"u":{"name":"alice"},"level":"info"}
"#
                .to_vec(),
            ))
            .unwrap();
        assert!(
            batch
                .column_by_name("u")
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
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let batch = Scanner::new(config)
            .scan_detached(Bytes::from(
                br#"{"a":"1","b":"2","c":"3"}
"#
                .to_vec(),
            ))
            .unwrap();
        // Single-type string field: bare name
        assert!(batch.column_by_name("a").is_some());
        assert!(batch.column_by_name("b").is_none());
    }

    #[test]
    fn test_pushdown_case_insensitive_wanted_field() {
        let config = ScanConfig {
            wanted_fields: vec![FieldSpec {
                // Mirrors SQL analyzer output when a query references `Level`.
                name: "Level".into(),
                aliases: vec![],
            }],
            extract_all: false,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let batch = Scanner::new(config)
            .scan_detached(Bytes::from(
                br#"{"level":"info","msg":"ok"}
"#
                .to_vec(),
            ))
            .expect("scan should succeed");

        // Regression (#2044): case mismatch must not silently drop the field.
        let level = batch
            .column_by_name("level")
            .expect("level column should be extracted");
        let level = level
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("level must be string");
        assert_eq!(level.value(0), "info");
        assert!(batch.column_by_name("msg").is_none());
    }
    #[test]
    fn test_line_capture() {
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: Some("body".to_string()),
            validate_utf8: false,
            row_predicate: None,
        };
        let batch = Scanner::new(config)
            .scan_detached(Bytes::from(b"{\"msg\":\"hi\"}\n".to_vec()))
            .unwrap();
        assert!(batch.column_by_name("body").is_some());
    }

    #[test]
    fn test_resource_columns_injected_detached() {
        let input = br#"{"message":"a"}
{"message":"b"}
"#;
        let scanner = Scanner::with_resource_attrs(
            ScanConfig::default(),
            vec![
                ("service.name".to_string(), "checkout".to_string()),
                ("k8s.namespace".to_string(), "prod".to_string()),
            ],
        );
        let mut scanner = scanner;
        let batch = scanner
            .scan_detached(Bytes::from(input.to_vec()))
            .expect("batch");

        let service = batch
            .column_by_name("resource.attributes.service.name")
            .expect("resource service col")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(service.value(0), "checkout");
        assert_eq!(service.value(1), "checkout");

        let ns = batch
            .column_by_name("resource.attributes.k8s.namespace")
            .expect("resource namespace col")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(ns.value(0), "prod");
        assert_eq!(ns.value(1), "prod");
    }

    #[test]
    fn test_resource_columns_injected() {
        // Mirrors test_resource_columns_injected_detached but uses scan()
        // (StringViewArray) instead of scan_detached() (StringArray).
        let input = br#"{"message":"a"}
{"message":"b"}
"#;
        let scanner = Scanner::with_resource_attrs(
            ScanConfig::default(),
            vec![
                ("service.name".to_string(), "checkout".to_string()),
                ("k8s.namespace".to_string(), "prod".to_string()),
            ],
        );
        let mut scanner = scanner;
        let batch = scanner.scan(Bytes::from(input.to_vec())).expect("batch");

        let service = batch
            .column_by_name("resource.attributes.service.name")
            .expect("resource service col")
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("utf8view");
        assert_eq!(service.value(0), "checkout");
        assert_eq!(service.value(1), "checkout");

        let ns = batch
            .column_by_name("resource.attributes.k8s.namespace")
            .expect("resource namespace col")
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("utf8view");
        assert_eq!(ns.value(0), "prod");
        assert_eq!(ns.value(1), "prod");

        // The canonical column name preserves the dotted resource key directly.
        assert!(
            batch
                .schema()
                .field_with_name("resource.attributes.service.name")
                .expect("field exists")
                .metadata()
                .is_empty()
        );
    }

    #[test]
    fn test_batch_reuse() {
        let mut s = default_scanner(4);
        let _ = s
            .scan_detached(Bytes::from(b"{\"x\":1}\n".to_vec()))
            .unwrap();
        let b = s
            .scan_detached(Bytes::from(b"{\"x\":2}\n".to_vec()))
            .unwrap();
        assert_eq!(b.num_rows(), 1);
    }
    #[test]
    fn test_bool_null() {
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(
                b"{\"a\":true,\"b\":false,\"c\":null}\n".to_vec(),
            ))
            .unwrap();
        // Single-type bool field: bare name
        assert!(
            batch
                .column_by_name("a")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("b")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }
    #[test]
    fn test_duplicate_keys() {
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(b"{\"a\":1,\"a\":2}\n".to_vec()))
            .unwrap();
        // Single-type int field: bare name (first-writer-wins for dup keys)
        assert_eq!(
            batch
                .column_by_name("a")
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
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(b"{\"big\":99999999999999999999}\n".to_vec()))
            .unwrap();
        assert!(batch.column_by_name("big").is_some());
    }
    #[test]
    fn test_i64_min_is_preserved_as_int() {
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(b"{\"big\":-9223372036854775808}\n".to_vec()))
            .unwrap();
        // Single-type int field: bare name
        let col = batch
            .column_by_name("big")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), i64::MIN);
    }
    #[test]
    fn test_empty_object() {
        assert_eq!(
            default_scanner(4)
                .scan_detached(Bytes::from(b"{}\n".to_vec()))
                .unwrap()
                .num_rows(),
            1
        );
    }
    #[test]
    fn test_array_value() {
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(
                br#"{"tags":["a","b"],"n":1}
"#
                .to_vec(),
            ))
            .unwrap();
        assert_eq!(
            batch
                .column_by_name("tags")
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
            .scan_detached(Bytes::from(
                br#"{"d":{"m":"has } and {"},"ok":true}
"#
                .to_vec(),
            ))
            .unwrap();
        assert!(
            batch
                .column_by_name("ok")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }
    #[test]
    fn test_escaped_string() {
        let batch = default_scanner(4)
            .scan_detached(Bytes::from(
                br#"{"msg":"hello \"world\""}
"#
                .to_vec(),
            ))
            .unwrap();
        assert!(
            batch
                .column_by_name("msg")
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
        assert_eq!(
            default_scanner(1024)
                .scan_detached(Bytes::from(input))
                .unwrap()
                .num_rows(),
            1000
        );
    }

    // --- Scanner (zero-copy) ---
    #[test]
    fn test_streaming_simple() {
        let mut s = Scanner::new(ScanConfig::default());
        let batch = s
            .scan(Bytes::from_static(
                b"{\"host\":\"web1\",\"status\":200}\n{\"host\":\"web2\"}\n",
            ))
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string field: bare name
        assert!(batch.column_by_name("host").is_some());
    }
    #[test]
    fn test_streaming_reuse() {
        let mut s = Scanner::new(ScanConfig::default());
        let _ = s.scan(Bytes::from_static(b"{\"x\":\"a\"}\n")).unwrap();
        let b = s.scan(Bytes::from_static(b"{\"x\":\"b\"}\n")).unwrap();
        assert_eq!(b.num_rows(), 1);
    }

    #[test]
    fn test_streaming_line_capture() {
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: Some("body".to_string()),
            validate_utf8: false,
            row_predicate: None,
        };
        let batch = Scanner::new(config)
            .scan(Bytes::from_static(b"{\"msg\":\"hi\"}\n"))
            .unwrap();
        assert!(
            batch.column_by_name("body").is_some(),
            "body column must be present when line_capture=true"
        );
        let raw_col = batch.column_by_name("body").unwrap();
        let raw_arr = raw_col
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("body must be StringViewArray in Scanner");
        assert_eq!(raw_arr.value(0), "{\"msg\":\"hi\"}");
    }

    // --- validate_utf8 option ---
    #[test]
    fn test_validate_utf8_accepts_valid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            row_predicate: None,
            ..ScanConfig::default()
        };
        let batch = Scanner::new(config)
            .scan_detached(Bytes::from(b"{\"msg\":\"hello\"}\n".to_vec()))
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_validate_utf8_returns_error_on_invalid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            row_predicate: None,
            ..ScanConfig::default()
        };
        // 0xFF is not valid UTF-8
        let result =
            Scanner::new(config).scan_detached(Bytes::from(b"{\"msg\":\"\xFF\"}\n".to_vec()));
        assert!(result.is_err());
    }

    #[test]
    fn test_streaming_validate_utf8_accepts_valid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            row_predicate: None,
            ..ScanConfig::default()
        };
        let batch = Scanner::new(config)
            .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_streaming_validate_utf8_returns_error_on_invalid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            row_predicate: None,
            ..ScanConfig::default()
        };
        let result = Scanner::new(config).scan(Bytes::from_static(b"{\"msg\":\"\xFF\"}\n"));
        assert!(result.is_err());
    }
}
