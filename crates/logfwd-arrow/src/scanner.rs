//! Scanner wrapper types that produce Arrow `RecordBatch` from raw bytes.
//!
//! These compose logfwd-core's generic `scan_streaming` with Arrow builders
//! to produce `RecordBatch`. The core scan logic lives in logfwd-core;
//! this crate provides the Arrow-specific builder implementations.

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use logfwd_core::json_scanner::scan_streaming;
use logfwd_core::scan_config::ScanConfig;
use logfwd_core::scanner::ScanBuilder;

use crate::storage_builder::StorageBuilder;
use crate::streaming_builder::StreamingBuilder;

// ---------------------------------------------------------------------------
// ScanBuilder impls for Arrow builders
// ---------------------------------------------------------------------------

impl ScanBuilder for StorageBuilder {
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
    fn append_null_by_idx(&mut self, idx: usize) {
        self.append_null_by_idx(idx);
    }
    #[inline(always)]
    fn append_raw(&mut self, line: &[u8]) {
        // Explicitly call the inherent method to avoid any ambiguity
        // with this trait method of the same name.
        StreamingBuilder::append_raw(self, line);
    }
}

// ---------------------------------------------------------------------------
// CopyScanner — StorageBuilder (self-contained, compressible)
// ---------------------------------------------------------------------------

/// SIMD scanner producing self-contained `RecordBatch` via `StorageBuilder`.
///
/// Output owns all its data — the input buffer can be freed after `scan()`.
pub struct CopyScanner {
    builder: StorageBuilder,
    config: ScanConfig,
}

impl CopyScanner {
    /// Create a new scanner with the given configuration.
    pub fn new(config: ScanConfig) -> Self {
        CopyScanner {
            builder: StorageBuilder::new(config.keep_raw),
            config,
        }
    }
    /// Scan an NDJSON buffer into a `RecordBatch`.
    ///
    /// The returned batch owns all its data — the input buffer can be
    /// freed immediately after this call returns.
    pub fn scan(&mut self, buf: &[u8]) -> Result<RecordBatch, ArrowError> {
        if self.config.validate_utf8 {
            std::str::from_utf8(buf).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "CopyScanner: input is not valid UTF-8: {e}"
                ))
            })?;
        }
        self.builder.begin_batch();
        scan_streaming(buf, &self.config, &mut self.builder);
        self.builder.finish_batch()
    }
}

// ---------------------------------------------------------------------------
// ZeroCopyScanner — StreamingBuilder (zero-copy hot path)
// ---------------------------------------------------------------------------

/// SIMD scanner producing zero-copy `RecordBatch` via `StreamingBuilder`.
///
/// String values are `StringViewArray` views into the reference-counted input
/// buffer (`bytes::Bytes`). The input buffer must stay alive while the
/// `RecordBatch` is in use.
pub struct ZeroCopyScanner {
    builder: StreamingBuilder,
    config: ScanConfig,
}

impl ZeroCopyScanner {
    /// Create a new streaming scanner with the given configuration.
    pub fn new(config: ScanConfig) -> Self {
        ZeroCopyScanner {
            builder: StreamingBuilder::new(config.keep_raw),
            config,
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
                ArrowError::InvalidArgumentError(format!(
                    "ZeroCopyScanner: input is not valid UTF-8: {e}"
                ))
            })?;
        }
        self.builder.begin_batch(buf.clone());
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
    pub fn scan_owned(&mut self, buf: bytes::Bytes) -> Result<RecordBatch, ArrowError> {
        if self.config.validate_utf8 {
            std::str::from_utf8(&buf).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "ZeroCopyScanner: input is not valid UTF-8: {e}"
                ))
            })?;
        }
        self.builder.begin_batch(buf.clone());
        scan_streaming(&buf, &self.config, &mut self.builder);
        self.builder.finish_batch_owned()
    }
}

#[cfg(test)]
mod tests {
    use crate::scanner::{CopyScanner, ZeroCopyScanner};
    use arrow::array::{Array, Int64Array, StringArray};
    use logfwd_core::scan_config::{FieldSpec, ScanConfig};

    fn default_scanner(_rows: usize) -> CopyScanner {
        CopyScanner::new(ScanConfig::default())
    }

    #[test]
    fn test_simple() {
        let input = b"{\"host\":\"web1\",\"status\":200,\"lat\":1.5}\n{\"host\":\"web2\",\"status\":404,\"lat\":0.3}\n";
        let batch = default_scanner(4).scan(input).unwrap();
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
        // Legacy single-underscore suffixed columns must NOT be emitted.
        assert!(
            batch.column_by_name("host_str").is_none(),
            "single-type string fields must not emit legacy suffixed columns"
        );
        assert!(
            batch.column_by_name("status_int").is_none(),
            "single-type int fields must not emit legacy suffixed columns"
        );
    }
    #[test]
    fn test_type_conflict() {
        use arrow::array::StructArray;
        use arrow::datatypes::DataType;
        let batch = default_scanner(4)
            .scan(b"{\"status\":200}\n{\"status\":\"OK\"}\n")
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
            .scan(b"{\"a\":\"hello\"}\n{\"b\":\"world\"}\n")
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string fields: bare names
        let a = batch.column_by_name("a").unwrap();
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
            keep_raw: false,
            validate_utf8: false,
        };
        let batch = CopyScanner::new(config)
            .scan(
                br#"{"a":"1","b":"2","c":"3"}
"#,
            )
            .unwrap();
        // Single-type string field: bare name
        assert!(batch.column_by_name("a").is_some());
        assert!(batch.column_by_name("b").is_none());
    }
    #[test]
    fn test_keep_raw() {
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: true,
            validate_utf8: false,
        };
        let batch = CopyScanner::new(config)
            .scan(b"{\"msg\":\"hi\"}\n")
            .unwrap();
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
        let batch = default_scanner(4)
            .scan(b"{\"a\":true,\"b\":false,\"c\":null}\n")
            .unwrap();
        // Single-type string field: bare name
        assert_eq!(
            batch
                .column_by_name("a")
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
            .scan(b"{\"big\":99999999999999999999}\n")
            .unwrap();
        assert!(batch.column_by_name("big").is_some());
    }
    #[test]
    fn test_i64_min_is_preserved_as_int() {
        let batch = default_scanner(4)
            .scan(b"{\"big\":-9223372036854775808}\n")
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
            .scan(
                br#"{"d":{"m":"has } and {"},"ok":true}
"#,
            )
            .unwrap();
        assert_eq!(
            batch
                .column_by_name("ok")
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
        assert_eq!(default_scanner(1024).scan(&input).unwrap().num_rows(), 1000);
    }

    // --- ZeroCopyScanner ---
    #[test]
    fn test_streaming_simple() {
        let mut s = ZeroCopyScanner::new(ScanConfig::default());
        let batch = s
            .scan(bytes::Bytes::from_static(
                b"{\"host\":\"web1\",\"status\":200}\n{\"host\":\"web2\"}\n",
            ))
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string field: bare name
        assert!(batch.column_by_name("host").is_some());
    }
    #[test]
    fn test_streaming_reuse() {
        let mut s = ZeroCopyScanner::new(ScanConfig::default());
        let _ = s
            .scan(bytes::Bytes::from_static(b"{\"x\":\"a\"}\n"))
            .unwrap();
        let b = s
            .scan(bytes::Bytes::from_static(b"{\"x\":\"b\"}\n"))
            .unwrap();
        assert_eq!(b.num_rows(), 1);
    }

    #[test]
    fn test_streaming_keep_raw() {
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: true,
            validate_utf8: false,
        };
        let batch = ZeroCopyScanner::new(config)
            .scan(bytes::Bytes::from_static(b"{\"msg\":\"hi\"}\n"))
            .unwrap();
        assert!(
            batch.column_by_name("_raw").is_some(),
            "_raw column must be present when keep_raw=true"
        );
        let raw_col = batch.column_by_name("_raw").unwrap();
        let raw_arr = raw_col
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("_raw must be StringViewArray in ZeroCopyScanner");
        assert_eq!(raw_arr.value(0), "{\"msg\":\"hi\"}");
    }

    // --- validate_utf8 option ---
    #[test]
    fn test_validate_utf8_accepts_valid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            ..ScanConfig::default()
        };
        let batch = CopyScanner::new(config)
            .scan(b"{\"msg\":\"hello\"}\n")
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_validate_utf8_returns_error_on_invalid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            ..ScanConfig::default()
        };
        // 0xFF is not valid UTF-8
        let result = CopyScanner::new(config).scan(b"{\"msg\":\"\xFF\"}\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_streaming_validate_utf8_accepts_valid_input() {
        let config = ScanConfig {
            validate_utf8: true,
            ..ScanConfig::default()
        };
        let batch = ZeroCopyScanner::new(config)
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
            ZeroCopyScanner::new(config).scan(bytes::Bytes::from_static(b"{\"msg\":\"\xFF\"}\n"));
        assert!(result.is_err());
    }
}
