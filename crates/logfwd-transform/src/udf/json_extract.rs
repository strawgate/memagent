// udf/json_extract.rs — DataFusion UDFs for extracting fields from raw JSON strings.
//
// These enable a "DataFusion-only JSON extraction" mode where the scanner keeps
// only the `_raw` column and all field extraction happens inside SQL:
//
//   SELECT json_get_str(_raw, 'level')      AS level,
//          json_get_int(_raw, 'status')     AS status,
//          json_get_int(_raw, 'duration_ms') AS duration_ms
//   FROM logs
//   WHERE json_get_str(_raw, 'level') = 'ERROR'
//
// The underlying extraction uses the same zero-copy byte scanning as the scanner:
// no UTF-8 validation, no heap allocations per row.  Escape sequences are
// preserved (not decoded), matching scanner behaviour.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Builder, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

// ---------------------------------------------------------------------------
// Low-level byte-level JSON helpers (mirrors scanner.rs internals)
// ---------------------------------------------------------------------------

#[inline(always)]
fn skip_ws(buf: &[u8], mut pos: usize) -> usize {
    let len = buf.len();
    while pos < len && matches!(buf[pos], b' ' | b'\t' | b'\r' | b'\n') {
        pos += 1;
    }
    pos
}

/// Scan a quoted JSON string starting at `pos` (pointing to the opening `"`).
/// Returns (content_bytes_without_quotes, pos_after_closing_quote).
/// Escape sequences are NOT decoded — content is returned verbatim.
#[inline]
fn scan_string(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    debug_assert_eq!(buf[pos], b'"');
    let start = pos + 1;
    let mut i = start;
    let len = buf.len();
    while i < len {
        match buf[i] {
            b'"' => return Some((&buf[start..i], i + 1)),
            b'\\' => i += 2, // skip escaped char
            _ => i += 1,
        }
    }
    None // unterminated string
}

/// Skip a nested JSON object or array (balanced braces/brackets), including
/// any strings inside.  Returns the position just after the closing delimiter.
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
            _ => pos += 1,
        }
    }
    pos
}

/// Skip any JSON value without extracting it.
#[inline]
fn skip_value(buf: &[u8], pos: usize) -> usize {
    let len = buf.len();
    if pos >= len {
        return pos;
    }
    match buf[pos] {
        b'"' => scan_string(buf, pos).map(|(_, end)| end).unwrap_or(len),
        b'{' | b'[' => skip_nested(buf, pos),
        _ => {
            let mut p = pos;
            while p < len
                && !matches!(buf[p], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
            {
                p += 1;
            }
            p
        }
    }
}

// ---------------------------------------------------------------------------
// Core extraction
// ---------------------------------------------------------------------------

/// Extract the value bytes for `key` from a top-level JSON object.
///
/// - For string values, returns the content between quotes (escapes not decoded).
/// - For number/bool values, returns the literal bytes.
/// - Returns `None` if the key is absent or the input is not a JSON object.
pub fn extract_json_field<'a>(json: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let len = json.len();
    let mut pos = skip_ws(json, 0);
    if pos >= len || json[pos] != b'{' {
        return None;
    }
    pos += 1; // skip '{'

    loop {
        pos = skip_ws(json, pos);
        if pos >= len || json[pos] == b'}' {
            break;
        }
        if json[pos] != b'"' {
            break; // malformed
        }

        let (found_key, after_key) = scan_string(json, pos)?;
        pos = after_key;

        pos = skip_ws(json, pos);
        if pos >= len || json[pos] != b':' {
            break;
        }
        pos += 1;
        pos = skip_ws(json, pos);
        if pos >= len {
            break;
        }

        if found_key == key {
            return Some(match json[pos] {
                b'"' => {
                    let (val, _) = scan_string(json, pos)?;
                    val
                }
                _ => {
                    let start = pos;
                    let end = skip_value(json, pos);
                    // Trim trailing whitespace from the raw value bytes.
                    let mut e = end;
                    while e > start && matches!(json[e - 1], b' ' | b'\t' | b'\n' | b'\r') {
                        e -= 1;
                    }
                    &json[start..e]
                }
            });
        } else {
            pos = skip_value(json, pos);
        }

        pos = skip_ws(json, pos);
        if pos < len && json[pos] == b',' {
            pos += 1;
        }
    }
    None
}

// ---------------------------------------------------------------------------
// json_get_str
// ---------------------------------------------------------------------------

/// UDF: `json_get_str(raw_json, key)` — extract a field from a JSON object as a string.
///
/// Returns the field value as a UTF-8 string, or NULL when:
/// - the key is absent
/// - the input is not a JSON object
/// - the `raw_json` value is NULL
///
/// String escape sequences are preserved (not decoded), matching scanner behaviour.
/// Non-string values (numbers, booleans) are returned as their literal text.
#[derive(Debug)]
pub struct JsonGetStrUdf {
    signature: Signature,
}

impl JsonGetStrUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonGetStrUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get_str"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let json_cv = &args.args[0];
        let key_cv = &args.args[1];

        // The key must be a compile-time scalar literal (e.g. json_get_str(_raw, 'level')).
        let key: Vec<u8> = match key_cv {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.as_bytes().to_vec(),
            _ => {
                return Err(DataFusionError::Plan(
                    "json_get_str: second argument must be a string literal".into(),
                ));
            }
        };

        match json_cv {
            ColumnarValue::Array(arr) => {
                let json_arr = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "json_get_str: first argument must be a Utf8 column".into(),
                        )
                    })?;

                let mut builder =
                    StringBuilder::with_capacity(json_arr.len(), json_arr.len() * 16);
                for row in json_arr.iter() {
                    match row {
                        Some(s) => match extract_json_field(s.as_bytes(), &key) {
                            Some(val) => {
                                // SAFETY: JSON field content (after quote stripping) is valid UTF-8.
                                builder.append_value(unsafe {
                                    std::str::from_utf8_unchecked(val)
                                });
                            }
                            None => builder.append_null(),
                        },
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let val = extract_json_field(s.as_bytes(), &key).map(|b| {
                    // SAFETY: as above.
                    unsafe { std::str::from_utf8_unchecked(b) }.to_string()
                });
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(val)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}

// ---------------------------------------------------------------------------
// json_get_int
// ---------------------------------------------------------------------------

/// UDF: `json_get_int(raw_json, key)` — extract a field from a JSON object as Int64.
///
/// Returns the integer value, or NULL when:
/// - the key is absent
/// - the value is not parseable as i64 (e.g. a string, float, boolean)
/// - the `raw_json` value is NULL
#[derive(Debug)]
pub struct JsonGetIntUdf {
    signature: Signature,
}

impl JsonGetIntUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonGetIntUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get_int"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let json_cv = &args.args[0];
        let key_cv = &args.args[1];

        let key: Vec<u8> = match key_cv {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.as_bytes().to_vec(),
            _ => {
                return Err(DataFusionError::Plan(
                    "json_get_int: second argument must be a string literal".into(),
                ));
            }
        };

        match json_cv {
            ColumnarValue::Array(arr) => {
                let json_arr = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "json_get_int: first argument must be a Utf8 column".into(),
                        )
                    })?;

                let mut builder = Int64Builder::with_capacity(json_arr.len());
                for row in json_arr.iter() {
                    let val = row
                        .and_then(|s| extract_json_field(s.as_bytes(), &key))
                        .and_then(logfwd_core::batch_builder::parse_int_fast);
                    match val {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let val = extract_json_field(s.as_bytes(), &key)
                    .and_then(logfwd_core::batch_builder::parse_int_fast);
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(val)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn test_extract_string_field() {
        let json = br#"{"level":"ERROR","msg":"disk full","status":500}"#;
        assert_eq!(extract_json_field(json, b"level"), Some(b"ERROR".as_slice()));
        assert_eq!(extract_json_field(json, b"msg"), Some(b"disk full".as_slice()));
    }

    #[test]
    fn test_extract_int_field() {
        let json = br#"{"level":"INFO","status":200,"duration_ms":42}"#;
        assert_eq!(extract_json_field(json, b"status"), Some(b"200".as_slice()));
        assert_eq!(extract_json_field(json, b"duration_ms"), Some(b"42".as_slice()));
    }

    #[test]
    fn test_missing_key_returns_none() {
        let json = br#"{"a":"1","b":"2"}"#;
        assert!(extract_json_field(json, b"c").is_none());
    }

    #[test]
    fn test_not_a_json_object() {
        assert!(extract_json_field(b"not json", b"a").is_none());
        assert!(extract_json_field(b"[1,2,3]", b"a").is_none());
        assert!(extract_json_field(b"", b"a").is_none());
    }

    #[test]
    fn test_nested_object_skipped() {
        let json = br#"{"meta":{"host":"web1"},"level":"WARN"}"#;
        // "meta" key returns the nested object as raw bytes
        let meta_val = extract_json_field(json, b"meta").unwrap();
        let s = unsafe { std::str::from_utf8_unchecked(meta_val) };
        assert!(s.starts_with('{'));
        // "level" is extracted correctly even though it comes after a nested object
        assert_eq!(extract_json_field(json, b"level"), Some(b"WARN".as_slice()));
    }

    #[test]
    fn test_json_get_str_udf_array() {
        use std::sync::Arc;
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::logical_expr::ScalarUDF;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("raw", DataType::Utf8, true)]));
        let raw: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"level":"INFO","status":200}"#),
            Some(r#"{"level":"ERROR","status":500}"#),
            None,
        ]));
        let batch = RecordBatch::try_new(schema, vec![raw]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let ctx = SessionContext::new();
            ctx.register_udf(ScalarUDF::from(JsonGetStrUdf::new()));
            let table = datafusion::datasource::MemTable::try_new(
                batch.schema(),
                vec![vec![batch]],
            )
            .unwrap();
            ctx.register_table("logs", Arc::new(table)).unwrap();

            let result = ctx
                .sql("SELECT json_get_str(raw, 'level') AS level FROM logs")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let batch = &result[0];
            let level = batch
                .column_by_name("level")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(level.value(0), "INFO");
            assert_eq!(level.value(1), "ERROR");
            assert!(level.is_null(2));
        });
    }

    #[test]
    fn test_json_get_int_udf_array() {
        use std::sync::Arc;
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::logical_expr::ScalarUDF;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("raw", DataType::Utf8, true)]));
        let raw: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"status":200,"duration_ms":15}"#),
            Some(r#"{"status":500,"duration_ms":3}"#),
            Some(r#"{"status":"not_int"}"#),
        ]));
        let batch = RecordBatch::try_new(schema, vec![raw]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let ctx = SessionContext::new();
            ctx.register_udf(ScalarUDF::from(JsonGetIntUdf::new()));
            let table = datafusion::datasource::MemTable::try_new(
                batch.schema(),
                vec![vec![batch]],
            )
            .unwrap();
            ctx.register_table("logs", Arc::new(table)).unwrap();

            let result = ctx
                .sql("SELECT json_get_int(raw, 'status') AS status FROM logs")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let batch = &result[0];
            let status = batch
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(status.value(0), 200);
            assert_eq!(status.value(1), 500);
            assert!(status.is_null(2)); // "not_int" → NULL
        });
    }
}
