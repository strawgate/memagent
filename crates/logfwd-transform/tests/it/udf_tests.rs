use std::sync::Arc;

use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use logfwd_transform::enrichment::GeoDatabase;
use logfwd_transform::enrichment::GeoResult;
use logfwd_transform::udf::{
    GeoLookupUdf, GrokUdf, HashUdf, JsonExtractMode, JsonExtractUdf, RegexpExtractUdf,
};

struct MockGeoDatabase;

impl GeoDatabase for MockGeoDatabase {
    fn lookup(&self, _ip: &str) -> Option<GeoResult> {
        None
    }
}

#[test]
fn test_hash_udf_string_view() {
    let udf = HashUdf::new();

    // Test with StringViewArray
    let string_view_array: Arc<dyn Array> = Arc::new(StringViewArray::from(vec!["test1", "test2"]));
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::UInt64, true));
    let args_view = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(string_view_array)],
        number_rows: 2,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result_view = udf.invoke_with_args(args_view).unwrap();

    // Test with StringArray
    let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec!["test1", "test2"]));
    let args_string = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(string_array)],
        number_rows: 2,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result_string = udf.invoke_with_args(args_string).unwrap();

    // Verify both produce the same result
    let view_arr = match result_view {
        ColumnarValue::Array(arr) => arr,
        _ => panic!("Expected array"),
    };
    let str_arr = match result_string {
        ColumnarValue::Array(arr) => arr,
        _ => panic!("Expected array"),
    };

    let view_u64 = view_arr
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    let str_u64 = str_arr
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();

    assert_eq!(view_u64.len(), 2);
    assert_eq!(view_u64.value(0), str_u64.value(0));
    assert_eq!(view_u64.value(1), str_u64.value(1));
}

#[test]
fn test_hash_udf_large_utf8_with_null() {
    let udf = HashUdf::new();
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::UInt64, true));

    // LargeStringArray with one non-null and one null value.
    let large_string_array: Arc<dyn Array> =
        Arc::new(LargeStringArray::from(vec![Some("value"), None]));
    let args_large = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(large_string_array)],
        number_rows: 2,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result_large = udf.invoke_with_args(args_large).unwrap();
    let large_arr = match result_large {
        ColumnarValue::Array(arr) => arr,
        _ => panic!("Expected array"),
    };
    let large_u64 = large_arr
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();

    assert_eq!(large_u64.len(), 2);
    // Non-null value should produce a hash.
    assert!(!large_u64.is_null(0));
    // Null should propagate: result for null input is null.
    assert!(large_u64.is_null(1));

    // Cross-type consistency: same string via StringArray should hash identically.
    let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("value"), None]));
    let args_str = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(string_array)],
        number_rows: 2,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result_str = udf.invoke_with_args(args_str).unwrap();
    let str_arr = match result_str {
        ColumnarValue::Array(arr) => arr,
        _ => panic!("Expected array"),
    };
    let str_u64 = str_arr
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(large_u64.value(0), str_u64.value(0));
}

#[test]
fn udf_rejects_wrong_arity() {
    let mock_geo: Arc<dyn GeoDatabase> = Arc::new(MockGeoDatabase);

    // (name, udf, expected_arity, error_substring)
    let cases: Vec<(&str, Box<dyn ScalarUDFImpl>, usize, &str)> = vec![
        (
            "hash",
            Box::new(HashUdf::new()),
            1,
            "expects exactly one argument",
        ),
        (
            "json_extract",
            Box::new(JsonExtractUdf::new(JsonExtractMode::Str)),
            2,
            "expects exactly two arguments",
        ),
        (
            "geo_lookup",
            Box::new(GeoLookupUdf::new(Arc::clone(&mock_geo))),
            1,
            "expects exactly one argument",
        ),
        (
            "grok",
            Box::new(GrokUdf::new()),
            2,
            "expects exactly two arguments",
        ),
        (
            "regexp_extract",
            Box::new(RegexpExtractUdf::new()),
            3,
            "expects exactly three arguments",
        ),
    ];

    let scalar = || {
        ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
            "val".to_string(),
        )))
    };

    for (name, udf, expected_arity, error_msg) in cases {
        // Return field type doesn't matter for arity validation.
        let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::Utf8, true));

        // Too few args
        let too_few = if expected_arity > 0 {
            (0..expected_arity - 1)
                .map(|_| scalar())
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        let args = ScalarFunctionArgs {
            args: too_few,
            number_rows: 0,
            return_field: Arc::clone(&return_field),
            arg_fields: vec![],
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(
            result.is_err(),
            "{name}: should reject too few args (expected {expected_arity})"
        );
        assert!(
            result.unwrap_err().to_string().contains(error_msg),
            "{name}: error must contain '{error_msg}'"
        );

        // Too many args
        let too_many = (0..=expected_arity).map(|_| scalar()).collect::<Vec<_>>();
        let args = ScalarFunctionArgs {
            args: too_many,
            number_rows: 1,
            return_field: Arc::clone(&return_field),
            arg_fields: vec![],
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(
            result.is_err(),
            "{name}: should reject too many args (expected {expected_arity})"
        );
        assert!(
            result.unwrap_err().to_string().contains(error_msg),
            "{name}: error must contain '{error_msg}'"
        );
    }
}
