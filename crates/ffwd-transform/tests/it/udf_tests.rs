use std::sync::Arc;

use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use ffwd_transform::enrichment::GeoDatabase;
use ffwd_transform::enrichment::GeoResult;
use ffwd_transform::udf::{
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
fn test_hash_udf_missing_args() {
    let udf = HashUdf::new();
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::UInt64, true));
    let args = ScalarFunctionArgs {
        args: vec![],
        number_rows: 0,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly one argument")
    );
}

#[test]
fn test_hash_udf_extra_args() {
    let udf = HashUdf::new();
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::UInt64, true));
    let scalar = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
        "val".to_string(),
    )));
    let args = ScalarFunctionArgs {
        args: vec![scalar.clone(), scalar],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly one argument")
    );
}

#[test]
fn test_json_extract_udf_missing_args() {
    let udf = JsonExtractUdf::new(JsonExtractMode::Str);
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::Utf8, true));
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Utf8(Some("val".to_string())),
        )],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly two arguments")
    );
}

#[test]
fn test_json_extract_udf_extra_args() {
    let udf = JsonExtractUdf::new(JsonExtractMode::Str);
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::Utf8, true));
    let scalar = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
        "val".to_string(),
    )));
    let args = ScalarFunctionArgs {
        args: vec![scalar.clone(), scalar.clone(), scalar],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly two arguments")
    );
}

#[test]
fn test_geo_lookup_udf_missing_args() {
    let udf = GeoLookupUdf::new(Arc::new(MockGeoDatabase));
    let return_field = Arc::new(arrow::datatypes::Field::new(
        "r",
        DataType::Struct(arrow::datatypes::Fields::empty()),
        true,
    ));
    let args = ScalarFunctionArgs {
        args: vec![],
        number_rows: 0,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly one argument")
    );
}

#[test]
fn test_geo_lookup_udf_extra_args() {
    let udf = GeoLookupUdf::new(Arc::new(MockGeoDatabase));
    let return_field = Arc::new(arrow::datatypes::Field::new(
        "r",
        DataType::Struct(arrow::datatypes::Fields::empty()),
        true,
    ));
    let scalar = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
        "1.2.3.4".to_string(),
    )));
    let args = ScalarFunctionArgs {
        args: vec![scalar.clone(), scalar],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly one argument")
    );
}

#[test]
fn test_grok_udf_missing_args() {
    let udf = GrokUdf::new();
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::Utf8, true));
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Utf8(Some("val".to_string())),
        )],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly two arguments")
    );
}

#[test]
fn test_grok_udf_extra_args() {
    let udf = GrokUdf::new();
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::Utf8, true));
    let scalar = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
        "val".to_string(),
    )));
    let args = ScalarFunctionArgs {
        args: vec![scalar.clone(), scalar.clone(), scalar],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly two arguments")
    );
}

#[test]
fn test_regexp_extract_udf_missing_args() {
    let udf = RegexpExtractUdf::new();
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::Utf8, true));
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
                "val".to_string(),
            ))),
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
                "pattern".to_string(),
            ))),
        ],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly three arguments")
    );
}

#[test]
fn test_regexp_extract_udf_extra_args() {
    let udf = RegexpExtractUdf::new();
    let return_field = Arc::new(arrow::datatypes::Field::new("r", DataType::Utf8, true));
    let scalar = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
        "val".to_string(),
    )));
    let args = ScalarFunctionArgs {
        args: vec![scalar.clone(), scalar.clone(), scalar.clone(), scalar],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expects exactly three arguments")
    );
}
