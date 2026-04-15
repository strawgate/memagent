use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray, UInt64Array};
use arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// UDF: hash(col) — computes a deterministic hash of a string, returning UInt64.
/// Used primarily for tail-based sampling decisions.
#[derive(Debug)]
pub struct HashUdf {
    signature: Signature,
}

impl Default for HashUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl HashUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for HashUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "hash() expects exactly one argument".to_string(),
            ));
        }

        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(array) => {
                let dt = array.data_type();
                let (_len, mut builder) = match dt {
                    DataType::Utf8 => {
                        let string_array = array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                datafusion::error::DataFusionError::Internal(
                                    "failed to downcast Utf8 to StringArray".to_string(),
                                )
                            })?;
                        let mut builder = UInt64Array::builder(string_array.len());
                        for i in 0..string_array.len() {
                            if string_array.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(fnv1a_hash(string_array.value(i).as_bytes()));
                            }
                        }
                        (string_array.len(), builder)
                    }
                    DataType::Utf8View => {
                        let string_array = array
                            .as_any()
                            .downcast_ref::<StringViewArray>()
                            .ok_or_else(|| {
                                datafusion::error::DataFusionError::Internal(
                                    "failed to downcast Utf8View to StringViewArray".to_string(),
                                )
                            })?;
                        let mut builder = UInt64Array::builder(string_array.len());
                        for i in 0..string_array.len() {
                            if string_array.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(fnv1a_hash(string_array.value(i).as_bytes()));
                            }
                        }
                        (string_array.len(), builder)
                    }
                    DataType::LargeUtf8 => {
                        let string_array = array
                            .as_any()
                            .downcast_ref::<LargeStringArray>()
                            .ok_or_else(|| {
                                datafusion::error::DataFusionError::Internal(
                                    "failed to downcast LargeUtf8 to LargeStringArray".to_string(),
                                )
                            })?;
                        let mut builder = UInt64Array::builder(string_array.len());
                        for i in 0..string_array.len() {
                            if string_array.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(fnv1a_hash(string_array.value(i).as_bytes()));
                            }
                        }
                        (string_array.len(), builder)
                    }
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "hash() expected string argument, got {:?}",
                            dt
                        )));
                    }
                };

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(val))
                | ScalarValue::Utf8View(Some(val))
                | ScalarValue::LargeUtf8(Some(val)),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::UInt64(Some(
                fnv1a_hash(val.as_bytes()),
            )))),
            ColumnarValue::Scalar(
                ScalarValue::Utf8(None)
                | ScalarValue::Utf8View(None)
                | ScalarValue::LargeUtf8(None),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::UInt64(None))),
            ColumnarValue::Scalar(_) => Err(datafusion::error::DataFusionError::Execution(
                "hash() expected string argument".to_string(),
            )),
        }
    }
}

fn fnv1a_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_deterministic() {
        let val1 = "test-trace-id-12345";
        let val2 = "test-trace-id-12345";

        let hash1 = fnv1a_hash(val1.as_bytes());
        let hash2 = fnv1a_hash(val2.as_bytes());

        assert_eq!(hash1, hash2);
        // Ensure it's stable across runs and Rust versions
        assert_eq!(hash1, 10607781026064820607);
    }
}
