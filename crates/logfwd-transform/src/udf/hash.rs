use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
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
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
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
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(array) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let mut builder = UInt64Array::builder(string_array.len());

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let val = string_array.value(i);
                        let mut hasher = DefaultHasher::new();
                        val.hash(&mut hasher);
                        builder.append_value(hasher.finish());
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(val))) => {
                let mut hasher = DefaultHasher::new();
                val.hash(&mut hasher);
                Ok(ColumnarValue::Scalar(ScalarValue::UInt64(Some(
                    hasher.finish(),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::UInt64(None)))
            }
            ColumnarValue::Scalar(_) => Err(datafusion::error::DataFusionError::Execution(
                "hash() expected string argument".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_deterministic() {
        let val1 = "test-trace-id-12345";
        let val2 = "test-trace-id-12345";

        let mut hasher1 = DefaultHasher::new();
        val1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        val2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }
}
