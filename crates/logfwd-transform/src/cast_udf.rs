//! Internal cast UDF implementations (`int` and `float`).

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// UDF: int(col) — safe cast from Utf8 to Int64, returns NULL on failure.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct IntCastUdf {
    signature: Signature,
}

impl IntCastUdf {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IntCastUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "int"
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
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(array) => {
                // Safe cast: returns NULL on parse failure.
                let result = arrow::compute::cast(array, &DataType::Int64)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                // Convert scalar to single-element array, cast, convert back.
                let array = scalar
                    .to_array()
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let result = arrow::compute::cast(&array, &DataType::Int64)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let scalar_val = datafusion::common::ScalarValue::try_from_array(&result, 0)?;
                Ok(ColumnarValue::Scalar(scalar_val))
            }
        }
    }
}

/// UDF: float(col) — safe cast from Utf8 to Float64, returns NULL on failure.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct FloatCastUdf {
    signature: Signature,
}

impl FloatCastUdf {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FloatCastUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "float"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(array) => {
                let result = arrow::compute::cast(array, &DataType::Float64)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar
                    .to_array()
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let result = arrow::compute::cast(&array, &DataType::Float64)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let scalar_val = datafusion::common::ScalarValue::try_from_array(&result, 0)?;
                Ok(ColumnarValue::Scalar(scalar_val))
            }
        }
    }
}
