//! Internal cast UDF implementations (`int` and `float`).

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

fn cast_or_passthrough_array(
    array: &std::sync::Arc<dyn arrow::array::Array>,
    target_type: &DataType,
) -> datafusion::common::Result<ColumnarValue> {
    if array.data_type() == target_type {
        return Ok(ColumnarValue::Array(array.clone()));
    }

    let result = arrow::compute::cast(array, target_type)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    Ok(ColumnarValue::Array(result))
}

fn cast_scalar_to(
    scalar: &datafusion::common::ScalarValue,
    target_type: &DataType,
) -> datafusion::common::Result<datafusion::common::ScalarValue> {
    let array = scalar
        .to_array()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let result = arrow::compute::cast(&array, target_type)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    datafusion::common::ScalarValue::try_from_array(&result, 0)
}

/// UDF: int(col) — safe cast from strings/numerics to Int64, returns NULL on
/// parse failure.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct IntCastUdf {
    signature: Signature,
}

impl IntCastUdf {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Numeric(1),
                ]),
                Volatility::Immutable,
            ),
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
            ColumnarValue::Array(array) => cast_or_passthrough_array(array, &DataType::Int64),
            ColumnarValue::Scalar(scalar @ datafusion::common::ScalarValue::Int64(_)) => {
                Ok(ColumnarValue::Scalar(scalar.clone()))
            }
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(cast_scalar_to(
                scalar,
                &DataType::Int64,
            )?)),
        }
    }
}

/// UDF: float(col) — safe cast from strings/numerics to Float64, returns NULL
/// on parse failure.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct FloatCastUdf {
    signature: Signature,
}

impl FloatCastUdf {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Numeric(1),
                ]),
                Volatility::Immutable,
            ),
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
            ColumnarValue::Array(array) => cast_or_passthrough_array(array, &DataType::Float64),
            ColumnarValue::Scalar(scalar @ datafusion::common::ScalarValue::Float64(_)) => {
                Ok(ColumnarValue::Scalar(scalar.clone()))
            }
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(cast_scalar_to(
                scalar,
                &DataType::Float64,
            )?)),
        }
    }
}
