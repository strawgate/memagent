use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray, UInt64Array};
use arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ---------------------------------------------------------------------------
// FNV-1a 64-bit — specification-stable hash for sampling decisions
// ---------------------------------------------------------------------------

/// FNV-1a 64-bit hash.
///
/// Uses the [Fowler–Noll–Vo](https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function)
/// specification — algorithm and constants are fixed by the spec and will
/// never change across Rust versions or platforms.
///
/// Unlike `std::collections::hash_map::DefaultHasher`, which the Rust stdlib
/// explicitly documents as NOT guaranteed to be stable across Rust versions,
/// FNV-1a always produces identical output for the same input regardless of
/// the Rust toolchain or target platform.  This makes it suitable for
/// deterministic tail-based sampling decisions that must be consistent
/// across process restarts, deployments, and Rust upgrades.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut hash = OFFSET_BASIS;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// UDF: hash(col) — computes a deterministic FNV-1a hash of a string, returning UInt64.
/// Used primarily for tail-based sampling decisions.
#[derive(Debug, PartialEq, Eq, Hash)]
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

/// Downcast `array` to `$array_ty`, iterate with null-propagation, and return
/// `(len, UInt64Array::builder)` ready to be finished.
///
/// All three string-type arms in `invoke_with_args` are structurally identical;
/// this macro eliminates the repetition while keeping the per-type error message.
macro_rules! hash_string_array {
    ($array:expr, $array_ty:ty, $type_name:literal) => {{
        let string_array = $array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                concat!("failed to downcast ", $type_name).to_string(),
            )
        })?;
        let mut builder = UInt64Array::builder(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(fnv1a_64(string_array.value(i).as_bytes()));
            }
        }
        (string_array.len(), builder)
    }};
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
                        hash_string_array!(array, StringArray, "Utf8 to StringArray")
                    }
                    DataType::Utf8View => {
                        hash_string_array!(array, StringViewArray, "Utf8View to StringViewArray")
                    }
                    DataType::LargeUtf8 => {
                        hash_string_array!(array, LargeStringArray, "LargeUtf8 to LargeStringArray")
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
            ) => Ok(ColumnarValue::Scalar(ScalarValue::UInt64(Some(fnv1a_64(
                val.as_bytes(),
            ))))),
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

#[cfg(kani)]
mod verification {
    use super::fnv1a_64;

    /// Prove the FNV-1a implementation matches the published spec constants.
    ///
    /// FNV-1a test vectors are fixed by the spec and published at
    /// https://fnv.sourceforge.net — these values must never change.
    /// Any regression (wrong constant, wrong op order XOR↔MUL, wrong seed)
    /// would silently break deterministic sampling across deployments.
    ///
    /// Note: the empty-string case proves `fnv1a_64(b"") == OFFSET_BASIS`,
    /// which verifies the seed is applied correctly (no accidental XOR of
    /// the first byte before the seed initialisation).
    #[kani::proof]
    fn verify_fnv1a_spec_constants() {
        assert_eq!(
            fnv1a_64(b""),
            14_695_981_039_346_656_037_u64,
            "empty string must equal OFFSET_BASIS"
        );
        assert_eq!(
            fnv1a_64(b"a"),
            12_638_187_200_555_641_996_u64,
            "single byte 'a' spec vector mismatch"
        );
        assert_eq!(
            fnv1a_64(b"foobar"),
            9_625_390_261_332_436_968_u64,
            "'foobar' spec vector mismatch"
        );
        kani::cover!(true, "all FNV-1a spec vectors verified");
    }

    /// Oracle proof: for all ≤4-byte inputs, production code matches a
    /// structurally independent FNV-1a reference implementation.
    ///
    /// The reference fuses XOR + multiply into a single expression
    /// `(hash ^ byte).wrapping_mul(PRIME)`, making the FNV-1a order
    /// (XOR-then-multiply) structurally explicit. If the production code
    /// were accidentally changed to FNV-1 (multiply-then-XOR), this proof
    /// would fail on any non-empty input — for example b"a" gives FNV-1a =
    /// 12638187200555641996 but FNV-1 = 84696351499449571.
    #[kani::proof]
    #[kani::unwind(6)] // ≤4 byte loop + 2 exit-check overhead
    #[kani::solver(kissat)]
    fn verify_fnv1a_oracle_bounded() {
        const N: usize = 4;
        let arr: [u8; N] = kani::any();
        let len: usize = kani::any_where(|&l| l <= N);
        let bytes = &arr[..len];

        // Reference: FNV-1a written as a single fused expression.
        // XOR-then-multiply order is the defining property of FNV-1a vs FNV-1.
        const OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
        const PRIME: u64 = 1_099_511_628_211;
        let mut reference = OFFSET_BASIS;
        let mut i = 0;
        while i < bytes.len() {
            reference = (reference ^ bytes[i] as u64).wrapping_mul(PRIME);
            i += 1;
        }

        assert_eq!(
            fnv1a_64(bytes),
            reference,
            "production FNV-1a must match fused-expression reference"
        );

        kani::cover!(bytes.is_empty(), "empty input: both return OFFSET_BASIS");
        kani::cover!(bytes.len() == 4, "4-byte input: full FNV-1a chain verified");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_known_value() {
        // Representative known-value assertion for determinism across versions.
        assert_eq!(fnv1a_64(b"test-trace-id-12345"), 10607781026064820607);
    }

    /// FNV-1a spec values — these are fixed by the algorithm and must never change.
    /// Regression test: if this fails, the hash function was replaced with a
    /// non-stable implementation and sampling decisions will break.
    #[test]
    fn test_hash_spec_stable_values() {
        // Known FNV-1a 64-bit values for these strings.
        assert_eq!(fnv1a_64(b""), 14695981039346656037);
        assert_eq!(fnv1a_64(b"a"), 12638187200555641996);
        assert_eq!(fnv1a_64(b"foobar"), 9625390261332436968);
    }

    #[test]
    fn test_hash_different_inputs_differ() {
        assert_ne!(fnv1a_64(b"trace-A"), fnv1a_64(b"trace-B"));
    }
}
