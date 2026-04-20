/// Reusable Arrow `RecordBatch` generators for property-based tests.
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, ListBuilder, NullArray,
    StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use arrow::record_batch::RecordBatch;
use proptest::prelude::*;

fn unicode_string(max_len: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 0..=max_len)
        .prop_map(|chars| chars.into_iter().collect())
}

fn list_i64_array(values: Vec<Option<Vec<Option<i64>>>>) -> ListArray {
    let mut builder = ListBuilder::new(arrow::array::Int64Builder::new());
    for row in values {
        match row {
            Some(items) => {
                for item in items {
                    builder.values().append_option(item);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    builder.finish()
}

fn struct_array(
    utf8_values: Vec<Option<String>>,
    int_values: Vec<Option<i64>>,
    valid_rows: Vec<bool>,
) -> StructArray {
    let fields: Fields = vec![
        Arc::new(Field::new("inner_text", DataType::Utf8, true)),
        Arc::new(Field::new("inner_int", DataType::Int64, true)),
    ]
    .into();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            utf8_values.iter().map(Option::as_deref).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(int_values)),
    ];
    let validity = Some(NullBuffer::from(valid_rows));
    StructArray::new(fields, arrays, validity)
}

fn arb_record_batch_impl(include_null_column: bool) -> impl Strategy<Value = RecordBatch> {
    (0usize..=20).prop_flat_map(move |rows| {
        let utf8 = proptest::collection::vec(prop::option::of(unicode_string(12)), rows);
        let ints = proptest::collection::vec(prop::option::of(any::<i64>()), rows);
        let floats = proptest::collection::vec(prop::option::of(any::<f64>()), rows);
        let bools = proptest::collection::vec(prop::option::of(any::<bool>()), rows);
        let ts_nanos = proptest::collection::vec(prop::option::of(any::<i64>()), rows);
        let list_values = proptest::collection::vec(
            prop::option::of(proptest::collection::vec(
                prop::option::of(any::<i64>()),
                0..=4,
            )),
            rows,
        );
        let struct_text = proptest::collection::vec(prop::option::of(unicode_string(8)), rows);
        let struct_int = proptest::collection::vec(prop::option::of(any::<i64>()), rows);
        let struct_valid = proptest::collection::vec(any::<bool>(), rows);

        (
            Just(rows),
            utf8,
            ints,
            floats,
            bools,
            ts_nanos,
            list_values,
            struct_text,
            struct_int,
            struct_valid,
            Just(include_null_column),
        )
            .prop_map(
                |(
                    rows,
                    utf8,
                    ints,
                    floats,
                    bools,
                    ts_nanos,
                    list_values,
                    struct_text,
                    struct_int,
                    struct_valid,
                    include_null_column,
                )| {
                    let mut fields = vec![
                        Field::new("text", DataType::Utf8, true),
                        Field::new("count", DataType::Int64, true),
                        Field::new("ratio", DataType::Float64, true),
                        Field::new("ok", DataType::Boolean, true),
                    ];
                    if include_null_column {
                        fields.push(Field::new("always_null", DataType::Null, true));
                    }
                    fields.extend([
                        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                        Field::new(
                            "items",
                            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                            true,
                        ),
                        Field::new(
                            "nested",
                            DataType::Struct(
                                vec![
                                    Arc::new(Field::new("inner_text", DataType::Utf8, true)),
                                    Arc::new(Field::new("inner_int", DataType::Int64, true)),
                                ]
                                .into(),
                            ),
                            true,
                        ),
                    ]);

                    let mut columns: Vec<ArrayRef> = vec![
                        Arc::new(StringArray::from(
                            utf8.iter().map(Option::as_deref).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(ints)),
                        Arc::new(Float64Array::from(floats)),
                        Arc::new(BooleanArray::from(bools)),
                    ];
                    if include_null_column {
                        columns.push(Arc::new(NullArray::new(rows)));
                    }
                    columns.extend([
                        Arc::new(TimestampNanosecondArray::from(ts_nanos)) as ArrayRef,
                        Arc::new(list_i64_array(list_values)) as ArrayRef,
                        Arc::new(struct_array(struct_text, struct_int, struct_valid)) as ArrayRef,
                    ]);

                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns).expect("generated batch must be valid")
                },
            )
    })
}

/// Strategy for batches including a `Null`-typed column.
pub fn arb_record_batch() -> impl Strategy<Value = RecordBatch> {
    arb_record_batch_impl(true)
}

/// Strategy for batches excluding the `Null`-typed column.
#[allow(dead_code)]
pub fn arb_record_batch_without_null_column() -> impl Strategy<Value = RecordBatch> {
    arb_record_batch_impl(false)
}
