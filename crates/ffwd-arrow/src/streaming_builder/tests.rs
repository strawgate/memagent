//! Tests for the streaming zero-copy Arrow RecordBatch builder.

#[cfg(test)]
mod tests {
    use super::super::*;
    use arrow::array::{Array, Float64Array, Int64Array, StructArray as ArrowStructArray};
    use arrow::datatypes::DataType;

    #[test]
    fn test_basic_string_and_int() {
        let json = br"not used directly";
        let buf = bytes::Bytes::from(json.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        let idx_name = b.resolve_field(b"name");
        let idx_age = b.resolve_field(b"age");

        b.begin_row();
        // "name" value at bytes 0..3 = "not"
        b.append_str_by_idx(idx_name, &buf[0..3]);
        b.append_int_by_idx(idx_age, b"42");
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Single-type fields use bare column names (no suffix).
        assert!(batch.column_by_name("name").is_some());
        assert!(batch.column_by_name("age").is_some());
    }

    #[test]
    fn test_type_conflict_produces_struct_column() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        let idx_status = b.resolve_field(b"status");

        // Row 0: status is an int
        b.begin_row();
        b.append_int_by_idx(idx_status, b"200");
        b.end_row();

        // Row 1: status is a string -- type conflict
        b.begin_row();
        b.append_str_by_idx(idx_status, &buf[0..0]); // empty but valid str
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
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
            .expect("status struct column must exist");
        assert!(
            matches!(status_col.data_type(), DataType::Struct(_)),
            "status must be Struct, got {:?}",
            status_col.data_type()
        );
        let sa = status_col
            .as_any()
            .downcast_ref::<ArrowStructArray>()
            .unwrap();
        let child_names: Vec<&str> = sa.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(child_names.contains(&"int"), "missing int child");
        assert!(child_names.contains(&"str"), "missing str child");
    }

    #[test]
    fn test_zero_copy_string_content() {
        let data = b"hello world foobar";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"msg");
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..5]); // "hello"
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx, &buf[6..11]); // "world"
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string field: bare name
        let col = batch
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(col.value(0), "hello");
        assert_eq!(col.value(1), "world");
    }

    #[test]
    fn test_missing_fields_produce_nulls() {
        let data = b"aabb";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        let idx_a = b.resolve_field(b"a");
        let idx_b = b.resolve_field(b"b");

        b.begin_row();
        b.append_str_by_idx(idx_a, &buf[0..2]);
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx_b, &buf[2..4]);
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string fields: bare names
        let a = batch.column_by_name("a").unwrap();
        assert!(!a.is_null(0));
        assert!(a.is_null(1));
        let b_col = batch.column_by_name("b").unwrap();
        assert!(b_col.is_null(0));
        assert!(!b_col.is_null(1));
    }

    #[test]
    fn test_duplicate_key_first_writer_wins() {
        let data = b"firstsecond";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"val");
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..5]); // "first"
        b.append_str_by_idx(idx, &buf[5..11]); // "second" -- should be ignored
        b.end_row();

        let batch = b.finish_batch().unwrap();
        // Single-type string field: bare name
        let col = batch
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(col.value(0), "first");
    }

    #[test]
    fn test_empty_batch() {
        let buf = bytes::Bytes::from_static(b"");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_batch_reuse() {
        let data1 = bytes::Bytes::from_static(b"hello");
        let data2 = bytes::Bytes::from_static(b"world");
        let mut b = StreamingBuilder::new(None);

        b.begin_batch(data1.clone());
        let idx = b.resolve_field(b"x");
        b.begin_row();
        b.append_str_by_idx(idx, &data1[0..5]);
        b.end_row();
        let b1 = b.finish_batch().unwrap();
        assert_eq!(b1.num_rows(), 1);

        // begin_batch clears field_index to prevent unbounded HashMap growth
        // under key churn (the core fix for this issue).  As a result,
        // resolve_field must be called again in each new batch to re-register
        // field names; any index obtained in a previous batch is no longer valid.
        b.begin_batch(data2.clone());
        let idx2 = b.resolve_field(b"x");
        b.begin_row();
        b.append_str_by_idx(idx2, &data2[0..5]);
        b.end_row();
        let b2 = b.finish_batch().unwrap();
        assert_eq!(b2.num_rows(), 1);
        // Column must exist and hold the value from the second batch.
        let col = b2
            .column_by_name("x")
            .expect("column x must exist in batch 2")
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("x must be StringViewArray");
        assert_eq!(col.value(0), "world");
    }

    #[test]
    fn test_float_values() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);

        let idx = b.resolve_field(b"lat");
        b.begin_row();
        b.append_float_by_idx(idx, b"3.25");
        b.end_row();

        let batch = b.finish_batch().unwrap();
        // Single-type float field: bare name
        let col = batch
            .column_by_name("lat")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 3.25).abs() < 1e-10);
    }

    /// Fields beyond index 63 do not get duplicate-key detection (the bitmask
    /// only covers 64 entries). This test verifies that writing to such fields
    /// does not panic and that the batch still builds correctly.
    #[test]
    fn test_no_panic_with_65_fields_duplicate_key() {
        // Build a buffer large enough to hold all the "val" strings we'll write.
        let payload = b"aabbccdd";
        let buf = bytes::Bytes::from(payload.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        // Resolve 65 fields so the 65th field has index 64 (>= 64).
        let mut indices = Vec::new();
        for i in 0..65u8 {
            let name = format!("field{}", i);
            indices.push(b.resolve_field(name.as_bytes()));
        }
        let idx_65 = indices[64]; // index 64 -- first field outside the bitmask

        b.begin_row();
        b.append_str_by_idx(idx_65, &buf[0..2]);
        b.append_str_by_idx(idx_65, &buf[2..4]);
        b.end_row();

        // Must not panic and must produce a valid batch.
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("field64").is_some());
    }

    /// `append_line` stores zero-copy views into the buffer when `line_capture` is true.
    #[test]
    fn test_append_line_line_capture_true() {
        let data = b"hello world\ngoodbye world\n";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(Some("body".to_string()));
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"msg");

        b.begin_row();
        b.append_line(&buf[0..11]); // "hello world"
        b.append_str_by_idx(idx, &buf[0..5]); // "hello"
        b.end_row();

        b.begin_row();
        b.append_line(&buf[12..25]); // "goodbye world"
        b.append_str_by_idx(idx, &buf[12..19]); // "goodbye"
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string field: bare name
        assert!(batch.column_by_name("msg").is_some());

        let raw_col = batch
            .column_by_name("body")
            .expect("body column must be present when line_capture=true");
        let raw_arr = raw_col
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("body must be StringViewArray");
        assert_eq!(raw_arr.value(0), "hello world");
        assert_eq!(raw_arr.value(1), "goodbye world");
    }

    #[test]
    fn test_line_capture_no_append_line_calls_returns_error() {
        // Regression test: line_capture=true with no append_line() calls must return
        // an error rather than silently producing a batch without the body column.
        let buf = bytes::Bytes::from_static(b"line1\nline2\n");
        let mut b = StreamingBuilder::new(Some("body".to_string()));
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"msg");

        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..5]); // no append_line call
        b.end_row();

        b.begin_row();
        b.append_str_by_idx(idx, &buf[6..11]); // no append_line call
        b.end_row();

        let result = b.finish_batch();
        assert!(
            result.is_err(),
            "line_capture=true with no append_line() calls must error, got a batch"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("cardinality mismatch"),
            "error should mention cardinality mismatch, got: {err}"
        );
    }

    #[test]
    fn test_append_line_invalid_utf8_preserves_lossy_text() {
        let buf = bytes::Bytes::from(vec![0xff, b'\n']);
        let mut b = StreamingBuilder::new(Some("body".to_string()));
        b.begin_batch(buf.clone());
        b.begin_row();
        b.append_line(&buf[0..1]);
        b.end_row();

        let batch = b
            .finish_batch()
            .expect("invalid UTF-8 line capture should preserve row cardinality");
        let body = batch
            .column_by_name("body")
            .expect("line capture column should be present");
        if let Some(arr) = body
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
        {
            assert_eq!(arr.value(0), "\u{fffd}");
        } else if let Some(arr) = body.as_any().downcast_ref::<arrow::array::StringArray>() {
            assert_eq!(arr.value(0), "\u{fffd}");
        } else {
            panic!("body column must be StringArray or StringViewArray");
        }
    }

    #[test]
    fn test_append_line_invalid_utf8_preserves_lossy_text_detached() {
        let buf = bytes::Bytes::from(vec![0xff, b'\n']);
        let mut b = StreamingBuilder::new(Some("body".to_string()));
        b.begin_batch(buf.clone());
        b.begin_row();
        b.append_line(&buf[0..1]);
        b.end_row();

        let batch = b
            .finish_batch_detached()
            .expect("invalid UTF-8 line capture should preserve row cardinality");
        let body = batch
            .column_by_name("body")
            .expect("line capture column should be present")
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("detached line capture must produce StringArray");
        assert_eq!(body.value(0), "\u{fffd}");
    }

    /// `append_line` is a no-op when `line_capture` is false -- `body` column absent.
    #[test]
    fn test_append_line_line_capture_false() {
        let data = b"hello world\n";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        b.begin_row();
        b.append_line(&buf[0..11]);
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert!(
            batch.column_by_name("body").is_none(),
            "body must not be present when line_capture=false"
        );
    }

    #[test]
    fn test_line_capture_name_collision_prefers_line_value() {
        let data = b"whole-lineparsed";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(Some("body".to_string()));
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"body");

        b.begin_row();
        b.append_line(&buf[0..10]); // "whole-line"
        b.append_str_by_idx(idx, &buf[10..16]); // "parsed"
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.schema().fields().len(), 1);
        let body = batch
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(body.value(0), "whole-line");
    }

    #[test]
    fn test_detached_line_capture_name_collision_prefers_line_value() {
        let data = b"whole-lineparsed";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(Some("body".to_string()));
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"body");

        b.begin_row();
        b.append_line(&buf[0..10]); // "whole-line"
        b.append_str_by_idx(idx, &buf[10..16]); // "parsed"
        b.end_row();

        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.schema().fields().len(), 1);
        let body = batch
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(body.value(0), "whole-line");
    }

    /// Duplicate `append_line` calls in one row must not corrupt later rows.
    /// First writer wins: only the first `append_line` value is kept per row.
    #[test]
    fn test_append_line_duplicate_first_writer_wins() {
        let buf = bytes::Bytes::from(b"firstsecondthird".to_vec());
        let mut b = StreamingBuilder::new(Some("body".to_string())); // line_capture=true
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"msg");

        // Row 0: two append_line calls — first should win
        b.begin_row();
        b.append_line(&buf[0..5]); // "first"
        b.append_line(&buf[5..11]); // "second" — duplicate, should be ignored
        b.append_str_by_idx(idx, &buf[0..5]);
        b.end_row();

        // Row 1: single append_line
        b.begin_row();
        b.append_line(&buf[11..16]); // "third"
        b.append_str_by_idx(idx, &buf[11..16]);
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let raw_col = batch
            .column_by_name("body")
            .expect("body column must be present");
        let raw_arr = raw_col
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("body must be StringViewArray");

        // Row 0 should have "first" (first writer wins), not "second"
        assert_eq!(
            raw_arr.value(0),
            "first",
            "row 0 body should be first writer"
        );
        // Row 1 should have "third", not shifted data from row 0's second append
        assert_eq!(raw_arr.value(1), "third", "row 1 body should be correct");
    }

    /// Duplicate `append_line` calls in detached mode must not corrupt later rows.
    #[test]
    fn test_append_line_duplicate_detached() {
        let buf = bytes::Bytes::from(b"firstsecondthird".to_vec());
        let mut b = StreamingBuilder::new(Some("body".to_string())); // line_capture=true
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"msg");

        // Row 0: two append_line calls — first should win
        b.begin_row();
        b.append_line(&buf[0..5]); // "first"
        b.append_line(&buf[5..11]); // "second" — duplicate, should be ignored
        b.append_str_by_idx(idx, &buf[0..5]);
        b.end_row();

        // Row 1: single append_line
        b.begin_row();
        b.append_line(&buf[11..16]); // "third"
        b.append_str_by_idx(idx, &buf[11..16]);
        b.end_row();

        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let raw_col = batch
            .column_by_name("body")
            .expect("body column must be present");
        let raw_arr = raw_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("body must be StringArray in detached mode");

        // Row 0 should have "first" (first writer wins), not "second"
        assert_eq!(
            raw_arr.value(0),
            "first",
            "row 0 body should be first writer"
        );
        // Row 1 should have "third", not shifted data from row 0's second append
        assert_eq!(raw_arr.value(1), "third", "row 1 body should be correct");
    }

    #[test]
    fn test_read_str_out_of_bounds() {
        let buf = bytes::Bytes::from_static(b"abcd");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);

        let res = b.read_str(0, 10, false); // Out of bounds length
        assert_eq!(res, None); // Handled safely

        let res2 = b.read_str(10, 2, false); // Out of bounds offset
        assert_eq!(res2, None); // Handled safely
    }

    #[test]
    fn test_append_line_duplicate_calls() {
        let data = b"hello world\n";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(Some("body".to_string()));
        b.begin_batch(buf.clone());

        b.begin_row();
        b.append_line(&buf[0..5]);
        b.append_line(&buf[6..11]); // duplicate call in same row
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let raw_col = batch
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(raw_col.value(0), "hello");
    }

    #[test]
    fn test_dedup_above_field_64() {
        let buf = bytes::Bytes::from_static(b"value");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        // Create 65 fields
        let mut indices = Vec::new();
        for i in 0..65u8 {
            let name = format!("field{}", i);
            indices.push(b.resolve_field(name.as_bytes()));
        }
        let idx_64 = indices[64]; // index 64

        // Write row 0 twice to same field 64
        b.begin_row();
        b.append_str_by_idx(idx_64, &buf[0..3]); // "val"
        b.append_str_by_idx(idx_64, &buf[3..5]); // bytes 3..5 of "value" == "ue", should be deduped
        b.end_row();

        // Write row 1 once to field 64
        b.begin_row();
        b.append_str_by_idx(idx_64, &buf[0..5]); // "value"
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let col = batch
            .column_by_name("field64")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();

        assert_eq!(col.value(0), "val");
        assert_eq!(col.value(1), "value");
    }

    /// Struct child "int" has correct values when rows are int-typed.
    #[test]
    fn struct_child_int_values_correct() {
        let buf = bytes::Bytes::from_static(b"ERR");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        let si = b.resolve_field(b"code");
        // Make it a conflict: rows 0,1 are int, row 2 is str
        b.begin_row();
        b.append_int_by_idx(si, b"100");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(si, b"200");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, &buf[0..3]);
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("code").expect("code struct");
        assert!(matches!(col.data_type(), DataType::Struct(_)));
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let int_child = sa.column_by_name("int").expect("int child");
        let int_arr = int_child.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 100);
        assert_eq!(int_arr.value(1), 200);
        assert!(int_arr.is_null(2));
    }

    /// Struct child "str" has correct values when rows are str-typed.
    #[test]
    fn struct_child_str_values_correct() {
        let buf = bytes::Bytes::from_static(b"helloworld");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        let si = b.resolve_field(b"msg");
        // Make it a conflict: row 0 is int, rows 1,2 are str
        b.begin_row();
        b.append_int_by_idx(si, b"1");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, &buf[0..5]); // "hello"
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, &buf[5..10]); // "world"
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("msg").expect("msg struct");
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let str_child = sa.column_by_name("str").expect("str child");
        let str_arr = str_child
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert!(str_arr.is_null(0));
        assert_eq!(str_arr.value(1), "hello");
        assert_eq!(str_arr.value(2), "world");
    }

    /// Struct NullBuffer: row is non-null iff at least one child is non-null.
    #[test]
    fn struct_null_iff_all_children_null() {
        let buf = bytes::Bytes::from_static(b"OK");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        let si = b.resolve_field(b"val");
        // Row 0: int=200, str=null
        b.begin_row();
        b.append_int_by_idx(si, b"200");
        b.end_row();
        // Row 1: int=null, str="OK"
        b.begin_row();
        b.append_str_by_idx(si, &buf[0..2]);
        b.end_row();
        // Row 2: int=null, str=null
        b.begin_row();
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("val").expect("val struct");
        assert!(!col.is_null(0), "row 0 should be non-null");
        assert!(!col.is_null(1), "row 1 should be non-null");
        assert!(col.is_null(2), "row 2 should be null");
    }

    /// Three-way conflict: int, float, str all appear -- struct has all three children.
    #[test]
    fn three_way_conflict_int_float_str() {
        let buf = bytes::Bytes::from_static(b"text");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        let si = b.resolve_field(b"mixed");
        b.begin_row();
        b.append_int_by_idx(si, b"42");
        b.end_row();
        b.begin_row();
        b.append_float_by_idx(si, b"3.14");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, &buf[0..4]);
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("mixed").expect("mixed struct");
        assert!(matches!(col.data_type(), DataType::Struct(_)));
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let child_names: Vec<&str> = sa.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(child_names.contains(&"int"), "missing int child");
        assert!(child_names.contains(&"float"), "missing float child");
        assert!(child_names.contains(&"str"), "missing str child");
    }

    /// Single-type int field stays as bare flat Int64 column, not a struct.
    #[test]
    fn single_type_field_stays_flat() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let si = b.resolve_field(b"count");
        b.begin_row();
        b.append_int_by_idx(si, b"1");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(si, b"2");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(si, b"3");
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("count").expect("count column");
        assert!(
            matches!(col.data_type(), DataType::Int64),
            "single-type int must be bare Int64, got {:?}",
            col.data_type()
        );
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 2);
        assert_eq!(arr.value(2), 3);
    }

    /// A 3-way conflict (int + float + str in the same field across rows) must
    /// produce a StructArray with all three children present.
    #[test]
    fn test_three_way_conflict_int_float_str() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"mixed");

        b.begin_row();
        b.append_int_by_idx(idx, b"42");
        b.end_row();

        b.begin_row();
        b.append_float_by_idx(idx, b"3.14");
        b.end_row();

        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..0]); // empty-but-valid string view
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let col = batch.column_by_name("mixed").expect("mixed must exist");
        assert!(
            matches!(col.data_type(), DataType::Struct(_)),
            "3-way conflict must produce Struct, got {:?}",
            col.data_type()
        );
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let names: Vec<&str> = sa
            .fields()
            .iter()
            .map(|f: &arrow::datatypes::FieldRef| f.name().as_str())
            .collect();
        assert!(names.contains(&"int"), "int child missing");
        assert!(names.contains(&"float"), "float child missing");
        assert!(names.contains(&"str"), "str child missing");

        // Row 0: int=42, float=null, str=null
        let int_idx = sa
            .fields()
            .iter()
            .position(|f: &arrow::datatypes::FieldRef| f.name() == "int")
            .unwrap();
        let int_col = sa
            .column(int_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(int_col.value(0), 42);
        assert!(int_col.is_null(1));
        assert!(int_col.is_null(2));

        // Struct-level null: no row should be all-null here
        assert!(!col.is_null(0));
        assert!(!col.is_null(1));
        assert!(!col.is_null(2));
    }

    /// After a conflict batch (struct column), a second batch with only a single
    /// type for the same field must emit a flat column — not a struct.
    #[test]
    fn test_batch_reuse_conflict_then_single_type_emits_flat() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(None);

        // Batch 1: conflict → struct
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"status");
        b.begin_row();
        b.append_int_by_idx(idx, b"200");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..0]);
        b.end_row();
        let b1 = b.finish_batch().unwrap();
        assert!(
            matches!(
                b1.column_by_name("status").unwrap().data_type(),
                DataType::Struct(_)
            ),
            "batch 1 must be struct"
        );

        // Batch 2: only int — must be flat Int64
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"status");
        b.begin_row();
        b.append_int_by_idx(idx, b"404");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(idx, b"500");
        b.end_row();
        let b2 = b.finish_batch().unwrap();
        assert!(
            matches!(
                b2.column_by_name("status").unwrap().data_type(),
                DataType::Int64
            ),
            "batch 2 must be flat Int64 after clean data, got {:?}",
            b2.column_by_name("status").unwrap().data_type()
        );
    }

    /// `field_index` must not grow without bound when every batch introduces a
    /// completely new set of field names (key churn).
    ///
    /// Previously, `begin_batch` never cleared `field_index`, so each unique
    /// field name from every past batch accumulated in the HashMap forever.
    /// After the fix, `begin_batch` clears `field_index` and resets
    /// `num_active`, bounding `fields.len()` to the high-water mark of unique
    /// fields in any *single* batch.
    #[test]
    fn field_index_stays_bounded_under_key_churn() {
        let mut b = StreamingBuilder::new(None);
        const BATCHES: usize = 20;

        for i in 0..BATCHES {
            let payload = format!("val{i}");
            let buf = bytes::Bytes::from(payload.clone().into_bytes());

            b.begin_batch(buf.clone());
            // Each batch uses a different unique field name.
            let name = format!("field_{i}");
            let idx = b.resolve_field(name.as_bytes());
            b.begin_row();
            b.append_str_by_idx(idx, &buf[..]);
            b.end_row();
            let batch = b.finish_batch().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert!(
                batch.column_by_name(&name).is_some(),
                "column {name} must exist"
            );
        }

        // After all batches, the fields Vec must not have grown to BATCHES
        // entries — it should only hold the high-water mark (1 here).
        assert_eq!(
            b.fields.len(),
            1,
            "fields Vec must be bounded by max unique fields per single batch, \
             got {} (expected 1)",
            b.fields.len()
        );
        assert_eq!(
            b.field_index.len(),
            1,
            "field_index must hold only the current batch's entries, \
             got {} (expected 1)",
            b.field_index.len()
        );
    }

    /// Repeated batches with the *same* field names reuse pre-allocated
    /// `FieldColumns` slots (inner Vec capacity is preserved across batches).
    #[test]
    fn stable_fields_reuse_slots_across_batches() {
        let data1 = bytes::Bytes::from_static(b"web1200");
        let data2 = bytes::Bytes::from_static(b"web2404");
        let mut b = StreamingBuilder::new(None);

        // Prime the builder so slots are allocated.
        b.begin_batch(data1.clone());
        let h = b.resolve_field(b"host");
        let s = b.resolve_field(b"status");
        b.begin_row();
        b.append_str_by_idx(h, &data1[0..4]); // "web1"
        b.append_int_by_idx(s, b"200");
        b.end_row();
        let _ = b.finish_batch().unwrap();

        // Second batch: same fields. Slots must be reused (fields.len() stays 2).
        b.begin_batch(data2.clone());
        let h2 = b.resolve_field(b"host");
        let s2 = b.resolve_field(b"status");
        b.begin_row();
        b.append_str_by_idx(h2, &data2[0..4]); // "web2"
        b.append_int_by_idx(s2, b"404");
        b.end_row();
        let batch = b.finish_batch().unwrap();

        assert_eq!(
            b.fields.len(),
            2,
            "fields Vec must not grow when same fields reappear"
        );
        assert_eq!(batch.num_rows(), 1);
        // Slot indices match here because both batches resolved fields in the
        // same order ("host" first, "status" second), so the slot-reuse
        // algorithm assigns the same indices.  The important invariant is that
        // `fields.len()` stayed bounded — not that the indices are identical.
        assert_eq!(h, h2, "same field name must resolve to the same slot index");
        assert_eq!(s, s2, "same field name must resolve to the same slot index");
        assert_eq!(
            batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .unwrap()
                .value(0),
            "web2"
        );
    }

    // -----------------------------------------------------------------------
    // Protocol-violation tests: debug_assert fires on incorrectly wired callers.
    // These are only meaningful in debug builds where debug_assert is active.
    // -----------------------------------------------------------------------

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "begin_row called outside of a batch")]
    fn test_begin_row_without_batch_panics() {
        let mut b = StreamingBuilder::new(None);
        b.begin_row(); // no begin_batch — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "end_row called without a matching begin_row")]
    fn test_end_row_without_begin_row_panics() {
        let buf = bytes::Bytes::from_static(b"x");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        b.end_row(); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "begin_batch called while inside a row")]
    fn test_begin_batch_inside_row_panics() {
        let buf = bytes::Bytes::from_static(b"x");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        b.begin_row();
        b.begin_batch(buf); // inside a row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_str_by_idx called outside of a row")]
    fn test_append_str_outside_row_panics() {
        let buf = bytes::Bytes::from_static(b"val");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"x");
        b.append_str_by_idx(idx, &buf[0..3]); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_int_by_idx called outside of a row")]
    fn test_append_int_outside_row_panics() {
        let buf = bytes::Bytes::from_static(b"42");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"n");
        b.append_int_by_idx(idx, b"42"); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_float_by_idx called outside of a row")]
    fn test_append_float_outside_row_panics() {
        let buf = bytes::Bytes::from_static(b"1.5");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"f");
        b.append_float_by_idx(idx, b"1.5"); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_null_by_idx called outside of a row")]
    fn test_append_null_outside_row_panics() {
        let buf = bytes::Bytes::from_static(b"x");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"n");
        b.append_null_by_idx(idx); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "finish_batch called outside of a batch")]
    fn test_finish_batch_without_batch_panics() {
        let mut b = StreamingBuilder::new(None);
        let _ = b.finish_batch(); // no begin_batch — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "resolve_field called outside of an active batch")]
    fn test_resolve_field_without_batch_panics() {
        let mut b = StreamingBuilder::new(None);
        b.resolve_field(b"x"); // no begin_batch — must panic
    }

    // --- finish_batch_detached tests ---

    #[test]
    fn test_detached_basic_string_and_int() {
        let json = b"{\"name\":\"alice\",\"age\":30}\n";
        let buf = bytes::Bytes::from(json.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        let idx_name = b.resolve_field(b"name");
        let idx_age = b.resolve_field(b"age");

        b.begin_row();
        b.append_str_by_idx(idx_name, &buf[9..14]); // "alice"
        b.append_int_by_idx(idx_age, b"30");
        b.end_row();

        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Strings are StringArray (Utf8), not StringViewArray (Utf8View)
        let name_col = batch.column_by_name("name").unwrap();
        assert_eq!(*name_col.data_type(), DataType::Utf8);
        let arr = name_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should be StringArray");
        assert_eq!(arr.value(0), "alice");

        // Int column unchanged
        let age_col = batch.column_by_name("age").unwrap();
        let int_arr = age_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 30);
    }

    #[test]
    fn test_detached_not_pinned_to_input() {
        let json = b"{\"msg\":\"hello world\"}\n";
        let buf = bytes::Bytes::from(json.to_vec());
        let buf_start = buf.as_ptr() as usize;
        let buf_end = buf_start + buf.len();

        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"msg");
        b.begin_row();
        b.append_str_by_idx(idx, &buf[8..19]); // "hello world"
        b.end_row();

        let batch = b.finish_batch_detached().unwrap();

        // Verify no buffer in the batch points into the input
        for col in batch.columns() {
            let arr_data = col.to_data();
            for buffer in arr_data.buffers() {
                let ptr = buffer.as_ptr() as usize;
                let end = ptr + buffer.len();
                assert!(
                    ptr >= buf_end || end <= buf_start,
                    "owned batch should not reference input buffer"
                );
            }
        }
    }

    #[test]
    fn test_detached_type_conflict_struct() {
        let buf = bytes::Bytes::from_static(b"unused_padding");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"val");
        b.begin_row();
        b.append_int_by_idx(idx, b"42");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..6]); // "unused"
        b.end_row();

        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let col = batch.column_by_name("val").unwrap();
        assert!(
            matches!(col.data_type(), DataType::Struct(_)),
            "conflict should produce Struct"
        );
        // The str child should be Utf8 (not Utf8View)
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let str_child = sa.column_by_name("str").unwrap();
        assert_eq!(*str_child.data_type(), DataType::Utf8);
    }

    #[test]
    fn test_detached_missing_fields_nulls() {
        let buf = bytes::Bytes::from_static(b"abcdef");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());

        let idx_a = b.resolve_field(b"a");
        let idx_b = b.resolve_field(b"b");

        b.begin_row();
        b.append_str_by_idx(idx_a, &buf[0..3]); // "abc"
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx_b, &buf[3..6]); // "def"
        b.end_row();

        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let a_col = batch
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(!a_col.is_null(0));
        assert!(a_col.is_null(1));
    }

    #[test]
    fn test_detached_batch_reuse() {
        let buf1 = bytes::Bytes::from_static(b"hello");
        let buf2 = bytes::Bytes::from_static(b"world");
        let mut b = StreamingBuilder::new(None);

        b.begin_batch(buf1.clone());
        let idx = b.resolve_field(b"x");
        b.begin_row();
        b.append_str_by_idx(idx, &buf1[..]);
        b.end_row();
        let batch1 = b.finish_batch_detached().unwrap();

        b.begin_batch(buf2.clone());
        let idx = b.resolve_field(b"x");
        b.begin_row();
        b.append_str_by_idx(idx, &buf2[..]);
        b.end_row();
        let batch2 = b.finish_batch_detached().unwrap();

        assert_eq!(batch1.num_rows(), 1);
        assert_eq!(batch2.num_rows(), 1);
        let arr = batch2
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "world");
    }

    #[test]
    fn test_detached_empty_batch() {
        let buf = bytes::Bytes::from_static(b"\n");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_line_capture_column_exists_on_empty_batch_in_both_finish_paths() {
        let mut view_builder = StreamingBuilder::new(Some("body".to_string()));
        view_builder.begin_batch(bytes::Bytes::from_static(b""));
        let view_batch = view_builder.finish_batch().expect("finish view batch");
        assert_eq!(view_batch.num_rows(), 0);
        assert!(
            view_batch.column_by_name("body").is_some(),
            "line capture column should exist on empty view batch"
        );

        let mut detached_builder = StreamingBuilder::new(Some("body".to_string()));
        detached_builder.begin_batch(bytes::Bytes::from_static(b""));
        let detached_batch = detached_builder
            .finish_batch_detached()
            .expect("finish detached batch");
        assert_eq!(detached_batch.num_rows(), 0);
        assert!(
            detached_batch.column_by_name("body").is_some(),
            "line capture column should exist on empty detached batch"
        );
    }

    #[test]
    fn test_detached_float_values() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"lat");
        b.begin_row();
        b.append_float_by_idx(idx, b"3.14");
        b.end_row();
        b.begin_row();
        b.end_row(); // missing → null
        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let col = batch
            .column_by_name("lat")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 3.14).abs() < 1e-10);
        assert!(col.is_null(1));
    }

    #[test]
    fn test_detached_line_capture() {
        let json = b"{\"msg\":\"hi\"}\n";
        let buf = bytes::Bytes::from(json.to_vec());
        let mut b = StreamingBuilder::new(Some("body".to_string()));
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"msg");
        b.begin_row();
        b.append_str_by_idx(idx, &buf[8..10]); // "hi"
        b.append_line(&buf[0..12]); // full line
        b.end_row();
        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let raw_col = batch.column_by_name("body").unwrap();
        assert_eq!(*raw_col.data_type(), DataType::Utf8);
        let arr = raw_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "{\"msg\":\"hi\"}");
    }

    #[test]
    fn test_detached_decoded_strings() {
        // Simulate a JSON string with escape sequences that go through
        // append_decoded_str_by_idx (decoded_buf path).
        let json = b"hello world padding";
        let buf = bytes::Bytes::from(json.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"msg");
        b.begin_row();
        // Decoded string goes into decoded_buf, not the original buffer
        b.append_decoded_str_by_idx(idx, b"decoded value");
        b.end_row();
        let batch = b.finish_batch_detached().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let col = batch
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "decoded value");
    }

    #[test]
    fn finish_batch_keeps_original_and_decoded_buffers_separate() {
        let json = b"original string value longer than inline";
        let buf = bytes::Bytes::from(json.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"msg");

        b.begin_row();
        b.append_str_by_idx(idx, &buf[..]);
        b.end_row();

        b.begin_row();
        b.append_decoded_str_by_idx(idx, b"decoded string value longer than inline");
        b.end_row();

        let batch = b.finish_batch().expect("finish batch");
        let col = batch
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();

        assert_eq!(col.value(0), "original string value longer than inline");
        assert_eq!(col.value(1), "decoded string value longer than inline");
        assert_eq!(
            col.data_buffers().len(),
            2,
            "decoded strings should add a second StringView block, not concatenate into one buffer"
        );
        assert_eq!(col.data_buffers()[0].len(), json.len());
        assert_eq!(
            col.data_buffers()[1].len(),
            b"decoded string value longer than inline".len()
        );
    }

    #[test]
    fn finish_batch_preserves_empty_decoded_string_without_decoded_block() {
        let json = b"original";
        let buf = bytes::Bytes::from(json.to_vec());
        let mut b = StreamingBuilder::new(None);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"msg");

        b.begin_row();
        b.append_decoded_str_by_idx(idx, b"");
        b.end_row();

        let batch = b.finish_batch().expect("finish batch");
        let col = batch
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();

        assert_eq!(col.value(0), "");
        assert_eq!(
            col.data_buffers().len(),
            1,
            "empty decoded strings should not require a decoded StringView block"
        );
    }

    #[test]
    fn test_resource_columns_injected_in_finish_batch() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(None);
        b.set_resource_attributes(&[
            ("service.name".to_string(), "checkout".to_string()),
            ("k8s.namespace".to_string(), "prod".to_string()),
        ]);
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"message");
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..4]);
        b.end_row();

        let batch = b.finish_batch().expect("finish batch");
        assert_eq!(batch.num_rows(), 1);

        let service = batch
            .column_by_name("resource.attributes.service.name")
            .expect("service resource column");
        let service = service
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("Utf8View resource column");
        assert_eq!(service.value(0), "checkout");

        let namespace = batch
            .column_by_name("resource.attributes.k8s.namespace")
            .expect("namespace resource column");
        let namespace = namespace
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("Utf8View resource column");
        assert_eq!(namespace.value(0), "prod");

        let schema = batch.schema();
        assert!(
            schema
                .field_with_name("resource.attributes.service.name")
                .expect("service field")
                .metadata()
                .is_empty()
        );
        assert!(
            schema
                .field_with_name("resource.attributes.k8s.namespace")
                .expect("namespace field")
                .metadata()
                .is_empty()
        );
    }

    #[test]
    fn test_resource_columns_exist_on_empty_batch_in_both_finish_paths() {
        let attrs = vec![("service.name".to_string(), "checkout".to_string())];

        let mut view_builder = StreamingBuilder::new(None);
        view_builder.set_resource_attributes(attrs.as_slice());
        view_builder.begin_batch(bytes::Bytes::from_static(b""));
        let view_batch = view_builder.finish_batch().expect("finish view batch");
        assert_eq!(view_batch.num_rows(), 0);
        assert!(
            view_batch
                .column_by_name("resource.attributes.service.name")
                .is_some(),
            "empty view batch should preserve resource.attributes.* schema"
        );

        let mut detached_builder = StreamingBuilder::new(None);
        detached_builder.set_resource_attributes(attrs.as_slice());
        detached_builder.begin_batch(bytes::Bytes::from_static(b""));
        let detached_batch = detached_builder
            .finish_batch_detached()
            .expect("finish detached batch");
        assert_eq!(detached_batch.num_rows(), 0);
        assert!(
            detached_batch
                .column_by_name("resource.attributes.service.name")
                .is_some(),
            "empty detached batch should preserve resource.attributes.* schema"
        );
    }

    /// Verify that finish_batch() and finish_batch_detached() produce the same
    /// data (same values, same nulls) just with different string types
    /// (Utf8View vs Utf8).
    #[test]
    fn test_finish_batch_equivalence() {
        use arrow::array::Array;

        let input = b"{\"name\":\"alice\",\"score\":95}\n{\"name\":\"bob\",\"extra\":\"x\"}\n{\"score\":100}\n";
        let buf = bytes::Bytes::from(input.to_vec());

        // Run through finish_batch (StringViewArray)
        let mut b1 = StreamingBuilder::new(None);
        b1.begin_batch(buf.clone());
        populate_builder(&mut b1, &buf, input);
        let view_batch = b1.finish_batch().unwrap();

        // Run through finish_batch_detached (StringArray)
        let mut b2 = StreamingBuilder::new(None);
        b2.begin_batch(buf.clone());
        populate_builder(&mut b2, &buf, input);
        let owned_batch = b2.finish_batch_detached().unwrap();

        // Same shape
        assert_eq!(view_batch.num_rows(), owned_batch.num_rows());
        assert_eq!(view_batch.num_columns(), owned_batch.num_columns());

        // Same column names
        let view_names: Vec<_> = view_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let owned_names: Vec<_> = owned_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(view_names, owned_names);

        // Same values at every position
        for name in &view_names {
            let view_col = view_batch.column_by_name(name).unwrap();
            let owned_col = owned_batch.column_by_name(name).unwrap();
            assert_eq!(
                view_col.len(),
                owned_col.len(),
                "column {name} length mismatch"
            );
            for row in 0..view_col.len() {
                assert_eq!(
                    view_col.is_null(row),
                    owned_col.is_null(row),
                    "column {name} row {row} null mismatch"
                );
                if !view_col.is_null(row) {
                    // Compare as string representation for type-agnostic comparison
                    let view_val = array_value_to_string(view_col, row);
                    let owned_val = array_value_to_string(owned_col, row);
                    assert_eq!(
                        view_val, owned_val,
                        "column {name} row {row} value mismatch"
                    );
                }
            }
        }
    }

    /// Populate a builder with multi-row input for equivalence testing.
    fn populate_builder(b: &mut StreamingBuilder, buf: &bytes::Bytes, _input: &[u8]) {
        // Row 0: {"name":"alice","score":95}
        let idx_name = b.resolve_field(b"name");
        let idx_score = b.resolve_field(b"score");
        let idx_extra = b.resolve_field(b"extra");
        b.begin_row();
        b.append_str_by_idx(idx_name, &buf[9..14]); // "alice"
        b.append_int_by_idx(idx_score, b"95");
        b.end_row();
        // Row 1: {"name":"bob","extra":"x"}
        b.begin_row();
        b.append_str_by_idx(idx_name, &buf[28..31]); // "bob" (approx offset)
        b.append_str_by_idx(idx_extra, &buf[0..1]); // just "{"
        b.end_row();
        // Row 2: {"score":100}
        b.begin_row();
        b.append_int_by_idx(idx_score, b"100");
        b.end_row();
    }

    fn array_value_to_string(col: &dyn Array, row: usize) -> String {
        use arrow::array::StringViewArray;
        if let Some(a) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            return a.value(row).to_string();
        }
        if let Some(a) = col.as_any().downcast_ref::<StringViewArray>() {
            return a.value(row).to_string();
        }
        if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            return a.value(row).to_string();
        }
        if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
            return a.value(row).to_string();
        }
        format!("<unknown type at row {row}>")
    }
}
