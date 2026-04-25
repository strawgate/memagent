//! Kani formal verification proofs for the streaming builder.
#![cfg(kani)]

use super::*;

/// Prove begin_batch resets per-batch builder state while preserving the
/// reusable field storage.
///
/// Unit tests cover the expensive `finish_batch_detached` / `RecordBatch`
/// integration path. Kani stays focused on the builder bookkeeping.
#[kani::proof]
#[kani::solver(kissat)]
fn verify_single_field_batch_created() {
    let mut b = StreamingBuilder::new(Some("line".to_string()));
    b.fields.push(FieldColumns::new(b"x"));
    b.num_active = 1;
    b.line_views.push((1, 2));
    b.decoded_buf.extend_from_slice(b"decoded");
    b.begin_batch(bytes::Bytes::from_static(b"test data pad"));
    assert_eq!(b.num_active, 0);
    assert_eq!(b.lifecycle.row_count(), 0);
    assert!(b.line_views.is_empty());
    assert!(b.decoded_buf.is_empty());
    assert_eq!(b.lifecycle.state(), BuilderState::InBatch);
    assert_eq!(b.fields.len(), 1);
}

/// Prove begin_row/end_row increments row_count exactly once per row.
#[kani::proof]
#[kani::unwind(5)]
#[kani::solver(kissat)]
fn verify_int_field_row_count_matches() {
    let num_rows: u32 = kani::any();
    kani::assume(num_rows <= 3);
    let mut b = StreamingBuilder::new(None);
    b.begin_batch(bytes::Bytes::from_static(b"pad"));
    for _ in 0..num_rows {
        b.begin_row();
        b.end_row();
    }
    assert_eq!(b.lifecycle.row_count(), num_rows);
    kani::cover!(num_rows == 0, "empty batch");
    kani::cover!(num_rows > 0, "non-empty batch");
}
