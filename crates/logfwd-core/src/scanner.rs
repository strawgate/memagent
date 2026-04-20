// scanner.rs — scanner-to-builder protocol boundary.
//
// Provides the ScanBuilder trait shared by logfwd-core's streaming JSON
// scanner and the Arrow-specific builders in logfwd-arrow.

// ---------------------------------------------------------------------------
// Protocol state
// ---------------------------------------------------------------------------

/// Protocol state for [`ScanBuilder`] implementations.
///
/// Tracks the current phase of a batch build cycle to let implementors
/// enforce the required call sequence with `debug_assert`:
///
/// ```text
/// Idle → begin_batch() → InBatch
///   InBatch → begin_row() → InRow
///     InRow → end_row() → InBatch
///   InBatch → finish_batch() → Idle
/// ```
///
/// `resolve_field` is legal in both `InBatch` and `InRow` states (field
/// indices may be pre-registered between rows).  All `append_*` calls are
/// only legal while `InRow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderState {
    /// No active batch. Initial state and state after `finish_batch`.
    Idle,
    /// A batch has been started via `begin_batch` but no row is open.
    InBatch,
    /// A row is open via `begin_row`; `end_row` has not yet been called.
    InRow,
}

// ---------------------------------------------------------------------------
// ScanBuilder trait — shared interface for both builders
// ---------------------------------------------------------------------------

/// Trait for building columnar output from scanned JSON fields.
///
/// Implementors receive field-level callbacks as the scanner walks JSON
/// objects. The call sequence per batch is:
///
/// ```text
/// // caller calls begin_batch() on the concrete type (not via this trait)
///   begin_row()
///     resolve_field(key) → idx
///     append_str_by_idx(idx, value)   // or int, float, null
///     ...more fields...
///   end_row()
///   ...more rows...
/// // caller invokes finish on the implementor directly
/// ```
///
/// **Initialization**: `begin_batch` is intentionally NOT part of this trait.
/// Each implementor has its own `begin_batch` method with a type-specific
/// signature (e.g. `StreamingBuilder::begin_batch(buf: Bytes)` takes the
/// input buffer for zero-copy view construction). Callers must invoke
/// `begin_batch` on the concrete type before passing it to
/// [`crate::json_scanner::scan_streaming`].
///
/// - `resolve_field`: maps a field name to a column index. Must be stable
///   within a batch (same key → same index). May create new columns.
/// - `append_*_by_idx`: stores a typed value at the current row for the
///   given column index. Byte slices are borrowed from the input buffer.
/// - `append_line`: stores the entire unparsed line (if line capture is enabled).
/// - First-write-wins: if a key appears twice in one row, the first value
///   is kept and subsequent writes are silently ignored.
///
/// Implementations live in `logfwd-arrow` (`StreamingBuilder`).
pub trait ScanBuilder {
    /// Start a new row.
    fn begin_row(&mut self);
    /// Finish the current row.
    fn end_row(&mut self);
    /// Resolve a field name to a column index.
    fn resolve_field(&mut self, key: &[u8]) -> usize;
    /// Append a string value at the given column index.
    fn append_str_by_idx(&mut self, idx: usize, value: &[u8]);
    /// Append a decoded string value at the given column index.
    ///
    /// Called when the value has been decoded from JSON escape sequences
    /// and may not be a direct subslice of the input buffer. Builders that
    /// use zero-copy views (e.g., `StreamingBuilder`) should override this
    /// to handle non-buffer bytes. The default delegates to
    /// [`append_str_by_idx`](Self::append_str_by_idx).
    fn append_decoded_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        self.append_str_by_idx(idx, value);
    }
    /// Append an integer value (as raw ASCII digits) at the given column index.
    fn append_int_by_idx(&mut self, idx: usize, value: &[u8]);
    /// Append a float value (as raw ASCII) at the given column index.
    fn append_float_by_idx(&mut self, idx: usize, value: &[u8]);
    /// Append a boolean value at the given column index.
    fn append_bool_by_idx(&mut self, idx: usize, value: bool);
    /// Record a null value at the given column index.
    fn append_null_by_idx(&mut self, idx: usize);
    /// Store the unparsed line (only called when line capture is enabled).
    fn append_line(&mut self, line: &[u8]);
}

// ---------------------------------------------------------------------------
// Kani proofs — BuilderState protocol
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod kani_proofs {
    use super::BuilderState;

    /// Operations for the Builder protocol.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum BuilderOp {
        BeginBatch,
        BeginRow,
        EndRow,
        FinishBatch,
        ResolveField,
        Append,
    }

    impl BuilderOp {
        fn from_u8(op: u8) -> Self {
            match op % 6 {
                0 => Self::BeginBatch,
                1 => Self::BeginRow,
                2 => Self::EndRow,
                3 => Self::FinishBatch,
                4 => Self::ResolveField,
                5 => Self::Append,
                _ => unreachable!(),
            }
        }
    }

    /// Simulate a state transition. Returns `Some(next_state)` for valid
    /// transitions and `None` for invalid ones.
    fn transition(state: BuilderState, op: BuilderOp) -> Option<BuilderState> {
        match (state, op) {
            (BuilderState::Idle, BuilderOp::BeginBatch) => Some(BuilderState::InBatch),
            (BuilderState::InBatch, BuilderOp::BeginRow) => Some(BuilderState::InRow),
            (BuilderState::InRow, BuilderOp::EndRow) => Some(BuilderState::InBatch),
            (BuilderState::InBatch, BuilderOp::FinishBatch) => Some(BuilderState::Idle),
            (BuilderState::InBatch, BuilderOp::ResolveField)
            | (BuilderState::InRow, BuilderOp::ResolveField) => Some(state),
            (BuilderState::InRow, BuilderOp::Append) => Some(state),
            _ => None, // invalid transition
        }
    }

    /// Independent oracle to verify the protocol.
    struct Oracle {
        in_batch: bool,
        in_row: bool,
        rows_opened: u8,
        rows_closed: u8,
    }

    impl Oracle {
        fn new() -> Self {
            Self {
                in_batch: false,
                in_row: false,
                rows_opened: 0,
                rows_closed: 0,
            }
        }

        fn check_and_apply(&mut self, op: BuilderOp) -> bool {
            match op {
                BuilderOp::BeginBatch => {
                    if self.in_batch {
                        return false;
                    }
                    self.in_batch = true;
                    true
                }
                BuilderOp::BeginRow => {
                    if !self.in_batch || self.in_row {
                        return false;
                    }
                    self.in_row = true;
                    self.rows_opened = self.rows_opened.saturating_add(1);
                    true
                }
                BuilderOp::EndRow => {
                    if !self.in_row {
                        return false;
                    }
                    self.in_row = false;
                    self.rows_closed = self.rows_closed.saturating_add(1);
                    true
                }
                BuilderOp::FinishBatch => {
                    if !self.in_batch || self.in_row {
                        return false;
                    }
                    self.in_batch = false;
                    true
                }
                BuilderOp::ResolveField => {
                    if !self.in_batch {
                        return false;
                    }
                    true
                }
                BuilderOp::Append => {
                    if !self.in_row {
                        return false;
                    }
                    true
                }
            }
        }
    }

    /// Prove that any sequence of valid transitions starting from `Idle`
    /// maintains semantic invariants over the trace.
    #[kani::proof]
    #[kani::unwind(8)]
    fn verify_valid_sequence_maintains_invariants() {
        let ops: [u8; 7] = kani::any();
        let mut state = BuilderState::Idle;
        let mut oracle = Oracle::new();
        let mut steps = 0u8;

        for i in 0..7 {
            let op = BuilderOp::from_u8(ops[i]);
            let transition_ok = transition(state, op);
            let oracle_ok = oracle.check_and_apply(op);

            // The protocol transition function and the oracle must agree on validity.
            assert_eq!(
                transition_ok.is_some(),
                oracle_ok,
                "transition and oracle disagree on validity"
            );

            if let Some(next) = transition_ok {
                state = next;
                steps += 1;
            }

            // Assert semantic invariants
            assert!(oracle.rows_opened >= oracle.rows_closed);
            if oracle.in_row {
                assert_eq!(oracle.rows_opened, oracle.rows_closed + 1);
            } else {
                assert_eq!(oracle.rows_opened, oracle.rows_closed);
            }

            match state {
                BuilderState::Idle => {
                    assert!(!oracle.in_batch);
                    assert!(!oracle.in_row);
                }
                BuilderState::InBatch => {
                    assert!(oracle.in_batch);
                    assert!(!oracle.in_row);
                }
                BuilderState::InRow => {
                    assert!(oracle.in_batch);
                    assert!(oracle.in_row);
                }
            }
        }

        // Guard vacuity: at least some transitions were taken
        kani::cover!(steps >= 3, "at least 3 valid transitions taken");
        kani::cover!(state == BuilderState::Idle, "ended in Idle");
        kani::cover!(state == BuilderState::InBatch, "ended in InBatch");
        kani::cover!(state == BuilderState::InRow, "ended in InRow");
    }

    /// Prove that the protocol cycle
    ///   begin_batch → (begin_row → end_row){n} → finish_batch
    /// always returns to `Idle` for any n in [0, 5].
    #[kani::proof]
    #[kani::unwind(8)]
    fn verify_protocol_cycle_returns_to_idle() {
        let n: u8 = kani::any();
        kani::assume(n <= 5);

        let mut state = BuilderState::Idle;

        // begin_batch
        state =
            transition(state, BuilderOp::BeginBatch).expect("begin_batch from Idle must succeed");
        assert!(state == BuilderState::InBatch);

        // n iterations of begin_row → end_row
        let mut i: u8 = 0;
        while i < n {
            state = transition(state, BuilderOp::BeginRow)
                .expect("begin_row from InBatch must succeed");
            assert!(state == BuilderState::InRow);
            state = transition(state, BuilderOp::EndRow).expect("end_row from InRow must succeed");
            assert!(state == BuilderState::InBatch);
            i += 1;
        }

        // finish_batch
        state = transition(state, BuilderOp::FinishBatch)
            .expect("finish_batch from InBatch must succeed");
        assert!(state == BuilderState::Idle);

        // Guard vacuity
        kani::cover!(n == 0, "zero rows");
        kani::cover!(n >= 1, "at least one row");
        kani::cover!(n >= 3, "multiple rows");
    }

    /// Prove that `append` (op 5) is never successfully called outside
    /// `InRow` state in any valid transition sequence.
    #[kani::proof]
    #[kani::unwind(8)]
    fn verify_append_requires_in_row() {
        let ops: [u8; 7] = kani::any();
        let mut state = BuilderState::Idle;
        let mut append_called = false;

        let mut i = 0;
        while i < 7 {
            let op = BuilderOp::from_u8(ops[i]);
            if let Some(next) = transition(state, op) {
                if op == BuilderOp::Append {
                    // append succeeded — state must be InRow
                    assert!(
                        state == BuilderState::InRow,
                        "append must only succeed in InRow"
                    );
                    append_called = true;
                }
                state = next;
            }
            i += 1;
        }

        // Guard vacuity: verify append was actually tested
        kani::cover!(append_called, "append was called at least once");
    }

    /// Explicitly prove that forbidden transitions are rejected
    #[kani::proof]
    fn verify_forbidden_transitions_rejected() {
        // From Idle
        assert!(transition(BuilderState::Idle, BuilderOp::BeginRow).is_none());
        assert!(transition(BuilderState::Idle, BuilderOp::EndRow).is_none());
        assert!(transition(BuilderState::Idle, BuilderOp::FinishBatch).is_none());
        assert!(transition(BuilderState::Idle, BuilderOp::ResolveField).is_none());
        assert!(transition(BuilderState::Idle, BuilderOp::Append).is_none());

        // From InBatch
        assert!(transition(BuilderState::InBatch, BuilderOp::BeginBatch).is_none());
        assert!(transition(BuilderState::InBatch, BuilderOp::EndRow).is_none());
        assert!(transition(BuilderState::InBatch, BuilderOp::Append).is_none());

        // From InRow
        assert!(transition(BuilderState::InRow, BuilderOp::BeginBatch).is_none());
        assert!(transition(BuilderState::InRow, BuilderOp::BeginRow).is_none());
        assert!(transition(BuilderState::InRow, BuilderOp::FinishBatch).is_none());
    }
}

// ---------------------------------------------------------------------------
// Proptest — BuilderState protocol
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptest_builder_state {
    use super::BuilderState;
    use proptest::prelude::*;
    use proptest::test_runner::Config as ProptestConfig;

    /// Simulate a state transition (same logic as Kani proofs).
    fn transition(state: BuilderState, op: u8) -> Option<BuilderState> {
        match (state, op % 6) {
            (BuilderState::Idle, 0) => Some(BuilderState::InBatch),
            (BuilderState::InBatch, 1) => Some(BuilderState::InRow),
            (BuilderState::InRow, 2) => Some(BuilderState::InBatch),
            (BuilderState::InBatch, 3) => Some(BuilderState::Idle),
            (BuilderState::InBatch, 4) | (BuilderState::InRow, 4) => Some(state),
            (BuilderState::InRow, 5) => Some(state),
            _ => None,
        }
    }

    // Generate a random valid operation sequence that ends with
    // `finish_batch`, then verify the final state is `Idle`.
    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            .. ProptestConfig::default()
        })]
        #[test]
        fn random_valid_sequence_ends_idle(ops in prop::collection::vec(0u8..6, 1..50)) {
            let mut state = BuilderState::Idle;

            // Apply valid transitions
            for &op in &ops {
                if let Some(next) = transition(state, op) {
                    state = next;
                }
            }

            // Now force a clean shutdown: close any open row, then finish batch
            if state == BuilderState::InRow {
                state = transition(state, 2).unwrap(); // end_row
            }
            if state == BuilderState::InBatch {
                state = transition(state, 3).unwrap(); // finish_batch
            }

            prop_assert_eq!(state, BuilderState::Idle, "clean shutdown must return to Idle");
        }
    }
}
