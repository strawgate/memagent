// row_protocol.rs — Row lifecycle state machine for columnar builders.
//
// Extracted from StreamingBuilder to isolate the batch/row call-sequence
// protocol.  This is the first scaffold toward a shared ColumnarBatchBuilder
// (see dev-docs/research/columnar-batch-builder.md).
//
// The protocol enforces:
//   Idle → begin_batch() → InBatch
//     InBatch → begin_row() → InRow
//       InRow → end_row() → InBatch
//     InBatch → finish_batch() → Idle

use logfwd_core::scanner::BuilderState;

/// Row lifecycle state machine for columnar builders.
///
/// Tracks the current batch/row phase, row count, per-row dedup bitmask,
/// and the per-row line-written flag.  These four fields are the minimal
/// state needed to enforce the call sequence contract documented on
/// [`BuilderState`] and [`ScanBuilder`](logfwd_core::scanner::ScanBuilder).
///
/// # Why runtime state instead of typestate
///
/// The batch/row protocol is driven by the scanner loop which dispatches
/// `begin_row`/`end_row` dynamically based on input data.  The transitions
/// cannot be encoded at compile time because the number of rows per batch
/// is data-dependent and the same builder instance is reused across
/// batches.  `debug_assert` guards catch illegal transitions in tests
/// while keeping zero overhead in release builds.
///
/// Transition methods are `#[inline(always)]` because they sit on the
/// scanner hot path and must not introduce call overhead.
pub(crate) struct RowLifecycle {
    /// Current phase of the batch build cycle.
    state: BuilderState,
    /// Number of rows completed (via `end_row`) in the current batch.
    row_count: u32,
    /// Per-row dedup bitmask for fields 0–63.  Reset in `begin_row`.
    written_bits: u64,
    /// Whether `append_line` has been called for the current row.
    line_written_this_row: bool,
}

impl RowLifecycle {
    /// Create a new lifecycle in the `Idle` state.
    pub(crate) fn new() -> Self {
        RowLifecycle {
            state: BuilderState::Idle,
            row_count: 0,
            written_bits: 0,
            line_written_this_row: false,
        }
    }

    // -----------------------------------------------------------------
    // State transitions
    // -----------------------------------------------------------------

    /// Transition to `InBatch`.  Resets row count and dedup state.
    ///
    /// # Panics (debug)
    /// Asserts that we are not currently inside a row.
    #[inline(always)]
    pub(crate) fn begin_batch(&mut self) {
        debug_assert!(
            matches!(self.state, BuilderState::Idle | BuilderState::InBatch),
            "begin_batch called while inside a row (missing end_row)"
        );
        self.state = BuilderState::InBatch;
        self.row_count = 0;
        self.written_bits = 0;
    }

    /// Transition from `InBatch` to `InRow`.  Resets per-row dedup state.
    ///
    /// # Panics (debug)
    /// Asserts that we are in the `InBatch` state.
    #[inline(always)]
    pub(crate) fn begin_row(&mut self) {
        debug_assert_eq!(
            self.state,
            BuilderState::InBatch,
            "begin_row called outside of a batch (call begin_batch first)"
        );
        self.written_bits = 0;
        self.line_written_this_row = false;
        self.state = BuilderState::InRow;
    }

    /// Transition from `InRow` back to `InBatch`.  Increments row count.
    ///
    /// # Panics (debug)
    /// Asserts that we are in the `InRow` state.
    ///
    /// # Panics (always)
    /// Panics if row count would overflow `u32::MAX`.
    #[inline(always)]
    pub(crate) fn end_row(&mut self) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "end_row called without a matching begin_row"
        );
        self.row_count = self
            .row_count
            .checked_add(1)
            .expect("row_count overflow: batch exceeds u32::MAX rows");
        self.state = BuilderState::InBatch;
    }

    /// Transition from `InBatch` to `Idle` after batch finalization.
    ///
    /// # Panics (debug)
    /// Asserts that we are in the `InBatch` state.
    #[inline(always)]
    pub(crate) fn finish_batch(&mut self) {
        debug_assert_eq!(
            self.state,
            BuilderState::InBatch,
            "finish_batch called outside of a batch"
        );
        self.state = BuilderState::Idle;
    }

    // -----------------------------------------------------------------
    // Accessors — all #[inline(always)] for the hot path
    // -----------------------------------------------------------------

    /// Current protocol state.
    #[inline(always)]
    pub(crate) fn state(&self) -> BuilderState {
        self.state
    }

    /// Number of completed rows in the current batch.
    #[inline(always)]
    pub(crate) fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Mutable reference to the per-row dedup bitmask.
    #[inline(always)]
    pub(crate) fn written_bits_mut(&mut self) -> &mut u64 {
        &mut self.written_bits
    }

    /// Whether `append_line` has already been called for the current row.
    #[inline(always)]
    pub(crate) fn line_written_this_row(&self) -> bool {
        self.line_written_this_row
    }

    /// Mark `append_line` as called for the current row.
    #[inline(always)]
    pub(crate) fn set_line_written(&mut self) {
        self.line_written_this_row = true;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_idle() {
        let lc = RowLifecycle::new();
        assert_eq!(lc.state(), BuilderState::Idle);
        assert_eq!(lc.row_count(), 0);
    }

    #[test]
    fn normal_lifecycle_transitions() {
        let mut lc = RowLifecycle::new();

        lc.begin_batch();
        assert_eq!(lc.state(), BuilderState::InBatch);
        assert_eq!(lc.row_count(), 0);

        lc.begin_row();
        assert_eq!(lc.state(), BuilderState::InRow);
        assert!(!lc.line_written_this_row());

        lc.end_row();
        assert_eq!(lc.state(), BuilderState::InBatch);
        assert_eq!(lc.row_count(), 1);

        lc.begin_row();
        lc.end_row();
        assert_eq!(lc.row_count(), 2);

        lc.finish_batch();
        assert_eq!(lc.state(), BuilderState::Idle);
    }

    #[test]
    fn begin_row_resets_dedup_state() {
        let mut lc = RowLifecycle::new();
        lc.begin_batch();

        // Simulate writing fields in the first row.
        lc.begin_row();
        *lc.written_bits_mut() = 0xFF;
        lc.set_line_written();
        assert!(lc.line_written_this_row());
        lc.end_row();

        // Second row starts fresh.
        lc.begin_row();
        assert_eq!(*lc.written_bits_mut(), 0);
        assert!(!lc.line_written_this_row());
        lc.end_row();
        lc.finish_batch();
    }

    #[test]
    fn begin_batch_resets_row_count() {
        let mut lc = RowLifecycle::new();

        lc.begin_batch();
        lc.begin_row();
        lc.end_row();
        lc.begin_row();
        lc.end_row();
        assert_eq!(lc.row_count(), 2);
        lc.finish_batch();

        // Second batch starts fresh.
        lc.begin_batch();
        assert_eq!(lc.row_count(), 0);
        lc.finish_batch();
    }

    #[test]
    fn empty_batch_allowed() {
        let mut lc = RowLifecycle::new();
        lc.begin_batch();
        assert_eq!(lc.row_count(), 0);
        lc.finish_batch();
        assert_eq!(lc.state(), BuilderState::Idle);
    }

    #[test]
    fn begin_batch_from_idle_and_in_batch_both_work() {
        let mut lc = RowLifecycle::new();
        // From Idle.
        lc.begin_batch();
        assert_eq!(lc.state(), BuilderState::InBatch);
        // From InBatch (restart without finishing — allowed by the protocol).
        lc.begin_batch();
        assert_eq!(lc.state(), BuilderState::InBatch);
        lc.finish_batch();
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "begin_row called outside of a batch")]
    fn begin_row_without_batch_panics() {
        let mut lc = RowLifecycle::new();
        lc.begin_row();
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "end_row called without a matching begin_row")]
    fn end_row_without_begin_row_panics() {
        let mut lc = RowLifecycle::new();
        lc.begin_batch();
        lc.end_row();
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "begin_batch called while inside a row")]
    fn begin_batch_inside_row_panics() {
        let mut lc = RowLifecycle::new();
        lc.begin_batch();
        lc.begin_row();
        lc.begin_batch();
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "finish_batch called outside of a batch")]
    fn finish_batch_from_idle_panics() {
        let mut lc = RowLifecycle::new();
        lc.finish_batch();
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "finish_batch called outside of a batch")]
    fn finish_batch_inside_row_panics() {
        let mut lc = RowLifecycle::new();
        lc.begin_batch();
        lc.begin_row();
        lc.finish_batch();
    }
}
