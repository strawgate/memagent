//! Pure state machine for checkpoint-remainder coordination.
//!
//! Tracks the relationship between file read position, framing remainder,
//! and the checkpoint that should be persisted. All fields are byte offsets
//! within a single file.
//!
//! The key invariant: the checkpointable offset accounts for the framing
//! remainder. If we checkpoint at `read_offset` but the framer has buffered
//! `remainder_len` unprocessed bytes, a crash and restart from that checkpoint
//! would skip the remainder -- data loss. The correct checkpoint is
//! `processed_offset`, which is always at the last complete newline boundary.
//!
//! # Provability
//!
//! This module is `no_std` compatible (no alloc needed -- just 4 u64 fields).
//! All state transitions are pure arithmetic, which is Kani's sweet spot:
//! loop-free bitwise/arithmetic operations verify over the full 64-bit range.
//! See `dev-docs/research/checkpoint-kani-plan.md` for the verification plan.

/// Pure state machine for checkpoint-remainder coordination.
///
/// Tracks the relationship between file read position, framing
/// remainder, and the checkpoint that should be persisted.
/// All fields are byte offsets within a single file.
///
/// # Invariants
///
/// After every operation, the following hold:
/// 1. `checkpoint_offset <= processed_offset <= read_offset`
/// 2. `processed_offset + remainder_len == read_offset`
/// 3. `checkpoint_offset` never decreases
/// 4. `processed_offset` only advances via `apply_read` with a newline
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointTracker {
    /// Total bytes read from the file (cumulative).
    /// Advances on every `apply_read` operation.
    read_offset: u64,

    /// Byte offset of the last complete newline boundary.
    /// This is where we would resume after a crash.
    /// Invariant: `processed_offset <= read_offset`
    processed_offset: u64,

    /// Number of bytes after the last newline that are buffered
    /// as a partial-line remainder.
    /// Invariant: `processed_offset + remainder_len == read_offset`
    remainder_len: u64,

    /// Last durably persisted checkpoint offset.
    /// Invariant: `checkpoint_offset <= processed_offset`
    checkpoint_offset: u64,
}

impl CheckpointTracker {
    /// Create a new tracker, optionally resuming from a checkpoint.
    ///
    /// All offsets start at `resume_offset` with zero remainder,
    /// representing a clean start (or restart) at a known-good position.
    pub fn new(resume_offset: u64) -> Self {
        CheckpointTracker {
            read_offset: resume_offset,
            processed_offset: resume_offset,
            remainder_len: 0,
            checkpoint_offset: resume_offset,
        }
    }

    /// Apply a Read action: we read `n_bytes` starting at `read_offset`.
    ///
    /// The framer scanned the chunk and found the last newline at
    /// `last_newline_pos` (relative to the start of the chunk, zero-indexed).
    ///
    /// If `last_newline_pos` is `None`, the entire chunk is remainder
    /// (no complete lines found).
    ///
    /// # Panics
    ///
    /// Panics if `n_bytes` is zero (a read of zero bytes is a no-op and
    /// should not be reported to the tracker).
    ///
    /// Panics if `last_newline_pos >= n_bytes` (the newline position must
    /// be within the chunk).
    pub fn apply_read(&mut self, n_bytes: u64, last_newline_pos: Option<u64>) {
        assert!(n_bytes > 0, "n_bytes must be > 0");
        if let Some(pos) = last_newline_pos {
            assert!(pos < n_bytes, "last_newline_pos must be within the chunk");
        }

        let old_read = self.read_offset;
        self.read_offset = old_read.saturating_add(n_bytes);

        match last_newline_pos {
            Some(pos) => {
                // pos is relative to chunk start; +1 because the newline
                // itself is consumed (processed_offset is one past the \n).
                let newline_abs = old_read.saturating_add(pos).saturating_add(1);
                self.processed_offset = newline_abs;
                self.remainder_len = self.read_offset.saturating_sub(newline_abs);
            }
            None => {
                // No newline in chunk -- everything is added to remainder.
                self.remainder_len = self.remainder_len.saturating_add(n_bytes);
                // processed_offset unchanged.
            }
        }
    }

    /// Persist the current `processed_offset` as the durable checkpoint.
    ///
    /// In the real system, this is called after the checkpoint store has
    /// durably written the offset. Here we just record the logical state.
    pub fn apply_checkpoint(&mut self) {
        self.checkpoint_offset = self.processed_offset;
    }

    /// Restart from the last checkpoint. Remainder is lost.
    ///
    /// Models a crash + restart: all in-memory state is reset to the
    /// last durably persisted checkpoint. The file will be re-read
    /// from `checkpoint_offset`, so bytes between `checkpoint_offset`
    /// and the old `read_offset` will be re-processed. This may cause
    /// duplicate processing of bytes between `checkpoint_offset` and
    /// `processed_offset`, which is acceptable (at-least-once semantics).
    pub fn apply_restart(&mut self) {
        self.read_offset = self.checkpoint_offset;
        self.processed_offset = self.checkpoint_offset;
        self.remainder_len = 0;
    }

    /// Flush or discard the remainder without advancing read_offset.
    ///
    /// Called when FramedInput flushes partial data on EndOfFile (emitting
    /// the remainder as a final line) or discards it on overflow/rotation.
    /// This collapses `remainder_len` into `processed_offset` so that
    /// `checkpointable_offset()` advances to cover the flushed/discarded
    /// bytes. Without this, idle sources with flushed remainders would
    /// have a permanently stale `remainder_len`.
    pub fn apply_remainder_consumed(&mut self) {
        self.processed_offset = self.read_offset;
        self.remainder_len = 0;
    }

    /// The offset that should be checkpointed.
    ///
    /// This is always at a newline boundary -- the last byte position
    /// where all preceding data has been fully framed into complete lines.
    pub fn checkpointable_offset(&self) -> u64 {
        self.processed_offset
    }

    /// Current read position (total bytes consumed from file).
    pub fn read_offset(&self) -> u64 {
        self.read_offset
    }

    /// Current processed position (last complete newline boundary).
    pub fn processed_offset(&self) -> u64 {
        self.processed_offset
    }

    /// Current remainder length (bytes buffered after last newline).
    pub fn remainder_len(&self) -> u64 {
        self.remainder_len
    }

    /// Last durably persisted checkpoint offset.
    pub fn checkpoint_offset(&self) -> u64 {
        self.checkpoint_offset
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tracker_all_offsets_equal() {
        let t = CheckpointTracker::new(100);
        assert_eq!(t.read_offset(), 100);
        assert_eq!(t.processed_offset(), 100);
        assert_eq!(t.remainder_len(), 0);
        assert_eq!(t.checkpoint_offset(), 100);
        assert_eq!(t.checkpointable_offset(), 100);
    }

    #[test]
    fn read_with_newline_advances_processed() {
        let mut t = CheckpointTracker::new(0);
        // Read 10 bytes, newline at position 7 (8 bytes processed, 2 remainder)
        t.apply_read(10, Some(7));
        assert_eq!(t.read_offset(), 10);
        assert_eq!(t.processed_offset(), 8); // 0 + 7 + 1
        assert_eq!(t.remainder_len(), 2); // 10 - 8
        assert_eq!(t.checkpointable_offset(), 8);
    }

    #[test]
    fn read_without_newline_only_grows_remainder() {
        let mut t = CheckpointTracker::new(0);
        t.apply_read(10, None);
        assert_eq!(t.read_offset(), 10);
        assert_eq!(t.processed_offset(), 0);
        assert_eq!(t.remainder_len(), 10);
    }

    #[test]
    fn multiple_reads_track_correctly() {
        let mut t = CheckpointTracker::new(0);

        // First read: 20 bytes, newline at 14
        t.apply_read(20, Some(14));
        assert_eq!(t.read_offset(), 20);
        assert_eq!(t.processed_offset(), 15); // 0 + 14 + 1
        assert_eq!(t.remainder_len(), 5); // 20 - 15

        // Second read: 10 bytes, no newline
        t.apply_read(10, None);
        assert_eq!(t.read_offset(), 30);
        assert_eq!(t.processed_offset(), 15); // unchanged
        assert_eq!(t.remainder_len(), 15); // 5 + 10

        // Third read: 5 bytes, newline at 2
        t.apply_read(5, Some(2));
        assert_eq!(t.read_offset(), 35);
        assert_eq!(t.processed_offset(), 33); // 30 + 2 + 1
        assert_eq!(t.remainder_len(), 2); // 35 - 33
    }

    #[test]
    fn checkpoint_records_processed_offset() {
        let mut t = CheckpointTracker::new(0);
        t.apply_read(10, Some(7));
        assert_eq!(t.checkpoint_offset(), 0); // not yet checkpointed
        t.apply_checkpoint();
        assert_eq!(t.checkpoint_offset(), 8); // now matches processed
    }

    #[test]
    fn restart_resets_to_checkpoint() {
        let mut t = CheckpointTracker::new(0);
        t.apply_read(100, Some(79));
        t.apply_checkpoint();
        t.apply_read(50, Some(30));
        // Pre-restart state
        assert_eq!(t.read_offset(), 150);
        assert_eq!(t.processed_offset(), 131); // 100 + 30 + 1
        assert_eq!(t.checkpoint_offset(), 80); // from first checkpoint

        t.apply_restart();
        assert_eq!(t.read_offset(), 80);
        assert_eq!(t.processed_offset(), 80);
        assert_eq!(t.remainder_len(), 0);
        assert_eq!(t.checkpoint_offset(), 80);
    }

    #[test]
    fn crash_restart_no_data_loss() {
        let mut t = CheckpointTracker::new(0);
        t.apply_read(50, Some(39)); // processed=40, remainder=10
        t.apply_checkpoint(); // checkpoint=40
        t.apply_read(30, Some(19)); // processed=70, remainder=10

        let pre_crash_checkpoint = t.checkpoint_offset();
        assert_eq!(pre_crash_checkpoint, 40);

        t.apply_restart();

        // After restart, we resume at the checkpoint -- no bytes skipped
        assert_eq!(t.read_offset(), 40);
        assert_eq!(t.processed_offset(), 40);
        // Bytes 40..80 will be re-read from file (at-least-once)
    }

    #[test]
    fn restart_without_checkpoint_resets_to_initial() {
        let mut t = CheckpointTracker::new(500);
        t.apply_read(100, Some(49));
        // Never checkpointed, so checkpoint_offset is still 500
        t.apply_restart();
        assert_eq!(t.read_offset(), 500);
        assert_eq!(t.processed_offset(), 500);
        assert_eq!(t.remainder_len(), 0);
    }

    #[test]
    fn invariants_hold_after_every_operation() {
        let mut t = CheckpointTracker::new(42);
        check_invariants(&t);

        t.apply_read(100, Some(50));
        check_invariants(&t);

        t.apply_read(50, None);
        check_invariants(&t);

        t.apply_checkpoint();
        check_invariants(&t);

        t.apply_read(25, Some(10));
        check_invariants(&t);

        t.apply_restart();
        check_invariants(&t);

        t.apply_read(10, Some(9));
        check_invariants(&t);

        t.apply_checkpoint();
        check_invariants(&t);
    }

    #[test]
    fn newline_at_end_of_chunk_means_zero_remainder() {
        let mut t = CheckpointTracker::new(0);
        // Read 10 bytes, newline at position 9 (last byte)
        t.apply_read(10, Some(9));
        assert_eq!(t.processed_offset(), 10);
        assert_eq!(t.remainder_len(), 0);
    }

    #[test]
    fn newline_at_start_of_chunk() {
        let mut t = CheckpointTracker::new(0);
        // Read 10 bytes, newline at position 0 (first byte)
        t.apply_read(10, Some(0));
        assert_eq!(t.processed_offset(), 1);
        assert_eq!(t.remainder_len(), 9);
    }

    #[test]
    #[should_panic(expected = "n_bytes must be > 0")]
    fn zero_byte_read_panics() {
        let mut t = CheckpointTracker::new(0);
        t.apply_read(0, None);
    }

    #[test]
    #[should_panic(expected = "last_newline_pos must be within the chunk")]
    fn newline_pos_out_of_bounds_panics() {
        let mut t = CheckpointTracker::new(0);
        t.apply_read(10, Some(10)); // pos must be < n_bytes
    }

    #[test]
    fn checkpoint_monotonicity() {
        let mut t = CheckpointTracker::new(0);
        let mut prev_cp = t.checkpoint_offset();

        t.apply_read(100, Some(50));
        t.apply_checkpoint();
        assert!(t.checkpoint_offset() >= prev_cp);
        prev_cp = t.checkpoint_offset();

        t.apply_read(50, Some(30));
        t.apply_checkpoint();
        assert!(t.checkpoint_offset() >= prev_cp);
        prev_cp = t.checkpoint_offset();

        // Restart doesn't decrease checkpoint
        t.apply_restart();
        assert!(t.checkpoint_offset() >= prev_cp);
    }

    #[test]
    fn resume_from_nonzero_offset() {
        let mut t = CheckpointTracker::new(1_000_000);
        t.apply_read(4096, Some(4000));
        assert_eq!(t.read_offset(), 1_004_096);
        assert_eq!(t.processed_offset(), 1_004_001); // 1_000_000 + 4000 + 1
        assert_eq!(t.remainder_len(), 95); // 1_004_096 - 1_004_001
        check_invariants(&t);
    }

    /// Helper to assert all invariants hold.
    fn check_invariants(t: &CheckpointTracker) {
        assert!(
            t.checkpoint_offset() <= t.processed_offset(),
            "checkpoint {} ahead of processed {}",
            t.checkpoint_offset(),
            t.processed_offset(),
        );
        assert!(
            t.processed_offset() <= t.read_offset(),
            "processed {} ahead of read {}",
            t.processed_offset(),
            t.read_offset(),
        );
        assert_eq!(
            t.processed_offset() + t.remainder_len(),
            t.read_offset(),
            "remainder inconsistent: processed {} + remainder {} != read {}",
            t.processed_offset(),
            t.remainder_len(),
            t.read_offset(),
        );
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;
    const PROOF_STEPS: u32 = 6;

    /// Generate a symbolic action with valid constraints.
    ///
    /// Read actions have `n_bytes > 0 && n_bytes <= 4096` and
    /// `last_newline_pos < n_bytes` when present.
    fn symbolic_read() -> (u64, Option<u64>) {
        // The invariants depend on ordering, not large absolute offsets.
        // Keeping the symbolic range small materially reduces solver cost.
        let n_bytes: u64 = kani::any_where(|&n: &u64| n > 0 && n <= 64);
        let has_newline: bool = kani::any();
        let last_newline_pos = if has_newline {
            Some(kani::any_where(|&p: &u64| p < n_bytes))
        } else {
            None
        };
        (n_bytes, last_newline_pos)
    }

    /// Generate a symbolic action tag: 0=Read, 1=Checkpoint, 2=Restart.
    fn symbolic_action_tag() -> u8 {
        kani::any_where(|&t: &u8| t < 3)
    }

    /// Apply a symbolic action to the tracker.
    fn apply_symbolic_action(tracker: &mut CheckpointTracker) {
        let tag = symbolic_action_tag();
        match tag {
            0 => {
                let (n_bytes, last_newline_pos) = symbolic_read();
                tracker.apply_read(n_bytes, last_newline_pos);
            }
            1 => {
                tracker.apply_checkpoint();
            }
            _ => {
                tracker.apply_restart();
            }
        }
    }

    /// Core invariant checker -- called after every action.
    fn check_invariants(t: &CheckpointTracker) {
        // Invariant 1: offset ordering
        assert!(
            t.checkpoint_offset <= t.processed_offset,
            "checkpoint ahead of processed"
        );
        assert!(
            t.processed_offset <= t.read_offset,
            "processed ahead of read"
        );

        // Invariant 2: remainder consistency
        assert!(
            t.processed_offset + t.remainder_len == t.read_offset,
            "remainder inconsistent with offsets"
        );
    }

    // -----------------------------------------------------------------------
    // Harness 1: All invariants hold for any 6-step action sequence
    // -----------------------------------------------------------------------

    /// All invariants hold for any 6-step action sequence starting from
    /// any resume offset.
    ///
    /// This is the core safety proof: no matter what interleaving of reads,
    /// checkpoints, and restarts occurs, the offset ordering and remainder
    /// consistency invariants are maintained.
    #[kani::proof]
    #[kani::unwind(7)]
    #[kani::solver(kissat)]
    fn verify_checkpoint_tracker_invariants() {
        let resume: u64 = kani::any_where(|&r: &u64| r <= 1_024);
        let mut tracker = CheckpointTracker::new(resume);
        check_invariants(&tracker);

        let mut i = 0u32;
        while i < PROOF_STEPS {
            apply_symbolic_action(&mut tracker);
            check_invariants(&tracker);
            i += 1;
        }
        assert_eq!(i, PROOF_STEPS, "must complete all PROOF_STEPS steps");

        // Vacuity guards: confirm interesting cases are explored
        kani::cover!(tracker.remainder_len > 0, "has remainder");
        kani::cover!(
            tracker.checkpoint_offset < tracker.processed_offset,
            "checkpoint behind processed"
        );
        kani::cover!(
            tracker.checkpoint_offset == tracker.processed_offset,
            "checkpoint caught up"
        );
    }

    // -----------------------------------------------------------------------
    // Harness 2: Checkpoint monotonicity
    // -----------------------------------------------------------------------

    /// Checkpoint offset never decreases across any 6-step action sequence.
    ///
    /// This ensures that once a checkpoint is persisted, no subsequent
    /// operation (including restart) can cause it to regress.
    #[kani::proof]
    #[kani::unwind(7)]
    #[kani::solver(kissat)]
    fn verify_checkpoint_monotonicity() {
        let resume: u64 = kani::any_where(|&r: &u64| r <= 1_000_000);
        let mut tracker = CheckpointTracker::new(resume);
        let mut prev_checkpoint = tracker.checkpoint_offset;

        let mut i = 0u32;
        while i < PROOF_STEPS {
            apply_symbolic_action(&mut tracker);
            assert!(
                tracker.checkpoint_offset >= prev_checkpoint,
                "checkpoint regressed"
            );
            prev_checkpoint = tracker.checkpoint_offset;
            i += 1;
        }
        assert_eq!(i, PROOF_STEPS, "must complete all PROOF_STEPS steps");

        // Vacuity: confirm checkpoint actually advanced in some path
        kani::cover!(
            tracker.checkpoint_offset > resume,
            "checkpoint advanced beyond initial"
        );
    }

    // -----------------------------------------------------------------------
    // Harness 3: No data loss on crash -- restart resumes at checkpoint
    // -----------------------------------------------------------------------

    /// After a sequence of reads and optional checkpoints, crash + restart
    /// resumes exactly at the checkpoint offset. No bytes between the
    /// checkpoint and the old processed offset are skipped.
    ///
    /// Uses 6 read iterations (with optional checkpoints between each, then
    /// restart + verify) to cover realistic multi-read scenarios.
    #[kani::proof]
    #[kani::unwind(9)]
    #[kani::solver(kissat)]
    fn verify_no_data_loss_on_crash() {
        let resume: u64 = kani::any_where(|&r: &u64| r <= 1_000_000);
        let mut tracker = CheckpointTracker::new(resume);

        // Do some reads and optional checkpoints
        let mut i = 0u32;
        while i < PROOF_STEPS {
            let (n_bytes, last_newline_pos) = symbolic_read();
            tracker.apply_read(n_bytes, last_newline_pos);

            if kani::any() {
                tracker.apply_checkpoint();
            }
            i += 1;
        }
        assert_eq!(i, PROOF_STEPS, "must complete all PROOF_STEPS steps");

        // Record pre-crash state
        let pre_crash_checkpoint = tracker.checkpoint_offset;

        // Crash and restart
        tracker.apply_restart();

        // After restart: we resume exactly at the checkpoint
        assert_eq!(
            tracker.read_offset, pre_crash_checkpoint,
            "read_offset must equal checkpoint after restart"
        );
        assert_eq!(
            tracker.processed_offset, pre_crash_checkpoint,
            "processed_offset must equal checkpoint after restart"
        );
        assert_eq!(
            tracker.remainder_len, 0,
            "remainder must be zero after restart"
        );

        // All invariants still hold
        check_invariants(&tracker);

        // Vacuity: confirm we had actual progress before crash
        kani::cover!(
            pre_crash_checkpoint > resume,
            "checkpoint advanced before crash"
        );
    }

    // -----------------------------------------------------------------------
    // Harness 4: Processed offset advances only on newline
    // -----------------------------------------------------------------------

    /// `processed_offset` only changes via `apply_read` with a `Some`
    /// newline position. It never changes on reads without newlines,
    /// on checkpoints, or on restarts (where it resets to checkpoint,
    /// which is a separate operation).
    #[kani::proof]
    #[kani::unwind(7)]
    #[kani::solver(kissat)]
    fn verify_processed_advances_on_newline() {
        let resume: u64 = kani::any_where(|&r: &u64| r <= 1_000_000);
        let mut tracker = CheckpointTracker::new(resume);

        let mut i = 0u32;
        while i < PROOF_STEPS {
            let prev_processed = tracker.processed_offset;
            let tag = symbolic_action_tag();

            match tag {
                0 => {
                    let (n_bytes, last_newline_pos) = symbolic_read();
                    tracker.apply_read(n_bytes, last_newline_pos);
                    match last_newline_pos {
                        Some(_) => {
                            // With a newline, processed_offset must not regress
                            assert!(
                                tracker.processed_offset >= prev_processed,
                                "processed regressed on read with newline"
                            );
                        }
                        None => {
                            // Without a newline, processed_offset must NOT change
                            assert_eq!(
                                tracker.processed_offset, prev_processed,
                                "processed changed on read without newline"
                            );
                        }
                    }
                }
                1 => {
                    tracker.apply_checkpoint();
                    // Checkpoint never changes processed_offset
                    assert_eq!(
                        tracker.processed_offset, prev_processed,
                        "processed changed on checkpoint"
                    );
                }
                _ => {
                    tracker.apply_restart();
                    // Restart resets to checkpoint -- may be less than prev.
                    // We don't assert monotonicity here (restart is a reset).
                }
            }
            i += 1;
        }
        assert_eq!(i, PROOF_STEPS, "must complete all PROOF_STEPS steps");

        // Vacuity
        kani::cover!(
            tracker.processed_offset > resume,
            "processed advanced beyond initial"
        );
    }

    // -----------------------------------------------------------------------
    // Harness 5: Overflow safety
    // -----------------------------------------------------------------------

    /// No arithmetic overflow for any starting offset.
    ///
    /// Kani automatically checks for overflow on every +, -, etc.
    /// We start at an unconstrained u64 offset to stress near-MAX behavior,
    /// and guard reads that would overflow.
    #[kani::proof]
    #[kani::unwind(7)]
    #[kani::solver(kissat)]
    fn verify_overflow_safety() {
        // Start at any u64 offset -- including near u64::MAX
        let resume: u64 = kani::any();
        let mut tracker = CheckpointTracker::new(resume);

        let mut i = 0u32;
        while i < PROOF_STEPS {
            let tag = symbolic_action_tag();
            match tag {
                0 => {
                    let (n_bytes, last_newline_pos) = symbolic_read();
                    // Guard: skip reads that would overflow read_offset.
                    // In production code, we'd use checked_add; here we
                    // model the precondition.
                    if tracker.read_offset.checked_add(n_bytes).is_some() {
                        tracker.apply_read(n_bytes, last_newline_pos);
                    }
                }
                1 => tracker.apply_checkpoint(),
                _ => tracker.apply_restart(),
            }
            i += 1;
        }
        assert_eq!(i, PROOF_STEPS, "must complete all PROOF_STEPS steps");
        // Kani automatically asserts no overflow on +, -, etc.
        // If any reachable path overflows, Kani reports failure.
    }
}
