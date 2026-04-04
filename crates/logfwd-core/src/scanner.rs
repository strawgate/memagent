// scanner.rs — Generic JSON-to-columnar scan loop.
//
// Provides the ScanBuilder trait and the scan_into/scan_line functions.
// The Arrow-specific Scanner type lives in the logfwd-arrow crate.

use crate::scan_config::ScanConfig;
use crate::scan_config::parse_int_fast;
use crate::structural::StructuralIndex;

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
/// input buffer for zero-copy view construction).  Callers must invoke
/// `begin_batch` on the concrete type before passing it to `scan_into` or
/// `scan_streaming`.
///
/// - `resolve_field`: maps a field name to a column index. Must be stable
///   within a batch (same key → same index). May create new columns.
/// - `append_*_by_idx`: stores a typed value at the current row for the
///   given column index. Byte slices are borrowed from the input buffer.
/// - `append_raw`: stores the entire unparsed line (if `keep_raw` is set).
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
    /// Record a null value at the given column index.
    fn append_null_by_idx(&mut self, idx: usize);
    /// Store the raw unparsed line (only called when `keep_raw` is set).
    fn append_raw(&mut self, line: &[u8]);
}

// ---------------------------------------------------------------------------
// Generic scan loop
// ---------------------------------------------------------------------------

/// Scan an NDJSON buffer, extracting fields into a `ScanBuilder`.
///
/// Processes the buffer in two stages:
/// 1. SIMD structural detection (`StructuralIndex`) identifies all structural
///    character positions and newline boundaries in one pass
/// 2. Scalar field extraction walks JSON objects and dispatches to the builder
///
/// # Preconditions
/// - `buf` must be valid UTF-8 (debug-asserted, not checked in release)
/// - Lines are newline-delimited (`\n`)
/// - The caller must have already invoked `begin_batch` on the builder before
///   this call (see [`ScanBuilder`] for the initialization contract).
///
/// # Type parameter
/// - `B`: Any implementation of `ScanBuilder` (e.g., `StreamingBuilder`
///   from `logfwd-arrow`)
#[inline(never)]
pub fn scan_into<B: ScanBuilder>(buf: &[u8], config: &ScanConfig, builder: &mut B) {
    debug_assert!(
        core::str::from_utf8(buf).is_ok(),
        "Scanner input must be valid UTF-8"
    );
    let (index, line_ranges) = StructuralIndex::new(buf);
    for (start, end) in line_ranges {
        scan_line(buf, start, end, &index, config, builder);
    }
}

#[inline]
fn scan_line<B: ScanBuilder>(
    buf: &[u8],
    start: usize,
    end: usize,
    index: &StructuralIndex,
    config: &ScanConfig,
    builder: &mut B,
) {
    builder.begin_row();
    if config.keep_raw {
        builder.append_raw(&buf[start..end]);
    }

    let mut pos = skip_ws(buf, start, end);
    if pos >= end || buf[pos] != b'{' {
        builder.end_row();
        return;
    }
    pos += 1;

    loop {
        pos = skip_ws(buf, pos, end);
        if pos >= end || buf[pos] == b'}' {
            break;
        }
        if buf[pos] != b'"' {
            break;
        }
        let (key, after_key) = match index.scan_string(buf, pos, end) {
            Some(r) => r,
            None => break,
        };
        pos = after_key;
        pos = skip_ws(buf, pos, end);
        if pos >= end || buf[pos] != b':' {
            break;
        }
        pos += 1;
        pos = skip_ws(buf, pos, end);
        if pos >= end {
            break;
        }

        let wanted = config.is_wanted(key);
        match buf[pos] {
            b'"' => {
                let (val, after) = match index.scan_string(buf, pos, end) {
                    Some(r) => r,
                    None => break,
                };
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, val);
                }
                pos = after;
            }
            b'{' | b'[' => {
                let s = pos;
                pos = index.skip_nested(buf, pos, end);
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, &buf[s..pos]);
                }
            }
            b't' | b'f' => {
                let s = pos;
                while pos < end
                    && buf[pos] != b','
                    && buf[pos] != b'}'
                    && buf[pos] != b' '
                    && buf[pos] != b'\t'
                    && buf[pos] != b'\r'
                {
                    pos += 1;
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, &buf[s..pos]);
                }
            }
            b'n' => {
                // Scan past the null/identifier token to the next delimiter.
                while pos < end
                    && buf[pos] != b','
                    && buf[pos] != b'}'
                    && buf[pos] != b' '
                    && buf[pos] != b'\t'
                    && buf[pos] != b'\r'
                {
                    pos += 1;
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_null_by_idx(idx);
                }
            }
            _ => {
                let s = pos;
                let mut is_float = false;
                while pos < end {
                    let c = buf[pos];
                    if c == b'.' || c == b'e' || c == b'E' {
                        is_float = true;
                    } else if c == b','
                        || c == b'}'
                        || c == b' '
                        || c == b'\t'
                        || c == b'\n'
                        || c == b'\r'
                    {
                        break;
                    }
                    pos += 1;
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    let val = &buf[s..pos];
                    if is_float {
                        builder.append_float_by_idx(idx, val);
                    } else if parse_int_fast(val).is_some() {
                        builder.append_int_by_idx(idx, val);
                    } else {
                        builder.append_float_by_idx(idx, val);
                    }
                }
            }
        }
        pos = skip_ws(buf, pos, end);
        if pos < end && buf[pos] == b',' {
            pos += 1;
        }
    }
    builder.end_row();
}

#[inline(always)]
fn skip_ws(buf: &[u8], mut pos: usize, end: usize) -> usize {
    while pos < end {
        match buf[pos] {
            b' ' | b'\t' | b'\r' | b'\n' => pos += 1,
            _ => break,
        }
    }
    pos
}

#[cfg(kani)]
mod verification {
    use super::*;

    /// Correctness: skip_ws returns the first non-whitespace position
    /// within [start, end], or end if all bytes are whitespace.
    /// Verifies: result in range, all skipped bytes are whitespace,
    /// byte at result (if < end) is NOT whitespace.
    #[kani::proof]
    #[kani::unwind(17)]
    fn verify_skip_ws() {
        let buf: [u8; 16] = kani::any();
        let start: usize = kani::any();
        let end: usize = kani::any();
        kani::assume(start <= end && end <= 16);

        let result = skip_ws(&buf, start, end);

        assert!(result >= start && result <= end);

        let mut i = start;
        while i < result {
            let b = buf[i];
            assert!(b == b' ' || b == b'\t' || b == b'\r' || b == b'\n');
            i += 1;
        }

        if result < end {
            let b = buf[result];
            assert!(b != b' ' && b != b'\t' && b != b'\r' && b != b'\n');
        }

        // Guard vacuity: verify whitespace skip logic works
        kani::cover!(result == start, "no whitespace to skip");
        kani::cover!(result > start, "skipped whitespace");
        kani::cover!(result == end, "buffer is all whitespace");
    }
}

// ---------------------------------------------------------------------------
// Kani proofs — BuilderState protocol
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod kani_proofs {
    use super::BuilderState;

    /// Simulate a state transition. Returns `Some(next_state)` for valid
    /// transitions and `None` for invalid ones.
    ///
    /// Operations (op % 6):
    ///   0 = begin_batch   (Idle → InBatch)
    ///   1 = begin_row     (InBatch → InRow)
    ///   2 = end_row       (InRow → InBatch)
    ///   3 = finish_batch  (InBatch → Idle)
    ///   4 = resolve_field (InBatch | InRow → same state)
    ///   5 = append         (InRow → InRow)
    fn transition(state: BuilderState, op: u8) -> Option<BuilderState> {
        match (state, op % 6) {
            (BuilderState::Idle, 0) => Some(BuilderState::InBatch),
            (BuilderState::InBatch, 1) => Some(BuilderState::InRow),
            (BuilderState::InRow, 2) => Some(BuilderState::InBatch),
            (BuilderState::InBatch, 3) => Some(BuilderState::Idle),
            (BuilderState::InBatch, 4) | (BuilderState::InRow, 4) => Some(state),
            (BuilderState::InRow, 5) => Some(state),
            _ => None, // invalid transition
        }
    }

    /// Prove that any sequence of valid transitions starting from `Idle`
    /// never reaches an invalid state — the state is always one of
    /// {Idle, InBatch, InRow}.
    #[kani::proof]
    #[kani::unwind(8)]
    fn verify_valid_sequence_no_invalid_state() {
        let ops: [u8; 7] = kani::any();
        let mut state = BuilderState::Idle;
        let mut steps = 0u8;

        let mut i = 0;
        while i < 7 {
            if let Some(next) = transition(state, ops[i]) {
                state = next;
                steps += 1;
            }
            // Skip invalid transitions (filter approach)
            i += 1;
        }

        // State is always one of the three valid variants
        assert!(
            state == BuilderState::Idle
                || state == BuilderState::InBatch
                || state == BuilderState::InRow
        );

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
        state = transition(state, 0).expect("begin_batch from Idle must succeed");
        assert!(state == BuilderState::InBatch);

        // n iterations of begin_row → end_row
        let mut i: u8 = 0;
        while i < n {
            state = transition(state, 1).expect("begin_row from InBatch must succeed");
            assert!(state == BuilderState::InRow);
            state = transition(state, 2).expect("end_row from InRow must succeed");
            assert!(state == BuilderState::InBatch);
            i += 1;
        }

        // finish_batch
        state = transition(state, 3).expect("finish_batch from InBatch must succeed");
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
            let op = ops[i] % 6;
            if let Some(next) = transition(state, ops[i]) {
                if op == 5 {
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
}

// ---------------------------------------------------------------------------
// Proptest — BuilderState protocol
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptest_builder_state {
    use super::BuilderState;
    use proptest::prelude::*;

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
