#![cfg(kani)]

/// Pure model of the `eof_emitted` state transition in `FileTailer::poll()`.
///
/// Mirrors the logic from the `ReadResult::Data`, `ReadResult::TruncatedThenData`,
/// and `ReadResult::NoData` arms exactly, making the invariant explicitly testable.
///
/// Returns `(new_eof_emitted, should_emit_eof_event)`.
fn eof_transition(eof_emitted: bool, had_data: bool) -> (bool, bool) {
    if had_data {
        (false, false)
    } else if !eof_emitted {
        (true, true)
    } else {
        (true, false)
    }
}

/// EndOfFile is emitted at most once per no-data streak: the event fires only
/// when `eof_emitted` transitions from `false` to `true`, never while it is
/// already `true`.
#[kani::proof]
fn verify_eof_emitted_at_most_once_per_no_data_streak() {
    let eof_emitted: bool = kani::any();
    let (_, fires) = eof_transition(eof_emitted, false); // NoData
    if fires {
        assert!(!eof_emitted, "EndOfFile may only fire when flag was false");
    }
    kani::cover!(fires, "EndOfFile event fired");
    kani::cover!(
        !fires && eof_emitted,
        "EndOfFile suppressed — already emitted"
    );
}

/// Data always resets the eof_emitted flag to false and never fires EndOfFile.
///
/// This ensures a fresh EndOfFile can be emitted the next time reads stall,
/// correctly signalling the downstream framer to flush any partial-line remainder.
#[kani::proof]
fn verify_data_resets_eof_flag() {
    let eof_emitted: bool = kani::any();
    let (new_flag, fires) = eof_transition(eof_emitted, true); // Data
    assert!(!new_flag, "Data must reset eof_emitted to false");
    assert!(!fires, "Data must not emit EndOfFile");
    kani::cover!(eof_emitted, "eof_emitted was true before data arrived");
    kani::cover!(!eof_emitted, "eof_emitted was already false");
}

/// Two consecutive NoData polls emit EndOfFile exactly once (on the first).
#[kani::proof]
fn verify_two_no_data_polls_emit_exactly_once() {
    let (state1, fires1) = eof_transition(false, false); // first NoData
    let (state2, fires2) = eof_transition(state1, false); // second NoData
    assert!(fires1, "first NoData poll must emit EndOfFile");
    assert!(!fires2, "second NoData poll must not emit again");
    assert!(state1 && state2, "flag stays true after both polls");
    kani::cover!(fires1 && !fires2, "only first NoData emits EOF");
    kani::cover!(state2, "eof_emitted remains true after second NoData");
}

/// After data resets the flag, the next NoData streak fires EndOfFile again.
///
/// Sequence: NoData → Data → NoData.  Both stalls must emit exactly one event.
#[kani::proof]
fn verify_eof_fires_again_after_data_resets_flag() {
    let (after_nodata, fires1) = eof_transition(false, false); // first stall
    let (after_data, _) = eof_transition(after_nodata, true); // data arrives
    let (_, fires2) = eof_transition(after_data, false); // second stall
    assert!(fires1, "first stall must emit EndOfFile");
    assert!(fires2, "second stall must emit EndOfFile after data reset");
    kani::cover!(fires1, "first no-data streak emitted EOF");
    kani::cover!(fires2, "second no-data streak emitted EOF after reset");
}
