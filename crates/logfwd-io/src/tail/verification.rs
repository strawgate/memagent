#![cfg(kani)]

/// File tailer waits for two consecutive no-data polls before emitting EOF.
const EOF_IDLE_POLLS_BEFORE_EMIT: u8 = 2;

/// Pure model of the file-tail `EndOfFile` emission state machine.
///
/// Returns `(new_eof_emitted, new_idle_polls, should_emit_eof_event)`.
fn eof_transition(eof_emitted: bool, idle_polls: u8, had_data: bool) -> (bool, u8, bool) {
    if had_data {
        (false, 0, false)
    } else {
        let next_idle = idle_polls.saturating_add(1);
        let should_emit = !eof_emitted && next_idle >= EOF_IDLE_POLLS_BEFORE_EMIT;
        (eof_emitted || should_emit, next_idle, should_emit)
    }
}

/// EndOfFile can only fire when we cross the idle-poll threshold for the first
/// time in a no-data streak.
#[cfg(kani)]
#[kani::proof]
fn verify_eof_emitted_at_most_once_per_no_data_streak() {
    let eof_emitted: bool = kani::any();
    let idle_polls: u8 = kani::any();
    let (_, _, fires) = eof_transition(eof_emitted, idle_polls, false); // NoData
    if fires {
        assert!(!eof_emitted, "EndOfFile may only fire when flag was false");
        assert!(
            idle_polls.saturating_add(1) >= EOF_IDLE_POLLS_BEFORE_EMIT,
            "EndOfFile may only fire once idle threshold is reached"
        );
    }
    kani::cover!(fires, "EndOfFile event fired");
    kani::cover!(
        !fires && eof_emitted,
        "EndOfFile suppressed after already emitted in same no-data streak"
    );
}

/// Data/truncation paths must reset both EOF state and idle streak.
#[kani::proof]
fn verify_data_resets_eof_flag() {
    let eof_emitted: bool = kani::any();
    let idle_polls: u8 = kani::any();
    let (new_flag, new_idle, fires) = eof_transition(eof_emitted, idle_polls, true); // Data
    assert!(!new_flag, "Data must reset eof_emitted to false");
    assert_eq!(new_idle, 0, "Data must reset idle poll streak");
    assert!(!fires, "Data must not emit EndOfFile");
    kani::cover!(eof_emitted, "eof_emitted was true before data arrived");
    kani::cover!(idle_polls > 0, "idle streak existed before data");
}

/// Two consecutive no-data polls emit EOF exactly once (on the second poll).
#[kani::proof]
fn verify_two_no_data_polls_emit_exactly_once() {
    let (state1, idle1, fires1) = eof_transition(false, 0, false); // first NoData
    let (state2, idle2, fires2) = eof_transition(state1, idle1, false); // second NoData
    assert!(!fires1, "first NoData poll must not emit EndOfFile");
    assert!(fires2, "second NoData poll must emit EndOfFile");
    assert!(state2, "flag becomes true once EOF fires");
    assert!(
        idle2 >= EOF_IDLE_POLLS_BEFORE_EMIT,
        "idle streak reached EOF threshold"
    );
    kani::cover!(!fires1 && fires2, "only second NoData emits EOF");
}

/// After reset, a fresh two-poll no-data streak emits EOF again.
#[kani::proof]
fn verify_eof_fires_again_after_data_resets_flag() {
    let (state1, idle1, fires1) = eof_transition(false, 0, false);
    let (state2, idle2, fires2) = eof_transition(state1, idle1, false);
    let (after_data, after_idle, fires3) = eof_transition(state2, idle2, true);
    let (state4, idle4, fires4) = eof_transition(after_data, after_idle, false);
    let (_, _, fires5) = eof_transition(state4, idle4, false);
    assert!(!fires1);
    assert!(fires2, "first no-data streak should fire on second poll");
    assert!(!fires3, "data reset must not emit EOF");
    assert!(!fires4, "new no-data streak first poll must not emit EOF");
    assert!(fires5, "new no-data streak second poll must emit EOF");
    kani::cover!(fires2, "first post-threshold EOF fired");
    kani::cover!(fires5, "EOF fires again after data reset");
}
