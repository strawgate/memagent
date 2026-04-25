#![cfg(kani)]

use super::state::{
    EOF_IDLE_POLLS_BEFORE_EMIT, EofModelState, MAX_BACKOFF_MS, backoff_delay_ms,
    backoff_transition, eof_model_transition, shutdown_should_emit_eof,
};

/// EndOfFile can only fire when we cross the idle-poll threshold for the first
/// time in a no-data streak.
#[kani::proof]
fn verify_eof_emitted_at_most_once_per_no_data_streak() {
    let emitted: bool = kani::any();
    let idle_polls: u8 = kani::any();
    let idle_window_elapsed: bool = kani::any();
    let state = EofModelState {
        emitted,
        idle_polls,
    };

    let (_, fires) = eof_model_transition(state, false, idle_window_elapsed);
    if fires {
        assert!(!emitted, "EndOfFile may only fire when flag was false");
        assert!(
            idle_polls.saturating_add(1) >= EOF_IDLE_POLLS_BEFORE_EMIT,
            "EndOfFile may only fire once idle threshold is reached"
        );
        assert!(
            idle_window_elapsed,
            "EndOfFile may only fire once minimum idle time has elapsed"
        );
    }

    kani::cover!(fires, "EndOfFile event fired");
    kani::cover!(
        !fires && emitted,
        "EndOfFile suppressed after already emitted in same no-data streak"
    );
}

/// Data/truncation paths must reset both EOF state and idle streak.
#[kani::proof]
fn verify_data_resets_eof_state() {
    let state = EofModelState {
        emitted: kani::any(),
        idle_polls: kani::any(),
    };
    let (next, fires) = eof_model_transition(state, true, kani::any()); // Data

    assert_eq!(
        next,
        EofModelState::default(),
        "Data must reset EOF model state"
    );
    assert!(!fires, "Data must not emit EndOfFile");
    kani::cover!(state.emitted, "eof_emitted was true before data arrived");
    kani::cover!(state.idle_polls > 0, "idle streak existed before data");
}

/// Two consecutive no-data polls emit EOF exactly once (on the second poll).
#[kani::proof]
fn verify_two_no_data_polls_emit_exactly_once() {
    let (state1, fires1) = eof_model_transition(EofModelState::default(), false, false);
    let (state2, fires2) = eof_model_transition(state1, false, true);

    assert!(!fires1, "first NoData poll must not emit EndOfFile");
    assert!(fires2, "second NoData poll must emit EndOfFile");
    assert!(state2.emitted, "flag becomes true once EOF fires");
    assert!(
        state2.idle_polls >= EOF_IDLE_POLLS_BEFORE_EMIT,
        "idle streak reached EOF threshold"
    );
    kani::cover!(!fires1 && fires2, "only second NoData emits EOF");
}

/// After reset, a fresh two-poll no-data streak emits EOF again.
#[kani::proof]
fn verify_eof_fires_again_after_data_resets_flag() {
    let (state1, fires1) = eof_model_transition(EofModelState::default(), false, false);
    let (state2, fires2) = eof_model_transition(state1, false, true);
    let (after_data, fires3) = eof_model_transition(state2, true, false);
    let (state4, fires4) = eof_model_transition(after_data, false, false);
    let (_, fires5) = eof_model_transition(state4, false, true);

    assert!(!fires1);
    assert!(fires2, "first no-data streak should fire on second poll");
    assert!(!fires3, "data reset must not emit EOF");
    assert!(!fires4, "new no-data streak first poll must not emit EOF");
    assert!(fires5, "new no-data streak second poll must emit EOF");
    kani::cover!(fires2, "first post-threshold EOF fired");
    kani::cover!(fires5, "EOF fires again after data reset");
}

#[kani::proof]
fn verify_shutdown_eof_requires_caught_up_offset() {
    let offset: u64 = kani::any();
    let file_size: u64 = kani::any();

    let should_emit = shutdown_should_emit_eof(offset, file_size);
    assert_eq!(
        should_emit,
        offset >= file_size,
        "shutdown EOF predicate must match caught-up offset check"
    );

    if should_emit {
        assert!(
            offset >= file_size,
            "shutdown EOF may only emit when offset is caught up"
        );
    } else {
        assert!(
            offset < file_size,
            "shutdown EOF must be suppressed while unread bytes remain"
        );
    }

    kani::cover!(should_emit, "shutdown EOF can emit for caught-up file");
    kani::cover!(
        !should_emit,
        "shutdown EOF can be suppressed for unread suffix"
    );
}

#[kani::proof]
fn verify_backoff_is_capped_and_non_zero_after_error() {
    let current_errors: u32 = kani::any();
    let (next_errors, next_backoff) = backoff_transition(current_errors, true);

    assert!(
        next_errors >= current_errors,
        "error transitions must not decrease consecutive error count"
    );
    assert!(
        next_backoff.is_some(),
        "error transition must set a backoff"
    );
    if let Some(ms) = next_backoff {
        assert!(ms > 0, "backoff delay must be positive");
        assert!(ms <= MAX_BACKOFF_MS, "backoff delay must be capped");
    }
    kani::cover!(
        next_backoff == Some(MAX_BACKOFF_MS),
        "backoff cap is reachable"
    );
}

#[kani::proof]
fn verify_backoff_resets_on_clean_poll() {
    let current_errors: u32 = kani::any();
    let (next_errors, next_backoff) = backoff_transition(current_errors, false);
    assert_eq!(next_errors, 0, "clean poll must reset consecutive errors");
    assert!(next_backoff.is_none(), "clean poll must clear backoff");
}

#[kani::proof]
fn verify_backoff_delay_is_monotonic_in_error_count() {
    let a: u32 = kani::any();
    let b: u32 = kani::any();
    if a <= b {
        assert!(
            backoff_delay_ms(a) <= backoff_delay_ms(b),
            "backoff delay must be monotonic as error streak grows"
        );
    }
    kani::cover!(a < b, "strict growth case explored");
}
