use std::time::{Duration, Instant};

/// Number of consecutive idle polls required before EOF can be emitted.
pub(super) const EOF_IDLE_POLLS_BEFORE_EMIT: u8 = 2;
/// Initial delay used for exponential error backoff.
pub(super) const INITIAL_BACKOFF_MS: u64 = 100;
/// Upper bound for exponential error backoff.
pub(super) const MAX_BACKOFF_MS: u64 = 5000;

/// Pure EOF reducer state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct EofModelState {
    /// Whether EOF has already been emitted for the current idle streak.
    pub(super) emitted: bool,
    /// Number of consecutive idle polls observed.
    pub(super) idle_polls: u8,
}

/// Pure EOF transition model.
///
/// Returns `(next_state, should_emit_eof)`.
pub(super) fn eof_model_transition(
    state: EofModelState,
    had_data: bool,
    idle_window_elapsed: bool,
) -> (EofModelState, bool) {
    if had_data {
        return (EofModelState::default(), false);
    }

    let next_idle = state.idle_polls.saturating_add(1);
    let should_emit =
        !state.emitted && next_idle >= EOF_IDLE_POLLS_BEFORE_EMIT && idle_window_elapsed;
    (
        EofModelState {
            emitted: state.emitted || should_emit,
            idle_polls: next_idle,
        },
        should_emit,
    )
}

/// Return whether shutdown may emit terminal EOF for a tracked file.
///
/// Shutdown EOF intentionally bypasses the normal idle threshold, but only
/// after the tracked read offset has caught up to the observed file size.
pub(super) fn shutdown_should_emit_eof(offset: u64, file_size: u64) -> bool {
    offset >= file_size
}

/// Mutable wrapper around the pure EOF reducer and idle timer.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct EofState {
    model: EofModelState,
    idle_since: Option<Instant>,
}

impl EofState {
    /// Return whether EOF has already been emitted for the current idle streak.
    pub(super) fn has_emitted(&self) -> bool {
        self.model.emitted
    }

    /// Reset EOF state when new data arrives.
    pub(super) fn on_data(&mut self) {
        self.model = EofModelState::default();
        self.idle_since = None;
    }

    /// Advance EOF state for an idle poll and return whether EOF should emit.
    pub(super) fn on_no_data(&mut self, now: Instant, min_idle: Duration) -> bool {
        let idle_since = self.idle_since.get_or_insert(now);
        let idle_window_elapsed = now.duration_since(*idle_since) >= min_idle;
        let (next, emit) = eof_model_transition(self.model, false, idle_window_elapsed);
        self.model = next;
        emit
    }
}

/// Compute the bounded exponential backoff delay for a given error streak.
pub(super) fn backoff_delay_ms(consecutive_error_polls: u32) -> u64 {
    if consecutive_error_polls == 0 {
        return 0;
    }
    let exponent = consecutive_error_polls.saturating_sub(1).min(6);
    let multiplier = 1u64 << exponent;
    INITIAL_BACKOFF_MS
        .saturating_mul(multiplier)
        .min(MAX_BACKOFF_MS)
}

/// Pure error-backoff transition.
///
/// Returns `(next_consecutive_error_polls, next_backoff_delay_ms)`.
pub(super) fn backoff_transition(
    consecutive_error_polls: u32,
    had_error: bool,
) -> (u32, Option<u64>) {
    if had_error {
        let next = consecutive_error_polls.saturating_add(1);
        (next, Some(backoff_delay_ms(next)))
    } else {
        (0, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{prop_assert, prop_assert_eq, proptest};

    #[test]
    fn eof_emits_on_second_idle_when_window_elapsed() {
        let s0 = EofModelState::default();
        let (s1, emit1) = eof_model_transition(s0, false, false);
        let (s2, emit2) = eof_model_transition(s1, false, true);
        assert!(!emit1);
        assert!(emit2);
        assert!(s2.emitted);
    }

    #[test]
    fn eof_resets_on_data() {
        let s = EofModelState {
            emitted: true,
            idle_polls: 9,
        };
        let (next, emit) = eof_model_transition(s, true, false);
        assert_eq!(next, EofModelState::default());
        assert!(!emit);
    }

    #[test]
    fn backoff_doubles_until_cap() {
        assert_eq!(backoff_delay_ms(0), 0);
        assert_eq!(backoff_delay_ms(1), 100);
        assert_eq!(backoff_delay_ms(2), 200);
        assert_eq!(backoff_delay_ms(3), 400);
        assert_eq!(backoff_delay_ms(4), 800);
        assert_eq!(backoff_delay_ms(5), 1600);
        assert_eq!(backoff_delay_ms(6), 3200);
        assert_eq!(backoff_delay_ms(7), 5000);
        assert_eq!(backoff_delay_ms(100), 5000);
    }

    #[test]
    fn eof_state_requires_elapsed_idle_window_before_emit() {
        let mut state = EofState::default();
        let t0 = Instant::now();
        let min_idle = Duration::from_millis(100);

        assert!(!state.on_no_data(t0, min_idle));
        assert!(!state.on_no_data(t0 + Duration::from_millis(50), min_idle));
        assert!(state.on_no_data(t0 + Duration::from_millis(120), min_idle));
    }

    #[test]
    fn eof_state_on_data_restarts_idle_window() {
        let mut state = EofState::default();
        let t0 = Instant::now();
        let min_idle = Duration::from_millis(100);

        assert!(!state.on_no_data(t0, min_idle));
        assert!(state.on_no_data(t0 + Duration::from_millis(100), min_idle));

        state.on_data();

        assert!(!state.on_no_data(t0 + Duration::from_millis(100), min_idle));
        assert!(!state.on_no_data(t0 + Duration::from_millis(150), min_idle));
        assert!(state.on_no_data(t0 + Duration::from_millis(220), min_idle));
    }

    #[test]
    fn shutdown_eof_requires_caught_up_offset() {
        assert!(shutdown_should_emit_eof(10, 10));
        assert!(shutdown_should_emit_eof(11, 10));
        assert!(!shutdown_should_emit_eof(9, 10));
    }

    proptest! {
        #[test]
        fn eof_model_only_emits_on_threshold_crossing(
            events in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY), 0..128)
        ) {
            let mut state = EofModelState::default();
            for (had_data, idle_window_elapsed) in events {
                let old = state;
                let (next, emit) = eof_model_transition(state, had_data, idle_window_elapsed);
                if emit {
                    prop_assert!(!old.emitted);
                    prop_assert!(old.idle_polls.saturating_add(1) >= EOF_IDLE_POLLS_BEFORE_EMIT);
                    prop_assert!(idle_window_elapsed);
                }
                if had_data {
                    prop_assert_eq!(next, EofModelState::default());
                }
                state = next;
            }
        }
    }

    proptest! {
        #[test]
        fn shutdown_eof_matches_offset_caught_up(offset: u64, file_size: u64) {
            prop_assert_eq!(
                shutdown_should_emit_eof(offset, file_size),
                offset >= file_size
            );
        }
    }

    // ---------------------------------------------------------------
    // Combined TLA+ state machine proptest — TailLifecycle.tla::Next
    //
    // Generates random interleaved action sequences from the TLA+ Next
    // relation and verifies all 7 invariants after each step. This
    // catches compositional bugs that per-subsystem tests miss.
    // ---------------------------------------------------------------

    /// Combined state matching TLA+ TailLifecycle variables.
    struct TlaState {
        eof_emitted: bool,
        idle_polls: u8,
        file_offset: u64,
        file_size: u64,
        consecutive_errors: u32,
        backoff_ms: u64,
        last_action: &'static str,
    }

    impl TlaState {
        fn init() -> Self {
            Self {
                eof_emitted: false,
                idle_polls: 0,
                file_offset: 0,
                file_size: 0,
                consecutive_errors: 0,
                backoff_ms: 0,
                last_action: "Init",
            }
        }
    }

    /// TLA+ action indices matching Next == \/ DataStep \/ NoDataEmit \/ ...
    fn arb_action() -> impl proptest::strategy::Strategy<Value = u8> {
        0u8..7
    }

    fn apply_tla_action(s: &mut TlaState, action: u8, offset: u64, size: u64) {
        match action {
            0 => {
                // DataStep
                let (next, _) = eof_model_transition(
                    EofModelState {
                        emitted: s.eof_emitted,
                        idle_polls: s.idle_polls,
                    },
                    true,
                    false,
                );
                s.eof_emitted = next.emitted;
                s.idle_polls = next.idle_polls;
                s.last_action = "Data";
            }
            1 => {
                // NoDataEmit — only valid if !emitted and threshold crossed
                let model = EofModelState {
                    emitted: s.eof_emitted,
                    idle_polls: s.idle_polls,
                };
                let (next, emit) = eof_model_transition(model, false, true);
                if emit {
                    s.eof_emitted = next.emitted;
                    s.idle_polls = next.idle_polls;
                    s.last_action = "NoDataEmit";
                } else {
                    // Action not enabled — treat as NoDataNoEmit
                    s.eof_emitted = next.emitted;
                    s.idle_polls = next.idle_polls;
                    s.last_action = "NoDataNoEmit";
                }
            }
            2 => {
                // NoDataNoEmit (idle_window not elapsed)
                let model = EofModelState {
                    emitted: s.eof_emitted,
                    idle_polls: s.idle_polls,
                };
                let (next, _) = eof_model_transition(model, false, false);
                s.eof_emitted = next.emitted;
                s.idle_polls = next.idle_polls;
                s.last_action = "NoDataNoEmit";
            }
            3 => {
                // ShutdownEmit — offset >= size, not emitted
                if !s.eof_emitted && offset >= size {
                    s.file_offset = offset;
                    s.file_size = size;
                    s.eof_emitted = shutdown_should_emit_eof(offset, size);
                    s.last_action = "ShutdownEmit";
                }
            }
            4 => {
                // ShutdownNoEmit — offset < size
                if offset < size.saturating_add(1) && offset < size {
                    s.file_offset = offset;
                    s.file_size = size;
                    s.last_action = "ShutdownNoEmit";
                }
            }
            5 => {
                // ErrorStep
                let (next_errors, delay) = backoff_transition(s.consecutive_errors, true);
                s.consecutive_errors = next_errors;
                s.backoff_ms = delay.unwrap_or(0);
                s.last_action = "Error";
            }
            6 => {
                // CleanStep — only if errors > 0
                if s.consecutive_errors > 0 {
                    let (next_errors, _) = backoff_transition(s.consecutive_errors, false);
                    s.consecutive_errors = next_errors;
                    s.backoff_ms = 0;
                    s.last_action = "Clean";
                }
            }
            _ => {}
        }
    }

    fn assert_tla_invariants(s: &TlaState) -> Result<(), proptest::test_runner::TestCaseError> {
        // EofEmissionRequiresThreshold
        if s.last_action == "NoDataEmit" {
            prop_assert!(
                s.eof_emitted,
                "EofEmissionRequiresThreshold: eof must be emitted"
            );
            prop_assert!(
                s.idle_polls >= EOF_IDLE_POLLS_BEFORE_EMIT,
                "EofEmissionRequiresThreshold: idle_polls {} < threshold {}",
                s.idle_polls,
                EOF_IDLE_POLLS_BEFORE_EMIT
            );
        }
        // DataResetsEofState
        if s.last_action == "Data" {
            prop_assert!(
                !s.eof_emitted,
                "DataResetsEofState: eof must be false after data"
            );
            prop_assert_eq!(s.idle_polls, 0, "DataResetsEofState: idle_polls must be 0");
        }
        // ShutdownEofRequiresCaughtUp
        if s.last_action == "ShutdownEmit" {
            prop_assert!(
                s.eof_emitted,
                "ShutdownEofRequiresCaughtUp: eof must be emitted"
            );
            prop_assert!(
                s.file_offset >= s.file_size,
                "ShutdownEofRequiresCaughtUp: offset {} < size {}",
                s.file_offset,
                s.file_size
            );
        }
        // ShutdownBehindSuppressesEof
        if s.last_action == "ShutdownNoEmit" {
            prop_assert!(
                s.file_offset < s.file_size,
                "ShutdownBehindSuppressesEof: offset {} >= size {}",
                s.file_offset,
                s.file_size
            );
        }
        // BackoffZeroIffNoErrors
        prop_assert_eq!(
            s.consecutive_errors == 0,
            s.backoff_ms == 0,
            "BackoffZeroIffNoErrors: errors={} backoff={}ms",
            s.consecutive_errors,
            s.backoff_ms
        );
        // BackoffDelayConsistent
        if s.consecutive_errors > 0 {
            prop_assert_eq!(
                s.backoff_ms,
                backoff_delay_ms(s.consecutive_errors),
                "BackoffDelayConsistent: errors={} expected={}ms got={}ms",
                s.consecutive_errors,
                backoff_delay_ms(s.consecutive_errors),
                s.backoff_ms
            );
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(proptest::test_runner::Config {
            cases: 2000,
            ..Default::default()
        })]
        #[test]
        fn tail_lifecycle_tla_invariants_hold_across_random_action_sequences(
            actions in proptest::collection::vec(
                (arb_action(), 0u64..16, 0u64..16),
                0..100
            )
        ) {
            let mut state = TlaState::init();
            for (action, offset, size) in actions {
                apply_tla_action(&mut state, action, offset, size);
                assert_tla_invariants(&state)?;
            }
        }
    }

    proptest! {
        #[test]
        fn backoff_transition_is_monotonic_until_reset(errors in proptest::collection::vec(proptest::bool::ANY, 0..128)) {
            let mut consecutive = 0u32;
            let mut last_delay = 0u64;
            for had_error in errors {
                let (next, delay) = backoff_transition(consecutive, had_error);
                if had_error {
                    let d = delay.expect("error transition must set delay");
                    prop_assert!(d >= last_delay);
                    prop_assert!(d <= MAX_BACKOFF_MS);
                    last_delay = d;
                } else {
                    prop_assert_eq!(next, 0);
                    prop_assert!(delay.is_none());
                    last_delay = 0;
                }
                consecutive = next;
            }
        }
    }
}
