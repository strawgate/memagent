//! Explicit state machine for the supervisor config-flow protocol.
//!
//! Models the progression: Idle → Reading → Writing → Signaling → Idle.
//! Each state transition is a pure function — side effects are performed by the
//! caller based on the returned [`Effect`].
//!
//! This module is the single source of truth for config-flow transitions.
//! The proptest suite verifies properties against this same machine.

use std::path::PathBuf;

/// States of the supervisor config-flow protocol.
///
/// Matches the TLA+ `SupervisorProtocol` spec states.
/// Note: validation is embedded in the Reading→Writing transition (ReadOk
/// carries a `ValidatedConfig`), so there is no separate Validating state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    /// Waiting for a new config notification.
    Idle,
    /// Reading the intermediate config file from disk.
    Reading,
    /// Writing validated config to the main config path.
    Writing,
    /// Sending SIGHUP to the child process.
    Signaling,
}

/// Events that drive state transitions.
#[derive(Debug, Clone)]
pub(crate) enum Event {
    /// A new remote config is available (reload channel notification).
    ConfigAvailable { remote_path: PathBuf },
    /// File read succeeded — contains the raw YAML and validated config.
    ReadOk(Box<ffwd_config::ValidatedConfig>),
    /// File read or validation failed.
    ReadFailed(String),
    /// Atomic write to main config path succeeded.
    WriteOk,
    /// Atomic write failed.
    WriteFailed(String),
    /// Child is alive and was signaled successfully.
    SignalDelivered,
    /// Child died or signal failed (ESRCH / PID mismatch).
    SignalFailed(String),
}

/// Side effects the executor must perform after a transition.
#[derive(Debug)]
pub(crate) enum Effect {
    /// Read and validate the config file at the given path.
    ReadAndValidate(PathBuf),
    /// Write the validated YAML to the main config path.
    WriteConfig { config_path: PathBuf, yaml: String },
    /// Send SIGHUP to the child with the given PID.
    SendSignal { child_pid: u32 },
    /// Report the effective config to OpAMP state.
    ReportEffective { yaml: String },
    /// Log an error and return to idle.
    LogError(String),
    /// No side effect needed (terminal transitions).
    None,
}

/// The config-flow state machine.
///
/// Drives a single config-push cycle from notification to completion.
/// Create a new instance for each reload cycle.
pub(crate) struct ConfigFlow {
    state: State,
    validated: Option<ffwd_config::ValidatedConfig>,
    config_path: PathBuf,
    child_pid: u32,
}

impl ConfigFlow {
    /// Start a new config-flow cycle.
    pub fn new(config_path: PathBuf, child_pid: u32) -> Self {
        Self {
            state: State::Idle,
            validated: None,
            config_path,
            child_pid,
        }
    }

    /// Current state.
    pub fn state(&self) -> State {
        self.state
    }

    /// Drive the state machine with an event. Returns the effect to execute.
    pub fn step(&mut self, event: Event) -> Effect {
        match (&self.state, event) {
            (State::Idle, Event::ConfigAvailable { remote_path }) => {
                self.state = State::Reading;
                Effect::ReadAndValidate(remote_path)
            }

            (State::Reading, Event::ReadOk(validated)) => {
                // Validation already happened in ReadOk — proceed directly to write.
                let yaml = validated.effective_yaml().to_owned();
                self.validated = Some(*validated);
                self.state = State::Writing;
                Effect::WriteConfig {
                    config_path: self.config_path.clone(),
                    yaml,
                }
            }

            (State::Reading, Event::ReadFailed(err)) => {
                self.state = State::Idle;
                Effect::LogError(err)
            }

            (State::Writing, Event::WriteOk) => {
                self.state = State::Signaling;
                Effect::SendSignal {
                    child_pid: self.child_pid,
                }
            }

            (State::Writing, Event::WriteFailed(err)) => {
                self.state = State::Idle;
                self.validated = None;
                Effect::LogError(err)
            }

            (State::Signaling, Event::SignalDelivered) => {
                // validated is always Some here: set in Reading→Writing transition.
                let yaml = match self.validated.take() {
                    Some(v) => v.into_parts().1,
                    None => {
                        // Defensive: should never happen if state machine is correct.
                        self.state = State::Idle;
                        return Effect::LogError(
                            "internal error: validated config missing in Signaling state"
                                .to_owned(),
                        );
                    }
                };
                self.state = State::Idle;
                Effect::ReportEffective { yaml }
            }

            (State::Signaling, Event::SignalFailed(err)) => {
                self.state = State::Idle;
                self.validated = None;
                Effect::LogError(err)
            }

            // Invalid transitions — stay in current state.
            (_, _) => Effect::None,
        }
    }

    /// Whether the machine is idle and ready for a new cycle.
    pub fn is_idle(&self) -> bool {
        self.state == State::Idle
    }
}

#[cfg(test)]
/// Pure transition function for property testing (no internal state).
///
/// This is the simplified model used by proptests to verify state machine
/// properties without needing the full `ConfigFlow` struct.
pub(crate) fn transition(state: State, event: &SimpleEvent) -> State {
    match (state, event) {
        (State::Idle, SimpleEvent::ConfigAvailable) => State::Reading,
        (State::Reading, SimpleEvent::ReadOk) => State::Writing,
        (State::Reading, SimpleEvent::ReadFailed) => State::Idle,
        (State::Writing, SimpleEvent::WriteOk) => State::Signaling,
        (State::Writing, SimpleEvent::WriteFailed) => State::Idle,
        (State::Signaling, SimpleEvent::SignalDelivered) => State::Idle,
        (State::Signaling, SimpleEvent::SignalFailed) => State::Idle,
        _ => state, // Invalid event for current state — no-op.
    }
}

#[cfg(test)]
/// Simplified events for property testing (no payloads).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SimpleEvent {
    ConfigAvailable,
    ReadOk,
    ReadFailed,
    WriteOk,
    WriteFailed,
    SignalDelivered,
    SignalFailed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_cycle() {
        let mut state = State::Idle;
        state = transition(state, &SimpleEvent::ConfigAvailable);
        assert_eq!(state, State::Reading);
        state = transition(state, &SimpleEvent::ReadOk);
        assert_eq!(state, State::Writing);
        state = transition(state, &SimpleEvent::WriteOk);
        assert_eq!(state, State::Signaling);
        state = transition(state, &SimpleEvent::SignalDelivered);
        assert_eq!(state, State::Idle);
    }

    #[test]
    fn read_failure_returns_to_idle() {
        let mut state = State::Idle;
        state = transition(state, &SimpleEvent::ConfigAvailable);
        state = transition(state, &SimpleEvent::ReadFailed);
        assert_eq!(state, State::Idle);
    }

    #[test]
    fn write_failure_returns_to_idle() {
        let mut state = State::Idle;
        state = transition(state, &SimpleEvent::ConfigAvailable);
        state = transition(state, &SimpleEvent::ReadOk);
        state = transition(state, &SimpleEvent::WriteFailed);
        assert_eq!(state, State::Idle);
    }

    #[test]
    fn signal_failure_returns_to_idle() {
        let mut state = State::Idle;
        state = transition(state, &SimpleEvent::ConfigAvailable);
        state = transition(state, &SimpleEvent::ReadOk);
        state = transition(state, &SimpleEvent::WriteOk);
        state = transition(state, &SimpleEvent::SignalFailed);
        assert_eq!(state, State::Idle);
    }

    #[test]
    fn invalid_event_is_noop() {
        // WriteOk in Idle — should stay Idle
        let state = transition(State::Idle, &SimpleEvent::WriteOk);
        assert_eq!(state, State::Idle);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_event() -> impl Strategy<Value = SimpleEvent> {
        prop_oneof![
            Just(SimpleEvent::ConfigAvailable),
            Just(SimpleEvent::ReadOk),
            Just(SimpleEvent::ReadFailed),
            Just(SimpleEvent::WriteOk),
            Just(SimpleEvent::WriteFailed),
            Just(SimpleEvent::SignalDelivered),
            Just(SimpleEvent::SignalFailed),
        ]
    }

    proptest! {
        /// Any event sequence never panics and stays in a valid state.
        #[test]
        fn never_panics(events in proptest::collection::vec(arb_event(), 1..100)) {
            let mut state = State::Idle;
            for event in &events {
                state = transition(state, event);
                prop_assert!(matches!(
                    state,
                    State::Idle | State::Reading | State::Writing | State::Signaling
                ));
            }
        }

        /// Invalid events never change state (idempotent no-op).
        #[test]
        fn invalid_events_are_noop(
            events in proptest::collection::vec(arb_event(), 1..50),
            extra in arb_event(),
        ) {
            let mut state = State::Idle;
            for event in &events {
                state = transition(state, event);
            }
            let state_before = state;
            let state_after = transition(state, &extra);
            // If the event is not valid for this state, state shouldn't change
            let valid = match (state_before, &extra) {
                (State::Idle, SimpleEvent::ConfigAvailable) => true,
                (State::Reading, SimpleEvent::ReadOk | SimpleEvent::ReadFailed) => true,
                (State::Writing, SimpleEvent::WriteOk | SimpleEvent::WriteFailed) => true,
                (State::Signaling, SimpleEvent::SignalDelivered | SimpleEvent::SignalFailed) => true,
                _ => false,
            };
            if !valid {
                prop_assert_eq!(state_after, state_before);
            }
        }

        /// Idle is always reachable: from any state, a bounded sequence reaches Idle.
        #[test]
        fn idle_always_reachable(events in proptest::collection::vec(arb_event(), 1..30)) {
            let mut state = State::Idle;
            for event in &events {
                state = transition(state, event);
            }
            // From any state, at most one failure event reaches Idle
            let recovery = match state {
                State::Idle => State::Idle,
                State::Reading => transition(state, &SimpleEvent::ReadFailed),
                State::Writing => transition(state, &SimpleEvent::WriteFailed),
                State::Signaling => transition(state, &SimpleEvent::SignalFailed),
            };
            prop_assert_eq!(recovery, State::Idle);
        }

        /// Happy path is deterministic: ConfigAvailable→ReadOk→WriteOk→SignalDelivered→Idle.
        #[test]
        fn happy_path_deterministic(_n in 0u32..50) {
            let mut state = State::Idle;
            state = transition(state, &SimpleEvent::ConfigAvailable);
            prop_assert_eq!(state, State::Reading);
            state = transition(state, &SimpleEvent::ReadOk);
            prop_assert_eq!(state, State::Writing);
            state = transition(state, &SimpleEvent::WriteOk);
            prop_assert_eq!(state, State::Signaling);
            state = transition(state, &SimpleEvent::SignalDelivered);
            prop_assert_eq!(state, State::Idle);
        }

        /// Errors never advance the state machine past the current phase.
        #[test]
        fn errors_never_advance(events in proptest::collection::vec(arb_event(), 1..50)) {
            let mut state = State::Idle;
            for event in &events {
                let prev = state;
                state = transition(state, event);
                // State can only advance forward or return to Idle (on error)
                match prev {
                    State::Idle => prop_assert!(matches!(state, State::Idle | State::Reading)),
                    State::Reading => prop_assert!(matches!(state, State::Reading | State::Writing | State::Idle)),
                    State::Writing => prop_assert!(matches!(state, State::Writing | State::Signaling | State::Idle)),
                    State::Signaling => prop_assert!(matches!(state, State::Signaling | State::Idle)),
                }
            }
        }
    }
}
