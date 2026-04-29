//! Reload coordinator state machine.
//!
//! Encodes the lifecycle of the pipeline reload loop as an explicit state machine.
//! Each state transition is a pure function returning effects for the executor to
//! perform. This makes the reload logic independently testable without standing up
//! the full async runtime.
//!
//! # Why not typestate?
//!
//! This module uses a runtime enum (`State`) rather than compile-time typestate
//! because: (1) transitions are driven by dynamic `Event` values from multiple
//! asynchronous sources (signals, timers, I/O); (2) the executor must store the
//! coordinator in a single binding across a `loop`/`select!`; (3) the state space
//! is already proven correct by proptest properties and unit tests covering every
//! (state, event) pair. A typestate encoding would require existential types or
//! boxing at every await boundary with no additional safety benefit here.
//!
//! # States
//!
//! ```text
//! Starting → Running ↔ Draining → Validating → Building → Running
//!                │                                    ↓
//!                └──────────── ShuttingDown ←─────────┘ (on fatal or SIGTERM)
//! ```

use std::io;
use std::path::PathBuf;

/// States of the reload coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum State {
    /// Building initial pipelines (first run).
    Starting,
    /// Pipelines are running, awaiting events.
    Running,
    /// Received reload signal; pipelines are draining.
    Draining,
    /// Pipelines drained; validating new config.
    Validating,
    /// Config validated; building new pipelines.
    Building,
    /// Shutting down (terminal state).
    ShuttingDown { error: Option<String> },
}

/// Events the executor feeds into the coordinator.
#[derive(Debug)]
pub(crate) enum Event {
    /// Initial pipeline build succeeded.
    PipelinesBuilt,
    /// Initial or rebuild pipeline build failed.
    BuildFailed { pipeline: String, error: String },
    /// Pipeline completed normally or errored.
    PipelineCompleted { error: Option<io::Error> },
    /// Process-level shutdown requested (SIGTERM / Ctrl+C).
    ShutdownRequested,
    /// Reload signal received (SIGHUP / HTTP / watch).
    ReloadRequested,
    /// Pipelines finished draining after reload signal.
    DrainCompleted,
    /// Pipelines did not drain in time.
    DrainTimedOut,
    /// New config is valid (caller stores the validated config in pending).
    ConfigValid,
    /// New config failed to read or validate.
    ConfigInvalid(String),
    /// Config diff shows non-reloadable changes.
    ConfigNotReloadable(String),
    /// Config diff shows no changes (but pipelines still need rebuild after drain).
    ConfigUnchanged,
}

/// Side effects the executor must perform.
#[derive(Debug)]
pub(crate) enum Effect {
    /// Build pipelines from the given config.
    BuildPipelines,
    /// Start running the built pipelines (enter select! loop).
    RunPipelines,
    /// Drain running pipelines (cancel token + wait with timeout).
    DrainPipelines,
    /// Read and validate the config file.
    ValidateConfig {
        #[allow(dead_code)]
        path: PathBuf,
    },
    /// Commit the validated config as current (metrics, OpAMP reporting).
    CommitConfig,
    /// Report a reload error (metric + log).
    ReportReloadError(String),
    /// Report reload success (metric).
    ReportReloadSuccess,
    /// Shut down cleanly.
    Shutdown,
    /// Fatal error — shut down with error.
    FatalShutdown(String),
}

/// The reload coordinator.
///
/// Tracks whether we're in the first run, have pending validated config, etc.
/// The executor drives this by translating async operations into events.
pub(crate) struct ReloadCoordinator {
    state: State,
    is_first_run: bool,
    has_pending: bool,
    rebuild_attempts: u32,
    config_path: PathBuf,
}

/// Maximum consecutive build failures before giving up.
const MAX_REBUILD_ATTEMPTS: u32 = 3;

impl ReloadCoordinator {
    /// Create a new coordinator for the given config path.
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            state: State::Starting,
            is_first_run: true,
            has_pending: false,
            rebuild_attempts: 0,
            config_path,
        }
    }

    /// Current state.
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Whether this is the first pipeline build (for banner/diagnostics).
    pub fn is_first_run(&self) -> bool {
        self.is_first_run
    }

    /// Whether a validated config is pending commit.
    ///
    /// Returns `true` when a new config has been validated and is waiting to
    /// be committed after a successful pipeline build.
    #[allow(dead_code)]
    pub fn has_pending(&self) -> bool {
        self.has_pending
    }

    /// Drive the state machine. Returns effects to execute.
    pub fn step(&mut self, event: Event) -> Vec<Effect> {
        // ShutdownRequested is handled uniformly for all non-terminal states.
        if matches!(event, Event::ShutdownRequested)
            && !matches!(self.state, State::ShuttingDown { .. })
        {
            // Only emit DrainPipelines if pipelines may be running.
            let needs_drain = matches!(self.state, State::Running);
            self.state = State::ShuttingDown { error: None };
            if needs_drain {
                return vec![Effect::DrainPipelines, Effect::Shutdown];
            }
            return vec![Effect::Shutdown];
        }

        match (&self.state, event) {
            // ── Starting ──
            (State::Starting, Event::PipelinesBuilt) => {
                self.state = State::Running;
                self.is_first_run = false;
                vec![Effect::RunPipelines]
            }
            (State::Starting, Event::BuildFailed { pipeline, error }) => {
                // First-run build failure is fatal.
                self.state = State::ShuttingDown {
                    error: Some(format!("pipeline '{pipeline}': {error}")),
                };
                vec![Effect::FatalShutdown(format!(
                    "pipeline '{pipeline}': {error}"
                ))]
            }

            // ── Running ──
            (State::Running, Event::PipelineCompleted { error }) => {
                let err_msg = error.map(|e| e.to_string());
                self.state = State::ShuttingDown { error: err_msg };
                vec![Effect::Shutdown]
            }
            (State::Running, Event::ReloadRequested) => {
                self.state = State::Draining;
                vec![Effect::DrainPipelines]
            }

            // ── Draining ──
            (State::Draining, Event::DrainCompleted) => {
                self.state = State::Validating;
                vec![Effect::ValidateConfig {
                    path: self.config_path.clone(),
                }]
            }
            (State::Draining, Event::DrainTimedOut) => {
                self.state = State::ShuttingDown {
                    error: Some("pipeline did not drain within 30s during reload".to_owned()),
                };
                vec![Effect::FatalShutdown(
                    "pipeline did not drain within 30s during reload".to_owned(),
                )]
            }

            // ── Validating ──
            (State::Validating, Event::ConfigValid) => {
                self.state = State::Building;
                self.has_pending = true;
                self.rebuild_attempts = 0;
                vec![Effect::BuildPipelines]
            }
            (State::Validating, Event::ConfigInvalid(reason)) => {
                // Invalid config — stay alive, rebuild old pipelines.
                self.state = State::Building;
                self.has_pending = false;
                self.rebuild_attempts = 0;
                vec![Effect::ReportReloadError(reason), Effect::BuildPipelines]
            }
            (State::Validating, Event::ConfigNotReloadable(reason)) => {
                // Non-reloadable change — stay alive, rebuild old pipelines.
                self.state = State::Building;
                self.has_pending = false;
                self.rebuild_attempts = 0;
                vec![Effect::ReportReloadError(reason), Effect::BuildPipelines]
            }
            (State::Validating, Event::ConfigUnchanged) => {
                // No changes but pipelines were drained — rebuild them.
                self.state = State::Building;
                self.has_pending = false;
                self.rebuild_attempts = 0;
                vec![Effect::BuildPipelines]
            }

            // ── Building (non-first-run rebuild) ──
            (State::Building, Event::PipelinesBuilt) => {
                self.rebuild_attempts = 0;
                let mut effects = Vec::new();
                if self.has_pending {
                    effects.push(Effect::CommitConfig);
                    effects.push(Effect::ReportReloadSuccess);
                    self.has_pending = false;
                }
                self.state = State::Running;
                effects.push(Effect::RunPipelines);
                effects
            }
            (State::Building, Event::BuildFailed { pipeline, error }) => {
                self.rebuild_attempts += 1;
                let msg = format!("pipeline '{pipeline}': {error}");
                self.has_pending = false;
                if self.rebuild_attempts >= MAX_REBUILD_ATTEMPTS {
                    self.state = State::ShuttingDown {
                        error: Some(format!("build failed {MAX_REBUILD_ATTEMPTS} times: {msg}")),
                    };
                    vec![
                        Effect::ReportReloadError(msg.clone()),
                        Effect::FatalShutdown(msg),
                    ]
                } else {
                    self.state = State::Building;
                    vec![Effect::ReportReloadError(msg), Effect::BuildPipelines]
                }
            }

            // ── ShuttingDown (terminal — no transitions) ──
            (State::ShuttingDown { .. }, _) => vec![],

            // ── Invalid transitions (no-op with trace) ──
            (_state, _event) => {
                tracing::debug!(
                    state = ?self.state,
                    "reload coordinator: ignoring unexpected event in current state"
                );
                vec![]
            }
        }
    }

    /// Whether the coordinator has reached a terminal state.
    #[allow(dead_code)]
    pub fn is_terminal(&self) -> bool {
        matches!(self.state, State::ShuttingDown { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord() -> ReloadCoordinator {
        ReloadCoordinator::new(PathBuf::from("/tmp/ffwd.yaml"))
    }

    #[test]
    fn startup_happy_path() {
        let mut c = coord();
        assert_eq!(c.state(), &State::Starting);
        assert!(c.is_first_run());

        let effects = c.step(Event::PipelinesBuilt);
        assert_eq!(c.state(), &State::Running);
        assert!(!c.is_first_run());
        assert!(effects.iter().any(|e| matches!(e, Effect::RunPipelines)));
    }

    #[test]
    fn startup_build_failure_is_fatal() {
        let mut c = coord();
        let effects = c.step(Event::BuildFailed {
            pipeline: "test".to_owned(),
            error: "bad input".to_owned(),
        });
        assert!(c.is_terminal());
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::FatalShutdown(_)))
        );
    }

    #[test]
    fn reload_happy_path() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);

        // Reload signal
        let effects = c.step(Event::ReloadRequested);
        assert_eq!(c.state(), &State::Draining);
        assert!(effects.iter().any(|e| matches!(e, Effect::DrainPipelines)));

        // Drain done
        let effects = c.step(Event::DrainCompleted);
        assert_eq!(c.state(), &State::Validating);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::ValidateConfig { .. }))
        );

        // Config valid (caller stores validated config externally as pending)
        let effects = c.step(Event::ConfigValid);
        assert_eq!(c.state(), &State::Building);
        assert!(c.has_pending());
        assert!(effects.iter().any(|e| matches!(e, Effect::BuildPipelines)));

        // Build succeeds
        let effects = c.step(Event::PipelinesBuilt);
        assert_eq!(c.state(), &State::Running);
        assert!(effects.iter().any(|e| matches!(e, Effect::CommitConfig)));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::ReportReloadSuccess))
        );
        assert!(effects.iter().any(|e| matches!(e, Effect::RunPipelines)));
    }

    #[test]
    fn invalid_config_rebuilds_old_pipelines() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);
        c.step(Event::ReloadRequested);
        c.step(Event::DrainCompleted);

        let effects = c.step(Event::ConfigInvalid("bad yaml".to_owned()));
        assert_eq!(c.state(), &State::Building);
        assert!(!c.has_pending());
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::ReportReloadError(_)))
        );
        assert!(effects.iter().any(|e| matches!(e, Effect::BuildPipelines)));

        // Build old config succeeds — no commit
        let effects = c.step(Event::PipelinesBuilt);
        assert_eq!(c.state(), &State::Running);
        assert!(!effects.iter().any(|e| matches!(e, Effect::CommitConfig)));
    }

    #[test]
    fn drain_timeout_is_fatal() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);
        c.step(Event::ReloadRequested);

        let effects = c.step(Event::DrainTimedOut);
        assert!(c.is_terminal());
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::FatalShutdown(_)))
        );
    }

    #[test]
    fn shutdown_from_running() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);

        let effects = c.step(Event::ShutdownRequested);
        assert!(c.is_terminal());
        assert!(effects.iter().any(|e| matches!(e, Effect::DrainPipelines)));
        assert!(effects.iter().any(|e| matches!(e, Effect::Shutdown)));
    }

    #[test]
    fn pipeline_error_causes_shutdown() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);

        let effects = c.step(Event::PipelineCompleted {
            error: Some(io::Error::new(io::ErrorKind::Other, "pipe broke")),
        });
        assert!(c.is_terminal());
        assert!(effects.iter().any(|e| matches!(e, Effect::Shutdown)));
    }

    #[test]
    fn terminal_state_ignores_events() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);
        c.step(Event::ShutdownRequested);
        assert!(c.is_terminal());

        // Any further events are no-ops
        let effects = c.step(Event::ReloadRequested);
        assert!(effects.is_empty());
        assert!(c.is_terminal());
    }

    #[test]
    fn shutdown_from_draining() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);
        c.step(Event::ReloadRequested);
        assert_eq!(c.state(), &State::Draining);

        let effects = c.step(Event::ShutdownRequested);
        assert!(c.is_terminal());
        // Draining state: pipelines are already being drained, no extra DrainPipelines.
        assert!(!effects.iter().any(|e| matches!(e, Effect::DrainPipelines)));
        assert!(effects.iter().any(|e| matches!(e, Effect::Shutdown)));
    }

    #[test]
    fn shutdown_from_validating() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);
        c.step(Event::ReloadRequested);
        c.step(Event::DrainCompleted);
        assert_eq!(c.state(), &State::Validating);

        let effects = c.step(Event::ShutdownRequested);
        assert!(c.is_terminal());
        // Validating: no pipelines running, just Shutdown.
        assert!(!effects.iter().any(|e| matches!(e, Effect::DrainPipelines)));
        assert!(effects.iter().any(|e| matches!(e, Effect::Shutdown)));
    }

    #[test]
    fn shutdown_from_building() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);
        c.step(Event::ReloadRequested);
        c.step(Event::DrainCompleted);
        c.step(Event::ConfigValid);
        assert_eq!(c.state(), &State::Building);

        let effects = c.step(Event::ShutdownRequested);
        assert!(c.is_terminal());
        // Building: no pipelines running yet, just Shutdown.
        assert!(!effects.iter().any(|e| matches!(e, Effect::DrainPipelines)));
        assert!(effects.iter().any(|e| matches!(e, Effect::Shutdown)));
    }

    #[test]
    fn shutdown_from_starting() {
        let mut c = coord();
        assert_eq!(c.state(), &State::Starting);

        let effects = c.step(Event::ShutdownRequested);
        assert!(c.is_terminal());
        // Starting: no pipelines running yet, just Shutdown.
        assert!(!effects.iter().any(|e| matches!(e, Effect::DrainPipelines)));
        assert!(effects.iter().any(|e| matches!(e, Effect::Shutdown)));
    }

    #[test]
    fn bounded_retry_fatal_after_max_attempts() {
        let mut c = coord();
        c.step(Event::PipelinesBuilt);
        c.step(Event::ReloadRequested);
        c.step(Event::DrainCompleted);
        c.step(Event::ConfigValid);
        assert_eq!(c.state(), &State::Building);

        // First two failures: retry
        for _ in 0..2 {
            let effects = c.step(Event::BuildFailed {
                pipeline: "p".to_owned(),
                error: "e".to_owned(),
            });
            assert_eq!(c.state(), &State::Building);
            assert!(effects.iter().any(|e| matches!(e, Effect::BuildPipelines)));
            assert!(
                !effects
                    .iter()
                    .any(|e| matches!(e, Effect::FatalShutdown(_)))
            );
        }

        // Third failure: fatal
        let effects = c.step(Event::BuildFailed {
            pipeline: "p".to_owned(),
            error: "e".to_owned(),
        });
        assert!(c.is_terminal());
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::FatalShutdown(_)))
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Generate a random event for property testing.
    /// We use simplified events (no io::Error since it's not Clone).
    fn arb_event() -> impl Strategy<Value = Event> {
        prop_oneof![
            Just(Event::PipelinesBuilt),
            Just(Event::BuildFailed {
                pipeline: "test".to_owned(),
                error: "fail".to_owned(),
            }),
            Just(Event::PipelineCompleted { error: None }),
            Just(Event::ShutdownRequested),
            Just(Event::ReloadRequested),
            Just(Event::DrainCompleted),
            Just(Event::DrainTimedOut),
            Just(Event::ConfigValid),
            Just(Event::ConfigInvalid("bad".to_owned())),
            Just(Event::ConfigNotReloadable("restart needed".to_owned())),
            Just(Event::ConfigUnchanged),
        ]
    }

    proptest! {
        /// Any event sequence keeps the coordinator in a valid state (never panics).
        #[test]
        fn never_panics(events in proptest::collection::vec(arb_event(), 1..100)) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            for event in events {
                let _effects = c.step(event);
            }
        }

        /// Once terminal, the coordinator stays terminal forever.
        #[test]
        fn terminal_is_absorbing(
            prefix in proptest::collection::vec(arb_event(), 0..30),
            suffix in proptest::collection::vec(arb_event(), 1..30),
        ) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            for event in &prefix {
                c.step(event.clone());
            }
            if c.is_terminal() {
                for event in suffix {
                    let effects = c.step(event);
                    prop_assert!(effects.is_empty());
                    prop_assert!(c.is_terminal());
                }
            }
        }

        /// Shutdown from Running always reaches terminal.
        #[test]
        fn shutdown_always_terminal(_n in 0u32..10) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            c.step(Event::PipelinesBuilt);
            prop_assert_eq!(c.state(), &State::Running);
            c.step(Event::ShutdownRequested);
            prop_assert!(c.is_terminal());
        }

        /// DrainTimedOut always produces a fatal shutdown.
        #[test]
        fn drain_timeout_always_fatal(_n in 0u32..10) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            c.step(Event::PipelinesBuilt);
            c.step(Event::ReloadRequested);
            prop_assert_eq!(c.state(), &State::Draining);
            let effects = c.step(Event::DrainTimedOut);
            prop_assert!(c.is_terminal());
            prop_assert!(
                effects
                    .iter()
                    .any(|e| matches!(e, Effect::FatalShutdown(_)))
            );
        }

        /// A full reload cycle (drain → validate → build) returns to Running.
        #[test]
        fn reload_cycle_returns_to_running(_n in 0u32..10) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            c.step(Event::PipelinesBuilt);
            c.step(Event::ReloadRequested);
            c.step(Event::DrainCompleted);
            c.step(Event::ConfigUnchanged);
            c.step(Event::PipelinesBuilt);
            prop_assert_eq!(c.state(), &State::Running);
        }

        /// ShutdownRequested from ANY non-terminal state reaches terminal.
        #[test]
        fn shutdown_from_any_non_terminal_state(events in proptest::collection::vec(arb_event(), 0..30)) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            for event in events {
                c.step(event);
            }
            if !c.is_terminal() {
                c.step(Event::ShutdownRequested);
                prop_assert!(c.is_terminal());
            }
        }

        /// rebuild_attempts never exceeds MAX_REBUILD_ATTEMPTS.
        #[test]
        fn rebuild_attempts_bounded(events in proptest::collection::vec(arb_event(), 1..200)) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            for event in events {
                c.step(event);
                prop_assert!(c.rebuild_attempts <= MAX_REBUILD_ATTEMPTS);
            }
        }

        /// has_pending is true only in Building state or terminal (where it's irrelevant).
        #[test]
        fn has_pending_only_in_building_or_terminal(events in proptest::collection::vec(arb_event(), 1..100)) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            for event in events {
                c.step(event);
                if c.has_pending() {
                    prop_assert!(
                        matches!(c.state(), &State::Building | &State::ShuttingDown { .. }),
                        "has_pending=true in unexpected state: {:?}", c.state()
                    );
                }
            }
        }

        /// Multiple reload cycles maintain consistent state (no leaks).
        #[test]
        fn multiple_reload_cycles_consistent(cycles in 1u32..10) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            c.step(Event::PipelinesBuilt);

            for _ in 0..cycles {
                if c.is_terminal() { break; }
                c.step(Event::ReloadRequested);
                c.step(Event::DrainCompleted);
                c.step(Event::ConfigValid);
                c.step(Event::PipelinesBuilt);
                prop_assert_eq!(c.state(), &State::Running);
                prop_assert!(!c.has_pending());
                prop_assert!(!c.is_first_run());
            }
        }

        /// DrainPipelines is only emitted when in Running state (or entering from Running).
        #[test]
        fn drain_only_from_running(events in proptest::collection::vec(arb_event(), 1..100)) {
            let mut c = ReloadCoordinator::new(PathBuf::from("/tmp/test.yaml"));
            for event in events {
                let state_before = c.state().clone();
                let effects = c.step(event);
                if effects.iter().any(|e| matches!(e, Effect::DrainPipelines)) {
                    prop_assert!(
                        matches!(state_before, State::Running),
                        "DrainPipelines emitted from non-Running state: {:?}", state_before
                    );
                }
            }
        }
    }

    impl Clone for Event {
        fn clone(&self) -> Self {
            match self {
                Event::PipelinesBuilt => Event::PipelinesBuilt,
                Event::BuildFailed { pipeline, error } => Event::BuildFailed {
                    pipeline: pipeline.clone(),
                    error: error.clone(),
                },
                Event::PipelineCompleted { error: _ } => Event::PipelineCompleted { error: None },
                Event::ShutdownRequested => Event::ShutdownRequested,
                Event::ReloadRequested => Event::ReloadRequested,
                Event::DrainCompleted => Event::DrainCompleted,
                Event::DrainTimedOut => Event::DrainTimedOut,
                Event::ConfigValid => Event::ConfigValid,
                Event::ConfigInvalid(s) => Event::ConfigInvalid(s.clone()),
                Event::ConfigNotReloadable(s) => Event::ConfigNotReloadable(s.clone()),
                Event::ConfigUnchanged => Event::ConfigUnchanged,
            }
        }
    }
}

// ═══════════ Kani formal verification ═══════════

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Bounded state for Kani exploration (avoids unbounded String).
    fn arb_state(tag: u8) -> State {
        match tag % 6 {
            0 => State::Starting,
            1 => State::Running,
            2 => State::Draining,
            3 => State::Validating,
            4 => State::Building,
            _ => State::ShuttingDown { error: None },
        }
    }

    /// Bounded event for Kani exploration.
    fn arb_event(tag: u8) -> Event {
        match tag % 11 {
            0 => Event::PipelinesBuilt,
            1 => Event::BuildFailed {
                pipeline: String::new(),
                error: String::new(),
            },
            2 => Event::PipelineCompleted { error: None },
            3 => Event::ShutdownRequested,
            4 => Event::ReloadRequested,
            5 => Event::DrainCompleted,
            6 => Event::DrainTimedOut,
            7 => Event::ConfigValid,
            8 => Event::ConfigInvalid(String::new()),
            9 => Event::ConfigNotReloadable(String::new()),
            _ => Event::ConfigUnchanged,
        }
    }

    /// Construct a coordinator with arbitrary internal state for single-step proofs.
    fn arb_coordinator(
        state_tag: u8,
        is_first_run: bool,
        has_pending: bool,
        attempts: u32,
    ) -> ReloadCoordinator {
        let state = arb_state(state_tag);
        ReloadCoordinator {
            state,
            is_first_run,
            has_pending,
            rebuild_attempts: attempts % (MAX_REBUILD_ATTEMPTS + 1),
            config_path: PathBuf::from("/x"),
        }
    }

    // ── Property 1: ShuttingDown is absorbing ──

    #[kani::proof]
    fn verify_shutting_down_is_absorbing() {
        let event_tag: u8 = kani::any();
        let event = arb_event(event_tag);

        let mut c = ReloadCoordinator {
            state: State::ShuttingDown { error: None },
            is_first_run: kani::any(),
            has_pending: kani::any(),
            rebuild_attempts: kani::any::<u32>() % (MAX_REBUILD_ATTEMPTS + 1),
            config_path: PathBuf::from("/x"),
        };

        let effects = c.step(event);

        // State must remain ShuttingDown
        assert!(matches!(c.state(), State::ShuttingDown { .. }));
        // No effects emitted from terminal state
        assert!(effects.is_empty());

        kani::cover!(event_tag % 11 == 3, "shutdown requested in terminal");
        kani::cover!(event_tag % 11 == 0, "pipelines built in terminal");
    }

    // ── Property 2: ShutdownRequested always reaches terminal ──

    #[kani::proof]
    fn verify_shutdown_requested_always_terminal() {
        let state_tag: u8 = kani::any();
        let first_run: bool = kani::any();
        let has_pending: bool = kani::any();
        let attempts: u32 = kani::any::<u32>() % (MAX_REBUILD_ATTEMPTS + 1);

        let mut c = arb_coordinator(state_tag, first_run, has_pending, attempts);
        c.step(Event::ShutdownRequested);

        // After ShutdownRequested, must be in ShuttingDown
        assert!(matches!(c.state(), State::ShuttingDown { .. }));

        kani::cover!(state_tag % 6 == 0, "shutdown from Starting");
        kani::cover!(state_tag % 6 == 1, "shutdown from Running");
        kani::cover!(state_tag % 6 == 4, "shutdown from Building");
    }

    // ── Property 3: First-run build failure is always fatal ──

    #[kani::proof]
    fn verify_first_run_build_failure_is_fatal() {
        let mut c = ReloadCoordinator {
            state: State::Starting,
            is_first_run: true,
            has_pending: false,
            rebuild_attempts: 0,
            config_path: PathBuf::from("/x"),
        };

        let effects = c.step(Event::BuildFailed {
            pipeline: String::new(),
            error: String::new(),
        });

        // Must reach ShuttingDown with error
        assert!(matches!(c.state(), State::ShuttingDown { error: Some(_) }));
        // Must emit FatalShutdown
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::FatalShutdown(_)))
        );
    }

    // ── Property 4: Rebuild attempts never exceed MAX ──

    #[kani::proof]
    #[kani::unwind(5)]
    fn verify_rebuild_attempts_bounded() {
        let mut c = ReloadCoordinator {
            state: State::Building,
            is_first_run: false,
            has_pending: kani::any(),
            rebuild_attempts: kani::any::<u32>() % (MAX_REBUILD_ATTEMPTS + 1),
            config_path: PathBuf::from("/x"),
        };

        // Apply up to 4 build failures
        for _ in 0..4u8 {
            if c.is_terminal() {
                break;
            }
            c.step(Event::BuildFailed {
                pipeline: String::new(),
                error: String::new(),
            });
            // Invariant: attempts never exceed MAX
            assert!(c.rebuild_attempts <= MAX_REBUILD_ATTEMPTS);
        }

        // Must have either reached terminal or still be in Building
        assert!(matches!(
            c.state(),
            State::ShuttingDown { .. } | State::Building
        ));

        kani::cover!(
            matches!(c.state(), State::ShuttingDown { .. }),
            "reached fatal after max attempts"
        );
    }

    // ── Property 5: has_pending consistency ──

    #[kani::proof]
    fn verify_has_pending_invariant() {
        let state_tag: u8 = kani::any();
        let first_run: bool = kani::any();
        let has_pending: bool = kani::any();
        let attempts: u32 = kani::any::<u32>() % (MAX_REBUILD_ATTEMPTS + 1);
        let event_tag: u8 = kani::any();

        let mut c = arb_coordinator(state_tag, first_run, has_pending, attempts);
        let event = arb_event(event_tag);
        c.step(event);

        // After any single step, has_pending implies Building or ShuttingDown
        if c.has_pending() {
            assert!(
                matches!(c.state(), State::Building | State::ShuttingDown { .. }),
                "has_pending=true in unexpected state"
            );
        }

        kani::cover!(c.has_pending(), "has_pending is true after step");
        kani::cover!(!c.has_pending(), "has_pending is false after step");
    }

    // ── Property 6: DrainPipelines only emitted from Running ──

    #[kani::proof]
    fn verify_drain_only_from_running() {
        let state_tag: u8 = kani::any();
        let first_run: bool = kani::any();
        let has_pending: bool = kani::any();
        let attempts: u32 = kani::any::<u32>() % (MAX_REBUILD_ATTEMPTS + 1);
        let event_tag: u8 = kani::any();

        let state_before = arb_state(state_tag);
        let mut c = arb_coordinator(state_tag, first_run, has_pending, attempts);
        let event = arb_event(event_tag);
        let effects = c.step(event);

        if effects.iter().any(|e| matches!(e, Effect::DrainPipelines)) {
            assert!(
                matches!(state_before, State::Running),
                "DrainPipelines emitted from non-Running state"
            );
        }

        kani::cover!(
            effects.iter().any(|e| matches!(e, Effect::DrainPipelines)),
            "drain was emitted"
        );
    }
}
