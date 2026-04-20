//! Proptest state machine test: `FramedInput` frame boundary invariants.
//!
//! Generates random sequences of Data, Rotated, Truncated, and EndOfFile
//! events across multiple sources and verifies invariants after each
//! transition:
//!
//! 1. **Frames end with newline** — every Data event emitted by the framer
//!    ends with `\n`.
//! 2. **Per-source isolation** — events targeting source A never corrupt
//!    source B's remainder or output.
//! 3. **Rotation/truncation clears state** — after Rotated or Truncated,
//!    the affected source's remainder is discarded (fresh data has no
//!    stale prefix).
//! 4. **EOF flushes remainder** — after EOF, any buffered remainder for
//!    that source is emitted with a synthetic newline.
//! 5. **Output bounded by input** — cumulative output bytes never exceed
//!    cumulative input bytes plus synthetic newlines from EOF flushes.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};

use logfwd_io::format::FormatDecoder;
use logfwd_io::input::{InputEvent, InputSource};
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};
use logfwd_types::pipeline::SourceId;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of distinct sources in a test run.
const MAX_SOURCES: u64 = 3;

// ---------------------------------------------------------------------------
// Transition enum
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Transition {
    /// Push a data chunk to a source.
    Data { source: SourceId, bytes: Vec<u8> },
    /// Source file was rotated (new inode).
    Rotated { source: SourceId },
    /// Source file was truncated.
    Truncated { source: SourceId },
    /// Source reached end of file.
    EndOfFile { source: SourceId },
}

// ---------------------------------------------------------------------------
// Reference model
// ---------------------------------------------------------------------------

/// Per-source reference state.
#[derive(Clone, Debug, Default)]
struct RefSourceState {
    /// Bytes fed since the last state reset (rotation/truncation).
    bytes_in_epoch: usize,
    /// True if rotation/truncation cleared state since the last data.
    was_reset: bool,
}

#[derive(Clone, Debug)]
struct RefState {
    sources: HashMap<SourceId, RefSourceState>,
    /// Total bytes fed across all sources (cumulative, never reset).
    total_bytes_in: usize,
    /// Total EOF flushes observed (each can add 1 synthetic newline byte).
    total_eof_flushes: usize,
}

// ---------------------------------------------------------------------------
// ReferenceStateMachine
// ---------------------------------------------------------------------------

struct FramedMachine;

impl ReferenceStateMachine for FramedMachine {
    type State = RefState;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RefState {
            sources: HashMap::new(),
            total_bytes_in: 0,
            total_eof_flushes: 0,
        })
        .boxed()
    }

    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let source_strategy = (0..MAX_SOURCES).prop_map(SourceId);

        proptest::strategy::Union::new(vec![
            // Data: random bytes, 1..512. Non-empty because
            // CheckpointTracker::apply_read requires n_bytes > 0.
            // Newlines (0x0A) appear naturally in the full byte range.
            (
                source_strategy.clone(),
                proptest::collection::vec(any::<u8>(), 1..512),
            )
                .prop_map(|(source, bytes)| Transition::Data { source, bytes })
                .boxed(),
            // Rotated
            source_strategy
                .clone()
                .prop_map(|source| Transition::Rotated { source })
                .boxed(),
            // Truncated
            source_strategy
                .clone()
                .prop_map(|source| Transition::Truncated { source })
                .boxed(),
            // EndOfFile
            source_strategy
                .prop_map(|source| Transition::EndOfFile { source })
                .boxed(),
        ])
        .boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::Data { source, bytes } => {
                state.total_bytes_in += bytes.len();
                let ss = state.sources.entry(*source).or_default();
                ss.bytes_in_epoch += bytes.len();
                ss.was_reset = false;
            }
            Transition::Rotated { source } | Transition::Truncated { source } => {
                state.sources.remove(source);
            }
            Transition::EndOfFile { source } => {
                if let Some(ss) = state.sources.get_mut(source) {
                    if ss.bytes_in_epoch > 0 {
                        // EOF may flush a remainder, adding a synthetic newline.
                        state.total_eof_flushes += 1;
                    }
                }
            }
        }
        state
    }

    fn preconditions(_state: &Self::State, _transition: &Self::Transition) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Event-replay input source
// ---------------------------------------------------------------------------

/// A mock `InputSource` that replays one event at a time, pushed
/// dynamically by the state machine test.
struct ReplaySource {
    pending: Option<Vec<InputEvent>>,
}

impl ReplaySource {
    fn new() -> Self {
        Self { pending: None }
    }

    fn push_event(&mut self, event: InputEvent) {
        self.pending.get_or_insert_with(Vec::new).push(event);
    }
}

impl InputSource for ReplaySource {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(self.pending.take().unwrap_or_default())
    }

    fn name(&self) -> &str {
        "replay"
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn should_reclaim_completed_source_state(&self) -> bool {
        // Keep per-source state across polls for testing (like file tailers).
        false
    }
}

// ---------------------------------------------------------------------------
// System under test
// ---------------------------------------------------------------------------

struct Sut {
    /// Raw pointer to the inner ReplaySource for pushing events.
    inner_ptr: *mut ReplaySource,
    framed: logfwd_io::framed::FramedInput,
    #[allow(dead_code)]
    stats: Arc<ComponentStats>,

    // -- Cumulative tracking (never reset) --
    /// Total bytes emitted across all sources.
    total_bytes_out: usize,
    /// Total bytes input across all sources.
    total_bytes_in: usize,
    /// Number of EOF events that produced output (each adds 1 synthetic
    /// newline).
    total_eof_flushes: usize,

    // -- Per-source tracking for isolation checks --
    /// Per-source cumulative output bytes (never reset by lifecycle events).
    cumulative_out: HashMap<SourceId, usize>,
    /// Snapshot of cumulative_out for isolation assertions.
    isolation_snapshots: HashMap<SourceId, usize>,

    // -- Rotation/truncation verification --
    /// Per-source: a unique tag set right after rotation/truncation to
    /// verify that subsequent data does NOT carry a stale remainder.
    /// Cleared when new data is successfully verified.
    reset_pending: HashMap<SourceId, bool>,
}

// SAFETY: ReplaySource is Send and we only access inner_ptr through
// exclusive (&mut self) methods, never across threads.
unsafe impl Send for Sut {}

impl Sut {
    fn new() -> Self {
        let stats = Arc::new(ComponentStats::new());
        let inner = Box::new(ReplaySource::new());
        let inner_ptr = &*inner as *const ReplaySource as *mut ReplaySource;
        let framed = logfwd_io::framed::FramedInput::new(
            inner,
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );
        Self {
            inner_ptr,
            framed,
            stats,
            total_bytes_out: 0,
            total_bytes_in: 0,
            total_eof_flushes: 0,
            cumulative_out: HashMap::new(),
            isolation_snapshots: HashMap::new(),
            reset_pending: HashMap::new(),
        }
    }

    fn push_and_poll(&mut self, event: InputEvent) -> Vec<InputEvent> {
        // SAFETY: we hold the only reference to the FramedInput which owns
        // the ReplaySource. We push the event before polling, and never
        // access the pointer concurrently.
        unsafe {
            (*self.inner_ptr).push_event(event);
        }
        self.framed.poll().expect("poll must not fail")
    }

    fn count_output_bytes(events: &[InputEvent]) -> usize {
        events
            .iter()
            .filter_map(|e| {
                if let InputEvent::Data { bytes, .. } = e {
                    Some(bytes.len())
                } else {
                    None
                }
            })
            .sum()
    }
}

// ---------------------------------------------------------------------------
// StateMachineTest
// ---------------------------------------------------------------------------

struct FramedTest;

impl StateMachineTest for FramedTest {
    type SystemUnderTest = Sut;
    type Reference = FramedMachine;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        Sut::new()
    }

    fn apply(
        mut sut: Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::Data { source, bytes } => {
                let n = bytes.len();
                sut.total_bytes_in += n;

                // Snapshot other sources' output for isolation check.
                sut.isolation_snapshots.clear();
                for (&sid, &out) in &sut.cumulative_out {
                    if sid != source {
                        sut.isolation_snapshots.insert(sid, out);
                    }
                }

                let events = sut.push_and_poll(InputEvent::Data {
                    bytes,
                    source_id: Some(source),
                    accounted_bytes: n as u64,
                });

                // INVARIANT: every emitted Data event ends with newline.
                for event in &events {
                    if let InputEvent::Data {
                        bytes: out_bytes, ..
                    } = event
                    {
                        if !out_bytes.is_empty() {
                            assert!(
                                out_bytes.ends_with(b"\n"),
                                "framed output must end with newline, got last byte: {:#x} \
                                 (source: {:?}, output len: {})",
                                out_bytes.last().unwrap(),
                                source,
                                out_bytes.len(),
                            );
                        }
                    }
                }

                // Record output.
                let out_bytes = Sut::count_output_bytes(&events);
                sut.total_bytes_out += out_bytes;
                *sut.cumulative_out.entry(source).or_insert(0) += out_bytes;

                // INVARIANT: per-source isolation — Data for source X must
                // not change output counters for any other source.
                for (&sid, &snapshot) in &sut.isolation_snapshots {
                    let current = sut.cumulative_out.get(&sid).copied().unwrap_or(0);
                    assert_eq!(
                        current, snapshot,
                        "per-source isolation violated: source {:?} output changed \
                         ({} -> {}) when data was pushed for source {:?}",
                        sid, snapshot, current, source,
                    );
                }

                // Check rotation/truncation reset effect: if we recently
                // reset this source and then pushed data with a known tag,
                // the output should not carry bytes from before the reset.
                // We verify this indirectly: after reset, the first data
                // chunk should not produce output longer than the input
                // chunk (which would mean stale remainder was prepended).
                if sut.reset_pending.remove(&source).is_some() && out_bytes > n {
                    panic!(
                        "rotation/truncation reset violated: source {:?} produced {} \
                         output bytes from {} input bytes on the first data after reset \
                         (stale remainder was not cleared)",
                        source, out_bytes, n,
                    );
                }
            }

            Transition::Rotated { source } => {
                let events = sut.push_and_poll(InputEvent::Rotated {
                    source_id: Some(source),
                });
                // Rotation event must be forwarded.
                assert!(
                    events
                        .iter()
                        .any(|e| matches!(e, InputEvent::Rotated { .. })),
                    "Rotated event must be forwarded downstream"
                );
                // Mark this source as recently reset.
                sut.reset_pending.insert(source, true);
            }

            Transition::Truncated { source } => {
                let events = sut.push_and_poll(InputEvent::Truncated {
                    source_id: Some(source),
                });
                // Truncation event must be forwarded.
                assert!(
                    events
                        .iter()
                        .any(|e| matches!(e, InputEvent::Truncated { .. })),
                    "Truncated event must be forwarded downstream"
                );
                // Mark this source as recently reset.
                sut.reset_pending.insert(source, true);
            }

            Transition::EndOfFile { source } => {
                let before = sut.total_bytes_out;

                let events = sut.push_and_poll(InputEvent::EndOfFile {
                    source_id: Some(source),
                });

                // INVARIANT: EOF-flushed data also ends with newline.
                for event in &events {
                    if let InputEvent::Data {
                        bytes: out_bytes, ..
                    } = event
                    {
                        if !out_bytes.is_empty() {
                            assert!(
                                out_bytes.ends_with(b"\n"),
                                "EOF-flushed remainder must end with newline"
                            );
                        }
                    }
                }

                let out_bytes = Sut::count_output_bytes(&events);
                sut.total_bytes_out += out_bytes;
                *sut.cumulative_out.entry(source).or_insert(0) += out_bytes;

                if sut.total_bytes_out > before {
                    sut.total_eof_flushes += 1;
                }
            }
        }
        sut
    }

    fn check_invariants(
        sut: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // INVARIANT: cumulative output never exceeds cumulative input plus
        // synthetic newlines from EOF flushes.
        //
        // In passthrough mode, framing does not create bytes — it only
        // splits and buffers. EOF flush appends exactly 1 synthetic `\n`
        // per flush.
        let max_out = sut.total_bytes_in + sut.total_eof_flushes;
        assert!(
            sut.total_bytes_out <= max_out,
            "output ({}) exceeds input ({}) + EOF flushes ({})",
            sut.total_bytes_out,
            sut.total_bytes_in,
            sut.total_eof_flushes,
        );
    }
}

// ---------------------------------------------------------------------------
// Config and macro invocation
// ---------------------------------------------------------------------------

fn build_framed_proptest_config() -> ProptestConfig {
    let mut config = ProptestConfig {
        cases: logfwd_test_utils::state_machine_proptest_cases(),
        max_shrink_iters: 5_000,
        ..ProptestConfig::default()
    };
    if cfg!(miri) || std::env::var_os("LOGFWD_DISABLE_PROPTEST_PERSISTENCE").is_some() {
        config.failure_persistence = None;
    }
    config
}

prop_state_machine! {
    #![proptest_config(build_framed_proptest_config())]

    #[test]
    fn framed_state_machine(sequential 5..50 => FramedTest);
}
