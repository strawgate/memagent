//! Property tests that compare the legacy `poll()` path against the shared-buffer
//! `poll_into()` path for the same raw event sequences.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use ffwd_io::format::FormatDecoder;
use ffwd_io::framed::FramedInput;
use ffwd_io::input::{CriMetadata, FramedReadEvent, InputEvent, InputSource};
use ffwd_io::tail::ByteOffset;
use ffwd_types::diagnostics::{ComponentHealth, ComponentStats};
use ffwd_types::pipeline::SourceId;
use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;

const MAX_SOURCES: u64 = 3;

#[derive(Clone, Copy, Debug)]
enum FormatKind {
    Passthrough,
    PassthroughJson,
    Cri,
    Auto,
}

impl FormatKind {
    fn build(self, stats: Arc<ComponentStats>) -> FormatDecoder {
        match self {
            Self::Passthrough => FormatDecoder::passthrough(stats),
            Self::PassthroughJson => FormatDecoder::passthrough_json(stats),
            Self::Cri => FormatDecoder::cri(2 * 1024 * 1024, stats),
            Self::Auto => FormatDecoder::auto(2 * 1024 * 1024, stats),
        }
    }
}

#[derive(Clone, Debug)]
enum Transition {
    Data { source: SourceId, bytes: Vec<u8> },
    Rotated { source: SourceId },
    Truncated { source: SourceId },
    EndOfFile { source: SourceId },
}

#[derive(Default)]
struct ReplayState {
    poll_events: VecDeque<Vec<InputEvent>>,
    shutdown_events: VecDeque<Vec<InputEvent>>,
    offsets: HashMap<SourceId, ByteOffset>,
}

#[derive(Clone)]
struct ReplaySource {
    name: &'static str,
    shared: Arc<Mutex<ReplayState>>,
}

impl ReplaySource {
    fn new(name: &'static str, shared: Arc<Mutex<ReplayState>>) -> Self {
        Self { name, shared }
    }
}

impl InputSource for ReplaySource {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(self
            .shared
            .lock()
            .expect("replay source lock")
            .poll_events
            .pop_front()
            .unwrap_or_default())
    }

    fn poll_shutdown(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(self
            .shared
            .lock()
            .expect("replay source lock")
            .shutdown_events
            .pop_front()
            .unwrap_or_default())
    }

    fn name(&self) -> &str {
        self.name
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        self.shared
            .lock()
            .expect("replay source lock")
            .offsets
            .iter()
            .map(|(sid, offset)| (*sid, *offset))
            .collect()
    }

    fn should_reclaim_completed_source_state(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum NormalizedEvent {
    Data {
        bytes: Vec<u8>,
        source_id: Option<SourceId>,
        cri_metadata: Option<CriMetadata>,
    },
    Rotated {
        source_id: Option<SourceId>,
    },
    Truncated {
        source_id: Option<SourceId>,
    },
}

fn format_strategy() -> impl Strategy<Value = FormatKind> {
    prop_oneof![
        Just(FormatKind::Passthrough),
        Just(FormatKind::PassthroughJson),
        Just(FormatKind::Cri),
        Just(FormatKind::Auto),
    ]
}

fn transition_strategy() -> impl Strategy<Value = Transition> {
    let source = (0..MAX_SOURCES).prop_map(SourceId);
    prop_oneof![
        (source.clone(), prop::collection::vec(any::<u8>(), 1..128))
            .prop_map(|(source, bytes)| Transition::Data { source, bytes }),
        source
            .clone()
            .prop_map(|source| Transition::Rotated { source }),
        source
            .clone()
            .prop_map(|source| Transition::Truncated { source }),
        source.prop_map(|source| Transition::EndOfFile { source }),
    ]
}

fn to_input_event(transition: &Transition) -> InputEvent {
    match transition {
        Transition::Data { source, bytes } => InputEvent::Data {
            bytes: Bytes::from(bytes.clone()),
            source_id: Some(*source),
            accounted_bytes: bytes.len() as u64,
            cri_metadata: None,
        },
        Transition::Rotated { source } => InputEvent::Rotated {
            source_id: Some(*source),
        },
        Transition::Truncated { source } => InputEvent::Truncated {
            source_id: Some(*source),
        },
        Transition::EndOfFile { source } => InputEvent::EndOfFile {
            source_id: Some(*source),
        },
    }
}

fn push_poll_transition(shared: &Arc<Mutex<ReplayState>>, transition: &Transition) {
    let event = to_input_event(transition);
    let mut guard = shared.lock().expect("replay source lock");
    if let Transition::Data { source, bytes } = transition {
        let offset = guard.offsets.entry(*source).or_insert(ByteOffset(0));
        offset.0 += bytes.len() as u64;
    }
    guard.poll_events.push_back(vec![event]);
}

fn push_shutdown_transition(shared: &Arc<Mutex<ReplayState>>, transition: &Transition) {
    let event = to_input_event(transition);
    let mut guard = shared.lock().expect("replay source lock");
    if let Transition::Data { source, bytes } = transition {
        let offset = guard.offsets.entry(*source).or_insert(ByteOffset(0));
        offset.0 += bytes.len() as u64;
    }
    guard.shutdown_events.push_back(vec![event]);
}

fn normalize_legacy(events: Vec<InputEvent>) -> Vec<NormalizedEvent> {
    events
        .into_iter()
        .filter_map(|event| match event {
            InputEvent::Data {
                bytes,
                source_id,
                cri_metadata,
                ..
            } => Some(NormalizedEvent::Data {
                bytes: bytes.to_vec(),
                source_id,
                cri_metadata,
            }),
            InputEvent::Rotated { source_id } => Some(NormalizedEvent::Rotated { source_id }),
            InputEvent::Truncated { source_id } => Some(NormalizedEvent::Truncated { source_id }),
            InputEvent::Batch { .. } | InputEvent::EndOfFile { .. } => None,
        })
        .collect()
}

fn normalize_buffered(events: Vec<FramedReadEvent>, dst: &BytesMut) -> Vec<NormalizedEvent> {
    events
        .into_iter()
        .map(|event| match event {
            FramedReadEvent::Data {
                range,
                source_id,
                cri_metadata,
            } => NormalizedEvent::Data {
                bytes: dst[range].to_vec(),
                source_id,
                cri_metadata,
            },
            FramedReadEvent::Rotated { source_id } => NormalizedEvent::Rotated { source_id },
            FramedReadEvent::Truncated { source_id } => NormalizedEvent::Truncated { source_id },
        })
        .collect()
}

struct EquivalenceHarness {
    legacy_shared: Arc<Mutex<ReplayState>>,
    buffered_shared: Arc<Mutex<ReplayState>>,
    legacy: FramedInput,
    buffered: FramedInput,
    dst: BytesMut,
}

impl EquivalenceHarness {
    fn new(format: FormatKind) -> Self {
        let legacy_shared = Arc::new(Mutex::new(ReplayState::default()));
        let buffered_shared = Arc::new(Mutex::new(ReplayState::default()));
        let legacy_stats = Arc::new(ComponentStats::new());
        let buffered_stats = Arc::new(ComponentStats::new());
        let legacy = FramedInput::new(
            Box::new(ReplaySource::new("legacy", Arc::clone(&legacy_shared))),
            format.build(Arc::clone(&legacy_stats)),
            legacy_stats,
        );
        let buffered = FramedInput::new(
            Box::new(ReplaySource::new("buffered", Arc::clone(&buffered_shared))),
            format.build(Arc::clone(&buffered_stats)),
            buffered_stats,
        );
        Self {
            legacy_shared,
            buffered_shared,
            legacy,
            buffered,
            dst: BytesMut::new(),
        }
    }

    fn apply_poll_transition(&mut self, transition: &Transition) {
        push_poll_transition(&self.legacy_shared, transition);
        push_poll_transition(&self.buffered_shared, transition);
        let legacy = normalize_legacy(self.legacy.poll().expect("legacy poll"));
        let buffered = normalize_buffered(
            self.buffered
                .poll_into(&mut self.dst)
                .expect("buffered poll_into")
                .expect("FramedInput shared-buffer path"),
            &self.dst,
        );
        assert_eq!(legacy, buffered);
        assert_eq!(
            sorted_checkpoints(self.legacy.checkpoint_data()),
            sorted_checkpoints(self.buffered.checkpoint_data())
        );
    }

    fn apply_shutdown_transition(&mut self, transition: &Transition) {
        push_shutdown_transition(&self.legacy_shared, transition);
        push_shutdown_transition(&self.buffered_shared, transition);
        let legacy = normalize_legacy(self.legacy.poll_shutdown().expect("legacy poll_shutdown"));
        let buffered = normalize_buffered(
            self.buffered
                .poll_shutdown_into(&mut self.dst)
                .expect("buffered poll_shutdown_into")
                .expect("FramedInput shared-buffer shutdown path"),
            &self.dst,
        );
        assert_eq!(legacy, buffered);
        assert_eq!(
            sorted_checkpoints(self.legacy.checkpoint_data()),
            sorted_checkpoints(self.buffered.checkpoint_data())
        );
    }
}

fn sorted_checkpoints(mut checkpoints: Vec<(SourceId, ByteOffset)>) -> Vec<(SourceId, ByteOffset)> {
    checkpoints.sort_by_key(|(source_id, _)| source_id.0);
    checkpoints
}

fn build_equivalence_proptest_config() -> ProptestConfig {
    let mut config = ProptestConfig {
        cases: ffwd_test_utils::state_machine_proptest_cases(),
        max_shrink_iters: 5_000,
        ..ProptestConfig::default()
    };
    if cfg!(miri) || std::env::var_os("FFWD_DISABLE_PROPTEST_PERSISTENCE").is_some() {
        config.failure_persistence = None;
    }
    config
}

proptest! {
    #![proptest_config(build_equivalence_proptest_config())]

    #[test]
    fn poll_into_matches_poll_for_random_sequences(
        format in format_strategy(),
        transitions in prop::collection::vec(transition_strategy(), 1..40),
    ) {
        let mut harness = EquivalenceHarness::new(format);
        for transition in &transitions {
            harness.apply_poll_transition(transition);
        }
    }

    #[test]
    fn poll_shutdown_into_matches_poll_shutdown_for_random_sequences(
        format in format_strategy(),
        poll_transitions in prop::collection::vec(transition_strategy(), 0..20),
        shutdown_transitions in prop::collection::vec(transition_strategy(), 1..20),
    ) {
        let mut harness = EquivalenceHarness::new(format);
        for transition in &poll_transitions {
            harness.apply_poll_transition(transition);
        }
        for transition in &shutdown_transitions {
            harness.apply_shutdown_transition(transition);
        }
    }
}
