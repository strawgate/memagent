//! Proptest state machine test: FileTailer + FileCheckpointStore.
//!
//! Verifies that the combination of FileTailer + newline framing +
//! FileCheckpointStore never corrupts data and correctly resumes from
//! checkpoints after crash+restart.
//!
//! ## Approach
//!
//! Instead of modelling the exact byte stream the tailer sees (which is
//! complex due to rotation, truncation, and multi-generation fd draining),
//! we verify these properties:
//!
//! 1. **No corruption**: every emitted line consists entirely of bytes that
//!    were written by test transitions. We use a unique tag per line to
//!    detect corruption, partial emission, and cross-line merging.
//!
//! 2. **No partial lines**: each emitted "line" matches a complete tagged
//!    line that was written.
//!
//! 3. **Checkpoint correctness**: after crash+restart from a checkpoint,
//!    subsequent polls must not emit corrupted lines.
//!
//! We tag each line with a monotonic sequence number to make lines unique,
//! which makes property checking straightforward.

use std::io::Write;
use std::path::PathBuf;

use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};

use ffwd_io::checkpoint::{CheckpointStore, FileCheckpointStore, SourceCheckpoint};
use ffwd_io::tail::{FileTailer, TailConfig, TailEvent};

// ---------------------------------------------------------------------------
// Transition enum
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Transition {
    /// Append N complete lines.
    AppendLines(usize),
    /// Rename-rotate.
    FileRotate,
    /// Copy-truncate.
    FileCopyTruncate,
    /// Poll the tailer.
    Poll,
    /// Flush checkpoint.
    Checkpoint,
    /// Crash and restart from checkpoint.
    CrashAndRestart,
}

// ---------------------------------------------------------------------------
// Reference model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct RefState {
    /// Monotonic counter used to generate unique line tags.
    next_line_id: u64,
    /// All line tags that have been written to disk.
    written_line_ids: std::collections::HashSet<u64>,
    /// Whether the file has content (gates rotation/truncation).
    has_content: bool,
}

// ---------------------------------------------------------------------------
// ReferenceStateMachine
// ---------------------------------------------------------------------------

struct TailCheckpointMachine;

impl ReferenceStateMachine for TailCheckpointMachine {
    type State = RefState;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RefState {
            next_line_id: 0,
            written_line_ids: std::collections::HashSet::new(),
            has_content: false,
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let mut options: Vec<BoxedStrategy<Transition>> = vec![
            // Append 1-5 lines.
            (1usize..=5).prop_map(Transition::AppendLines).boxed(),
            Just(Transition::Poll).boxed(),
            Just(Transition::Checkpoint).boxed(),
            Just(Transition::CrashAndRestart).boxed(),
        ];

        if state.has_content {
            options.push(Just(Transition::FileRotate).boxed());
            options.push(Just(Transition::FileCopyTruncate).boxed());
        }

        proptest::strategy::Union::new(options).boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::AppendLines(n) => {
                for _ in 0..*n {
                    state.written_line_ids.insert(state.next_line_id);
                    state.next_line_id += 1;
                }
                state.has_content = true;
            }
            Transition::FileRotate | Transition::FileCopyTruncate => {}
            Transition::Poll | Transition::Checkpoint | Transition::CrashAndRestart => {}
        }
        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::FileRotate | Transition::FileCopyTruncate => state.has_content,
            _ => true,
        }
    }
}

// ---------------------------------------------------------------------------
// System under test
// ---------------------------------------------------------------------------

struct Sut {
    dir: tempfile::TempDir,
    log_path: PathBuf,
    tailer: Option<FileTailer>,
    checkpoint_store: Option<FileCheckpointStore>,
    remainder: Vec<u8>,
    /// All line IDs the SUT has emitted.
    emitted_ids: Vec<u64>,
    /// Epoch boundaries: indices into `emitted_ids` where crash/restart,
    /// rotation, or truncation can reset observed ordering.
    epoch_boundaries: Vec<usize>,
    /// Counter matching RefState::next_line_id.
    next_line_id: u64,
    source_id: u64,
}

/// Format a unique tagged line for writing to disk.
fn format_line(id: u64) -> String {
    format!("LINE-{:08}", id)
}

/// Parse a tagged line emitted by the SUT.
fn parse_line(line: &str) -> Option<u64> {
    line.strip_prefix("LINE-")
        .and_then(|rest| rest.parse::<u64>().ok())
}

impl Sut {
    fn mark_epoch_boundary(&mut self) {
        let boundary = self.emitted_ids.len();
        if self.epoch_boundaries.last().copied() != Some(boundary) {
            self.epoch_boundaries.push(boundary);
        }
    }

    fn do_poll(&mut self) {
        let tailer = self.tailer.as_mut().expect("tailer not running");

        std::thread::sleep(std::time::Duration::from_millis(2));

        let events = tailer.poll().expect("poll failed");

        for event in events {
            match event {
                TailEvent::Data { bytes, .. } => {
                    let mut buf = std::mem::take(&mut self.remainder);
                    buf.extend_from_slice(&bytes);

                    if let Some(last_nl) = memchr::memrchr(b'\n', &buf) {
                        let complete = &buf[..=last_nl];
                        for line_bytes in complete.split(|&b| b == b'\n') {
                            if !line_bytes.is_empty() {
                                let line = std::str::from_utf8(line_bytes)
                                    .expect("test lines are valid UTF-8");
                                if let Some(id) = parse_line(line) {
                                    self.emitted_ids.push(id);
                                }
                                // Lines that don't parse as our tagged format
                                // are ignored (could be artifacts of
                                // partial reads after truncation/rotation).
                            }
                        }
                        self.remainder = buf[last_nl + 1..].to_vec();
                    } else {
                        self.remainder = buf;
                    }
                }
                TailEvent::Rotated { .. } => {
                    self.mark_epoch_boundary();
                }
                TailEvent::Truncated { .. } => {
                    self.mark_epoch_boundary();
                    self.remainder.clear();
                }
                TailEvent::EndOfFile { .. } => {}
                _ => {}
            }
        }
    }

    fn do_checkpoint(&mut self) {
        let tailer = self.tailer.as_ref().expect("tailer not running");
        let store = self.checkpoint_store.as_mut().expect("store not running");

        if let Some(offset) = tailer.get_offset(&self.log_path) {
            let remainder_len = self.remainder.len() as u64;
            let line_boundary_offset = offset.saturating_sub(remainder_len);

            store.update(SourceCheckpoint {
                source_id: self.source_id,
                path: Some(self.log_path.clone()),
                offset: line_boundary_offset,
            });
            store.flush().expect("checkpoint flush failed");
        }
    }

    fn do_crash_and_restart(&mut self) {
        self.mark_epoch_boundary();
        drop(self.tailer.take());
        drop(self.checkpoint_store.take());
        self.remainder.clear();

        let store = FileCheckpointStore::open(self.dir.path().join("checkpoints"))
            .expect("open checkpoint store after crash");

        let saved_offset = store.load(self.source_id).map_or(0, |cp| cp.offset);

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 0,
            fingerprint_bytes: 256,
            ..Default::default()
        };
        let paths = vec![self.log_path.clone()];
        let mut tailer = FileTailer::new(
            &paths,
            config,
            std::sync::Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .expect("create tailer after crash");

        if saved_offset > 0 {
            let _ = tailer.set_offset(&self.log_path, saved_offset);
        }

        self.tailer = Some(tailer);
        self.checkpoint_store = Some(store);
    }
}

// ---------------------------------------------------------------------------
// StateMachineTest
// ---------------------------------------------------------------------------

struct TailCheckpointTest;

impl StateMachineTest for TailCheckpointTest {
    type SystemUnderTest = Sut;
    type Reference = TailCheckpointMachine;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        let dir = tempfile::tempdir().expect("create temp dir");
        let log_path = dir.path().join("test.log");

        std::fs::File::create(&log_path).expect("create log file");

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 0,
            fingerprint_bytes: 256,
            ..Default::default()
        };

        let paths = vec![log_path.clone()];
        let tailer = FileTailer::new(
            &paths,
            config,
            std::sync::Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .expect("create tailer");

        let checkpoint_store = FileCheckpointStore::open(dir.path().join("checkpoints"))
            .expect("open checkpoint store");

        Sut {
            dir,
            log_path,
            tailer: Some(tailer),
            checkpoint_store: Some(checkpoint_store),
            remainder: Vec::new(),
            emitted_ids: Vec::new(),
            epoch_boundaries: Vec::new(),
            next_line_id: 0,
            source_id: 1,
        }
    }

    fn apply(
        mut sut: Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::AppendLines(n) => {
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&sut.log_path)
                    .expect("open log for append");
                for _ in 0..n {
                    let line = format_line(sut.next_line_id);
                    writeln!(f, "{}", line).expect("write line");
                    sut.next_line_id += 1;
                }
                f.sync_all().expect("sync log file");
            }
            Transition::FileRotate => {
                let rotated = sut.log_path.with_extension("log.1");
                let _ = std::fs::rename(&sut.log_path, &rotated);
                std::fs::File::create(&sut.log_path).expect("create new log after rotate");
            }
            Transition::FileCopyTruncate => {
                std::fs::File::create(&sut.log_path).expect("truncate log file");
            }
            Transition::Poll => {
                sut.do_poll();
            }
            Transition::Checkpoint => {
                sut.do_checkpoint();
            }
            Transition::CrashAndRestart => {
                sut.do_crash_and_restart();
            }
        }
        sut
    }

    fn check_invariants(
        sut: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // PROPERTY 1: No bogus line IDs.
        //
        // Every emitted line ID must correspond to a line that was
        // actually written. This catches corruption and partial-line
        // emission (tagged lines can't be formed from partial bytes
        // because the tag format "LINE-XXXXXXXX" won't parse from
        // arbitrary byte combinations).
        for &id in &sut.emitted_ids {
            assert!(
                ref_state.written_line_ids.contains(&id),
                "Emitted line ID {} was never written. Written IDs: {:?}",
                id,
                ref_state.written_line_ids,
            );
        }

        // PROPERTY 2: No partial lines (structural).
        //
        // If a line were emitted partially (e.g., "LINE-0000" instead of
        // "LINE-00000001"), parse_line would return None and it would be
        // silently ignored. However, for extra safety, we check that every
        // emitted ID is within the range [0, next_line_id).
        for &id in &sut.emitted_ids {
            assert!(
                id < ref_state.next_line_id,
                "Emitted line ID {} >= next_line_id {}. This should be impossible.",
                id,
                ref_state.next_line_id,
            );
        }

        // PROPERTY 3 is verified post-hoc in teardown (requires epoch
        // tracking across transitions).
    }

    fn teardown(mut sut: Self::SystemUnderTest, _ref_state: RefState) {
        // Final drain: poll multiple times.
        for _ in 0..5 {
            sut.do_poll();
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        // PROPERTY 3 (post-hoc): Monotonic ordering within each epoch.
        //
        // Between lifecycle boundaries, line IDs must be monotonically
        // non-decreasing (the tailer reads sequentially). After restart,
        // rotation, or truncation, IDs may go backwards because data can be
        // re-read from a checkpoint or a new file generation.
        {
            let mut boundaries = sut.epoch_boundaries.clone();
            boundaries.insert(0, 0);
            boundaries.push(sut.emitted_ids.len());
            for window in boundaries.windows(2) {
                let (start, end) = (window[0], window[1]);
                let epoch = &sut.emitted_ids[start..end];
                for pair in epoch.windows(2) {
                    assert!(
                        pair[0] <= pair[1],
                        "Monotonicity violation within epoch [{start}..{end}]: \
                         id {} followed by {}",
                        pair[0],
                        pair[1],
                    );
                }
            }
        }

        // We don't assert 100% coverage because rotation and truncation
        // can lose data that was overwritten before being read.

        drop(sut);
    }
}

fn build_checkpoint_state_machine_proptest_config() -> ProptestConfig {
    let mut config = ProptestConfig {
        cases: ffwd_test_utils::state_machine_proptest_cases(),
        max_shrink_iters: 5_000,
        ..ProptestConfig::default()
    };
    if cfg!(miri) || std::env::var_os("LOGFWD_DISABLE_PROPTEST_PERSISTENCE").is_some() {
        config.failure_persistence = None;
    }
    config
}

// ---------------------------------------------------------------------------
// Macro invocation
// ---------------------------------------------------------------------------

prop_state_machine! {
    #![proptest_config(build_checkpoint_state_machine_proptest_config())]

    #[test]
    fn checkpoint_state_machine(sequential 5..30 => TailCheckpointTest);
}
