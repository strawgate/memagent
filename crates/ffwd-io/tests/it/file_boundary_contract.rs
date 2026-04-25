//! Replay-style contract tests for file input boundaries.
//!
//! These tests exercise the real FileTailer -> FileInput -> FramedInput path
//! and assert boundary invariants beyond simple line counts.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ffwd_io::format::FormatDecoder;
use ffwd_io::framed::FramedInput;
use ffwd_io::input::{FileInput, InputSource, SourceEvent};
use ffwd_io::tail::{ByteOffset, TailConfig};
use ffwd_types::diagnostics::ComponentStats;
use ffwd_types::pipeline::SourceId;

fn make_stats() -> Arc<ComponentStats> {
    Arc::new(ComponentStats::new())
}

fn make_framed_file_input(paths: &[PathBuf]) -> FramedInput {
    let stats = make_stats();
    let input = FileInput::new(
        "contract-file".into(),
        paths,
        TailConfig {
            start_from_end: false,
            poll_interval_ms: 0,
            glob_rescan_interval_ms: 0,
            ..Default::default()
        },
        Arc::clone(&stats),
    )
    .expect("create file input");

    FramedInput::new(
        Box::new(input),
        FormatDecoder::passthrough(Arc::clone(&stats)),
        stats,
    )
}

fn make_framed_glob_input(pattern: &str) -> FramedInput {
    let stats = make_stats();
    let input = FileInput::new_with_globs(
        "contract-glob".into(),
        &[pattern],
        TailConfig {
            start_from_end: false,
            poll_interval_ms: 0,
            glob_rescan_interval_ms: 0,
            ..Default::default()
        },
        Arc::clone(&stats),
    )
    .expect("create glob file input");

    FramedInput::new(
        Box::new(input),
        FormatDecoder::passthrough(Arc::clone(&stats)),
        stats,
    )
}

/// Poll `input` until at least `min_items` data events are collected or `timeout` elapses.
///
/// Keeps polling through multiple rounds so tests that expect data from N files
/// are not flaky when one file delivers its data on an earlier poll than another.
fn poll_until_data(
    input: &mut dyn InputSource,
    timeout: Duration,
    min_items: usize,
) -> Vec<(Option<SourceId>, Bytes)> {
    let deadline = Instant::now() + timeout;
    let mut collected = Vec::new();

    while Instant::now() < deadline {
        for event in input.poll().expect("poll file input") {
            if let SourceEvent::Data {
                bytes, source_id, ..
            } = event
            {
                collected.push((source_id, bytes));
            }
        }

        if collected.len() >= min_items {
            return collected;
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("timed out waiting for {} file input items", min_items);
}

fn checkpoint_map(input: &dyn InputSource) -> HashMap<SourceId, ByteOffset> {
    input.checkpoint_data().into_iter().collect()
}

#[test]
fn checkpoint_advances_only_at_real_newline_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("app.log");
    fs::write(&log_path, b"alpha\nbr").unwrap();

    let mut input = make_framed_file_input(std::slice::from_ref(&log_path));

    let emitted = poll_until_data(&mut input, Duration::from_secs(2), 1);
    assert_eq!(emitted.len(), 1, "expected one completed line");
    let (source_id, bytes) = &emitted[0];
    let source_id = source_id.expect("file input must carry source id");
    assert_eq!(&bytes[..], &b"alpha\n"[..]);

    let checkpoints = input.checkpoint_data();
    assert_eq!(checkpoints.len(), 1, "expected one checkpoint entry");
    assert_eq!(checkpoints[0].0, source_id);
    assert_eq!(
        checkpoints[0].1,
        ByteOffset(b"alpha\n".len() as u64),
        "checkpoint must stop at the last newline, not the partial remainder"
    );

    let mut file = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
    file.write_all(b"avo\n").unwrap();
    file.flush().unwrap();

    let emitted = poll_until_data(&mut input, Duration::from_secs(2), 1);
    assert_eq!(emitted.len(), 1, "expected the resumed partial line");
    assert_eq!(&emitted[0].1[..], &b"bravo\n"[..]);

    let full_len = fs::metadata(&log_path).unwrap().len();
    let checkpoints = input.checkpoint_data();
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].0, source_id);
    assert_eq!(
        checkpoints[0].1,
        ByteOffset(full_len),
        "checkpoint should advance once the partial line becomes complete"
    );
}

#[test]
fn glob_sources_keep_partial_lines_and_checkpoints_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let a_path = dir.path().join("a.log");
    let b_path = dir.path().join("b.log");
    fs::write(&a_path, b"hello-from-A").unwrap();
    fs::write(&b_path, b"hello-from-B").unwrap();

    let pattern = format!("{}/*.log", dir.path().display());
    let mut input = make_framed_glob_input(&pattern);

    let initial_events = input.poll().expect("initial poll");
    assert!(
        initial_events
            .iter()
            .all(|event| !matches!(event, SourceEvent::Data { .. })),
        "partial lines from different files must stay buffered, not emitted early"
    );

    let initial_checkpoints = input.checkpoint_data();
    assert_eq!(
        initial_checkpoints.len(),
        2,
        "expected checkpoint state for both files"
    );
    assert!(
        initial_checkpoints
            .iter()
            .all(|(_, offset)| *offset == ByteOffset(0)),
        "no newline means neither source is checkpointable yet"
    );

    let mut a_file = fs::OpenOptions::new().append(true).open(&a_path).unwrap();
    a_file.write_all(b"-done\n").unwrap();
    a_file.flush().unwrap();

    let mut b_file = fs::OpenOptions::new().append(true).open(&b_path).unwrap();
    b_file.write_all(b"-done\n").unwrap();
    b_file.flush().unwrap();

    let emitted = poll_until_data(&mut input, Duration::from_secs(2), 2);
    assert_eq!(emitted.len(), 2, "expected one completed line per source");

    let mut emitted_by_source: HashMap<SourceId, Bytes> = HashMap::new();
    for (source_id, bytes) in emitted {
        let source_id = source_id.expect("file input must carry source id");
        emitted_by_source.insert(source_id, bytes);
    }

    assert_eq!(
        emitted_by_source.len(),
        2,
        "each file must have its own source identity"
    );

    let unique_values: HashSet<Bytes> = emitted_by_source.values().cloned().collect();
    let expected_values: HashSet<Bytes> = [
        Bytes::from_static(b"hello-from-A-done\n"),
        Bytes::from_static(b"hello-from-B-done\n"),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        unique_values, expected_values,
        "per-source remainders must not cross-contaminate"
    );

    let checkpoints = input.checkpoint_data();
    assert_eq!(
        checkpoints.len(),
        2,
        "expected checkpoint entries for both files"
    );
    for (source_id, offset) in checkpoints {
        let expected = emitted_by_source
            .get(&source_id)
            .expect("checkpoint source must correspond to emitted source");
        assert_eq!(
            offset,
            ByteOffset(expected.len() as u64),
            "checkpoint must advance independently per source"
        );
    }
}

#[test]
fn glob_sources_eof_flush_remainders_and_advance_checkpoints_independently() {
    let dir = tempfile::tempdir().unwrap();
    let a_path = dir.path().join("a.log");
    let b_path = dir.path().join("b.log");
    fs::write(&a_path, b"tail-A").unwrap();
    fs::write(&b_path, b"tail-B").unwrap();

    let pattern = format!("{}/*.log", dir.path().display());
    let mut input = make_framed_glob_input(&pattern);

    let initial_events = input.poll().expect("initial poll");
    assert!(
        initial_events
            .iter()
            .all(|event| !matches!(event, SourceEvent::Data { .. })),
        "partial lines should remain buffered until EOF flush"
    );

    let initial_checkpoints = checkpoint_map(&input);
    assert_eq!(
        initial_checkpoints.len(),
        2,
        "expected both sources to be tracked"
    );
    assert!(
        initial_checkpoints
            .values()
            .all(|offset| *offset == ByteOffset(0)),
        "neither source should be checkpointable before EOF flush"
    );

    let emitted = poll_until_data(&mut input, Duration::from_secs(2), 2);
    let mut emitted_by_source = HashMap::new();
    for (source_id, bytes) in emitted {
        let source_id = source_id.expect("EOF flush must preserve source identity");
        emitted_by_source.insert(source_id, bytes);
    }

    let expected_values: HashSet<Bytes> = [
        Bytes::from_static(b"tail-A\n"),
        Bytes::from_static(b"tail-B\n"),
    ]
    .into_iter()
    .collect();
    let actual_values: HashSet<Bytes> = emitted_by_source.values().cloned().collect();
    assert_eq!(
        actual_values, expected_values,
        "EOF flush must emit each source remainder independently"
    );

    let checkpoints = checkpoint_map(&input);
    assert_eq!(
        checkpoints.len(),
        2,
        "expected checkpoint entries for both files"
    );
    for (source_id, emitted_bytes) in emitted_by_source {
        let expected_file_len = match &emitted_bytes[..] {
            b"tail-A\n" => fs::metadata(&a_path).unwrap().len(),
            b"tail-B\n" => fs::metadata(&b_path).unwrap().len(),
            _ => panic!("unexpected emitted EOF-flush payload"),
        };
        assert_eq!(
            checkpoints.get(&source_id),
            Some(&ByteOffset(expected_file_len)),
            "EOF flush should advance checkpoint to the real file length"
        );
    }
}

#[test]
fn checkpoint_restore_keeps_complete_and_partial_sources_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let a_path = dir.path().join("a.log");
    let b_path = dir.path().join("b.log");
    fs::write(&a_path, b"alpha\n").unwrap();
    fs::write(&b_path, b"br").unwrap();

    let pattern = format!("{}/*.log", dir.path().display());
    let mut first_run = make_framed_glob_input(&pattern);

    let emitted = poll_until_data(&mut first_run, Duration::from_secs(2), 1);
    assert_eq!(emitted.len(), 1, "only the complete source should emit");
    let complete_source = emitted[0].0.expect("file input must carry source id");
    assert_eq!(&emitted[0].1[..], &b"alpha\n"[..]);

    let checkpoints = checkpoint_map(&first_run);
    assert_eq!(
        checkpoints.len(),
        2,
        "expected both sources in checkpoint state"
    );
    assert_eq!(
        checkpoints.get(&complete_source),
        Some(&ByteOffset(fs::metadata(&a_path).unwrap().len())),
        "the complete source should checkpoint at its file length"
    );

    let partial_source = checkpoints
        .iter()
        .find_map(|(source_id, offset)| {
            (*source_id != complete_source).then_some((*source_id, *offset))
        })
        .expect("expected a second source");
    assert_eq!(
        partial_source.1,
        ByteOffset(0),
        "the partial source must remain at checkpoint 0 before restart"
    );

    let mut second_run = make_framed_glob_input(&pattern);
    for (source_id, offset) in checkpoints {
        second_run.set_offset_by_source(source_id, offset.0);
    }

    let mut b_file = fs::OpenOptions::new().append(true).open(&b_path).unwrap();
    b_file.write_all(b"avo-long\n").unwrap();
    b_file.flush().unwrap();

    let resumed = poll_until_data(&mut second_run, Duration::from_secs(2), 1);
    assert_eq!(resumed.len(), 1, "only the incomplete source should resume");
    let resumed_source = resumed[0]
        .0
        .expect("resumed file input must carry source id");
    assert_ne!(
        resumed_source, complete_source,
        "the already-checkpointed source must not replay on restart"
    );
    assert_eq!(
        &resumed[0].1[..],
        &b"bravo-long\n"[..],
        "the partial source should resume from its last checkpoint boundary"
    );

    let resumed_checkpoints = checkpoint_map(&second_run);
    assert_eq!(
        resumed_checkpoints.get(&complete_source),
        Some(&ByteOffset(fs::metadata(&a_path).unwrap().len())),
        "restored complete-source checkpoint should stay intact"
    );
    assert_eq!(
        resumed_checkpoints.get(&resumed_source),
        Some(&ByteOffset(fs::metadata(&b_path).unwrap().len())),
        "resumed partial source should advance to its full file length"
    );
}
