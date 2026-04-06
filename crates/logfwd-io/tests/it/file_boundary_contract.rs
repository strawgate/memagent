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

use logfwd_types::pipeline::SourceId;
use logfwd_io::format::FormatDecoder;
use logfwd_io::framed::FramedInput;
use logfwd_io::input::{FileInput, InputEvent, InputSource};
use logfwd_io::tail::{ByteOffset, TailConfig};
use logfwd_types::diagnostics::ComponentStats;

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
    )
    .expect("create glob file input");

    FramedInput::new(
        Box::new(input),
        FormatDecoder::passthrough(Arc::clone(&stats)),
        stats,
    )
}

fn poll_until_data(
    input: &mut dyn InputSource,
    timeout: Duration,
) -> Vec<(Option<SourceId>, Vec<u8>)> {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let mut emitted = Vec::new();
        for event in input.poll().expect("poll file input") {
            if let InputEvent::Data { bytes, source_id } = event {
                emitted.push((source_id, bytes));
            }
        }

        if !emitted.is_empty() {
            return emitted;
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("timed out waiting for file input data");
}

#[test]
fn checkpoint_advances_only_at_real_newline_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("app.log");
    fs::write(&log_path, b"alpha\nbr").unwrap();

    let mut input = make_framed_file_input(std::slice::from_ref(&log_path));

    let emitted = poll_until_data(&mut input, Duration::from_secs(2));
    assert_eq!(emitted.len(), 1, "expected one completed line");
    let (source_id, bytes) = &emitted[0];
    let source_id = source_id.expect("file input must carry source id");
    assert_eq!(bytes, b"alpha\n");

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

    let emitted = poll_until_data(&mut input, Duration::from_secs(2));
    assert_eq!(emitted.len(), 1, "expected the resumed partial line");
    assert_eq!(emitted[0].1, b"bravo\n");

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
            .all(|event| !matches!(event, InputEvent::Data { .. })),
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

    let emitted = poll_until_data(&mut input, Duration::from_secs(2));
    assert_eq!(emitted.len(), 2, "expected one completed line per source");

    let mut emitted_by_source: HashMap<SourceId, Vec<u8>> = HashMap::new();
    for (source_id, bytes) in emitted {
        let source_id = source_id.expect("file input must carry source id");
        emitted_by_source.insert(source_id, bytes);
    }

    assert_eq!(
        emitted_by_source.len(),
        2,
        "each file must have its own source identity"
    );

    let unique_values: HashSet<Vec<u8>> = emitted_by_source.values().cloned().collect();
    let expected_values: HashSet<Vec<u8>> = [
        b"hello-from-A-done\n".to_vec(),
        b"hello-from-B-done\n".to_vec(),
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
