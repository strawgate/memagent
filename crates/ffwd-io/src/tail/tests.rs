//! Tests for the file tailer module, split into submodules by category.

use super::*;
use ffwd_types::diagnostics::ComponentStats;
use std::fs::{self, File};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod basic_tail;
mod checkpoints;
mod glob_discovery;
mod glob_helpers;
mod regression;
mod shutdown;

fn create_test_stats() -> Arc<ComponentStats> {
    Arc::new(ComponentStats::new())
}

/// Probe the filesystem to see whether `nlink` drops to 0 after `unlink(2)`
/// while a file descriptor remains open. Deletion detection in
/// `has_metadata_indicating_deletion` depends on that behavior; some sandbox
/// and overlay filesystems keep `nlink` at 1. Tests that rely on seeing a
/// deleted file call this and skip when the platform can't report the signal.
///
/// The probe touches the filesystem; cache the result in a `OnceLock` so it
/// runs at most once per test process.
#[cfg(unix)]
fn filesystem_tracks_nlink_after_unlink() -> bool {
    use std::os::unix::fs::MetadataExt;
    use std::sync::OnceLock;

    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nlink-probe");
        fs::write(&path, b"x").expect("write probe");
        let file = File::open(&path).expect("open probe");
        fs::remove_file(&path).expect("unlink probe");
        // `print_stderr` is a workspace-level warn and the tail-source scan
        // (`test_tail_uses_tracing_not_eprintln`) forbids `eprintln!` in this
        // module outright, so skips stay silent.
        file.metadata().map(|m| m.nlink() == 0).unwrap_or(false)
    })
}

fn poll_until<F>(
    tailer: &mut FileTailer,
    timeout: Duration,
    mut predicate: F,
    failure_message: &str,
) -> Vec<TailEvent>
where
    F: FnMut(&[TailEvent], &FileTailer) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let events = tailer.poll().unwrap();
        if predicate(&events, tailer) {
            return events;
        }
        assert!(Instant::now() < deadline, "{failure_message}");
        std::thread::sleep(Duration::from_millis(10));
    }
}
