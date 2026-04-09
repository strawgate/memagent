use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::glob::expand_glob_patterns;
use super::identity::identify_file;
use super::reader::FileReader;
use super::tailer::TailEvent;

fn mark_watch_dir_result(path: &Path, result: io::Result<()>) -> bool {
    if let Err(e) = result {
        tracing::warn!(path = %path.display(), error = %e, "tail.watch_dir_failed");
        true
    } else {
        false
    }
}

fn mark_open_result(path: &Path, context: &'static str, result: io::Result<()>) -> bool {
    if let Err(e) = result {
        tracing::warn!(path = %path.display(), error = %e, context, "tail.open_failed");
        true
    } else {
        false
    }
}

/// Owns the filesystem watcher and file/glob discovery state.
pub(super) struct FileDiscovery {
    pub(super) watcher: notify::RecommendedWatcher,
    pub(super) watched_dirs: HashSet<PathBuf>,
    pub(super) glob_patterns: Vec<String>,
    pub(super) watch_paths: Vec<PathBuf>,
    pub(super) fs_events: crossbeam_channel::Receiver<notify::Result<notify::Event>>,
    pub(super) last_glob_rescan: Instant,
}

impl FileDiscovery {
    pub(super) fn watch_dir(&mut self, dir: &Path) -> io::Result<()> {
        if self.watched_dirs.contains(dir) {
            return Ok(());
        }
        use notify::Watcher;
        self.watcher
            .watch(dir, notify::RecursiveMode::NonRecursive)
            .map_err(io::Error::other)?;
        self.watched_dirs.insert(dir.to_path_buf());
        Ok(())
    }

    pub(super) fn rescan_globs(&mut self, reader: &mut FileReader) -> bool {
        let mut had_error = false;
        if self.glob_patterns.is_empty() {
            return had_error;
        }

        let pattern_refs: Vec<&str> = self.glob_patterns.iter().map(String::as_str).collect();
        let candidates = expand_glob_patterns(&pattern_refs);

        let existing: HashSet<PathBuf> = self.watch_paths.iter().cloned().collect();
        let new_paths: Vec<PathBuf> = candidates
            .into_iter()
            .filter(|p| !existing.contains(p))
            .collect();

        for path in new_paths {
            if let Some(parent) = path.parent() {
                had_error |= mark_watch_dir_result(parent, self.watch_dir(parent));
            }

            had_error |= mark_open_result(
                &path,
                "rescan",
                reader.open_file_at(&path, reader.config.start_from_end),
            );

            self.watch_paths.push(path);
        }
        had_error
    }

    pub(super) fn drain_events(&self) -> (bool, bool) {
        let mut something_changed = false;
        let mut had_error = false;
        while let Ok(res) = self.fs_events.try_recv() {
            match res {
                Ok(_event) => {
                    something_changed = true;
                }
                Err(e) => {
                    tracing::error!(error = %e, "tail.fs_event_error");
                    had_error = true;
                }
            }
        }
        (something_changed, had_error)
    }

    pub(super) fn detect_changes(
        &self,
        reader: &mut FileReader,
        events: &mut Vec<TailEvent>,
    ) -> bool {
        let mut had_error = false;
        let watch_paths = self.watch_paths.clone();
        for path in &watch_paths {
            if !path.exists() {
                continue;
            }

            let current_identity = match identify_file(path, reader.config.fingerprint_bytes) {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "tail.identify_failed");
                    had_error = true;
                    continue;
                }
            };

            let is_rotated = reader.files.get(path).is_some_and(|tailed| {
                tailed.identity.device != current_identity.device
                    || tailed.identity.inode != current_identity.inode
            });
            let is_new = !reader.files.contains_key(path);

            if is_rotated {
                let pre_rotate_source_id = reader.source_id_for_path(path);
                had_error |= reader.drain_file(path, pre_rotate_source_id, events);

                events.push(TailEvent::Rotated {
                    path: path.clone(),
                    source_id: pre_rotate_source_id,
                });
                let _ = reader.files.remove(path);
                had_error |= mark_open_result(path, "rotation", reader.open_file_at(path, false));
            } else if is_new {
                had_error |= mark_open_result(
                    path,
                    "new_file",
                    reader.open_file_at(path, reader.config.start_from_end),
                );
            }
        }
        had_error
    }

    pub(super) fn cleanup_deleted(
        &mut self,
        reader: &mut FileReader,
        events: &mut Vec<TailEvent>,
    ) -> bool {
        let mut had_error = false;
        let deleted: Vec<PathBuf> = reader
            .files
            .iter()
            .filter(|(_, tailed)| {
                tailed
                    .file
                    .metadata()
                    .map(|m| metadata_indicates_deleted(&m))
                    .unwrap_or(true)
            })
            .map(|(path, _)| path.clone())
            .collect();
        for path in &deleted {
            let source_id = reader.source_id_for_path(path);
            had_error |= reader.drain_file(path, source_id, events);
            reader.files.remove(path);
            reader.evicted_offsets.remove(path);
        }
        if !self.glob_patterns.is_empty() && !deleted.is_empty() {
            let deleted_set: HashSet<&PathBuf> = deleted.iter().collect();
            self.watch_paths.retain(|p| !deleted_set.contains(p));
        }
        had_error
    }
}

#[cfg(unix)]
fn metadata_indicates_deleted(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    meta.nlink() == 0
}

#[cfg(not(unix))]
fn metadata_indicates_deleted(_meta: &std::fs::Metadata) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::io::{self, Write};
    use std::time::{Duration, Instant};

    use logfwd_types::pipeline::SourceId;

    use super::super::identity::FileIdentity;
    use super::super::reader::{EvictedFile, FileReader};
    use super::super::tailer::{TailConfig, TailEvent};
    use super::*;

    fn test_reader() -> FileReader {
        FileReader {
            files: HashMap::new(),
            read_buf: vec![0u8; 128],
            evicted_offsets: HashMap::new(),
            scratch_paths: Vec::new(),
            config: TailConfig {
                start_from_end: true,
                poll_interval_ms: 10,
                glob_rescan_interval_ms: 0,
                ..Default::default()
            },
        }
    }

    fn test_watcher() -> (
        notify::RecommendedWatcher,
        crossbeam_channel::Receiver<notify::Result<notify::Event>>,
    ) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let watcher = notify::recommended_watcher(move |res| {
            let _ = tx.send(res);
        })
        .expect("watcher");
        (watcher, rx)
    }

    fn data_for_path(events: &[TailEvent], path: &Path) -> String {
        let mut out = String::new();
        for event in events {
            if let TailEvent::Data { path: p, bytes, .. } = event
                && p == path
            {
                out.push_str(&String::from_utf8_lossy(bytes));
            }
        }
        out
    }

    #[test]
    fn detect_changes_respects_start_from_end_for_evicted_identity_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replaced.log");
        fs::write(&path, b"old replaced content\n").unwrap();

        let mut reader = test_reader();
        reader.evicted_offsets.insert(
            path.clone(),
            EvictedFile {
                identity: FileIdentity {
                    device: 111,
                    inode: 222,
                    fingerprint: 333,
                },
                offset: 7,
                path: path.clone(),
                source_id: SourceId(123),
            },
        );

        let (watcher, rx) = test_watcher();

        let discovery = FileDiscovery {
            watcher,
            watched_dirs: HashSet::new(),
            glob_patterns: Vec::new(),
            watch_paths: vec![path.clone()],
            fs_events: rx,
            last_glob_rescan: Instant::now() - Duration::from_secs(1),
        };

        let mut events = Vec::new();
        let had_error = discovery.detect_changes(&mut reader, &mut events);
        assert!(!had_error, "detect_changes should succeed");
        let had_error = reader.read_all(&mut events);
        assert!(!had_error, "read_all should succeed");

        let initial_text = data_for_path(&events, &path);
        assert!(
            !initial_text.contains("old replaced content"),
            "replaced file should open at EOF when start_from_end=true and evicted identity mismatches"
        );

        {
            let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(f, "fresh content").unwrap();
        }

        let mut events = Vec::new();
        let had_error = reader.read_all(&mut events);
        assert!(!had_error, "read_all after append should succeed");

        let appended_text = data_for_path(&events, &path);
        assert!(
            appended_text.contains("fresh content"),
            "newly appended content should be read after opening at EOF"
        );
        assert!(
            !appended_text.contains("old replaced content"),
            "historical content must remain skipped"
        );
    }

    #[test]
    fn rescan_globs_noop_when_no_patterns() {
        let (watcher, rx) = test_watcher();
        let mut discovery = FileDiscovery {
            watcher,
            watched_dirs: HashSet::new(),
            glob_patterns: Vec::new(),
            watch_paths: Vec::new(),
            fs_events: rx,
            last_glob_rescan: Instant::now(),
        };
        let mut reader = test_reader();

        let had_error = discovery.rescan_globs(&mut reader);
        assert!(!had_error);
        assert!(discovery.watch_paths.is_empty());
    }

    #[test]
    fn detect_changes_handles_identify_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let non_file = dir.path().join("logs");
        fs::create_dir_all(&non_file).expect("create dir");

        let (watcher, rx) = test_watcher();
        let discovery = FileDiscovery {
            watcher,
            watched_dirs: HashSet::new(),
            glob_patterns: Vec::new(),
            watch_paths: vec![non_file],
            fs_events: rx,
            last_glob_rescan: Instant::now(),
        };
        let mut reader = test_reader();
        let mut events = Vec::new();

        let had_error = discovery.detect_changes(&mut reader, &mut events);
        assert!(
            had_error,
            "identify failure should be surfaced as had_error"
        );
        assert!(events.is_empty(), "no data events should be emitted");
    }

    #[cfg(unix)]
    #[test]
    fn rescan_globs_reports_open_file_error_for_unreadable_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("secret.log");
        fs::write(&path, b"hidden\n").expect("write log");

        let mut perms = fs::metadata(&path).expect("metadata").permissions();
        perms.set_mode(0o000);
        fs::set_permissions(&path, perms).expect("chmod 000");

        let pattern = format!("{}/**/*.log", dir.path().display());
        let (watcher, rx) = test_watcher();
        let mut discovery = FileDiscovery {
            watcher,
            watched_dirs: HashSet::new(),
            glob_patterns: vec![pattern],
            watch_paths: Vec::new(),
            fs_events: rx,
            last_glob_rescan: Instant::now(),
        };
        let mut reader = test_reader();

        let had_error = discovery.rescan_globs(&mut reader);
        assert!(had_error, "rescan should surface open_file_at failure");
        assert!(
            discovery.watch_paths.contains(&path),
            "path should still be tracked for future retries"
        );
    }

    #[test]
    fn test_watcher_callback_forwards_fs_events_to_channel() {
        use notify::Watcher;

        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("event.log");

        let (mut watcher, rx) = test_watcher();
        watcher
            .watch(dir.path(), notify::RecursiveMode::NonRecursive)
            .expect("watch dir");
        fs::write(&file, b"event\n").expect("write file");

        let _received = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("expected fs event from watcher callback");
    }

    #[test]
    fn helper_result_markers_flag_errors() {
        let path = PathBuf::from("x.log");
        assert!(!mark_watch_dir_result(&path, Ok(())));
        assert!(mark_watch_dir_result(
            &path,
            Err(io::Error::other("watch failed"))
        ));

        assert!(!mark_open_result(&path, "test", Ok(())));
        assert!(mark_open_result(
            &path,
            "test",
            Err(io::Error::other("open failed"))
        ));
    }
}
