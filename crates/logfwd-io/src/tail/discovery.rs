use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use logfwd_types::pipeline::SourceId;

use super::glob::expand_glob_patterns;
use super::identity::{FileIdentity, identify_file};
use super::reader::FileReader;
use super::state::shutdown_should_emit_eof;
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

            let previous_fingerprint_len = reader
                .files
                .get(path)
                .map_or(reader.config.fingerprint_bytes as u64, |tailed| {
                    tailed.fingerprint_len
                });
            let fingerprint_bytes = if previous_fingerprint_len > 0
                && previous_fingerprint_len < reader.config.fingerprint_bytes as u64
            {
                previous_fingerprint_len as usize
            } else {
                reader.config.fingerprint_bytes
            };

            let current_identity = match identify_file(path, fingerprint_bytes) {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "tail.identify_failed");
                    had_error = true;
                    continue;
                }
            };

            let is_rotated = reader.files.get(path).is_some_and(|tailed| {
                let is_previous_handle_deleted = tailed
                    .file
                    .metadata()
                    .is_ok_and(|meta| has_metadata_indicating_deletion(&meta));
                let previous_identity = FileIdentity {
                    device: tailed.identity.device,
                    inode: tailed.identity.inode,
                    fingerprint: tailed.comparison_fingerprint,
                };
                should_rotate_file(
                    &previous_identity,
                    &current_identity,
                    is_previous_handle_deleted,
                    tailed.fingerprint_len,
                )
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
        let deleted = deleted_paths(reader);
        for path in &deleted {
            let source_id = reader.source_id_for_path(path);
            had_error |= reader.drain_file(path, source_id, events);
            reader.files.remove(path);
            reader.evicted_offsets.remove(path);
        }
        self.forget_deleted_watch_paths(&deleted);
        had_error
    }

    pub(super) fn cleanup_deleted_for_shutdown(
        &mut self,
        reader: &mut FileReader,
        events: &mut Vec<TailEvent>,
    ) -> (bool, Vec<(PathBuf, Option<SourceId>)>) {
        let mut had_error = false;
        let mut eof_targets = Vec::new();
        let deleted = deleted_paths(reader);
        let mut drained_deleted = Vec::new();

        for path in &deleted {
            let pre_drain_source_id = reader.source_id_for_path(path);
            had_error |= reader.drain_file(path, pre_drain_source_id, events);
            let post_drain_source_id = reader.source_id_for_path(path);
            let should_remove = match reader.files.get(path) {
                Some(tailed) => match tailed.file.metadata().map(|meta| meta.len()) {
                    Ok(file_size) => {
                        let is_caught_up = shutdown_should_emit_eof(tailed.offset, file_size);
                        if is_caught_up
                            && post_drain_source_id.is_some()
                            && !tailed.eof_state.has_emitted()
                        {
                            eof_targets.push((path.clone(), post_drain_source_id));
                        }
                        is_caught_up
                    }
                    Err(_) => true,
                },
                None => true,
            };
            if should_remove {
                drained_deleted.push(path.clone());
                reader.files.remove(path);
                reader.evicted_offsets.remove(path);
            }
        }
        self.forget_deleted_watch_paths(&drained_deleted);

        (had_error, eof_targets)
    }

    fn forget_deleted_watch_paths(&mut self, deleted: &[PathBuf]) {
        if !self.glob_patterns.is_empty() && !deleted.is_empty() {
            let deleted_set: HashSet<&PathBuf> = deleted.iter().collect();
            self.watch_paths.retain(|p| !deleted_set.contains(p));
        }
    }
}

fn deleted_paths(reader: &FileReader) -> Vec<PathBuf> {
    reader
        .files
        .iter()
        .filter(|(_, tailed)| {
            tailed
                .file
                .metadata()
                .ok()
                .is_none_or(|m| has_metadata_indicating_deletion(&m))
        })
        .map(|(path, _)| path.clone())
        .collect()
}

#[cfg(unix)]
fn has_metadata_indicating_deletion(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    meta.nlink() == 0
}

#[cfg(not(unix))]
fn has_metadata_indicating_deletion(_meta: &std::fs::Metadata) -> bool {
    false
}

#[inline]
fn should_identity_rotate(
    previous: &FileIdentity,
    current: &FileIdentity,
    previous_fingerprint_len: u64,
) -> bool {
    if previous.device != current.device || previous.inode != current.inode {
        return true;
    }

    // Empty-file sentinel can transition from fingerprint=0 to non-zero when
    // the same inode receives its first bytes. That is not a rotation.
    if previous.fingerprint == 0 {
        return false;
    }

    previous_fingerprint_len > 0 && previous.fingerprint != current.fingerprint
}

#[inline]
fn should_rotate_file(
    previous: &FileIdentity,
    current: &FileIdentity,
    is_previous_handle_deleted: bool,
    previous_fingerprint_len: u64,
) -> bool {
    is_previous_handle_deleted
        || should_identity_rotate(previous, current, previous_fingerprint_len)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::io::{self, Write};
    use std::time::{Duration, Instant};

    use logfwd_types::pipeline::SourceId;

    use super::super::identity::FileIdentity;
    use super::super::reader::{EvictedFile, FileReader, TailedFile};
    use super::super::state::EofState;
    use super::super::tailer::{TailConfig, TailEvent};
    use super::*;

    fn forward_fs_event(
        tx: &crossbeam_channel::Sender<notify::Result<notify::Event>>,
        res: notify::Result<notify::Event>,
    ) {
        let _ = tx.send(res);
    }

    fn test_reader() -> FileReader {
        FileReader {
            files: HashMap::new(),
            read_buf: vec![0u8; 128],
            evicted_offsets: HashMap::new(),
            scratch_paths: Vec::new(),
            last_read_had_data: false,
            last_read_hit_budget: false,
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
            forward_fs_event(&tx, res);
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
                comparison_fingerprint: 333,
                fingerprint_len: 1024,
                eof_state: EofState::default(),
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
            last_glob_rescan: Instant::now()
                .checked_sub(Duration::from_secs(1))
                .expect("one second before now should be representable"),
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
    fn detect_changes_uses_promoted_fingerprint_window_after_short_file_grows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("promoted.log");
        fs::write(&path, b"LINE-00000000\n").unwrap();

        let mut reader = FileReader {
            config: TailConfig {
                start_from_end: false,
                fingerprint_bytes: 32,
                poll_interval_ms: 10,
                glob_rescan_interval_ms: 0,
                ..Default::default()
            },
            ..test_reader()
        };
        reader.open_file_at(&path, false).unwrap();
        assert!(matches!(
            reader.read_new_data(&path).expect("initial read"),
            super::super::reader::ReadResult::Data(_)
        ));

        {
            let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"LINE-00000001\nLINE-00000002\n").unwrap();
        }
        assert!(matches!(
            reader.read_new_data(&path).expect("growth read"),
            super::super::reader::ReadResult::Data(_)
        ));

        let tailed = reader.files.get(&path).expect("tailed file");
        assert_eq!(tailed.fingerprint_len, 32);
        let source_id = tailed.identity.source_id();

        fs::write(&path, b"LINE-00000000\nrewritten-after-promoted-window\n").unwrap();

        let (watcher, rx) = test_watcher();
        let discovery = FileDiscovery {
            watcher,
            watched_dirs: HashSet::new(),
            glob_patterns: Vec::new(),
            watch_paths: vec![path.clone()],
            fs_events: rx,
            last_glob_rescan: Instant::now(),
        };

        let mut events = Vec::new();
        let had_error = discovery.detect_changes(&mut reader, &mut events);
        assert!(!had_error, "detect_changes should succeed");
        assert!(
            events.iter().any(|event| matches!(
                event,
                TailEvent::Rotated {
                    path: event_path,
                    source_id: event_source_id,
                } if event_path == &path && *event_source_id == Some(source_id)
            )),
            "rewrite preserving the original short prefix should still rotate after fingerprint promotion"
        );
    }

    #[test]
    fn identity_matching_partial_fingerprint_is_not_rotation() {
        let previous = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 3,
        };
        let current = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 3,
        };
        assert!(
            !should_identity_rotate(&previous, &current, 12),
            "matching same-inode partial fingerprint must not be treated as rotation"
        );
    }

    #[test]
    fn identity_partial_fingerprint_drift_is_rotation() {
        let previous = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 3,
        };
        let current = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 4,
        };
        assert!(
            should_identity_rotate(&previous, &current, 12),
            "same-inode partial fingerprint drift means the verified prefix changed"
        );
    }

    #[test]
    fn identity_mature_fingerprint_drift_is_rotation() {
        let previous = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 3,
        };
        let current = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 4,
        };
        assert!(
            should_identity_rotate(&previous, &current, 1024),
            "same-inode mature fingerprint drift must be treated as rotation"
        );
    }

    #[test]
    fn identity_empty_sentinel_fingerprint_update_is_not_rotation() {
        let previous = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 0,
        };
        let current = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 4,
        };
        assert!(
            !should_identity_rotate(&previous, &current, 0),
            "empty-file fingerprint sentinel must not rotate when first bytes appear"
        );
    }

    #[test]
    fn deleted_handle_always_rotates_even_when_identity_matches() {
        let previous = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 3,
        };
        let current = FileIdentity {
            device: 1,
            inode: 2,
            fingerprint: 3,
        };
        assert!(
            should_rotate_file(&previous, &current, true, 0),
            "unlinked previous handle must rotate to the current path file"
        );
    }

    #[test]
    fn detect_changes_treats_fingerprint_mismatch_as_rotation_for_same_inode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("inode-reuse.log");
        fs::write(&path, vec![b'x'; 4096]).expect("write file");

        let mut file = fs::File::open(&path).expect("open file");
        let current_identity =
            super::super::identity::identify_open_file(&mut file, 1024).expect("identify current");

        let stale_identity = FileIdentity {
            device: current_identity.device,
            inode: current_identity.inode,
            fingerprint: current_identity.fingerprint ^ 0xA5A5_A5A5_A5A5_A5A5,
        };

        let mut reader = test_reader();
        reader.files.insert(
            path.clone(),
            TailedFile {
                comparison_fingerprint: stale_identity.fingerprint,
                identity: stale_identity,
                fingerprint_len: 1024,
                file,
                offset: 2048,
                last_read: Instant::now(),
                eof_state: EofState::default(),
            },
        );

        let (watcher, rx) = test_watcher();
        let discovery = FileDiscovery {
            watcher,
            watched_dirs: HashSet::new(),
            glob_patterns: Vec::new(),
            watch_paths: vec![path.clone()],
            fs_events: rx,
            last_glob_rescan: Instant::now(),
        };

        let mut events = Vec::new();
        let had_error = discovery.detect_changes(&mut reader, &mut events);
        assert!(!had_error, "detect_changes should succeed");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TailEvent::Rotated { path: p, .. } if *p == path)),
            "fingerprint mismatch with same inode must trigger rotation event"
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

        // Root bypasses UNIX permission checks, so the open would succeed and
        // this test could not observe an error. Skip when we'd be masked out.
        if fs::read(&path).is_ok() {
            eprintln!(
                "skipping rescan_globs_reports_open_file_error_for_unreadable_file: \
                 running as root (or equivalent) so `chmod 000` does not deny the open"
            );
            return;
        }

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
        let (tx, rx) = crossbeam_channel::unbounded();
        forward_fs_event(&tx, Ok(notify::Event::new(notify::EventKind::Any)));
        let received = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("expected callback-forwarded event")
            .expect("callback should forward successful events");
        assert!(matches!(received.kind, notify::EventKind::Any));
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
