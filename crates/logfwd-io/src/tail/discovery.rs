use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::glob::expand_glob_patterns;
use super::identity::identify_file;
use super::reader::FileReader;
use super::tailer::TailEvent;

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
            if let Some(parent) = path.parent()
                && let Err(e) = self.watch_dir(parent)
            {
                tracing::warn!(path = %parent.display(), error = %e, "tail.watch_dir_failed");
                had_error = true;
            }

            if let Err(e) = reader.open_file_at(&path, reader.config.start_from_end) {
                tracing::warn!(path = %path.display(), error = %e, "tail.open_failed");
                had_error = true;
            }

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
                if let Err(e) = reader.open_file_at(path, false) {
                    tracing::warn!(path = %path.display(), error = %e, "tail.open_after_rotation_failed");
                    had_error = true;
                }
            } else if is_new {
                let has_evicted = reader.evicted_offsets.contains_key(path);
                let seek_end = !has_evicted && reader.config.start_from_end;
                if let Err(e) = reader.open_file_at(path, seek_end) {
                    tracing::warn!(path = %path.display(), error = %e, "tail.open_new_file_failed");
                    had_error = true;
                }
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
