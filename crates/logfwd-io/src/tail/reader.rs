use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use logfwd_types::pipeline::SourceId;

use super::identity::{ByteOffset, FileIdentity, compute_fingerprint, identify_open_file};
use super::state::{EOF_IDLE_POLLS_BEFORE_EMIT, EofState};
use super::tailer::{TailConfig, TailEvent};

/// State tracked per tailed file.
pub(super) use super::lifecycle::Active as TailedFile;

/// Saved state for a file evicted from the open-file LRU cache.
pub(super) use super::lifecycle::EvictedClosedCached as EvictedFile;

/// Internal result from read_new_data — distinguishes truncation from no-data.
pub(super) enum ReadResult {
    Data(bytes::BytesMut),
    TruncatedThenData(bytes::BytesMut),
    Truncated,
    NoData,
}

pub(super) fn classify_empty_read_result(was_truncated: bool) -> ReadResult {
    if was_truncated {
        ReadResult::Truncated
    } else {
        ReadResult::NoData
    }
}

/// Return the prefix length actually represented by a stored fingerprint.
///
/// `fingerprint` is the stored fingerprint value, `file_size` is the current
/// file size, and `fingerprint_bytes` is the configured fingerprint window. A
/// zero fingerprint or disabled fingerprint window returns 0. Otherwise the
/// returned length is `min(file_size, fingerprint_bytes)`, because a file can
/// be shorter than the configured identity window when it is first observed.
pub(super) fn observed_fingerprint_len(
    fingerprint: u64,
    file_size: u64,
    fingerprint_bytes: usize,
) -> u64 {
    if fingerprint == 0 || fingerprint_bytes == 0 {
        0
    } else {
        file_size.min(fingerprint_bytes as u64)
    }
}

fn evicted_matches_open_file(
    evicted: &EvictedFile,
    current_identity: &FileIdentity,
    file: &mut File,
    file_size: u64,
    fingerprint_bytes: usize,
) -> io::Result<bool> {
    if evicted.identity.device != current_identity.device
        || evicted.identity.inode != current_identity.inode
    {
        return Ok(false);
    }

    if evicted.fingerprint_len == 0 {
        return Ok(evicted.comparison_fingerprint == current_identity.fingerprint);
    }

    if evicted.fingerprint_len < fingerprint_bytes as u64 {
        let current_prefix = compute_fingerprint(file, evicted.fingerprint_len as usize)?;
        if evicted.comparison_fingerprint != current_prefix {
            return Ok(false);
        }
        return Ok(evicted.offset <= evicted.fingerprint_len || file_size < evicted.offset);
    }

    Ok(evicted.comparison_fingerprint == current_identity.fingerprint)
}

/// Owns the open file descriptors, read buffer, and byte-level I/O.
pub(super) struct FileReader {
    pub(super) files: HashMap<PathBuf, TailedFile>,
    pub(super) evicted_offsets: HashMap<PathBuf, EvictedFile>,
    pub(super) scratch_paths: Vec<PathBuf>,
    pub(super) last_read_had_data: bool,
    pub(super) last_read_hit_budget: bool,
    pub(super) config: TailConfig,
}

impl FileReader {
    pub(super) const MAX_READ_PER_POLL: usize = 4 * 1024 * 1024;

    fn eof_min_idle_duration(&self) -> Duration {
        Duration::from_millis(
            self.config
                .poll_interval_ms
                .saturating_mul(EOF_IDLE_POLLS_BEFORE_EMIT as u64),
        )
    }

    pub(super) fn open_file_at(&mut self, path: &Path, start_from_end: bool) -> io::Result<()> {
        let mut file = File::open(path)?;
        let identity = identify_open_file(&mut file, self.config.fingerprint_bytes)?;
        let file_size = file.metadata()?.len();
        let fingerprint_len = observed_fingerprint_len(
            identity.fingerprint,
            file_size,
            self.config.fingerprint_bytes,
        );
        let source_id = identity.source_id();

        let evicted = self.evicted_offsets.remove(path).or_else(|| {
            // Rotation/rename-safe restore: if this file was evicted under a
            // different path, match by source id for non-empty files.
            if source_id == SourceId(0) {
                return None;
            }
            let match_key = self
                .evicted_offsets
                .iter()
                .find(|(_, e)| e.source_id == source_id)
                .map(|(k, _)| k.clone())?;
            self.evicted_offsets.remove(&match_key)
        });

        let mut stored_identity = identity.clone();
        let mut stored_comparison_fingerprint = identity.fingerprint;
        let mut stored_fingerprint_len = fingerprint_len;
        let mut stored_eof_state = EofState::default();
        let offset = if let Some(evicted) = evicted {
            if evicted_matches_open_file(
                &evicted,
                &identity,
                &mut file,
                file_size,
                self.config.fingerprint_bytes,
            )? {
                stored_identity = evicted.identity.clone();
                stored_comparison_fingerprint = evicted.comparison_fingerprint;
                stored_fingerprint_len = evicted.fingerprint_len;
                stored_eof_state = evicted.eof_state;
                if evicted.offset > file_size {
                    tracing::warn!(
                        path = %path.display(),
                        saved_offset = evicted.offset,
                        file_size,
                        "evicted offset exceeds file size — preserving for truncation detection"
                    );
                }
                file.seek(SeekFrom::Start(evicted.offset))?
            } else if evicted.identity.fingerprint == 0
                && evicted.offset == 0
                && evicted.identity.device == identity.device
                && evicted.identity.inode == identity.inode
            {
                // Empty-file sentinel case: fingerprint can legitimately change
                // from 0 to non-zero when data is first appended.
                0
            } else {
                tracing::warn!(path = %path.display(), evicted_identity = ?evicted.identity, current_identity = ?identity, "evicted offset identity mismatch — ignoring saved offset");
                if start_from_end {
                    file.seek(SeekFrom::End(0))?
                } else {
                    0
                }
            }
        } else if start_from_end {
            file.seek(SeekFrom::End(0))?
        } else {
            0
        };

        self.files.insert(
            path.to_path_buf(),
            TailedFile {
                identity: stored_identity,
                comparison_fingerprint: stored_comparison_fingerprint,
                fingerprint_len: stored_fingerprint_len,
                file,
                offset,
                read_buf: bytes::BytesMut::new(),
                last_read: Instant::now(),
                eof_state: stored_eof_state,
            },
        );

        Ok(())
    }

    pub(super) fn read_new_data(&mut self, path: &Path) -> io::Result<ReadResult> {
        let fingerprint_bytes = self.config.fingerprint_bytes;
        let per_file_budget = self
            .config
            .per_file_read_budget_bytes
            .clamp(1, Self::MAX_READ_PER_POLL);

        let tailed = match self.files.get_mut(path) {
            Some(t) => t,
            None => return Ok(ReadResult::NoData),
        };

        tailed.read_new_data(per_file_budget, fingerprint_bytes)
    }

    pub(super) fn drain_file(
        &mut self,
        path: &Path,
        source_id: Option<SourceId>,
        events: &mut Vec<TailEvent>,
    ) -> bool {
        let pre_read_source_id = source_id;
        match self.read_new_data(path) {
            Ok(ReadResult::Data(data)) => {
                if let Some(tailed) = self.files.get_mut(path) {
                    tailed.eof_state.on_data();
                }
                let source_id = self.source_id_for_path(path);
                events.push(TailEvent::Data {
                    path: path.to_path_buf(),
                    bytes: data,
                    source_id,
                });
                false
            }
            Ok(ReadResult::TruncatedThenData(data)) => {
                events.push(TailEvent::Truncated {
                    path: path.to_path_buf(),
                    source_id: pre_read_source_id,
                });
                if let Some(tailed) = self.files.get_mut(path) {
                    tailed.eof_state.on_data();
                }
                let source_id = self.source_id_for_path(path);
                events.push(TailEvent::Data {
                    path: path.to_path_buf(),
                    bytes: data,
                    source_id,
                });
                false
            }
            Ok(ReadResult::Truncated) => {
                if let Some(tailed) = self.files.get_mut(path) {
                    tailed.eof_state.on_data();
                }
                events.push(TailEvent::Truncated {
                    path: path.to_path_buf(),
                    source_id: pre_read_source_id,
                });
                false
            }
            Ok(ReadResult::NoData) => false,
            Err(e) => {
                tracing::error!(path = %path.display(), error = %e, "tail.drain_file_error");
                true
            }
        }
    }

    pub(super) fn read_all(&mut self, events: &mut Vec<TailEvent>) -> bool {
        let mut had_error = false;
        self.last_read_had_data = false;
        self.last_read_hit_budget = false;
        let mut paths = std::mem::take(&mut self.scratch_paths);
        paths.clear();
        paths.extend(self.files.keys().cloned());
        let eof_min_idle = self.eof_min_idle_duration();
        let per_file_budget = self
            .config
            .per_file_read_budget_bytes
            .clamp(1, Self::MAX_READ_PER_POLL);

        for path in &paths {
            let pre_read_source_id = self.source_id_for_path(path);
            match self.read_new_data(path) {
                Ok(ReadResult::Data(data)) => {
                    self.last_read_had_data = true;
                    self.last_read_hit_budget |= data.len() >= per_file_budget;
                    if let Some(tailed) = self.files.get_mut(path) {
                        tailed.eof_state.on_data();
                    }
                    let source_id = self.source_id_for_path(path);
                    events.push(TailEvent::Data {
                        path: path.clone(),
                        bytes: data,
                        source_id,
                    });
                }
                Ok(ReadResult::TruncatedThenData(data)) => {
                    self.last_read_had_data = true;
                    self.last_read_hit_budget |= data.len() >= per_file_budget;
                    events.push(TailEvent::Truncated {
                        path: path.clone(),
                        source_id: pre_read_source_id,
                    });
                    if let Some(tailed) = self.files.get_mut(path) {
                        tailed.eof_state.on_data();
                    }
                    let source_id = self.source_id_for_path(path);
                    events.push(TailEvent::Data {
                        path: path.clone(),
                        bytes: data,
                        source_id,
                    });
                }
                Ok(ReadResult::Truncated) => {
                    if let Some(tailed) = self.files.get_mut(path) {
                        tailed.eof_state.on_data();
                    }
                    events.push(TailEvent::Truncated {
                        path: path.clone(),
                        source_id: pre_read_source_id,
                    });
                }
                Ok(ReadResult::NoData) => {
                    let mut emit_eof = false;
                    if let Some(tailed) = self.files.get_mut(path) {
                        emit_eof = tailed.eof_state.on_no_data(Instant::now(), eof_min_idle);
                    }
                    if emit_eof {
                        let source_id = self.source_id_for_path(path);
                        events.push(TailEvent::EndOfFile {
                            path: path.clone(),
                            source_id,
                        });
                    }
                }
                Err(e) => {
                    tracing::error!(path = %path.display(), error = %e, "tail.read_error");
                    had_error = true;
                }
            }
        }
        self.scratch_paths = paths;
        had_error
    }

    pub(super) fn evict_lru(&mut self, max_open: usize) {
        if self.files.len() > max_open {
            let mut by_age: Vec<(PathBuf, Instant)> = self
                .files
                .iter()
                .map(|(path, tailed)| (path.clone(), tailed.last_read))
                .collect();
            by_age.sort_by_key(|(_, last_read)| *last_read);
            let to_remove = self.files.len() - max_open;
            for (path, _) in by_age.into_iter().take(to_remove) {
                if let Some(tailed) = self.files.remove(&path) {
                    let source_id = tailed.identity.source_id();
                    self.evicted_offsets.insert(
                        path.clone(),
                        EvictedFile {
                            identity: tailed.identity,
                            comparison_fingerprint: tailed.comparison_fingerprint,
                            fingerprint_len: tailed.fingerprint_len,
                            eof_state: tailed.eof_state,
                            offset: tailed.offset,
                            path,
                            source_id,
                        },
                    );
                }
            }
        }
    }

    pub(super) fn get_offset(&self, path: &Path) -> Option<u64> {
        self.files.get(path).map(|f| f.offset)
    }

    pub(super) fn set_offset(&mut self, path: &Path, offset: u64) -> io::Result<()> {
        if let Some(tailed) = self.files.get_mut(path) {
            let file_size = tailed.file.metadata()?.len();
            let safe_offset = if offset > file_size {
                tracing::warn!(path = %path.display(), saved_offset = offset, file_size, "checkpoint offset exceeds file size — resetting to 0");
                0
            } else {
                offset
            };
            tailed.offset = safe_offset;
            tailed.file.seek(SeekFrom::Start(safe_offset))?;
            return Ok(());
        }
        if let Some(evicted) = self.evicted_offsets.get_mut(path) {
            evicted.offset = offset;
        }
        Ok(())
    }

    pub(super) fn set_offset_by_source(
        &mut self,
        source_id: SourceId,
        offset: u64,
    ) -> io::Result<()> {
        // `SourceId(0)` is a sentinel for "unknown/invalid source"; never apply
        // checkpoint offsets to it.
        if source_id == SourceId(0) {
            return Ok(());
        }
        for tailed in self.files.values_mut() {
            if tailed.identity.source_id() == source_id {
                let file_size = tailed.file.metadata()?.len();
                let safe_offset = if offset > file_size {
                    tracing::warn!(
                        source_id = source_id.0,
                        saved_offset = offset,
                        file_size,
                        "checkpoint source offset exceeds file size — resetting to 0"
                    );
                    0
                } else {
                    offset
                };
                tailed.offset = safe_offset;
                tailed.file.seek(SeekFrom::Start(safe_offset))?;
                return Ok(());
            }
        }
        for evicted in self.evicted_offsets.values_mut() {
            if evicted.source_id == source_id {
                evicted.offset = offset;
                return Ok(());
            }
        }
        Ok(())
    }

    pub(super) fn num_files(&self) -> usize {
        self.files.len()
    }

    pub(super) fn source_id_for_path(&self, path: &Path) -> Option<SourceId> {
        self.files.get(path).and_then(|tailed| {
            let sid = tailed.identity.source_id();
            if sid == SourceId(0) { None } else { Some(sid) }
        })
    }

    pub(super) fn file_offsets(&self) -> Vec<(SourceId, ByteOffset)> {
        let active = self
            .files
            .iter()
            .filter(|(_, tailed)| tailed.identity.fingerprint != 0)
            .map(|(_, tailed)| (tailed.identity.source_id(), ByteOffset(tailed.offset)));

        let evicted = self
            .evicted_offsets
            .values()
            .filter(|e| e.identity.fingerprint != 0)
            .map(|e| (e.source_id, ByteOffset(e.offset)));

        active.chain(evicted).collect()
    }

    pub(super) fn file_paths(&self) -> Vec<(SourceId, PathBuf)> {
        let active = self
            .files
            .iter()
            .filter(|(_, tailed)| tailed.identity.fingerprint != 0)
            .map(|(path, tailed)| (tailed.identity.source_id(), path.clone()));

        let evicted = self
            .evicted_offsets
            .values()
            .filter(|e| e.identity.fingerprint != 0)
            .map(|e| (e.source_id, e.path.clone()));

        active.chain(evicted).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn classify_empty_read_result_matches_truncation_flag() {
        assert!(matches!(
            classify_empty_read_result(true),
            ReadResult::Truncated
        ));
        assert!(matches!(
            classify_empty_read_result(false),
            ReadResult::NoData
        ));
    }

    fn test_reader() -> FileReader {
        FileReader {
            files: HashMap::new(),
            evicted_offsets: HashMap::new(),
            scratch_paths: Vec::new(),
            last_read_had_data: false,
            last_read_hit_budget: false,
            config: TailConfig {
                poll_interval_ms: 0,
                ..TailConfig::default()
            },
        }
    }

    #[test]
    fn set_offset_by_source_ignores_zero_source_id_for_evicted_entries() {
        let path = PathBuf::from("placeholder.log");
        let mut reader = test_reader();
        reader.evicted_offsets.insert(
            path.clone(),
            EvictedFile {
                identity: FileIdentity {
                    device: 1,
                    inode: 2,
                    fingerprint: 0,
                },
                comparison_fingerprint: 0,
                fingerprint_len: 0,
                eof_state: EofState::default(),
                offset: 7,
                path: path.clone(),
                source_id: SourceId(0),
            },
        );

        reader
            .set_offset_by_source(SourceId(0), 11)
            .expect("zero source id should be ignored safely");

        let updated = reader
            .evicted_offsets
            .get(&path)
            .expect("evicted entry should remain present")
            .offset;
        assert_eq!(updated, 7, "sentinel SourceId(0) must be a no-op");
    }

    #[test]
    fn set_offset_updates_evicted_entry_when_file_is_not_active() {
        let path = PathBuf::from("evicted.log");
        let mut reader = test_reader();
        reader.evicted_offsets.insert(
            path.clone(),
            EvictedFile {
                identity: FileIdentity {
                    device: 1,
                    inode: 2,
                    fingerprint: 123,
                },
                comparison_fingerprint: 123,
                fingerprint_len: 3,
                eof_state: EofState::default(),
                offset: 4,
                path: path.clone(),
                source_id: SourceId(99),
            },
        );

        reader
            .set_offset(&path, 17)
            .expect("set_offset should update evicted entry");

        assert_eq!(
            reader
                .evicted_offsets
                .get(&path)
                .expect("evicted entry should remain present")
                .offset,
            17
        );
    }

    #[test]
    fn set_offset_preserves_evicted_entry_offset_when_file_shrunk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("evicted.log");
        fs::write(&path, b"abc").unwrap();
        let mut reader = test_reader();
        reader.evicted_offsets.insert(
            path.clone(),
            EvictedFile {
                identity: FileIdentity {
                    device: 1,
                    inode: 2,
                    fingerprint: 123,
                },
                comparison_fingerprint: 123,
                fingerprint_len: 3,
                eof_state: EofState::default(),
                offset: 2,
                path: path.clone(),
                source_id: SourceId(99),
            },
        );

        reader
            .set_offset(&path, 17)
            .expect("set_offset should update evicted entry");

        assert_eq!(
            reader
                .evicted_offsets
                .get(&path)
                .expect("evicted entry should remain present")
                .offset,
            17
        );
    }

    #[test]
    fn set_offset_by_source_preserves_evicted_entry_offset_when_file_shrunk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("evicted.log");
        fs::write(&path, b"abc").unwrap();
        let mut reader = test_reader();
        reader.evicted_offsets.insert(
            path.clone(),
            EvictedFile {
                identity: FileIdentity {
                    device: 1,
                    inode: 2,
                    fingerprint: 123,
                },
                comparison_fingerprint: 123,
                fingerprint_len: 3,
                eof_state: EofState::default(),
                offset: 2,
                path: path.clone(),
                source_id: SourceId(99),
            },
        );

        reader
            .set_offset_by_source(SourceId(99), 17)
            .expect("set_offset_by_source should update evicted entry");

        assert_eq!(
            reader
                .evicted_offsets
                .get(&path)
                .expect("evicted entry should remain present")
                .offset,
            17
        );
    }

    #[test]
    fn open_file_at_recovers_evicted_offset_across_rename() {
        let dir = tempfile::tempdir().unwrap();
        let old_path = dir.path().join("old.log");
        fs::write(&old_path, b"abcdef").unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&old_path, false).unwrap();
        reader.set_offset(&old_path, 3).unwrap();
        reader.evict_lru(0);

        let new_path = dir.path().join("new.log");
        fs::rename(&old_path, &new_path).unwrap();

        reader.open_file_at(&new_path, true).unwrap();
        assert_eq!(
            reader.get_offset(&new_path),
            Some(3),
            "offset should be recovered by source id across rename"
        );
    }

    #[test]
    fn open_file_at_empty_evicted_entry_starts_from_zero_after_first_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.log");
        File::create(&path).unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();
        reader.evict_lru(0);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"hello").unwrap();

        reader.open_file_at(&path, true).unwrap();
        assert_eq!(
            reader.get_offset(&path),
            Some(0),
            "empty-file evicted sentinel should resume from byte 0"
        );
    }

    #[test]
    fn open_file_at_preserves_evicted_partial_identity_while_file_grows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial-identity.log");
        fs::write(&path, b"LINE-00000000\n").unwrap();

        let mut reader = FileReader {
            config: TailConfig {
                fingerprint_bytes: 32,
                ..TailConfig::default()
            },
            ..test_reader()
        };
        reader.open_file_at(&path, false).unwrap();
        let source_id = reader
            .source_id_for_path(&path)
            .expect("non-empty file should have source id");
        reader.evict_lru(0);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"LINE-00000001\nLINE-00000002\n").unwrap();
        drop(f);

        reader.open_file_at(&path, false).unwrap();
        assert_eq!(
            reader.source_id_for_path(&path),
            Some(source_id),
            "reopened file should keep the established source identity while the same file grows"
        );
    }

    #[test]
    fn drain_file_uses_fresh_source_id_after_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncate.log");
        fs::write(&path, b"aaaa").unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();
        reader.set_offset(&path, 4).unwrap();
        let pre_source = reader.source_id_for_path(&path);

        let mut f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(b"bb").unwrap();
        drop(f);

        let mut events = Vec::new();
        let had_error = reader.drain_file(&path, pre_source, &mut events);
        assert!(!had_error);
        assert_eq!(events.len(), 2, "expected truncated + data events");

        assert!(
            matches!(&events[0], TailEvent::Truncated { source_id, .. } if *source_id == pre_source),
            "first event should be Truncated with pre-read source id"
        );
        assert!(
            matches!(&events[1], TailEvent::Data { source_id, .. } if *source_id == reader.source_id_for_path(&path)),
            "second event should be Data with refreshed source id"
        );
    }

    #[test]
    fn read_all_re_emits_eof_after_truncated_to_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("eof-reset.log");
        fs::write(&path, b"abc").unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();

        let mut events = Vec::new();
        assert!(!reader.read_all(&mut events));
        assert!(matches!(events.first(), Some(TailEvent::Data { .. })));

        events.clear();
        assert!(!reader.read_all(&mut events));
        assert!(
            events.is_empty(),
            "first idle poll should not emit EOF; wait for a second idle poll"
        );

        events.clear();
        assert!(!reader.read_all(&mut events));
        assert!(matches!(events.first(), Some(TailEvent::EndOfFile { .. })));

        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        events.clear();
        assert!(!reader.read_all(&mut events));
        assert!(matches!(events.first(), Some(TailEvent::Truncated { .. })));

        events.clear();
        assert!(!reader.read_all(&mut events));
        assert!(
            events.is_empty(),
            "first idle poll after truncation should not emit EOF"
        );

        events.clear();
        assert!(!reader.read_all(&mut events));
        assert!(
            matches!(events.first(), Some(TailEvent::EndOfFile { .. })),
            "expected EOF event after truncation reset"
        );
    }

    #[test]
    fn read_all_eof_waits_for_min_idle_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("eof-idle-window.log");
        fs::write(&path, b"abc").unwrap();

        let mut reader = FileReader {
            config: TailConfig {
                poll_interval_ms: 10,
                ..TailConfig::default()
            },
            ..test_reader()
        };
        reader.open_file_at(&path, false).unwrap();

        let mut events = Vec::new();
        assert!(!reader.read_all(&mut events));
        assert!(matches!(events.first(), Some(TailEvent::Data { .. })));

        events.clear();
        assert!(!reader.read_all(&mut events));
        assert!(
            events.is_empty(),
            "first idle poll should not emit EOF before minimum idle window"
        );

        events.clear();
        assert!(!reader.read_all(&mut events));
        assert!(
            events.is_empty(),
            "second idle poll still should not emit EOF if minimum idle time has not elapsed"
        );

        sleep(Duration::from_millis(25));
        events.clear();
        assert!(!reader.read_all(&mut events));
        assert!(
            matches!(events.first(), Some(TailEvent::EndOfFile { .. })),
            "EOF should emit once the idle window has elapsed"
        );
    }

    #[test]
    fn read_new_data_missing_path_returns_no_data() {
        let mut reader = test_reader();
        let missing = PathBuf::from("missing.log");
        let got = reader.read_new_data(&missing).expect("read_new_data");
        assert!(matches!(got, ReadResult::NoData));
    }

    #[test]
    fn open_file_at_preserves_evicted_offset_beyond_file_size_for_truncation_detection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("clamp.log");
        fs::write(&path, b"abc").unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();
        let identity = reader.files.get(&path).unwrap().identity.clone();
        let source_id = identity.source_id();
        let _ = reader.files.remove(&path);
        reader.evicted_offsets.insert(
            path.clone(),
            EvictedFile {
                comparison_fingerprint: identity.fingerprint,
                identity,
                fingerprint_len: 3,
                eof_state: EofState::default(),
                offset: 999,
                path: path.clone(),
                source_id,
            },
        );

        reader.open_file_at(&path, false).unwrap();
        assert_eq!(reader.get_offset(&path), Some(999));

        let got = reader.read_new_data(&path).unwrap();
        assert!(
            matches!(got, ReadResult::TruncatedThenData(bytes) if bytes.as_ref() == b"abc"),
            "reader should preserve the saved offset until read_new_data emits truncation"
        );
    }

    #[test]
    fn open_file_at_rejects_evicted_partial_offset_beyond_verified_prefix_when_file_grew() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial-prefix-rewrite.log");
        fs::write(&path, b"prefix").unwrap();

        let mut reader = FileReader {
            config: TailConfig {
                fingerprint_bytes: 16,
                ..TailConfig::default()
            },
            ..test_reader()
        };
        reader.open_file_at(&path, false).unwrap();
        let identity = reader.files.get(&path).unwrap().identity.clone();
        let source_id = identity.source_id();
        let _ = reader.files.remove(&path);
        reader.evicted_offsets.insert(
            path.clone(),
            EvictedFile {
                comparison_fingerprint: identity.fingerprint,
                identity,
                fingerprint_len: 6,
                eof_state: EofState::default(),
                offset: 10,
                path: path.clone(),
                source_id,
            },
        );

        fs::write(&path, b"prefix-rewritten-after-eviction").unwrap();

        reader.open_file_at(&path, false).unwrap();
        assert_eq!(
            reader.get_offset(&path),
            Some(0),
            "partial-prefix matches must not restore offsets beyond verified bytes after growth"
        );
        assert_ne!(
            reader.source_id_for_path(&path),
            Some(source_id),
            "rewritten file should get a fresh source identity"
        );

        let got = reader.read_new_data(&path).unwrap();
        assert!(
            matches!(got, ReadResult::Data(bytes) if bytes.as_ref() == b"prefix-rewritten-after-eviction"),
            "failed partial-prefix restores must leave the cursor at byte 0"
        );
    }

    #[test]
    fn open_file_at_identity_mismatch_uses_start_from_beginning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mismatch.log");
        fs::write(&path, b"abcdef").unwrap();

        let mut reader = test_reader();
        reader.evicted_offsets.insert(
            path.clone(),
            EvictedFile {
                identity: FileIdentity {
                    device: 999,
                    inode: 999,
                    fingerprint: 999,
                },
                comparison_fingerprint: 999,
                fingerprint_len: 6,
                eof_state: EofState::default(),
                offset: 5,
                path: path.clone(),
                source_id: SourceId(5),
            },
        );

        reader.open_file_at(&path, false).unwrap();
        assert_eq!(reader.get_offset(&path), Some(0));
    }

    #[test]
    fn read_new_data_promotes_empty_fingerprint_after_first_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-first.log");
        File::create(&path).unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();
        assert_eq!(reader.files.get(&path).unwrap().identity.fingerprint, 0);

        fs::write(&path, b"hello").unwrap();
        let got = reader.read_new_data(&path).unwrap();
        assert!(matches!(got, ReadResult::Data(_)));
        assert_ne!(reader.files.get(&path).unwrap().identity.fingerprint, 0);
    }

    #[test]
    fn read_new_data_promotes_partial_fingerprint_window_while_file_grows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fingerprint-growth.log");
        let initial = b"LINE-00000000\n";
        fs::write(&path, initial).unwrap();

        let mut reader = FileReader {
            config: TailConfig {
                fingerprint_bytes: 32,
                ..TailConfig::default()
            },
            ..test_reader()
        };
        reader.open_file_at(&path, false).unwrap();

        let got = reader.read_new_data(&path).unwrap();
        assert!(matches!(got, ReadResult::Data(_)));
        let first_fingerprint = reader.files.get(&path).unwrap().identity.fingerprint;
        let first_comparison_fingerprint = reader.files.get(&path).unwrap().comparison_fingerprint;
        let first_fingerprint_len = reader.files.get(&path).unwrap().fingerprint_len;

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"LINE-00000001\nLINE-00000002\n").unwrap();
        drop(f);

        let got = reader.read_new_data(&path).unwrap();
        assert!(matches!(got, ReadResult::Data(_)));

        let updated = reader.files.get(&path).unwrap();
        assert_eq!(first_fingerprint_len, initial.len() as u64);
        assert_eq!(
            updated.fingerprint_len, reader.config.fingerprint_bytes as u64,
            "fingerprint window should promote once enough bytes are observable"
        );
        assert_eq!(
            updated.identity.fingerprint, first_fingerprint,
            "source identity should remain stable while the same file grows"
        );
        assert_ne!(
            updated.comparison_fingerprint, first_comparison_fingerprint,
            "comparison fingerprint should cover the larger observed prefix"
        );
    }

    #[test]
    fn truncation_refreshes_comparison_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncate-refresh.log");
        let original = b"original-long-enough-for-window-and-offset";
        let rewritten = b"rewritten-full-window";
        fs::write(&path, original).unwrap();

        let mut reader = FileReader {
            config: TailConfig {
                fingerprint_bytes: 16,
                ..TailConfig::default()
            },
            ..test_reader()
        };
        reader.open_file_at(&path, false).unwrap();
        let got = reader.read_new_data(&path).unwrap();
        assert!(matches!(got, ReadResult::Data(_)));
        assert!(original.len() > rewritten.len());
        let before = reader.files.get(&path).unwrap().comparison_fingerprint;

        fs::write(&path, rewritten).unwrap();
        let got = reader.read_new_data(&path).unwrap();
        assert!(matches!(got, ReadResult::TruncatedThenData(_)));

        let tailed = reader.files.get(&path).unwrap();
        assert_ne!(
            tailed.comparison_fingerprint, before,
            "truncated content should refresh the comparison fingerprint"
        );
        assert_eq!(
            tailed.comparison_fingerprint, tailed.identity.fingerprint,
            "truncation refresh should keep comparison and identity fingerprints aligned"
        );
        assert_eq!(
            tailed.fingerprint_len, reader.config.fingerprint_bytes as u64,
            "full-window truncation should not rely on later promotion"
        );
    }

    #[test]
    fn read_new_data_truncated_without_new_bytes_returns_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncate-empty.log");
        fs::write(&path, b"abc").unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();
        reader.set_offset(&path, 3).unwrap();
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let got = reader.read_new_data(&path).unwrap();
        assert!(matches!(got, ReadResult::Truncated));
    }

    #[test]
    fn read_all_marks_error_for_unreadable_open_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unreadable.log");
        fs::write(&path, b"abc").unwrap();

        let mut reader = test_reader();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        reader.files.insert(
            path,
            TailedFile {
                identity: FileIdentity {
                    device: 1,
                    inode: 2,
                    fingerprint: 3,
                },
                comparison_fingerprint: 3,
                fingerprint_len: 3,
                file,
                offset: 0,
                read_buf: bytes::BytesMut::new(),
                last_read: Instant::now(),
                eof_state: EofState::default(),
            },
        );

        let mut events = Vec::new();
        let had_error = reader.read_all(&mut events);
        assert!(had_error);
        assert!(events.is_empty());
    }

    #[test]
    fn drain_file_marks_error_for_unreadable_open_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unreadable-drain.log");
        fs::write(&path, b"abc").unwrap();

        let mut reader = test_reader();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        reader.files.insert(
            path.clone(),
            TailedFile {
                identity: FileIdentity {
                    device: 1,
                    inode: 2,
                    fingerprint: 3,
                },
                comparison_fingerprint: 3,
                fingerprint_len: 3,
                file,
                offset: 0,
                read_buf: bytes::BytesMut::new(),
                last_read: Instant::now(),
                eof_state: EofState::default(),
            },
        );

        let mut events = Vec::new();
        let had_error = reader.drain_file(&path, Some(SourceId(3)), &mut events);
        assert!(had_error);
        assert!(events.is_empty());
    }

    #[test]
    fn set_offset_clamps_to_zero_when_beyond_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("offset-clamp.log");
        fs::write(&path, b"abc").unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();
        reader.set_offset(&path, 99).unwrap();
        assert_eq!(reader.get_offset(&path), Some(0));
    }

    #[test]
    fn set_offset_by_source_no_match_is_noop() {
        let mut reader = test_reader();
        reader.evicted_offsets.insert(
            PathBuf::from("other.log"),
            EvictedFile {
                identity: FileIdentity {
                    device: 1,
                    inode: 2,
                    fingerprint: 3,
                },
                comparison_fingerprint: 3,
                fingerprint_len: 3,
                eof_state: EofState::default(),
                offset: 7,
                path: PathBuf::from("other.log"),
                source_id: SourceId(1),
            },
        );

        reader.set_offset_by_source(SourceId(999), 42).unwrap();
        assert_eq!(
            reader
                .evicted_offsets
                .get(&PathBuf::from("other.log"))
                .expect("evicted entry")
                .offset,
            7
        );
    }

    #[test]
    fn drain_file_emits_truncated_without_data_when_file_becomes_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncate-only.log");
        fs::write(&path, b"abc").unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();
        reader.set_offset(&path, 3).unwrap();
        let pre_source = reader.source_id_for_path(&path);
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let mut events = Vec::new();
        let had_error = reader.drain_file(&path, pre_source, &mut events);
        assert!(!had_error);
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], TailEvent::Truncated { source_id, .. } if *source_id == pre_source),
            "truncated-only read should emit a single Truncated event"
        );
    }

    #[test]
    fn set_offset_by_source_applies_in_range_offset_to_active_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("by-source.log");
        fs::write(&path, b"abcdef").unwrap();

        let mut reader = test_reader();
        reader.open_file_at(&path, false).unwrap();
        let source_id = reader
            .source_id_for_path(&path)
            .expect("active file should have source id");

        reader.set_offset_by_source(source_id, 2).unwrap();
        assert_eq!(reader.get_offset(&path), Some(2));
    }
}
