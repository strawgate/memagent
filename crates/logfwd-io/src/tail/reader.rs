use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Instant;

use logfwd_types::pipeline::SourceId;

use super::identity::{ByteOffset, FileIdentity, compute_fingerprint, identify_open_file};
use super::tailer::{TailConfig, TailEvent};

/// State tracked per tailed file.
pub(super) struct TailedFile {
    pub(super) identity: FileIdentity,
    pub(super) file: File,
    pub(super) offset: u64,
    pub(super) last_read: Instant,
    pub(super) eof_emitted: bool,
}

/// Saved state for a file evicted from the open-file LRU cache.
pub(super) struct EvictedFile {
    pub(super) identity: FileIdentity,
    pub(super) offset: u64,
    pub(super) path: PathBuf,
    pub(super) source_id: SourceId,
}

/// Internal result from read_new_data — distinguishes truncation from no-data.
pub(super) enum ReadResult {
    Data(Vec<u8>),
    TruncatedThenData(Vec<u8>),
    Truncated,
    NoData,
}

/// Owns the open file descriptors, read buffer, and byte-level I/O.
pub(super) struct FileReader {
    pub(super) files: HashMap<PathBuf, TailedFile>,
    pub(super) read_buf: Vec<u8>,
    pub(super) evicted_offsets: HashMap<PathBuf, EvictedFile>,
    pub(super) scratch_paths: Vec<PathBuf>,
    pub(super) config: TailConfig,
}

impl FileReader {
    pub(super) const MAX_READ_PER_POLL: usize = 4 * 1024 * 1024;

    pub(super) fn open_file_at(&mut self, path: &Path, start_from_end: bool) -> io::Result<()> {
        let mut file = File::open(path)?;
        let identity = identify_open_file(&mut file, self.config.fingerprint_bytes)?;
        let file_size = file.metadata()?.len();
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

        let offset = if let Some(evicted) = evicted {
            if evicted.identity == identity {
                let safe_offset = if evicted.offset > file_size {
                    tracing::warn!(
                        path = %path.display(),
                        saved_offset = evicted.offset,
                        file_size,
                        "evicted offset exceeds file size — resetting to 0"
                    );
                    0
                } else {
                    evicted.offset
                };
                file.seek(SeekFrom::Start(safe_offset))?
            } else if evicted.identity.fingerprint == 0
                && evicted.offset == 0
                && evicted.identity.device == identity.device
                && evicted.identity.inode == identity.inode
            {
                // Empty-file sentinel case: fingerprint can legitimately change
                // from 0 to non-zero when data is first appended.
                0
            } else {
                tracing::warn!(
                    path = %path.display(),
                    evicted_identity = ?evicted.identity,
                    current_identity = ?identity,
                    "evicted offset identity mismatch — ignoring saved offset"
                );
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
                identity,
                file,
                offset,
                last_read: Instant::now(),
                eof_emitted: false,
            },
        );

        Ok(())
    }

    pub(super) fn read_new_data(&mut self, path: &Path) -> io::Result<ReadResult> {
        let tailed = match self.files.get_mut(path) {
            Some(t) => t,
            None => return Ok(ReadResult::NoData),
        };

        let meta = tailed.file.metadata()?;
        let current_size = meta.len();

        let was_truncated = current_size < tailed.offset;
        if was_truncated {
            tailed.offset = 0;
            tailed.eof_emitted = false;
            tailed.file.seek(SeekFrom::Start(0))?;
            tailed.identity.fingerprint =
                compute_fingerprint(&mut tailed.file, self.config.fingerprint_bytes)?;
        }

        if current_size <= tailed.offset {
            return Ok(if was_truncated {
                ReadResult::Truncated
            } else {
                ReadResult::NoData
            });
        }

        let per_file_budget = self
            .config
            .per_file_read_budget_bytes
            .clamp(1, Self::MAX_READ_PER_POLL);
        let mut result = Vec::with_capacity(self.config.read_buf_size);
        loop {
            let remaining = per_file_budget.saturating_sub(result.len());
            if remaining == 0 {
                break;
            }
            let read_len = remaining.min(self.read_buf.len());
            let n = tailed.file.read(&mut self.read_buf[..read_len])?;
            if n == 0 {
                break;
            }
            result.extend_from_slice(&self.read_buf[..n]);
            tailed.offset += n as u64;
        }

        if result.is_empty() {
            return Ok(if was_truncated {
                ReadResult::Truncated
            } else {
                ReadResult::NoData
            });
        }

        tailed.last_read = Instant::now();

        if tailed.identity.fingerprint == 0 {
            let current_pos = tailed.file.stream_position()?;
            tailed.file.seek(SeekFrom::Start(0))?;
            let new_fp = compute_fingerprint(&mut tailed.file, self.config.fingerprint_bytes)?;
            tailed.file.seek(SeekFrom::Start(current_pos))?;
            if new_fp != 0 {
                tailed.identity.fingerprint = new_fp;
            }
        }

        Ok(if was_truncated {
            ReadResult::TruncatedThenData(result)
        } else {
            ReadResult::Data(result)
        })
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
                    tailed.eof_emitted = false;
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
                    tailed.eof_emitted = false;
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
                    tailed.eof_emitted = false;
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
        let mut paths = std::mem::take(&mut self.scratch_paths);
        paths.clear();
        paths.extend(self.files.keys().cloned());

        for path in &paths {
            let pre_read_source_id = self.source_id_for_path(path);
            match self.read_new_data(path) {
                Ok(ReadResult::Data(data)) => {
                    if let Some(tailed) = self.files.get_mut(path) {
                        tailed.eof_emitted = false;
                    }
                    let source_id = self.source_id_for_path(path);
                    events.push(TailEvent::Data {
                        path: path.clone(),
                        bytes: data,
                        source_id,
                    });
                }
                Ok(ReadResult::TruncatedThenData(data)) => {
                    events.push(TailEvent::Truncated {
                        path: path.clone(),
                        source_id: pre_read_source_id,
                    });
                    if let Some(tailed) = self.files.get_mut(path) {
                        tailed.eof_emitted = false;
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
                        tailed.eof_emitted = false;
                    }
                    events.push(TailEvent::Truncated {
                        path: path.clone(),
                        source_id: pre_read_source_id,
                    });
                }
                Ok(ReadResult::NoData) => {
                    if let Some(tailed) = self.files.get_mut(path)
                        && !tailed.eof_emitted
                    {
                        tailed.eof_emitted = true;
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
                tracing::warn!(
                    path = %path.display(),
                    saved_offset = offset,
                    file_size,
                    "checkpoint offset exceeds file size — resetting to 0"
                );
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

    use super::*;

    fn test_reader() -> FileReader {
        FileReader {
            files: HashMap::new(),
            read_buf: vec![0u8; 64],
            evicted_offsets: HashMap::new(),
            scratch_paths: Vec::new(),
            config: TailConfig::default(),
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

        match &events[0] {
            TailEvent::Truncated { source_id, .. } => {
                assert_eq!(
                    *source_id, pre_source,
                    "truncated event keeps pre-read source id"
                );
            }
            _ => panic!("unexpected first event kind"),
        }
        match &events[1] {
            TailEvent::Data { source_id, .. } => {
                assert_eq!(
                    *source_id,
                    reader.source_id_for_path(&path),
                    "data event should use refreshed source id after truncation"
                );
            }
            _ => panic!("unexpected second event kind"),
        }
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
            matches!(events.first(), Some(TailEvent::EndOfFile { .. })),
            "expected EOF event after truncation reset"
        );
    }
}
