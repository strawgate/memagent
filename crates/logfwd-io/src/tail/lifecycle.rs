// Scaffolding module: several lifecycle states and the generic `Session` wrapper
// are defined ahead of their first use.  Suppress dead-code warnings for the
// entire module until the remaining transitions are wired up.
#![allow(dead_code)]

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::time::Instant;

use logfwd_types::pipeline::SourceId;

use super::identity::FileIdentity;
use super::state::EofState;

/// Untracked state: file is not known to the tailer.
pub struct Untracked;

/// Discovered but unopened: file path is known from a glob, but not yet stat'd or opened.
pub struct DiscoveredUnopened;

/// Active state: file is open and actively being tailed.
pub struct Active {
    pub identity: FileIdentity,
    pub comparison_fingerprint: u64,
    pub fingerprint_len: u64,
    pub file: File,
    pub offset: u64,
    pub last_read: Instant,
    pub eof_state: EofState,
}

/// Evicted, closed, and cached: file was closed due to LRU, state is preserved.
pub struct EvictedClosedCached {
    pub identity: FileIdentity,
    pub comparison_fingerprint: u64,
    pub fingerprint_len: u64,
    pub eof_state: EofState,
    pub offset: u64,
    pub path: PathBuf,
    pub source_id: SourceId,
}

/// Deleted but cleanup pending: file removed from disk, but we haven't flushed remaining buffers/events.
pub struct DeletedCleanupPending;

/// Terminal, removed: file is fully removed from tailer tracking.
pub struct TerminalRemoved;

/// Generic session wrapper over a specific state.
pub struct Session<S> {
    pub state: S,
}

impl<S> Session<S> {
    pub fn new(state: S) -> Self {
        Self { state }
    }
}

impl Active {
    pub(super) fn read_new_data(
        &mut self,
        read_buf: &mut [u8],
        per_file_budget: usize,
        fingerprint_bytes: usize,
    ) -> io::Result<super::reader::ReadResult> {
        use super::identity::compute_fingerprint;
        use super::reader::observed_fingerprint_len;
        use std::io::{Read, Seek, SeekFrom};

        let meta = self.file.metadata()?;
        let current_size = meta.len();

        let was_truncated = current_size < self.offset;
        if was_truncated {
            self.offset = 0;
            self.eof_state.on_data();
            self.file.seek(SeekFrom::Start(0))?;
            self.identity.fingerprint = compute_fingerprint(&mut self.file, fingerprint_bytes)?;
            self.comparison_fingerprint = self.identity.fingerprint;
            self.fingerprint_len = observed_fingerprint_len(
                self.identity.fingerprint,
                current_size,
                fingerprint_bytes,
            );
        }

        if current_size <= self.offset {
            return Ok(if was_truncated {
                super::reader::ReadResult::Truncated
            } else {
                super::reader::ReadResult::NoData
            });
        }

        let mut result = Vec::with_capacity(per_file_budget);
        loop {
            let remaining = per_file_budget.saturating_sub(result.len());
            if remaining == 0 {
                break;
            }
            let read_len = remaining.min(read_buf.len());
            let n = self.file.read(&mut read_buf[..read_len])?;
            if n == 0 {
                break;
            }
            result.extend_from_slice(&read_buf[..n]);
            self.offset += n as u64;
        }

        if result.is_empty() {
            return Ok(if was_truncated {
                super::reader::ReadResult::Truncated
            } else {
                super::reader::ReadResult::NoData
            });
        }

        self.last_read = Instant::now();

        if fingerprint_bytes > 0
            && self.fingerprint_len < fingerprint_bytes as u64
            && current_size > self.fingerprint_len
        {
            let new_fp = compute_fingerprint(&mut self.file, fingerprint_bytes)?;
            if new_fp != 0 {
                if self.identity.fingerprint == 0 {
                    self.identity.fingerprint = new_fp;
                }
                self.comparison_fingerprint = new_fp;
                self.fingerprint_len =
                    observed_fingerprint_len(new_fp, current_size, fingerprint_bytes);
            }
        }

        Ok(if was_truncated {
            super::reader::ReadResult::TruncatedThenData(result)
        } else {
            super::reader::ReadResult::Data(result)
        })
    }
}
