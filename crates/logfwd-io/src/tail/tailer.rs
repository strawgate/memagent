use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use logfwd_types::diagnostics::ComponentHealth;
use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::pipeline::SourceId;

use crate::poll_cadence::{AdaptivePollController, PollCadenceSignal};
use crate::polling_input_health::{PollingInputHealthEvent, reduce_polling_input_health};

use super::discovery::FileDiscovery;
use super::glob::expand_glob_patterns;
use super::identity::ByteOffset;
use super::reader::FileReader;
use super::state::{backoff_transition, shutdown_should_emit_eof};

/// Events emitted by the tailer.
#[non_exhaustive]
pub enum TailEvent {
    Data {
        path: PathBuf,
        bytes: Vec<u8>,
        source_id: Option<SourceId>,
    },
    Rotated {
        path: PathBuf,
        source_id: Option<SourceId>,
    },
    Truncated {
        path: PathBuf,
        source_id: Option<SourceId>,
    },
    EndOfFile {
        path: PathBuf,
        source_id: Option<SourceId>,
    },
}

/// Configuration for the file tailer.
#[derive(Clone, Debug)]
pub struct TailConfig {
    pub poll_interval_ms: u64,
    pub read_buf_size: usize,
    pub fingerprint_bytes: usize,
    pub start_from_end: bool,
    pub glob_rescan_interval_ms: u64,
    pub max_open_files: usize,
    pub per_file_read_budget_bytes: usize,
    /// Maximum number of consecutive immediate repolls after a budget-saturated read.
    pub adaptive_fast_polls_max: u8,
}

impl Default for TailConfig {
    fn default() -> Self {
        TailConfig {
            poll_interval_ms: 250,
            read_buf_size: 256 * 1024,
            fingerprint_bytes: 1024,
            start_from_end: true,
            glob_rescan_interval_ms: 5000,
            max_open_files: 1024,
            per_file_read_budget_bytes: 256 * 1024,
            adaptive_fast_polls_max: 8,
        }
    }
}

/// The file tailer. Watches one or more file paths and yields data as it appears.
pub struct FileTailer {
    pub(super) discovery: FileDiscovery,
    pub(super) reader: FileReader,
    config: TailConfig,
    last_poll: Instant,
    pub(super) consecutive_error_polls: u32,
    pub(super) error_backoff_until: Option<Instant>,
    adaptive_poll: AdaptivePollController,
    last_poll_signal: PollCadenceSignal,
    health: ComponentHealth,
    stats: std::sync::Arc<ComponentStats>,
}

fn watch_parent_for(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    if parent.as_os_str().is_empty() {
        Some(PathBuf::from("."))
    } else {
        Some(parent.to_path_buf())
    }
}

impl FileTailer {
    /// Create a tailer for explicit file paths.
    ///
    /// The provided `paths` are watched immediately. `TailConfig` controls the
    /// polling cadence, read buffer sizing, glob rescans, and related tailing
    /// behavior. The shared `ComponentStats` handle is updated with file
    /// transport diagnostics for this tailer.
    ///
    /// Returns an error when the underlying file watcher or initial file setup
    /// cannot be created.
    pub fn new(
        paths: &[PathBuf],
        mut config: TailConfig,
        stats: std::sync::Arc<ComponentStats>,
    ) -> io::Result<Self> {
        config.read_buf_size = config.read_buf_size.max(1);
        let (tx, rx) = crossbeam_channel::unbounded();
        let adaptive_fast_polls_max = config.adaptive_fast_polls_max;

        let mut watcher = notify::recommended_watcher(move |res| {
            let _ = tx.send(res);
        })
        .map_err(io::Error::other)?;

        let mut watched_dirs = HashSet::new();
        for path in paths {
            if let Some(parent) = watch_parent_for(path)
                && watched_dirs.insert(parent.clone())
            {
                use notify::Watcher;
                watcher
                    .watch(&parent, notify::RecursiveMode::NonRecursive)
                    .map_err(io::Error::other)?;
            }
        }

        let read_buf_size = config.read_buf_size;

        let mut tailer = FileTailer {
            discovery: FileDiscovery {
                watcher,
                watched_dirs,
                glob_patterns: Vec::new(),
                watch_paths: paths.to_vec(),
                fs_events: rx,
                last_glob_rescan: Instant::now(),
            },
            reader: FileReader {
                files: HashMap::new(),
                read_buf: vec![0u8; read_buf_size],
                evicted_offsets: HashMap::new(),
                scratch_paths: Vec::new(),
                last_read_had_data: false,
                last_read_hit_budget: false,
                config: config.clone(),
            },
            config,
            last_poll: Instant::now(),
            consecutive_error_polls: 0,
            error_backoff_until: None,
            adaptive_poll: AdaptivePollController::new(adaptive_fast_polls_max),
            last_poll_signal: PollCadenceSignal::default(),
            health: ComponentHealth::Healthy,
            stats,
        };

        for path in paths {
            if path.exists() {
                if let Err(e) = tailer
                    .reader
                    .open_file_at(path, tailer.config.start_from_end)
                {
                    tracing::warn!(path = %path.display(), error = %e, "tail.open_failed");
                }
            } else {
                tracing::warn!(path = %path.display(), "tail.file_not_found — pipeline will wait until file appears");
            }
        }

        Ok(tailer)
    }

    /// Create a tailer for glob patterns.
    ///
    /// The provided `patterns` are expanded immediately and then watched for
    /// future matches. `TailConfig` controls the polling cadence, read buffer
    /// sizing, glob rescans, and related tailing behavior. The shared
    /// `ComponentStats` handle is updated with file transport diagnostics for
    /// this tailer.
    ///
    /// Returns an error when the underlying file watcher or initial file setup
    /// cannot be created.
    pub fn new_with_globs(
        patterns: &[&str],
        config: TailConfig,
        stats: std::sync::Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let initial_paths: Vec<PathBuf> = expand_glob_patterns(patterns);

        if initial_paths.is_empty() {
            for pattern in patterns {
                tracing::warn!(
                    pattern,
                    "tail.glob_no_matches — pipeline will wait until matching files appear"
                );
            }
        }

        let mut tailer = Self::new(&initial_paths, config, stats)?;
        tailer.discovery.glob_patterns = patterns.iter().map(ToString::to_string).collect();
        Ok(tailer)
    }

    #[cfg(test)]
    pub(super) const MAX_READ_PER_POLL: usize = FileReader::MAX_READ_PER_POLL;

    pub fn poll(&mut self) -> io::Result<Vec<TailEvent>> {
        let mut events = Vec::new();
        let (something_changed, mut had_error) = self.discovery.drain_events();

        if let Some(until) = self.error_backoff_until
            && Instant::now() < until
        {
            self.last_poll_signal = PollCadenceSignal::default();
            if had_error {
                self.update_error_backoff(true);
            }
            return Ok(events);
        }

        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
        let glob_rescan_due = self.config.glob_rescan_interval_ms > 0
            && self.discovery.last_glob_rescan.elapsed()
                >= Duration::from_millis(self.config.glob_rescan_interval_ms);
        let adaptive_fast_poll = self.adaptive_poll.should_fast_poll();
        let should_poll = something_changed
            || self.last_poll.elapsed() >= poll_interval
            || glob_rescan_due
            || adaptive_fast_poll;

        if !should_poll {
            self.last_poll_signal = PollCadenceSignal::default();
            if had_error {
                self.update_error_backoff(true);
            }
            return Ok(events);
        }
        self.last_poll = Instant::now();

        if glob_rescan_due {
            had_error |= self.discovery.rescan_globs(&mut self.reader);
            self.discovery.last_glob_rescan = Instant::now();
        }

        had_error |= self.discovery.detect_changes(&mut self.reader, &mut events);
        had_error |= self.reader.read_all(&mut events);
        had_error |= self
            .discovery
            .cleanup_deleted(&mut self.reader, &mut events);

        self.last_poll_signal = PollCadenceSignal {
            had_data: self.reader.last_read_had_data,
            hit_read_budget: self.reader.last_read_hit_budget,
        };
        self.adaptive_poll.observe_signal(self.last_poll_signal);

        self.reader.evict_lru(self.config.max_open_files);
        self.update_error_backoff(had_error);

        Ok(events)
    }

    /// Perform a terminal poll during runtime shutdown.
    ///
    /// Normal idle EOF is intentionally delayed until sustained idle so a
    /// still-growing file does not flush partial records too early. Shutdown is
    /// terminal for the current runtime, so this bypasses the poll-interval and
    /// idle-window gates for files that are already tracked: read any available
    /// bytes once, then emit EOF for every active file so framing can flush
    /// no-newline remainders. Shutdown intentionally does not rescan globs,
    /// because graceful drain should not discover unrelated new files.
    pub fn poll_shutdown(&mut self) -> io::Result<Vec<TailEvent>> {
        let mut events = Vec::new();
        let (_, mut had_error) = self.discovery.drain_events();

        had_error |= self.discovery.detect_changes(&mut self.reader, &mut events);
        had_error |= self.reader.read_all(&mut events);
        suppress_source_less_shutdown_eof(&mut events);
        let (cleanup_had_error, mut eof_targets) = self
            .discovery
            .cleanup_deleted_for_shutdown(&mut self.reader, &mut events);
        had_error |= cleanup_had_error;
        eof_targets.extend(self.shutdown_eof_targets());

        for (path, source_id) in eof_targets {
            events.push(TailEvent::EndOfFile { path, source_id });
        }

        self.last_poll = Instant::now();
        self.last_poll_signal = PollCadenceSignal {
            had_data: self.reader.last_read_had_data,
            hit_read_budget: self.reader.last_read_hit_budget,
        };
        self.adaptive_poll.observe_signal(self.last_poll_signal);
        self.reader.evict_lru(self.config.max_open_files);
        self.update_error_backoff(had_error);

        Ok(events)
    }

    fn shutdown_eof_targets(&self) -> Vec<(PathBuf, Option<SourceId>)> {
        let active = self.reader.files.iter().filter_map(|(path, tailed)| {
            let file_size = tailed.file.metadata().ok()?.len();
            let already_emitted = tailed.eof_state.has_emitted();
            if already_emitted || !shutdown_should_emit_eof(tailed.offset, file_size) {
                return None;
            }
            let source_id = nonzero_source_id(tailed.identity.source_id())?;
            Some((path.clone(), Some(source_id)))
        });

        let evicted = self.reader.evicted_offsets.values().filter_map(|evicted| {
            let file_size = std::fs::metadata(&evicted.path).ok()?.len();
            let already_emitted = evicted.eof_state.has_emitted();
            // Evicted files are not reopened during shutdown, so offset > size
            // means a truncation happened while the file was evicted.  Do not
            // flush stale pre-truncation framed state in that case.
            if already_emitted || evicted.offset != file_size {
                return None;
            }
            let source_id = nonzero_source_id(evicted.source_id)?;
            Some((evicted.path.clone(), Some(source_id)))
        });

        active.chain(evicted).collect()
    }

    pub fn health(&self) -> ComponentHealth {
        self.health
    }

    fn update_error_backoff(&mut self, had_error: bool) {
        let (next_error_polls, backoff_ms) =
            backoff_transition(self.consecutive_error_polls, had_error);
        self.consecutive_error_polls = next_error_polls;
        self.stats.file_error_polls.store(
            self.consecutive_error_polls,
            std::sync::atomic::Ordering::Relaxed,
        );

        if let Some(backoff_ms) = backoff_ms {
            self.error_backoff_until = Some(Instant::now() + Duration::from_millis(backoff_ms));
            tracing::warn!(
                consecutive_error_polls = self.consecutive_error_polls,
                backoff_ms,
                "tail.poll_backoff_after_error"
            );
            self.health = reduce_polling_input_health(
                self.health,
                PollingInputHealthEvent::ErrorBackoffObserved,
            );
        } else {
            self.error_backoff_until = None;
            self.health =
                reduce_polling_input_health(self.health, PollingInputHealthEvent::PollHealthy);
        }
    }

    pub fn get_offset(&self, path: &Path) -> Option<u64> {
        self.reader.get_offset(path)
    }

    pub fn set_offset(&mut self, path: &Path, offset: u64) -> io::Result<()> {
        self.reader.set_offset(path, offset)
    }

    pub fn set_offset_by_source(&mut self, source_id: SourceId, offset: u64) -> io::Result<()> {
        self.reader.set_offset_by_source(source_id, offset)
    }

    pub fn num_files(&self) -> usize {
        self.reader.num_files()
    }

    pub fn source_id_for_path(&self, path: &Path) -> Option<SourceId> {
        self.reader.source_id_for_path(path)
    }

    /// Source feedback from the last poll, used to decide whether the next poll should fast-path.
    pub fn get_poll_cadence_signal(&self) -> PollCadenceSignal {
        self.last_poll_signal
    }

    /// Maximum number of immediate repolls allowed after a budget-saturated read.
    pub fn get_adaptive_fast_polls_max(&self) -> u8 {
        self.config.adaptive_fast_polls_max
    }

    #[cfg(test)]
    pub(super) fn force_poll_due(&mut self) {
        let elapsed_ms = self.config.poll_interval_ms.max(1);
        self.last_poll = Instant::now()
            .checked_sub(Duration::from_millis(elapsed_ms))
            .expect("poll interval before now should be representable");
    }

    #[cfg(test)]
    pub(super) fn adaptive_fast_polls_remaining(&self) -> u8 {
        self.adaptive_poll.get_fast_polls_remaining()
    }

    pub fn file_offsets(&self) -> Vec<(SourceId, ByteOffset)> {
        self.reader.file_offsets()
    }

    pub fn file_paths(&self) -> Vec<(SourceId, PathBuf)> {
        self.reader.file_paths()
    }
}

fn suppress_source_less_shutdown_eof(events: &mut Vec<TailEvent>) {
    events.retain(|event| {
        !matches!(
            event,
            TailEvent::EndOfFile {
                source_id: None,
                ..
            }
        )
    });
}

fn nonzero_source_id(source_id: SourceId) -> Option<SourceId> {
    (source_id != SourceId(0)).then_some(source_id)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::watch_parent_for;

    #[test]
    fn watch_parent_for_relative_file_uses_current_dir() {
        assert_eq!(
            watch_parent_for(Path::new("app.log")).as_deref(),
            Some(Path::new("."))
        );
    }

    #[test]
    fn watch_parent_for_nested_path_uses_parent() {
        assert_eq!(
            watch_parent_for(Path::new("logs/app.log")).as_deref(),
            Some(Path::new("logs"))
        );
    }
}
