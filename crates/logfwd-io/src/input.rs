use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use arrow::record_batch::RecordBatch;
use logfwd_types::diagnostics::ComponentHealth;
use logfwd_types::pipeline::SourceId;

use crate::filter_hints::FilterHints;
use crate::poll_cadence::PollCadenceSignal;
use crate::tail::{ByteOffset, FileTailer, TailConfig, TailEvent};

/// Snapshot of source-driven cadence hints consumed by runtime poll loops.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InputCadence {
    /// Last poll feedback (data/no-data and read-budget saturation).
    pub signal: PollCadenceSignal,
    /// Maximum immediate repolls allowed after a budget-saturated read.
    pub adaptive_fast_polls_max: u8,
}

/// Events produced by an input source.
pub enum InputEvent {
    /// New data read from the source.
    ///
    /// `source_id` identifies which logical source produced the data (e.g.,
    /// which tailed file). `None` for push sources that don't track identity.
    /// `accounted_bytes` is the source-side byte count that should be charged
    /// to input diagnostics for this event. For raw byte inputs this is
    /// usually `bytes.len()`. Wrappers like `FramedInput` may consume this for
    /// accounting and forward downstream data with `accounted_bytes = 0`.
    Data {
        bytes: Vec<u8>,
        source_id: Option<SourceId>,
        accounted_bytes: u64,
    },
    /// New structured rows produced directly by the source.
    ///
    /// `accounted_bytes` is the source-side byte count represented by this
    /// batch for diagnostics. Structured receivers should set this from their
    /// accepted payload size rather than relying on Arrow memory-size proxies.
    Batch {
        batch: RecordBatch,
        source_id: Option<SourceId>,
        accounted_bytes: u64,
    },
    /// The underlying file was rotated (new inode).
    ///
    /// `source_id` identifies the source that was rotated. `None` for push
    /// sources or when the source identity is unknown.
    Rotated { source_id: Option<SourceId> },
    /// The underlying file was truncated.
    ///
    /// `source_id` identifies the source that was truncated. `None` for push
    /// sources or when the source identity is unknown.
    Truncated { source_id: Option<SourceId> },
    /// The source has been fully consumed and has no new data.
    ///
    /// Emitted once per "caught-up" transition (i.e., after the first poll
    /// cycle where reads find no new bytes).  Downstream components (e.g.
    /// `FramedInput`) use this to flush any partial-line remainder that was
    /// not terminated by a newline — a common situation for static files and
    /// for the last line written before a log rotation.
    ///
    /// `source_id` identifies the source that reached EOF. `None` for push
    /// sources or when the source identity is unknown.
    EndOfFile { source_id: Option<SourceId> },
}

/// Trait for input sources that produce raw bytes.
#[derive(Debug, Clone)]
pub struct TlsInputConfig {
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub client_ca_file: Option<String>,
    pub require_client_auth: bool,
}

pub trait InputSource: Send {
    /// Poll for new events. Returns empty vec if no new data.
    fn poll(&mut self) -> io::Result<Vec<InputEvent>>;

    /// Poll during runtime shutdown before buffered input is drained.
    ///
    /// Inputs with shutdown-specific terminal semantics should use this hook to
    /// make any source-internal buffered data observable to downstream framing
    /// before the runtime stops polling. The default is a no-op so ordinary
    /// inputs do not perform opportunistic reads during shutdown.
    ///
    /// If an implementation returns payload without EOF, the runtime will keep
    /// polling shutdown until the source emits EOF, reports finished, returns no
    /// events, or errors. Implementations that can observe unbounded live data
    /// should snapshot their terminal drain boundary internally rather than
    /// returning payload forever; the runtime only keeps an emergency hard cap
    /// to avoid hanging process shutdown on a misbehaving source.
    fn poll_shutdown(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(Vec::new())
    }

    /// Name of this input (from config).
    fn name(&self) -> &str;

    /// Coarse runtime health for readiness and diagnostics.
    fn health(&self) -> ComponentHealth;

    /// Whether the source has reached a terminal end-of-input state.
    ///
    /// Long-running inputs return `false`. Finite inputs such as stdin return
    /// `true` after emitting EOF so the runtime can drain and exit cleanly.
    fn is_finished(&self) -> bool {
        false
    }

    /// Apply filter hints for predicate pushdown. Inputs that support
    /// pushdown use these to skip data early (e.g., XDP severity filtering).
    /// Default implementation ignores hints — correct but slower.
    fn apply_hints(&mut self, _hints: &FilterHints) {}

    /// Source-specific polling feedback for adaptive cadence decisions.
    ///
    /// Default: no payload and no read-budget saturation signal.
    fn get_poll_cadence_signal(&self) -> PollCadenceSignal {
        PollCadenceSignal::default()
    }

    /// Upper bound for immediate repolls after a budget-saturated read.
    ///
    /// Default: disabled (0), so non-file inputs keep existing cadence.
    fn get_adaptive_fast_polls_max(&self) -> u8 {
        0
    }

    /// Snapshot cadence hints in one call to minimize forwarding surface for wrappers.
    ///
    /// Default implementation composes existing cadence hooks for compatibility.
    fn get_cadence(&self) -> InputCadence {
        InputCadence {
            signal: self.get_poll_cadence_signal(),
            adaptive_fast_polls_max: self.get_adaptive_fast_polls_max(),
        }
    }

    /// Return checkpoint data for all active sources.
    ///
    /// For file inputs, returns `(SourceId, ByteOffset)` per tailed file.
    /// Default: empty (push sources, generators).
    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        vec![]
    }

    /// Return source-id to canonical-path mappings for active file-backed sources.
    ///
    /// Default: empty (push sources, generators).
    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        vec![]
    }

    /// Return whether [`crate::framed::FramedInput`] may reclaim completed
    /// per-source decoder state immediately after a data event.
    ///
    /// Push-style sources should keep the default so high-cardinality source
    /// identities, such as UDP senders, do not accumulate idle state. File
    /// tailers override this to keep reusable buffers and checkpoint trackers
    /// across steady-state polls.
    fn should_reclaim_completed_source_state(&self) -> bool {
        true
    }

    /// Restore a file offset by SourceId (fingerprint). Default: no-op.
    ///
    /// Used for checkpoint restore — the checkpoint stores fingerprint + offset.
    /// The input source finds the matching file by fingerprint, not path.
    fn set_offset_by_source(&mut self, _source_id: SourceId, _offset: u64) {}
}

enum StdinMessage {
    Data(Vec<u8>),
    EndOfFile,
    Error(io::ErrorKind, String),
}

struct StdinPollOutcome {
    events: Vec<InputEvent>,
    is_drained: bool,
}

const STDIN_CHANNEL_BOUND: usize = 16;
const STDIN_READ_BUF_SIZE: usize = 64 * 1024;
const STDIN_MAX_EVENTS_PER_POLL: usize = STDIN_CHANNEL_BOUND;
const STDIN_MAX_SHUTDOWN_EVENTS: usize = STDIN_CHANNEL_BOUND;

/// Finite stdin source for command-line ingestion.
pub struct StdinInput {
    name: String,
    rx: mpsc::Receiver<StdinMessage>,
    is_finished: bool,
    pending_error: Option<(io::ErrorKind, String)>,
}

impl StdinInput {
    /// Spawn a background reader that forwards stdin chunks to the poll loop.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let (tx, rx) = mpsc::sync_channel(STDIN_CHANNEL_BOUND);
        let thread_tx = tx.clone();
        if let Err(err) = thread::Builder::new()
            .name("logfwd-stdin-reader".to_owned())
            .spawn(move || {
                let stdin = io::stdin();
                let mut stdin = stdin.lock();
                let mut buf = vec![0; STDIN_READ_BUF_SIZE];
                loop {
                    match stdin.read(&mut buf) {
                        Ok(0) => {
                            let _ = thread_tx.send(StdinMessage::EndOfFile);
                            break;
                        }
                        Ok(n) => {
                            if thread_tx
                                .send(StdinMessage::Data(buf[..n].to_vec()))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                        Err(err) => {
                            let kind = err.kind();
                            let message = err.to_string();
                            let _ = thread_tx.send(StdinMessage::Error(kind, message));
                            break;
                        }
                    }
                }
            })
        {
            let _ = tx.send(StdinMessage::Error(err.kind(), err.to_string()));
        }

        Self {
            name,
            rx,
            is_finished: false,
            pending_error: None,
        }
    }
}

impl InputSource for StdinInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(self
            .poll_with_event_limit(STDIN_MAX_EVENTS_PER_POLL, false)?
            .events)
    }

    fn poll_shutdown(&mut self) -> io::Result<Vec<InputEvent>> {
        let outcome = self.poll_with_event_limit(STDIN_MAX_SHUTDOWN_EVENTS, true)?;
        let mut events = outcome.events;
        if self.pending_error.is_none()
            && outcome.is_drained
            && !events
                .iter()
                .any(|event| matches!(event, InputEvent::EndOfFile { .. }))
        {
            self.is_finished = true;
            events.push(InputEvent::EndOfFile { source_id: None });
        }
        Ok(events)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        if self.is_finished {
            ComponentHealth::Stopped
        } else {
            ComponentHealth::Healthy
        }
    }

    fn is_finished(&self) -> bool {
        self.is_finished
    }
}

impl StdinInput {
    fn poll_with_event_limit(
        &mut self,
        max_events: usize,
        should_probe_after_limit_data: bool,
    ) -> io::Result<StdinPollOutcome> {
        if self.is_finished {
            return Ok(StdinPollOutcome {
                events: Vec::new(),
                is_drained: true,
            });
        }
        if let Some((kind, message)) = self.pending_error.take() {
            self.is_finished = true;
            return Err(io::Error::new(kind, message));
        }

        let mut events = Vec::new();
        let mut is_drained = false;
        while events.len() < max_events {
            match self.rx.try_recv() {
                Ok(StdinMessage::Data(bytes)) => {
                    let accounted_bytes = bytes.len() as u64;
                    events.push(InputEvent::Data {
                        bytes,
                        source_id: None,
                        accounted_bytes,
                    });
                }
                Ok(StdinMessage::EndOfFile) => {
                    self.is_finished = true;
                    is_drained = true;
                    events.push(InputEvent::EndOfFile { source_id: None });
                    break;
                }
                Ok(StdinMessage::Error(kind, message)) => {
                    if !events.is_empty() {
                        self.pending_error = Some((kind, message));
                        break;
                    }
                    self.is_finished = true;
                    return Err(io::Error::new(kind, message));
                }
                Err(mpsc::TryRecvError::Empty) => {
                    is_drained = true;
                    break;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.is_finished = true;
                    is_drained = true;
                    events.push(InputEvent::EndOfFile { source_id: None });
                    break;
                }
            }
        }
        if !is_drained && max_events != 0 && events.len() == max_events {
            match self.rx.try_recv() {
                Ok(StdinMessage::Data(bytes)) => {
                    let accounted_bytes = bytes.len() as u64;
                    events.push(InputEvent::Data {
                        bytes,
                        source_id: None,
                        accounted_bytes,
                    });
                    if should_probe_after_limit_data {
                        is_drained = self.probe_stdin_terminal_after_data(&mut events);
                    }
                }
                Ok(StdinMessage::EndOfFile) => {
                    self.is_finished = true;
                    is_drained = true;
                    events.push(InputEvent::EndOfFile { source_id: None });
                }
                Ok(StdinMessage::Error(kind, message)) => {
                    self.pending_error = Some((kind, message));
                }
                Err(mpsc::TryRecvError::Empty) => {
                    is_drained = true;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.is_finished = true;
                    is_drained = true;
                    events.push(InputEvent::EndOfFile { source_id: None });
                }
            }
        }

        Ok(StdinPollOutcome { events, is_drained })
    }

    fn probe_stdin_terminal_after_data(&mut self, events: &mut Vec<InputEvent>) -> bool {
        match self.rx.try_recv() {
            Ok(StdinMessage::Data(bytes)) => {
                let accounted_bytes = bytes.len() as u64;
                events.push(InputEvent::Data {
                    bytes,
                    source_id: None,
                    accounted_bytes,
                });
                // After consuming probe data, check once more whether the
                // channel is terminal so we don't suppress EOF when this was
                // the final chunk.
                self.is_stdin_terminal(events)
            }
            Ok(StdinMessage::EndOfFile) => {
                self.is_finished = true;
                events.push(InputEvent::EndOfFile { source_id: None });
                true
            }
            Ok(StdinMessage::Error(kind, message)) => {
                self.pending_error = Some((kind, message));
                false
            }
            Err(mpsc::TryRecvError::Empty) => true,
            Err(mpsc::TryRecvError::Disconnected) => {
                self.is_finished = true;
                events.push(InputEvent::EndOfFile { source_id: None });
                true
            }
        }
    }

    /// Single `try_recv` that checks for a terminal state without consuming
    /// additional data.  If the next message is `Data`, it is pushed to
    /// `events` but the channel is **not** probed further — the caller should
    /// treat the result as "not drained" because more items may remain.
    fn is_stdin_terminal(&mut self, events: &mut Vec<InputEvent>) -> bool {
        match self.rx.try_recv() {
            Ok(StdinMessage::Data(bytes)) => {
                let accounted_bytes = bytes.len() as u64;
                events.push(InputEvent::Data {
                    bytes,
                    source_id: None,
                    accounted_bytes,
                });
                false
            }
            Ok(StdinMessage::EndOfFile) => {
                self.is_finished = true;
                events.push(InputEvent::EndOfFile { source_id: None });
                true
            }
            Ok(StdinMessage::Error(kind, message)) => {
                self.pending_error = Some((kind, message));
                false
            }
            Err(mpsc::TryRecvError::Empty) => true,
            Err(mpsc::TryRecvError::Disconnected) => {
                self.is_finished = true;
                events.push(InputEvent::EndOfFile { source_id: None });
                true
            }
        }
    }
}

/// An input source backed by a `FileTailer`.
pub struct FileInput {
    name: String,
    tailer: FileTailer,
}

impl FileInput {
    /// Create a new `FileInput` wrapping a `FileTailer`.
    pub fn new(
        name: String,
        paths: &[PathBuf],
        config: TailConfig,
        stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        let tailer = FileTailer::new(paths, config, stats)?;
        Ok(FileInput { name, tailer })
    }

    /// Create a new `FileInput` from glob patterns.
    ///
    /// Patterns are expanded immediately and re-evaluated periodically to
    /// discover files created after startup (e.g., new Kubernetes pods).
    pub fn new_with_globs(
        name: String,
        patterns: &[&str],
        config: TailConfig,
        stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        let tailer = FileTailer::new_with_globs(patterns, config, stats)?;
        Ok(FileInput { name, tailer })
    }
}

impl InputSource for FileInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(tail_events_to_input_events(self.tailer.poll()?))
    }

    fn poll_shutdown(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(tail_events_to_input_events(self.tailer.poll_shutdown()?))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        self.tailer.health()
    }

    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        self.tailer.file_offsets()
    }

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        self.tailer.file_paths()
    }

    fn should_reclaim_completed_source_state(&self) -> bool {
        false
    }

    fn set_offset_by_source(&mut self, source_id: SourceId, offset: u64) {
        if let Err(e) = self.tailer.set_offset_by_source(source_id, offset) {
            tracing::warn!(source_id = source_id.0, error = %e, "failed to restore offset");
        }
    }

    fn get_poll_cadence_signal(&self) -> PollCadenceSignal {
        self.tailer.get_poll_cadence_signal()
    }

    fn get_adaptive_fast_polls_max(&self) -> u8 {
        self.tailer.get_adaptive_fast_polls_max()
    }
}

fn tail_events_to_input_events(tail_events: Vec<TailEvent>) -> Vec<InputEvent> {
    // source_id is embedded in each TailEvent at the time of creation
    // inside FileTailer::poll(), before any rotation mutates internal
    // state. No snapshot HashMap is needed here.
    let mut events = Vec::with_capacity(tail_events.len());
    for te in tail_events {
        match te {
            TailEvent::Data {
                bytes, source_id, ..
            } => {
                let accounted_bytes = bytes.len() as u64;
                events.push(InputEvent::Data {
                    bytes,
                    source_id,
                    accounted_bytes,
                });
            }
            TailEvent::Rotated { source_id, .. } => {
                events.push(InputEvent::Rotated { source_id });
            }
            TailEvent::Truncated { source_id, .. } => {
                events.push(InputEvent::Truncated { source_id });
            }
            TailEvent::EndOfFile { source_id, .. } => {
                events.push(InputEvent::EndOfFile { source_id });
            }
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stdin_from_messages(messages: Vec<StdinMessage>) -> StdinInput {
        let (tx, rx) = mpsc::sync_channel(messages.len().max(1));
        for message in messages {
            tx.send(message).expect("send test stdin message");
        }
        drop(tx);
        StdinInput {
            name: "stdin".to_owned(),
            rx,
            is_finished: false,
            pending_error: None,
        }
    }

    fn stdin_from_open_channel(
        messages: Vec<StdinMessage>,
    ) -> (StdinInput, mpsc::SyncSender<StdinMessage>) {
        let (tx, rx) = mpsc::sync_channel(messages.len().max(1));
        for message in messages {
            tx.send(message).expect("send test stdin message");
        }
        (
            StdinInput {
                name: "stdin".to_owned(),
                rx,
                is_finished: false,
                pending_error: None,
            },
            tx,
        )
    }

    fn count_stdin_shutdown_events(events: &[InputEvent]) -> (usize, usize) {
        let data_events = events
            .iter()
            .filter(|event| matches!(event, InputEvent::Data { .. }))
            .count();
        let eof_events = events
            .iter()
            .filter(|event| matches!(event, InputEvent::EndOfFile { source_id: None }))
            .count();
        (data_events, eof_events)
    }

    #[test]
    fn stdin_poll_defers_error_until_consumed_data_is_returned() {
        let mut input = stdin_from_messages(vec![
            StdinMessage::Data(b"first\n".to_vec()),
            StdinMessage::Error(io::ErrorKind::BrokenPipe, "stdin failed".to_owned()),
        ]);

        let events = input.poll().expect("data should be returned before error");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], InputEvent::Data { .. }));
        assert!(!input.is_finished());

        let err = match input.poll() {
            Ok(_) => panic!("pending read error should surface"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert!(input.is_finished());
    }

    #[test]
    fn stdin_poll_error_without_data_finishes_immediately() {
        let mut input = stdin_from_messages(vec![StdinMessage::Error(
            io::ErrorKind::UnexpectedEof,
            "stdin failed".to_owned(),
        )]);

        let err = match input.poll() {
            Ok(_) => panic!("read error should surface"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(input.is_finished());
    }

    #[test]
    fn stdin_poll_shutdown_drains_exact_limit_and_emits_eof() {
        let messages = (0..STDIN_MAX_SHUTDOWN_EVENTS)
            .map(|i| StdinMessage::Data(format!("chunk-{i}\n").into_bytes()))
            .collect();
        let (mut input, _tx) = stdin_from_open_channel(messages);

        let events = input.poll_shutdown().expect("shutdown poll should drain");
        let (data_events, eof_events) = count_stdin_shutdown_events(&events);

        assert_eq!(data_events, STDIN_MAX_SHUTDOWN_EVENTS);
        assert_eq!(eof_events, 1);
        assert!(input.is_finished());
    }

    #[test]
    fn stdin_poll_shutdown_drains_post_limit_probe_data_and_emits_eof() {
        let messages = (0..=STDIN_MAX_SHUTDOWN_EVENTS)
            .map(|i| StdinMessage::Data(format!("chunk-{i}\n").into_bytes()))
            .collect();
        let (mut input, _tx) = stdin_from_open_channel(messages);

        let events = input.poll_shutdown().expect("shutdown poll should drain");
        let (data_events, eof_events) = count_stdin_shutdown_events(&events);

        assert_eq!(data_events, STDIN_MAX_SHUTDOWN_EVENTS + 1);
        assert_eq!(eof_events, 1);
        assert!(input.is_finished());
    }

    #[test]
    fn stdin_poll_shutdown_does_not_emit_eof_when_drain_limit_is_hit() {
        let messages = (0..(STDIN_MAX_SHUTDOWN_EVENTS + 4))
            .map(|i| StdinMessage::Data(format!("chunk-{i}\n").into_bytes()))
            .collect();
        let (mut input, _tx) = stdin_from_open_channel(messages);

        let events = input
            .poll_shutdown()
            .expect("shutdown poll should return bounded events");
        let (data_events, eof_events) = count_stdin_shutdown_events(&events);

        assert_eq!(data_events, STDIN_MAX_SHUTDOWN_EVENTS + 3);
        assert_eq!(eof_events, 0);
        assert!(!input.is_finished());
    }

    #[test]
    fn stdin_poll_shutdown_emits_eof_when_post_limit_probe_data_is_final_chunk() {
        // Regression: after draining STDIN_MAX_SHUTDOWN_EVENTS chunks, if the
        // post-limit probe consumes Data that happens to be the last chunk
        // before the channel closes, the follow-up terminal check must detect
        // the closed channel and set is_drained so EOF is emitted.
        let messages = (0..(STDIN_MAX_SHUTDOWN_EVENTS + 2))
            .map(|i| StdinMessage::Data(format!("chunk-{i}\n").into_bytes()))
            .collect();
        let mut input = stdin_from_messages(messages);

        let events = input.poll_shutdown().expect("shutdown poll should drain");
        let (data_events, eof_events) = count_stdin_shutdown_events(&events);

        assert_eq!(data_events, STDIN_MAX_SHUTDOWN_EVENTS + 2);
        assert_eq!(eof_events, 1);
        assert!(input.is_finished());
    }

    #[test]
    fn stdin_poll_shutdown_emits_eof_for_exactly_max_plus_one_chunks() {
        // Edge case: exactly STDIN_MAX_SHUTDOWN_EVENTS + 1 data chunks with a
        // disconnected channel. The main loop drains MAX chunks, the post-limit
        // probe consumes the (MAX+1)th, and the follow-up terminal check must
        // detect the disconnected channel and emit synthetic EOF.
        let messages = (0..(STDIN_MAX_SHUTDOWN_EVENTS + 1))
            .map(|i| StdinMessage::Data(format!("chunk-{i}\n").into_bytes()))
            .collect();
        let mut input = stdin_from_messages(messages);

        let events = input.poll_shutdown().expect("shutdown poll should drain");
        let (data_events, eof_events) = count_stdin_shutdown_events(&events);

        assert_eq!(data_events, STDIN_MAX_SHUTDOWN_EVENTS + 1);
        assert_eq!(eof_events, 1);
        assert!(input.is_finished());
    }
}
