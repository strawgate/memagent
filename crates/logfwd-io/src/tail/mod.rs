//! File tailer: watches log files for new data and reads it as it arrives.
//!
//! Design decisions (from research):
//! - Watch the DIRECTORY, not individual files (editors do atomic save via rename)
//! - Use notify crate (kqueue on macOS, inotify on Linux) as a latency hint
//! - Poll as safety net (inotify misses events on NFS, overlayfs, queue overflow)
//! - Track files by (device, inode, fingerprint) to handle rotation and inode reuse
//! - Checkpoint offsets for crash recovery (atomic write via rename)
//!
//! The tailer yields raw byte chunks to the pipeline. It does NOT parse lines —
//! that's the pipeline's job.

mod discovery;
mod glob;
mod identity;
mod lifecycle;
mod reader;
mod state;
mod tailer;

pub use identity::ByteOffset;
pub use tailer::{FileTailer, TailConfig, TailEvent};

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod verification;
