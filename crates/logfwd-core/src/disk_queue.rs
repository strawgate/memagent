//! Disk-based Arrow batch queue for multi-core pipeline mode.
//!
//! Provides a simple, directory-based FIFO queue:
//! - [`DiskQueueWriter`] serializes RecordBatches as compressed Arrow IPC
//!   files in `{dir}/{seq:020}.lfarrow`.
//! - [`DiskQueueReader`] reads files in ascending sequence order, then
//!   deletes each file after successful deserialization.
//!
//! Writes are atomic on POSIX systems: data is written to a `.tmp` file and
//! `rename`d to its final name, so readers never observe partial writes.
//!
//! The on-disk format is the existing `compress.rs` wire format (16-byte
//! header + zstd payload) wrapping Arrow IPC stream bytes.  This means
//! the files are self-describing: schema is embedded, and corruption is
//! detected by the xxhash32 checksum in the header.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;

use crate::compress::{ChunkCompressor, decompress_chunk};
use crate::ipc::{batch_to_ipc, ipc_to_batch};

const FILE_EXT: &str = "lfarrow";

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Writes compressed Arrow IPC batches to a disk queue directory.
///
/// Each call to [`push`](DiskQueueWriter::push) serializes a batch, compresses
/// it, and atomically writes it as `{seq:020}.lfarrow` in the queue directory.
///
/// The writer can be re-opened after a restart; it scans for the highest
/// existing sequence number and resumes from there.
pub struct DiskQueueWriter {
    dir: PathBuf,
    seq: u64,
    compressor: ChunkCompressor,
    /// Total-size cap for the queue directory (0 = unlimited).
    max_bytes: u64,
}

impl DiskQueueWriter {
    /// Open (or create) a disk queue at `dir`.
    ///
    /// `max_bytes` limits the total size of all queue files; 0 means unlimited.
    pub fn new(dir: impl AsRef<Path>, max_bytes: u64) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let seq = highest_seq(&dir).map(|n| n + 1).unwrap_or(0);
        Ok(DiskQueueWriter {
            dir,
            seq,
            compressor: ChunkCompressor::new(1),
            max_bytes,
        })
    }

    /// Write a RecordBatch to the queue.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] if the queue is at or above
    /// `max_bytes`.  The caller should back off and retry.
    pub fn push(&mut self, batch: &RecordBatch) -> io::Result<()> {
        if self.max_bytes > 0 {
            let used = dir_size(&self.dir)?;
            if used >= self.max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "disk queue full: {} bytes used, limit {} bytes",
                        used, self.max_bytes
                    ),
                ));
            }
        }

        let ipc = batch_to_ipc(batch)?;
        let chunk = self.compressor.compress(&ipc)?;

        let seq = self.seq;
        self.seq += 1;

        let tmp_path = self.dir.join(format!("{seq:020}.tmp"));
        let final_path = self.dir.join(format!("{seq:020}.{FILE_EXT}"));

        fs::write(&tmp_path, &chunk.data)?;
        fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Reads compressed Arrow IPC batches from a disk queue directory.
///
/// Files are processed in ascending sequence order (lowest first).
/// Each successfully deserialized file is deleted from disk.
pub struct DiskQueueReader {
    dir: PathBuf,
}

impl DiskQueueReader {
    /// Open (or create) a disk queue reader at `dir`.
    pub fn new(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(DiskQueueReader { dir })
    }

    /// Pop the next batch from the queue.
    ///
    /// Returns `None` if the queue is currently empty.
    /// The file is deleted after successful deserialization.
    pub fn pop(&self) -> io::Result<Option<RecordBatch>> {
        let Some(path) = next_queue_file(&self.dir)? else {
            return Ok(None);
        };
        let data = fs::read(&path)?;
        let ipc = decompress_chunk(&data)?;
        let batch = ipc_to_batch(&ipc)?;
        fs::remove_file(&path)?;
        Ok(Some(batch))
    }

    /// Returns the number of pending batch files currently in the queue.
    pub fn pending(&self) -> io::Result<usize> {
        Ok(queue_files(&self.dir)?.len())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// List all `.lfarrow` files in `dir`, sorted ascending by name.
fn queue_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some(FILE_EXT))
        .collect();
    // Zero-padded names sort lexicographically == numerically.
    files.sort();
    Ok(files)
}

/// Return the path of the lowest-sequence file in the queue, if any.
fn next_queue_file(dir: &Path) -> io::Result<Option<PathBuf>> {
    Ok(queue_files(dir)?.into_iter().next())
}

/// Return the highest sequence number currently on disk, if any.
fn highest_seq(dir: &Path) -> Option<u64> {
    queue_files(dir)
        .ok()?
        .into_iter()
        .last()
        .and_then(|p| p.file_stem()?.to_str()?.parse::<u64>().ok())
}

/// Sum of all file sizes in `dir` (shallow, not recursive).
fn dir_size(dir: &Path) -> io::Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(dir)? {
        total += entry?.metadata()?.len();
    }
    Ok(total)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level_str", DataType::Utf8, true),
            Field::new("seq_int", DataType::Int64, true),
        ]));
        let levels: Vec<&str> = (0..n).map(|_| "INFO").collect();
        let seqs: Vec<i64> = (0..n as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(levels)),
                Arc::new(Int64Array::from(seqs)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn push_pop_roundtrip() {
        let dir = tempdir().unwrap();
        let mut writer = DiskQueueWriter::new(dir.path(), 0).unwrap();
        let reader = DiskQueueReader::new(dir.path()).unwrap();

        writer.push(&make_batch(5)).unwrap();
        let recovered = reader.pop().unwrap().expect("should have a batch");
        assert_eq!(recovered.num_rows(), 5);
        assert_eq!(reader.pop().unwrap(), None);
    }

    #[test]
    fn fifo_order() {
        let dir = tempdir().unwrap();
        let mut writer = DiskQueueWriter::new(dir.path(), 0).unwrap();
        let reader = DiskQueueReader::new(dir.path()).unwrap();

        for n in [3usize, 5, 7] {
            writer.push(&make_batch(n)).unwrap();
        }
        assert_eq!(reader.pop().unwrap().unwrap().num_rows(), 3);
        assert_eq!(reader.pop().unwrap().unwrap().num_rows(), 5);
        assert_eq!(reader.pop().unwrap().unwrap().num_rows(), 7);
        assert_eq!(reader.pop().unwrap(), None);
    }

    #[test]
    fn empty_queue_returns_none() {
        let dir = tempdir().unwrap();
        let reader = DiskQueueReader::new(dir.path()).unwrap();
        assert_eq!(reader.pop().unwrap(), None);
        assert_eq!(reader.pending().unwrap(), 0);
    }

    #[test]
    fn max_bytes_enforced() {
        let dir = tempdir().unwrap();
        // Limit of 1 byte — first write sees an empty dir so it succeeds,
        // but the second write finds the dir non-empty and is rejected.
        let mut writer = DiskQueueWriter::new(dir.path(), 1).unwrap();
        writer.push(&make_batch(10)).unwrap();
        let err = writer.push(&make_batch(10)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn resume_sequence_after_restart() {
        let dir = tempdir().unwrap();

        // Write 3 batches.
        {
            let mut w = DiskQueueWriter::new(dir.path(), 0).unwrap();
            for _ in 0..3 {
                w.push(&make_batch(1)).unwrap();
            }
        }

        // Pop 1 batch (seq 0 consumed).
        let reader = DiskQueueReader::new(dir.path()).unwrap();
        reader.pop().unwrap().unwrap();

        // Reopen writer — should resume from seq 3.
        let mut w2 = DiskQueueWriter::new(dir.path(), 0).unwrap();
        w2.push(&make_batch(99)).unwrap();

        // Drain: seqs 1, 2, 3.
        assert_eq!(reader.pop().unwrap().unwrap().num_rows(), 1);
        assert_eq!(reader.pop().unwrap().unwrap().num_rows(), 1);
        assert_eq!(reader.pop().unwrap().unwrap().num_rows(), 99);
        assert_eq!(reader.pop().unwrap(), None);
    }

    #[test]
    fn pending_count() {
        let dir = tempdir().unwrap();
        let mut writer = DiskQueueWriter::new(dir.path(), 0).unwrap();
        let reader = DiskQueueReader::new(dir.path()).unwrap();

        assert_eq!(reader.pending().unwrap(), 0);
        writer.push(&make_batch(1)).unwrap();
        assert_eq!(reader.pending().unwrap(), 1);
        writer.push(&make_batch(1)).unwrap();
        assert_eq!(reader.pending().unwrap(), 2);
        reader.pop().unwrap();
        assert_eq!(reader.pending().unwrap(), 1);
    }
}
