use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use logfwd_types::pipeline::SourceId;

/// Byte offset within a file. Newtype prevents mixing with SourceId.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteOffset(pub u64);

/// Identity of a file based on device + inode + content fingerprint.
/// Survives renames. Detects inode reuse via fingerprint mismatch.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
    pub fingerprint: u64,
}

impl FileIdentity {
    /// Derive a stable `SourceId` from the compound key (device, inode, fingerprint).
    ///
    /// Hashing all three fields prevents collisions between files that share
    /// the same first N bytes (same fingerprint) but live on different inodes.
    /// Two files on the same inode+device are the same file, so the fingerprint
    /// differentiates after inode reuse (e.g., log rotation).
    pub fn source_id(&self) -> SourceId {
        // Empty file sentinel: fingerprint 0 means no data to checkpoint.
        if self.fingerprint == 0 {
            return SourceId(0);
        }
        let mut h = xxhash_rust::xxh64::Xxh64::new(0);
        h.update(&self.device.to_le_bytes());
        h.update(&self.inode.to_le_bytes());
        h.update(&self.fingerprint.to_le_bytes());
        source_id_from_digest(h.digest())
    }
}

fn source_id_from_digest(digest: u64) -> SourceId {
    // Reserve SourceId(0) as the empty-file sentinel.
    if digest == 0 {
        SourceId(1)
    } else {
        SourceId(digest)
    }
}

/// Compute the fingerprint of a file: xxhash64 of the first N bytes.
pub(super) fn compute_fingerprint(file: &mut File, max_bytes: usize) -> io::Result<u64> {
    let pos = file.stream_position()?;
    file.seek(SeekFrom::Start(0))?;

    let mut buf = vec![0u8; max_bytes];
    let n = file.read(&mut buf)?;

    file.seek(SeekFrom::Start(pos))?;

    if n == 0 {
        // Empty file gets a sentinel fingerprint.
        return Ok(0);
    }
    Ok(xxhash_rust::xxh64::xxh64(&buf[..n], 0))
}

/// Build a FileIdentity from an already-open file handle.
pub(super) fn identify_open_file(
    file: &mut File,
    fingerprint_bytes: usize,
) -> io::Result<FileIdentity> {
    let meta = file.metadata()?;
    let fingerprint = compute_fingerprint(file, fingerprint_bytes)?;
    Ok(FileIdentity {
        device: meta.dev(),
        inode: meta.ino(),
        fingerprint,
    })
}

/// Build a FileIdentity for a path.
pub(super) fn identify_file(path: &Path, fingerprint_bytes: usize) -> io::Result<FileIdentity> {
    let mut file = File::open(path)?;
    identify_open_file(&mut file, fingerprint_bytes)
}

#[cfg(test)]
mod tests {
    use super::source_id_from_digest;
    use logfwd_types::pipeline::SourceId;

    #[test]
    fn source_id_digest_zero_is_remapped() {
        assert_eq!(source_id_from_digest(0), SourceId(1));
    }

    #[test]
    fn source_id_digest_nonzero_is_preserved() {
        assert_eq!(source_id_from_digest(42), SourceId(42));
    }
}
