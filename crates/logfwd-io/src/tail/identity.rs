use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
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
        let primary = source_digest(self.device, self.inode, self.fingerprint, 0);
        let fallback = source_digest(self.device, self.inode, self.fingerprint, 1);
        source_id_from_digests(primary, fallback)
    }
}

fn source_digest(device: u64, inode: u64, fingerprint: u64, seed: u64) -> u64 {
    let mut h = xxhash_rust::xxh64::Xxh64::new(seed);
    h.update(&device.to_le_bytes());
    h.update(&inode.to_le_bytes());
    h.update(&fingerprint.to_le_bytes());
    h.digest()
}

fn source_id_from_digests(primary: u64, fallback: u64) -> SourceId {
    // Reserve SourceId(0) as the empty-file sentinel.
    if primary != 0 {
        return SourceId(primary);
    }
    if fallback != 0 {
        return SourceId(fallback);
    }
    // Extremely unlikely: both seeds hashed to 0. Keep non-zero invariant.
    SourceId(1)
}

#[cfg(unix)]
fn metadata_device_inode(meta: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (meta.dev(), meta.ino())
}

#[cfg(not(unix))]
fn metadata_device_inode(_meta: &std::fs::Metadata) -> (u64, u64) {
    // Fallback for non-Unix builds. Windows can later switch to file-index based ids.
    (0, 0)
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
    let (device, inode) = metadata_device_inode(&meta);
    let fingerprint = compute_fingerprint(file, fingerprint_bytes)?;
    Ok(FileIdentity {
        device,
        inode,
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
    use super::source_id_from_digests;
    use logfwd_types::pipeline::SourceId;

    #[test]
    fn source_id_uses_primary_digest_when_nonzero() {
        assert_eq!(source_id_from_digests(42, 7), SourceId(42));
    }

    #[test]
    fn source_id_uses_fallback_digest_when_primary_is_zero() {
        assert_eq!(source_id_from_digests(0, 7), SourceId(7));
    }

    #[test]
    fn source_id_uses_nonzero_sentinel_when_both_digests_are_zero() {
        assert_eq!(source_id_from_digests(0, 0), SourceId(1));
    }
}
