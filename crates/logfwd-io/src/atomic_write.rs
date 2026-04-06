//! Atomic file write via write-tmp-fsync-rename-fsync pattern.
//!
//! Ensures durability across crashes on Linux/ext4 and macOS:
//! 1. Write data to `<path>.tmp`
//! 2. `fsync` the tmp file (data durable)
//! 3. `rename` tmp → final (atomic pointer swap)
//! 4. `fsync` the parent directory (rename entry durable)

use std::fs;
use std::io::{self, Write};
use std::path::Path;

/// Atomically write `data` to `path` using a temporary file + rename.
///
/// On success the file at `path` contains exactly `data` and both the
/// file contents and the directory entry are durable on disk. If the
/// process crashes at any point, `path` either contains the old data
/// (or doesn't exist yet) — never a partial write.
pub fn atomic_write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    // Append `.tmp` to the full filename — do NOT use `with_extension` which
    // would *replace* the last extension (e.g., `foo.json` → `foo.tmp`).
    let mut tmp_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no filename"))?
        .to_os_string();
    tmp_name.push(".tmp");
    let tmp_path = path.with_file_name(tmp_name);

    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(data)?;
        file.sync_all()?;
    }

    fs::rename(&tmp_path, path)?;

    // fsync the parent directory so the rename entry is durable.
    // On ext4, rename metadata lives in the directory — without this
    // fsync a power failure can revert the rename.
    //
    // `path.parent()` returns `Some("")` for bare filenames like `"file.txt"`,
    // which would fail to open. Skip the sync in that case — a bare filename
    // means we're in the current directory, which is unusual for atomic writes.
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let dir = fs::File::open(parent)?;
        dir.sync_data()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.json");

        atomic_write_file(&path, b"hello").unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "hello");
    }

    #[test]
    fn atomic_write_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.json");

        atomic_write_file(&path, b"first").unwrap();
        atomic_write_file(&path, b"second").unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "second");
    }

    #[test]
    fn atomic_write_no_leftover_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.json");

        atomic_write_file(&path, b"data").unwrap();

        // The .tmp file should not exist after a successful write.
        let tmp = dir.path().join("test.json.tmp");
        assert!(!tmp.exists());
    }
}
