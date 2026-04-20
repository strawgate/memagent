//! Download and extract competitor binaries from tarballs.

use std::io::{self, Read};
use std::path::{Path, PathBuf};

/// Returns (os, arch) for the current platform.
/// os: "linux" / "darwin", arch: "x86_64" / "aarch64" / "arm64".
pub fn platform() -> (String, String) {
    let os = std::env::consts::OS.to_string(); // "linux", "macos"
    let os = if os == "macos" {
        "darwin".to_string()
    } else {
        os
    };
    let arch = std::env::consts::ARCH.to_string(); // "x86_64", "aarch64"
    (os, arch)
}

/// Alternate arch string: x86_64→amd64, aarch64→arm64.
pub fn arch_alt(arch: &str) -> &str {
    match arch {
        "x86_64" => "amd64",
        "aarch64" | "arm64" => "arm64",
        other => other,
    }
}

/// Download a tarball from `url`, extract it, find `binary_name` inside,
/// and copy it to `bin_dir/<binary_name>`. Returns the path to the binary.
pub fn download_and_extract(url: &str, binary_name: &str, bin_dir: &Path) -> io::Result<PathBuf> {
    eprintln!("  Downloading: {url}");

    let resp = ureq::get(url)
        .call()
        .map_err(|e| io::Error::other(format!("download failed: {e}")))?;

    let reader = resp.into_body().into_reader();
    let gz = flate2::read::GzDecoder::new(reader);
    let mut archive = tar::Archive::new(gz);

    let dest = bin_dir.join(binary_name);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;

        // Match by filename — binary might be nested in a directory.
        let matches = path.file_name().is_some_and(|n| n == binary_name);

        if matches {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf)?;
            std::fs::write(&dest, &buf)?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755))?;
            }

            return Ok(dest);
        }
    }

    Err(io::Error::other(format!(
        "binary '{binary_name}' not found in archive"
    )))
}
