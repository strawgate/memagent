use std::path::{Path, PathBuf};

/// Extract the root directory from a glob pattern — the longest prefix path
/// before the first wildcard character (`*`, `?`, `[`, `{`).
///
/// Examples:
/// - `/var/log/*.log` → `/var/log`
/// - `/var/log/**/*.log` → `/var/log`
/// - `*.log` → `.`
pub(super) fn glob_root(pattern: &str) -> PathBuf {
    let wildcard_pos = pattern.find(['*', '?', '[', '{']).unwrap_or(pattern.len());
    let prefix = &pattern[..wildcard_pos];
    if prefix.is_empty() {
        return PathBuf::from(".");
    }
    if prefix.ends_with('/') {
        let trimmed = prefix.trim_end_matches('/');
        if trimmed.is_empty() {
            PathBuf::from("/")
        } else {
            PathBuf::from(trimmed)
        }
    } else {
        let parent = Path::new(prefix).parent().unwrap_or(Path::new(""));
        if parent.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            parent.to_path_buf()
        }
    }
}

/// Compute the maximum directory depth a glob pattern can match.
/// Counts path components below the root directory.
/// Returns `None` if the pattern contains `**` (unbounded depth).
pub(super) fn glob_max_depth(pattern: &str) -> Option<usize> {
    if pattern.contains("**") {
        return None;
    }
    let root = glob_root(pattern);
    let root_depth = root
        .components()
        .filter(|c| *c != std::path::Component::CurDir)
        .count();
    let total_depth = Path::new(pattern)
        .components()
        .filter(|c| *c != std::path::Component::CurDir)
        .count();
    Some(total_depth.saturating_sub(root_depth).max(1))
}

/// Expand a list of glob patterns into the set of matching `PathBuf` values.
///
/// Uses `globset::GlobSet` for fast multi-pattern matching and `walkdir` for
/// directory traversal with symlink loop detection.
///
/// Patterns that match no files are silently skipped. Errors from directory
/// traversal (e.g., permission denied) are also skipped.
pub(super) fn expand_glob_patterns(patterns: &[&str]) -> Vec<PathBuf> {
    if patterns.is_empty() {
        return Vec::new();
    }

    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        match globset::GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
        {
            Ok(g) => {
                builder.add(g);
            }
            Err(e) => {
                tracing::warn!(pattern, error = %e, "tail.invalid_glob_pattern");
            }
        }
    }
    let glob_set = match builder.build() {
        Ok(gs) => gs,
        Err(e) => {
            tracing::warn!(error = %e, "tail.globset_build_failed");
            return Vec::new();
        }
    };

    let mut roots: Vec<(PathBuf, Option<usize>)> = Vec::new();
    for pattern in patterns {
        let root = glob_root(pattern);
        let depth = glob_max_depth(pattern);
        if let Some(existing) = roots.iter_mut().find(|(r, _)| r == &root) {
            existing.1 = match (existing.1, depth) {
                (None, _) | (_, None) => None,
                (Some(a), Some(b)) => Some(a.max(b)),
            };
        } else {
            roots.push((root, depth));
        }
    }

    let mut paths = Vec::new();
    for (root, max_depth) in &roots {
        let mut walker = walkdir::WalkDir::new(root).follow_links(true);
        if let Some(d) = max_depth {
            walker = walker.max_depth(*d);
        }
        for entry in walker.into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let entry_path = entry.path();
            let stripped = entry_path.strip_prefix(".").unwrap_or(entry_path);
            let prefixed = Path::new(".").join(stripped);
            if glob_set.is_match(entry_path)
                || glob_set.is_match(stripped)
                || glob_set.is_match(&prefixed)
            {
                paths.push(entry.into_path());
            }
        }
    }
    paths.sort_unstable();
    paths.dedup();
    paths
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::expand_glob_patterns;

    #[test]
    fn expand_glob_patterns_deduplicates_overlapping_roots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let nested = dir.path().join("logs/app");
        fs::create_dir_all(&nested).expect("create nested dir");
        let log_file = nested.join("service.log");
        fs::write(&log_file, b"line\n").expect("write log");

        let p1 = format!("{}/**/*.log", dir.path().display());
        let p2 = format!("{}/logs/**/*.log", dir.path().display());
        let matches = expand_glob_patterns(&[&p1, &p2]);

        assert_eq!(matches, vec![log_file]);
    }
}
