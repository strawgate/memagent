use crate::types::ConfigError;
use std::path::Path;

pub(super) fn normalize_path_for_compare(path: &Path) -> std::path::PathBuf {
    path.canonicalize()
        .unwrap_or_else(|_| normalize_path_lexically(path))
}

pub(super) fn normalize_path_key_for_compare(path: &Path) -> std::path::PathBuf {
    let normalized = normalize_path_for_compare(path);
    #[cfg(windows)]
    {
        std::path::PathBuf::from(normalized.to_string_lossy().to_lowercase())
    }
    #[cfg(not(windows))]
    {
        normalized
    }
}

pub(super) fn validate_file_output_path_writable(
    pipeline_name: &str,
    output_label: &str,
    output_path: &str,
    base_path: Option<&Path>,
) -> Result<(), ConfigError> {
    let resolved = path_for_config_compare(output_path, base_path);
    let parent = match resolved.parent() {
        Some(parent) if parent.as_os_str().is_empty() => Path::new("."),
        Some(parent) => parent,
        None => Path::new("."),
    };

    let parent_meta = parent.metadata().map_err(|e| {
        ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{output_label}': file output parent directory '{}' is not usable: {e}",
            parent.display()
        ))
    })?;
    if !parent_meta.is_dir() {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{output_label}': file output parent '{}' is not a directory",
            parent.display()
        )));
    }
    if !resolved.exists() && parent_meta.permissions().readonly() {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{output_label}': file output parent '{}' is read-only",
            parent.display()
        )));
    }

    if resolved.exists() {
        let md = resolved.metadata().map_err(|e| {
            ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' output '{output_label}': failed to inspect file output path '{}': {e}",
                resolved.display()
            ))
        })?;
        if md.is_dir() {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' output '{output_label}': file output path '{}' is a directory",
                resolved.display()
            )));
        }
        if md.permissions().readonly() {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' output '{output_label}': file output path '{}' is read-only",
                resolved.display()
            )));
        }
    }

    Ok(())
}

pub(super) fn path_for_config_compare(path: &str, base_path: Option<&Path>) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(path);
    if path.is_relative()
        && let Some(base) = base_path
    {
        let abs_base = if base.is_relative() {
            std::env::current_dir().map_or_else(|_| base.to_path_buf(), |cwd| cwd.join(base))
        } else {
            base.to_path_buf()
        };
        return abs_base.join(path);
    }
    path
}

fn normalize_path_lexically(path: &Path) -> std::path::PathBuf {
    use std::path::Component;

    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let mut tail = out.components();
                match tail.next_back() {
                    Some(Component::Normal(_)) => {
                        out.pop();
                    }
                    Some(Component::CurDir) => {}
                    Some(Component::ParentDir) | None => out.push(component.as_os_str()),
                    Some(Component::RootDir) | Some(Component::Prefix(_)) => {}
                }
            }
            Component::Normal(_) | Component::RootDir | Component::Prefix(_) => {
                out.push(component.as_os_str());
            }
        }
    }

    if out.as_os_str().is_empty() {
        std::path::PathBuf::from(".")
    } else {
        out
    }
}

pub(super) fn is_glob_match_possible(glob_pattern: &str, file_path: &str) -> bool {
    let glob_path = Path::new(glob_pattern);
    let file = Path::new(file_path);

    let glob_dir = glob_path.parent().map(normalize_path_for_compare);
    let file_dir = file.parent().map(normalize_path_for_compare);

    let same_directory = matches!((&glob_dir, &file_dir), (Some(g), Some(f)) if g == f);
    let recursive_double_star = glob_pattern.contains("**");
    let recursive_root_match = if recursive_double_star {
        let prefix = glob_pattern
            .split("**")
            .next()
            .unwrap_or("")
            .trim_end_matches(std::path::MAIN_SEPARATOR);
        if prefix.is_empty() {
            false
        } else {
            let normalized_prefix = normalize_path_lexically(Path::new(prefix));
            let normalized_file = normalize_path_lexically(file);
            normalized_file.starts_with(&normalized_prefix)
        }
    } else {
        false
    };
    let recursive_prefix_match = if recursive_double_star {
        if let (Some(g), Some(f)) = (&glob_dir, &file_dir) {
            let mut prefix = std::path::PathBuf::new();
            let mut saw_recursive = false;
            for component in g.components() {
                if component.as_os_str() == "**" {
                    saw_recursive = true;
                    break;
                }
                prefix.push(component.as_os_str());
            }
            saw_recursive && f.starts_with(prefix)
        } else {
            false
        }
    } else {
        false
    };
    let directory_wildcard_prefix_match = {
        let raw_glob_dir = glob_path.parent().and_then(|p| p.to_str()).unwrap_or("");
        if raw_glob_dir.contains(['*', '?', '[']) {
            let prefix = raw_glob_dir
                .split(|c| ['*', '?', '['].contains(&c))
                .next()
                .unwrap_or("")
                .trim_end_matches(std::path::MAIN_SEPARATOR);
            if prefix.is_empty() {
                true
            } else {
                let normalized_prefix = normalize_path_lexically(Path::new(prefix));
                let normalized_file = normalize_path_lexically(file);
                normalized_file
                    .to_string_lossy()
                    .starts_with(normalized_prefix.to_string_lossy().as_ref())
            }
        } else {
            false
        }
    };

    if same_directory
        || recursive_prefix_match
        || recursive_root_match
        || directory_wildcard_prefix_match
    {
        if let Some(glob_name) = glob_path.file_name().and_then(|n| n.to_str())
            && let Some(file_name) = file.file_name().and_then(|n| n.to_str())
        {
            if let Some(ext) = glob_name.strip_prefix('*') {
                if ext.contains(['*', '?', '[']) {
                    return true;
                }
                return file_name.ends_with(ext);
            }

            if !glob_name.contains(['*', '?', '[']) {
                return glob_name == file_name;
            }

            return true;
        }
        return true;
    }
    false
}
