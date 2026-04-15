import re

file_path = "crates/logfwd-config/src/types.rs"
with open(file_path, "r") as f:
    content = f.read()

content = content.replace(
"""    #[serde(default)]
    pub start_position: Option<FileStartPosition>,
    #[serde(default)]
    pub encoding: Option<String>,
    #[serde(default)]
    pub follow_symlinks: Option<bool>,
    #[serde(default)]
    pub ignore_older_than_secs: Option<u64>,
    #[serde(default)]
    pub multiline: Option<MultilineConfig>,""",
"""    /// The position to start reading the file from. Options are "beginning" or "end".
    #[serde(default)]
    pub start_position: Option<FileStartPosition>,
    /// The encoding of the file. Valid options are "utf-8", "utf8", and "ascii".
    #[serde(default)]
    pub encoding: Option<String>,
    /// Whether to follow symlinks when discovering files.
    #[serde(default)]
    pub follow_symlinks: Option<bool>,
    /// Ignore files that have not been modified within this many seconds.
    #[serde(default)]
    pub ignore_older_than_secs: Option<u64>,
    /// Configuration for reading multiline logs.
    #[serde(default)]
    pub multiline: Option<MultilineConfig>,""")

with open(file_path, "w") as f:
    f.write(content)
