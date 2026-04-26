// xtask-verify: allow(pub_module_needs_tests) // trivial wrappers over OS APIs; behaviour is compile-time platform detection

/// The host operating system family detected at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostPlatform {
    Linux,
    Macos,
    Windows,
    Unsupported,
}

impl HostPlatform {
    /// Returns the lowercase platform name, or `None` for unsupported platforms.
    pub const fn as_str(self) -> Option<&'static str> {
        match self {
            Self::Linux => Some("linux"),
            Self::Macos => Some("macos"),
            Self::Windows => Some("windows"),
            Self::Unsupported => None,
        }
    }
}

/// Convert an `OsStr` to a `String`, replacing non-UTF-8 sequences with the replacement character.
pub fn os_to_string(value: &OsStr) -> String {
    value.to_string_lossy().to_string()
}

/// Convert a slice of `OsString` values to a single space-separated `String`.
pub fn os_vec_to_string(values: &[std::ffi::OsString]) -> String {
    let mut out = String::new();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        out.push_str(&value.to_string_lossy());
    }
    out
}

/// Return the current wall-clock time as nanoseconds since the Unix epoch.
pub fn get_unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Detect the host operating system at compile time and return the corresponding [`HostPlatform`].
pub fn detect_host_platform() -> HostPlatform {
    if cfg!(target_os = "linux") {
        HostPlatform::Linux
    } else if cfg!(target_os = "macos") {
        HostPlatform::Macos
    } else if cfg!(target_os = "windows") {
        HostPlatform::Windows
    } else {
        HostPlatform::Unsupported
    }
}