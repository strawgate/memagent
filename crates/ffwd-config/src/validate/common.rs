// xtask-verify: allow(pub_module_needs_tests) // sanitize_identifier tested via validate/tests.rs

use crate::types::{ConfigError, OutputConfigV2};

pub(super) const MAX_READ_BUF_SIZE: usize = 4_194_304;

pub(super) fn validation_error(message: impl Into<String>) -> ConfigError {
    ConfigError::Validation(message.into())
}

pub(super) fn validation_message(error: ConfigError) -> String {
    match error {
        ConfigError::Validation(message) => message,
        ConfigError::Io(error) => error.to_string(),
        ConfigError::Yaml(error) => error.to_string(),
    }
}

pub(super) fn output_label(output: &OutputConfigV2, index: usize) -> String {
    output
        .name()
        .map_or_else(|| format!("#{index}"), String::from)
}

pub(super) fn output_path_for_feedback_loop(output: &OutputConfigV2) -> Option<&str> {
    match output {
        OutputConfigV2::File(config) => config.path.as_deref(),
        _ => None,
    }
}

pub fn sanitize_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len().max(1));
    for (idx, ch) in name.chars().enumerate() {
        let valid = if idx == 0 {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        };
        out.push(if valid { ch } else { '_' });
    }
    if out.is_empty() { "_".to_string() } else { out }
}
