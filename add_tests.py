import re

file_path = "crates/logfwd-config/src/validate.rs"
with open(file_path, "r") as f:
    content = f.read()

tests = """
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InputType, PipelineConfig, InputConfig, InputTypeConfig, FileTypeConfig, FileStartPosition, MultilineConfig, MultilineMatchMode};
    use logfwd_types::pipeline::{Format, SourceId};

    #[test]
    fn test_file_input_validation() {
        let mut f = FileTypeConfig {
            path: "/tmp/foo".to_string(),
            poll_interval_ms: None,
            read_buf_size: None,
            per_file_read_budget_bytes: None,
            adaptive_fast_polls_max: None,
            max_open_files: None,
            glob_rescan_interval_ms: None,
            start_position: None,
            encoding: None,
            follow_symlinks: None,
            ignore_older_than_secs: None,
            multiline: None,
        };

        let mut config = PipelineConfig {
            name: None,
            inputs: vec![InputConfig {
                name: Some("test_in".into()),
                format: None,
                sql: None,
                type_config: InputTypeConfig::File(f.clone()),
            }],
            transforms: vec![],
            outputs: vec![],
        };

        // Should pass
        assert!(config.validate().is_ok());

        // Invalid encoding
        f.encoding = Some("latin1".to_string());
        config.inputs[0].type_config = InputTypeConfig::File(f.clone());
        assert!(config.validate().is_err());

        f.encoding = Some("utf-8".to_string());
        config.inputs[0].type_config = InputTypeConfig::File(f.clone());
        assert!(config.validate().is_ok());

        // Invalid regex
        f.multiline = Some(MultilineConfig {
            pattern: "[invalid regex".to_string(),
            negate: false,
            match_mode: MultilineMatchMode::Before,
        });
        config.inputs[0].type_config = InputTypeConfig::File(f.clone());
        assert!(config.validate().is_err());

        f.multiline = Some(MultilineConfig {
            pattern: "^started".to_string(),
            negate: false,
            match_mode: MultilineMatchMode::Before,
        });
        config.inputs[0].type_config = InputTypeConfig::File(f.clone());
        assert!(config.validate().is_ok());
    }
}
"""

content = content + tests

with open(file_path, "w") as f:
    f.write(content)
