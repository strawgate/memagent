//! Server configuration tests: diagnostics address validation and log level.

#[cfg(test)]
mod tests {
    use crate::test_yaml::{append_root_sections, single_pipeline_yaml};
    use crate::*;

    // -----------------------------------------------------------------------
    // Bug #725: server.diagnostics address validated at config load time
    // -----------------------------------------------------------------------

    #[test]
    fn valid_diagnostics_address_accepted() {
        let yaml = append_root_sections(
            single_pipeline_yaml("type: file\npath: /tmp/x.log", "type: stdout"),
            "server:\n  diagnostics: 127.0.0.1:9090\n",
        );
        Config::load_str(yaml).expect("valid diagnostics address");
    }

    #[test]
    fn invalid_diagnostics_address_rejected_at_validate() {
        // Before the fix, an invalid server.diagnostics address would pass
        // `validate` and only fail at runtime when the server tried to bind.
        let yaml = append_root_sections(
            single_pipeline_yaml("type: file\npath: /tmp/x.log", "type: stdout"),
            "server:\n  diagnostics: not-an-address\n",
        );
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("diagnostics"),
            "error should mention 'diagnostics': {msg}"
        );
        assert!(
            msg.contains("not a valid")
                || msg.contains("invalid")
                || msg.contains("missing a port"),
            "error should say address is invalid: {msg}"
        );
    }

    #[test]
    fn diagnostics_address_with_unset_env_var_rejected() {
        // Unset ${VAR} placeholders must be rejected at config-load time.
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
server:
  diagnostics: ${FFWD_DIAG_ADDR}
";
        assert_config_err!(yaml, "FFWD_DIAG_ADDR");
    }

    // -----------------------------------------------------------------------
    // Bug #481: server.log_level validation
    // -----------------------------------------------------------------------

    #[test]
    fn log_level_valid_values_accepted() {
        for level in ["trace", "debug", "info", "warn", "error", "INFO", "Warn"] {
            let yaml = append_root_sections(
                single_pipeline_yaml("type: file\npath: /tmp/x.log", "type: stdout"),
                &format!("server:\n  log_level: {level}\n"),
            );
            Config::load_str(yaml)
                .unwrap_or_else(|e| panic!("log_level '{level}' should be valid: {e}"));
        }
    }

    #[test]
    fn log_level_invalid_value_rejected() {
        let yaml = append_root_sections(
            single_pipeline_yaml("type: file\npath: /tmp/x.log", "type: stdout"),
            "server:\n  log_level: inof\n",
        );
        assert_config_err!(yaml, "log_level", "not a recognised log level");
    }
}
