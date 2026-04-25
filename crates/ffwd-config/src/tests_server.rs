//! Server configuration tests: diagnostics address validation and log level.

#[cfg(test)]
mod tests {
    use crate::*;

    // -----------------------------------------------------------------------
    // Bug #725: server.diagnostics address validated at config load time
    // -----------------------------------------------------------------------

    #[test]
    fn valid_diagnostics_address_accepted() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
server:
  diagnostics: 127.0.0.1:9090
";
        Config::load_str(yaml).expect("valid diagnostics address");
    }

    #[test]
    fn invalid_diagnostics_address_rejected_at_validate() {
        // Before the fix, an invalid server.diagnostics address would pass
        // `validate` and only fail at runtime when the server tried to bind.
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
server:
  diagnostics: not-an-address
";
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
  diagnostics: ${LOGFWD_DIAG_ADDR}
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("LOGFWD_DIAG_ADDR"),
            "error should mention the variable name: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Bug #481: server.log_level validation
    // -----------------------------------------------------------------------

    #[test]
    fn log_level_valid_values_accepted() {
        for level in ["trace", "debug", "info", "warn", "error", "INFO", "Warn"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: stdout\nserver:\n  log_level: {level}\n"
            );
            Config::load_str(&yaml)
                .unwrap_or_else(|e| panic!("log_level '{level}' should be valid: {e}"));
        }
    }

    #[test]
    fn log_level_invalid_value_rejected() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
server:
  log_level: inof
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("log_level"),
            "expected 'log_level' in error: {msg}"
        );
        assert!(
            msg.contains("not a recognised log level"),
            "expected 'not a recognised log level' in error: {msg}"
        );
    }
}
