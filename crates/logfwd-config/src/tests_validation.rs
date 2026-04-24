//! Validation tests: missing fields, invalid values, unimplemented types/formats,
//! endpoint schemes, layout conflicts, and pipeline-level knobs.

#[cfg(test)]
mod tests {
    use crate::*;
    use std::fs;

    #[test]
    fn validation_missing_input_path() {
        let yaml = r"
input:
  type: file
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("path"), "expected 'path' in error: {msg}");
    }

    #[test]
    fn validation_missing_output_endpoint() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("endpoint"),
            "expected 'endpoint' in error: {msg}"
        );
    }

    #[test]
    fn validation_otlp_gzip_is_allowed() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: http://collector:4318
  compression: gzip
";
        Config::load_str(yaml).expect("gzip OTLP compression should validate");
    }

    #[test]
    fn validation_storage_data_dir_existing_non_directory_rejected() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time must be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "logfwd-config-storage-non-dir-{}-{unique}.tmp",
            std::process::id()
        ));
        fs::write(&path, b"not-a-directory").expect("write temp file");

        let yaml = format!(
            r"
input:
  type: file
  path: /var/log/test.log
output:
  type: stdout
storage:
  data_dir: {}
",
            path.display()
        );

        let err = Config::load_str(&yaml).expect_err("non-directory storage.data_dir must fail");
        assert!(
            err.to_string().contains("exists but is not a directory"),
            "expected non-directory storage.data_dir rejection, got: {err}"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn validation_udp_requires_listen() {
        let yaml = r"
input:
  type: udp
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("listen"), "expected 'listen' in error: {msg}");
    }

    #[test]
    fn validation_otlp_requires_listen() {
        let yaml = r"
input:
  type: otlp
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("listen"), "expected 'listen' in error: {msg}");
    }

    #[test]
    fn validation_arrow_ipc_requires_listen() {
        let yaml = r"
input:
  type: arrow_ipc
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("listen"), "expected 'listen' in error: {msg}");
    }

    #[test]
    fn validation_mixed_simple_and_pipelines() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
pipelines:
  extra:
    inputs:
      - type: file
        path: /tmp/y.log
    outputs:
      - type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("mix"), "expected 'mix' in error: {msg}");
    }

    #[test]
    fn validation_no_pipelines() {
        let yaml = r"
server:
  log_level: info
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("must define"),
            "expected 'must define' in error: {msg}"
        );
    }

    #[test]
    fn removed_output_type_is_rejected_at_parse_time() {
        let yaml =
            "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: parquet\n  path: /tmp/x\n";
        let err = Config::load_str(yaml).expect_err("removed output type should fail to parse");
        let msg = err.to_string();
        assert!(
            msg.contains("parquet")
                && (msg.contains("unknown variant")
                    || msg.contains("does not have variant constructor")),
            "expected parse rejection for removed parquet output: {msg}"
        );
    }

    #[test]
    fn validation_unimplemented_input_format() {
        // Unimplemented input formats must be rejected at config validation time,
        // not silently treated as JSON which would corrupt data.
        for format in ["logfmt", "syslog"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\n  format: {format}\noutput:\n  type: stdout\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "validation should reject unimplemented format '{format}'"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not yet implemented"),
                "error message should mention 'not yet implemented' for '{format}': {msg}"
            );
        }
    }

    #[test]
    fn pipelines_form_rejects_top_level_transform() {
        let yaml = r"
transform: SELECT * FROM logs
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
";
        let err = Config::load_str(yaml).expect_err("top-level transform must be rejected");
        assert!(
            err.to_string()
                .contains("top-level `transform` cannot be used with `pipelines:`"),
            "unexpected validation error: {err}"
        );
    }

    #[test]
    fn validation_endpoint_missing_scheme() {
        // Scheme-less endpoints must be rejected for supported URL-based outputs.
        for otype in ["otlp", "elasticsearch"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: collector:4317\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "expected error for scheme-less endpoint with type '{otype}'"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("scheme"),
                "error should mention 'scheme' for '{otype}': {msg}"
            );
        }
    }

    #[test]
    fn validation_endpoint_valid_schemes() {
        // Both http:// and https:// must be accepted for supported URL-based outputs.
        for (otype, scheme) in [
            ("otlp", "http://"),
            ("otlp", "https://"),
            ("elasticsearch", "http://"),
            ("elasticsearch", "https://"),
        ] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: {scheme}collector:4317\n"
            );
            Config::load_str(&yaml)
                .unwrap_or_else(|e| panic!("scheme '{scheme}' should be valid for '{otype}': {e}"));
        }
    }

    #[test]
    fn validation_endpoint_unset_env_var_rejected() {
        // An endpoint referencing an unset env var must fail at config load
        // time with a clear error message naming the variable.
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_NONEXISTENT_ENDPOINT_VAR}
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("LOGFWD_NONEXISTENT_ENDPOINT_VAR"),
            "error should mention the variable name: {msg}"
        );
    }

    #[test]
    fn ipv6_empty_bracket_rejected() {
        let err = validate_host_port("[]:8080");
        assert!(err.is_err(), "expected error for empty IPv6 brackets");
        assert!(err.unwrap_err().to_string().contains("empty IPv6 address"));
    }

    #[test]
    fn ipv6_double_closing_bracket_rejected() {
        // "[::1]]:4317" has a double closing bracket; rfind would accept it by
        // treating "[::1]]" as the host — find (first ']') correctly rejects it.
        let err = validate_host_port("[::1]]:4317");
        assert!(
            err.is_err(),
            "expected error for double closing bracket '[::1]]:4317'"
        );
    }

    #[test]
    fn ipv6_valid_address_accepted() {
        assert!(validate_host_port("[::1]:8080").is_ok());
        assert!(validate_host_port("[2001:db8::1]:4317").is_ok());
        // IPv6 zone IDs (%eth0) not supported by std::net::Ipv6Addr — skip.
    }

    #[test]
    fn top_level_resource_attrs_rejected_with_pipelines_form() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
resource_attrs:
  service.name: my-service
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("top-level `resource_attrs`"),
            "error should mention top-level resource_attrs: {msg}"
        );
        assert!(
            msg.contains("pipelines"),
            "error should mention pipelines: {msg}"
        );
    }

    #[test]
    fn top_level_enrichment_rejected_with_pipelines_form() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
enrichment:
  - type: geo_database
    format: mmdb
    path: /tmp/geo.mmdb
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("top-level `enrichment`"),
            "error should mention top-level enrichment: {msg}"
        );
        assert!(
            msg.contains("pipelines"),
            "error should mention pipelines form: {msg}"
        );
    }

    #[test]
    fn workers_zero_is_rejected() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    workers: 0
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("workers"),
            "expected 'workers' in error: {msg}"
        );
        assert!(
            msg.contains("must be in range 1..=1024"),
            "expected range-bounded workers message in error: {msg}"
        );
    }

    #[test]
    fn batch_target_bytes_zero_is_rejected() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    batch_target_bytes: 0
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("batch_target_bytes"),
            "expected 'batch_target_bytes' in error: {msg}"
        );
    }

    #[test]
    fn poll_interval_ms_must_be_positive() {
        let yaml = r"
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
    poll_interval_ms: 0
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        // Now rejected at parse time via PositiveMillis.
        assert!(
            msg.contains("invalid value") || msg.contains("positive"),
            "got: {msg}"
        );
    }
}
