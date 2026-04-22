//! Input type tests: type acceptance, format validation, field rejection,
//! tuning knobs, TLS, and sensor-specific config.

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn all_input_formats() {
        // Implemented input formats should parse and validate successfully.
        for format in ["cri", "json", "raw", "auto"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\n  format: {format}\noutput:\n  type: stdout\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for format '{format}': {e}"));
        }
    }

    #[test]
    fn all_input_types() {
        for (itype, extra) in [
            ("file", "path: /tmp/x.log"),
            ("udp", "listen: 0.0.0.0:514"),
            ("tcp", "listen: 0.0.0.0:514"),
            ("otlp", "listen: 0.0.0.0:4317"),
            ("arrow_ipc", "listen: 0.0.0.0:4319"),
            ("http", "listen: 0.0.0.0:8080"),
            ("stdin", ""),
            ("generator", ""),
            ("linux_ebpf_sensor", ""),
            ("macos_es_sensor", ""),
            ("windows_ebpf_sensor", ""),
            ("journald", ""),
        ] {
            let yaml = format!("input:\n  type: {itype}\n  {extra}\noutput:\n  type: stdout\n");
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for {itype}: {e}"));
        }
    }

    #[test]
    fn all_output_types() {
        // Supported output types should parse and validate successfully.
        for (otype, extra) in [
            ("otlp", "endpoint: http://x:4317"),
            ("stdout", ""),
            ("\"null\"", ""),
            ("elasticsearch", "endpoint: http://x"),
            ("loki", "endpoint: http://x"),
            ("arrow_ipc", "endpoint: http://x"),
            ("file", "path: /tmp/x.ndjson"),
            ("tcp", "endpoint: 127.0.0.1:5140"),
            ("udp", "endpoint: 127.0.0.1:5140"),
        ] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  {extra}\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for {otype}: {e}"));
        }

        // Placeholder output types must be rejected at validation time.
        for (otype, extra) in [("parquet", "path: /tmp/x"), ("http", "endpoint: http://x")] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  {extra}\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "expected error for unimplemented type {otype}"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not yet implemented"),
                "expected 'not yet implemented' for {otype}: {msg}"
            );
        }
    }

    #[test]
    fn otlp_input_accepts_experimental_protobuf_decode_mode() {
        let yaml = r"
input:
  type: otlp
  listen: 127.0.0.1:4318
  protobuf_decode_mode: projected_fallback
output:
  type: stdout
";
        let cfg =
            Config::load_str(yaml).expect("otlp input with protobuf_decode_mode should parse");
        let pipe = &cfg.pipelines["default"];
        assert!(matches!(
            &pipe.inputs[0].type_config,
            InputTypeConfig::Otlp(o)
                if o.protobuf_decode_mode == Some(OtlpProtobufDecodeModeConfig::ProjectedFallback)
        ));
    }

    #[test]
    fn otlp_input_rejects_resource_prefix() {
        let yaml = r"
input:
  type: otlp
  listen: 127.0.0.1:4318
  resource_prefix: resource.attributes.
output:
  type: stdout
";
        let err = Config::load_str(yaml).expect_err("resource_prefix is no longer supported");
        let msg = err.to_string();
        assert!(
            msg.contains("resource_prefix") || msg.contains("unknown field"),
            "expected serde rejection of resource_prefix, got: {msg}"
        );
    }

    #[test]
    fn non_otlp_input_rejects_protobuf_decode_mode() {
        let yaml = r"
input:
  type: file
  path: /var/log/app.log
  protobuf_decode_mode: projected_fallback
output:
  type: stdout
";
        let err = Config::load_str(yaml).expect_err("protobuf_decode_mode must be otlp-only");
        let msg = err.to_string();
        assert!(
            msg.contains("protobuf_decode_mode") || msg.contains("unknown field"),
            "expected serde rejection of protobuf_decode_mode for file input, got: {msg}"
        );
    }

    #[test]
    fn stdin_input_accepts_supported_formats() {
        for format in ["auto", "cri", "json", "raw"] {
            let yaml =
                format!("input:\n  type: stdin\n  format: {format}\noutput:\n  type: stdout\n");
            let cfg = Config::load_str(&yaml)
                .unwrap_or_else(|e| panic!("failed for stdin format {format}: {e}"));
            let input = &cfg.pipelines["default"].inputs[0];
            assert_eq!(input.input_type(), InputType::Stdin);
        }
    }

    #[test]
    fn stdin_input_rejects_unsupported_format() {
        let yaml = r"
input:
  type: stdin
  format: logfmt
output:
  type: stdout
";
        let err = Config::load_str(yaml).expect_err("stdin should reject unsupported format");
        assert!(
            err.to_string()
                .contains("stdin input only supports format auto, cri, json, or raw"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn stdin_input_rejects_file_only_fields() {
        let yaml = r"
input:
  type: stdin
  path: /tmp/app.log
output:
  type: stdout
";
        let err = Config::load_str(yaml).expect_err("stdin should reject path");
        assert!(
            err.to_string().contains("unknown field `path`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn sensor_aliases_remain_supported_for_back_compat() {
        for itype in [
            "linux_sensor_beta",
            "macos_sensor_beta",
            "windows_sensor_beta",
        ] {
            let yaml = format!(
                "input:\n  type: {itype}\n  sensor_beta:\n    poll_interval_ms: 1000\noutput:\n  type: stdout\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| {
                panic!("legacy alias should remain supported for {itype}: {e}")
            });
        }
    }

    #[test]
    fn file_input_accepts_tuning_knobs() {
        let yaml = r"
input:
  type: file
  path: /tmp/test.log
  poll_interval_ms: 100
  read_buf_size: 1048576
  per_file_read_budget_bytes: 2097152
output:
  type: stdout
";
        let cfg = Config::load_str(yaml).unwrap();
        let pipe = &cfg.pipelines["default"];
        let input = &pipe.inputs[0];
        let f = match &input.type_config {
            InputTypeConfig::File(f) => f,
            _ => panic!("expected File type_config"),
        };
        assert_eq!(f.poll_interval_ms, PositiveMillis::new(100));
        assert_eq!(f.read_buf_size, Some(1048576));
        assert_eq!(f.per_file_read_budget_bytes, Some(2097152));
    }

    #[test]
    fn file_input_rejects_zero_tuning_knobs() {
        // Non-duration fields still rejected by validation.
        let validation_cases = [("read_buf_size", 0), ("per_file_read_budget_bytes", 0)];
        for (field, value) in validation_cases {
            let yaml = format!(
                r"
input:
  type: file
  path: /tmp/test.log
  {field}: {value}
output:
  type: stdout
"
            );
            let err = Config::load_str(&yaml).unwrap_err().to_string();
            assert!(
                err.contains(&format!("'{field}' must be at least 1")),
                "expected error about positive {field}, got: {err}"
            );
        }

        // Duration field (poll_interval_ms) is rejected at parse time via PositiveMillis.
        let yaml = r"
input:
  type: file
  path: /tmp/test.log
  poll_interval_ms: 0
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err().to_string();
        assert!(
            err.contains("invalid value") || err.contains("positive"),
            "expected zero-rejection error for poll_interval_ms, got: {err}"
        );
    }

    #[test]
    fn sensor_rejects_format_configuration() {
        let yaml = r"
input:
  type: linux_ebpf_sensor
  format: raw
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("sensor inputs do not support 'format'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn non_file_input_rejects_tuning_knobs() {
        // With the tagged-enum refactor, file-only tuning knobs are rejected by
        // serde `deny_unknown_fields` at parse time for non-file input types.
        let inputs = [
            ("http", "listen: 0.0.0.0:8080"),
            ("tcp", "listen: 0.0.0.0:8080"),
            ("generator", ""),
            ("otlp", "listen: 0.0.0.0:4318"),
            ("arrow_ipc", "listen: 0.0.0.0:4319"),
        ];
        let fields = [
            "poll_interval_ms: 100",
            "read_buf_size: 1024",
            "per_file_read_budget_bytes: 1024",
        ];

        for (in_type, extra) in inputs {
            for field in fields {
                let yaml = format!(
                    r"
input:
  type: {in_type}
  {extra}
  {field}
output:
  type: stdout
"
                );
                let err = Config::load_str(&yaml).unwrap_err().to_string();
                let field_name = field.split(':').next().unwrap();
                assert!(
                    err.contains(field_name) || err.contains("unknown field"),
                    "expected serde rejection of {field_name} for {in_type}, got: {err}"
                );
            }
        }
    }

    #[test]
    fn file_input_rejects_sensor_block() {
        // With the tagged-enum refactor, the `sensor` key is only valid inside
        // sensor input variants.  Serde rejects it at parse time for file inputs.
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
  sensor:
    poll_interval_ms: 1000
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("sensor") || err.to_string().contains("unknown field"),
            "expected serde rejection of sensor for file input: {}",
            err
        );
    }

    #[test]
    fn arrow_ipc_rejects_format_override() {
        let yaml = r"
input:
  type: arrow_ipc
  listen: 0.0.0.0:4319
  format: raw
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err().to_string();
        assert!(
            err.contains("'format' is not supported for arrow_ipc inputs"),
            "expected arrow_ipc format rejection, got: {err}"
        );
    }

    #[test]
    fn sensor_rejects_zero_poll_interval() {
        let yaml = r"
input:
  type: linux_ebpf_sensor
  sensor:
    poll_interval_ms: 0
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        // Now rejected at parse time via PositiveMillis.
        let msg = err.to_string();
        assert!(
            msg.contains("invalid value") || msg.contains("positive"),
            "expected zero-rejection error for poll_interval_ms, got: {msg}"
        );
    }

    #[test]
    fn sensor_rejects_zero_control_reload_interval() {
        let yaml = r"
input:
  type: linux_ebpf_sensor
  sensor:
    control_reload_interval_ms: 0
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        // Now rejected at parse time via PositiveMillis.
        let msg = err.to_string();
        assert!(
            msg.contains("invalid value") || msg.contains("positive"),
            "expected zero-rejection error for control_reload_interval_ms, got: {msg}"
        );
    }

    #[test]
    fn sensor_rejects_empty_control_path() {
        let yaml = r#"
input:
  type: linux_ebpf_sensor
  sensor:
    control_path: "   "
output:
  type: stdout
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("sensor.control_path must not be empty")
        );
    }

    #[test]
    fn sensor_rejects_unknown_enabled_family() {
        let yaml = r"
input:
  type: linux_ebpf_sensor
  sensor:
    enabled_families: [process, made_up_family]
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown sensor family 'made_up_family'")
        );
    }

    #[test]
    fn linux_sensor_accepts_event_type_filters() {
        let yaml = r"
input:
  type: linux_ebpf_sensor
  sensor:
    include_event_types: [exec, tcp_connect]
    exclude_event_types: [tcp_accept]
output:
  type: stdout
";
        Config::load_str(yaml).expect("known linux sensor event types should validate");
    }

    #[test]
    fn linux_sensor_rejects_unknown_event_type_filter() {
        let yaml = r"
input:
  type: linux_ebpf_sensor
  sensor:
    include_event_types: [process_exec]
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown sensor event type 'process_exec'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn linux_sensor_rejects_blank_event_type_filter() {
        let yaml = r#"
input:
  type: linux_ebpf_sensor
  sensor:
    exclude_event_types: ["  "]
output:
  type: stdout
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("sensor.exclude_event_types entries must not be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn linux_sensor_trims_whitespace_padded_event_types() {
        let yaml = r#"
input:
  type: linux_ebpf_sensor
  sensor:
    include_event_types: [" exec ", "  tcp_connect"]
    exclude_event_types: ["exit  "]
output:
  type: stdout
"#;
        let cfg = Config::load_str(yaml).expect("padded event types should validate");
        let pipeline = cfg.pipelines.values().next().unwrap();
        let input = &pipeline.inputs[0];
        match &input.type_config {
            InputTypeConfig::LinuxEbpfSensor(s) => {
                let sensor = s.sensor.as_ref().unwrap();
                assert_eq!(
                    sensor.include_event_types.as_ref().unwrap(),
                    &["exec", "tcp_connect"],
                    "include_event_types should be trimmed"
                );
                assert_eq!(
                    sensor.exclude_event_types.as_ref().unwrap(),
                    &["exit"],
                    "exclude_event_types should be trimmed"
                );
            }
            _ => panic!("expected LinuxEbpfSensor variant"),
        }
    }

    #[test]
    fn host_metrics_rejects_event_type_filters() {
        let yaml = r"
input:
  type: host_metrics
  sensor:
    include_event_types: [exec]
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains(
                "sensor.include_event_types and sensor.exclude_event_types are only supported for linux_ebpf_sensor inputs"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn max_open_files_zero_rejected() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
  max_open_files: 0
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("max_open_files must be at least 1"),
            "expected 'max_open_files must be at least 1' in error: {msg}"
        );
    }

    #[test]
    fn max_open_files_one_accepted() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
  max_open_files: 1
output:
  type: stdout
";
        Config::load_str(yaml).expect("max_open_files: 1 should be valid");
    }

    #[test]
    fn max_open_files_absent_accepted() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: stdout
";
        Config::load_str(yaml).expect("absent max_open_files should be valid");
    }

    #[test]
    fn adaptive_fast_polls_max_zero_accepted_for_file() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
  adaptive_fast_polls_max: 0
output:
  type: stdout
";
        Config::load_str(yaml).expect("adaptive_fast_polls_max: 0 should be valid for file");
    }

    #[test]
    fn adaptive_fast_polls_max_custom_accepted_for_file() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
  adaptive_fast_polls_max: 12
output:
  type: stdout
";
        Config::load_str(yaml).expect("adaptive_fast_polls_max should be configurable for file");
    }

    // -----------------------------------------------------------------------
    // Input-side field rejection
    // -----------------------------------------------------------------------

    #[test]
    fn file_input_rejects_listen() {
        // With the tagged-enum refactor, `listen` is not a valid field for file
        // inputs. Serde rejects it at parse time via `deny_unknown_fields`.
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
        listen: 127.0.0.1:9999
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("file input must reject listen");
    }

    #[test]
    fn tcp_input_rejects_path() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        path: /tmp/test.log
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("tcp input must reject path");
    }

    #[test]
    fn tcp_input_rejects_max_open_files() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        max_open_files: 128
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("tcp input must reject max_open_files");
    }

    #[test]
    fn tcp_input_rejects_adaptive_fast_polls_max() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        adaptive_fast_polls_max: 4
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("tcp input must reject adaptive_fast_polls_max");
    }

    #[test]
    fn tcp_accepts_tls_cert_and_key() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        tls:
          cert_file: /tmp/server.pem
          key_file: /tmp/server.key
    outputs:
      - type: "null"
"#;
        Config::load_str(yaml).expect("tcp tls with cert+key should validate");
    }

    #[test]
    fn tcp_tls_requires_cert_and_key_together() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        tls:
          cert_file: /tmp/server.pem
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("requires both tls.cert_file and tls.key_file"),
            "expected TCP cert/key pairing validation: {err}"
        );
    }

    #[test]
    fn tcp_mtls_accepts_required_client_ca() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        tls:
          cert_file: /tmp/server.pem
          key_file: /tmp/server.key
          client_ca_file: /tmp/ca.pem
          require_client_auth: true
    outputs:
      - type: "null"
"#;
        Config::load_str(yaml).expect("tcp mTLS with client CA should validate");
    }

    #[test]
    fn tcp_mtls_requires_client_ca() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        tls:
          cert_file: /tmp/server.pem
          key_file: /tmp/server.key
          require_client_auth: true
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("require_client_auth requires tls.client_ca_file"),
            "expected TCP mTLS client CA validation failure: {err}"
        );
    }

    #[test]
    fn tcp_client_ca_requires_mtls_enabled() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        tls:
          cert_file: /tmp/server.pem
          key_file: /tmp/server.key
          client_ca_file: /tmp/ca.pem
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("client_ca_file requires tls.require_client_auth: true"),
            "expected TCP client CA without mTLS validation failure: {err}"
        );
    }

    #[test]
    fn udp_rejects_tls_block() {
        // With the tagged-enum refactor, UDP has no `tls` field. Serde rejects
        // it at parse time via `deny_unknown_fields`.
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: udp
        listen: 0.0.0.0:514
        tls:
          cert_file: /tmp/server.pem
          key_file: /tmp/server.key
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("udp input must reject tls");
    }

    #[test]
    fn tcp_input_rejects_glob_rescan_interval_ms() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        glob_rescan_interval_ms: 5000
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("tcp input must reject glob_rescan_interval_ms");
    }

    #[test]
    fn arrow_ipc_input_valid_with_listen() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: arrow_ipc
        listen: 0.0.0.0:4317
    outputs:
      - type: "null"
"#;
        Config::load_str(yaml).expect("arrow_ipc input should validate when listen is provided");
    }
}
