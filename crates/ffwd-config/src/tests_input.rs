//! Input type tests: type acceptance, format validation, field rejection,
//! tuning knobs, TLS, and sensor-specific config.

#[cfg(test)]
mod tests {
    use crate::test_yaml::single_pipeline_yaml;
    use crate::*;

    #[test]
    fn all_input_formats() {
        // Implemented input formats should parse and validate successfully.
        for format in ["cri", "json", "raw", "auto"] {
            let yaml = single_pipeline_yaml(
                &format!("type: file\npath: /tmp/x.log\nformat: {format}"),
                "type: stdout",
            );
            Config::load_str(yaml).unwrap_or_else(|e| panic!("failed for format '{format}': {e}"));
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
            ("s3", "s3:\n  bucket: log-bucket"),
            ("stdin", ""),
            ("generator", ""),
            ("linux_ebpf_sensor", ""),
            ("macos_es_sensor", ""),
            ("windows_ebpf_sensor", ""),
            ("journald", ""),
            ("host_metrics", ""),
        ] {
            let input = if extra.is_empty() {
                format!("type: {itype}")
            } else {
                format!("type: {itype}\n{extra}")
            };
            let yaml = single_pipeline_yaml(&input, "type: stdout");
            Config::load_str(yaml).unwrap_or_else(|e| panic!("failed for {itype}: {e}"));
        }
    }

    #[test]
    fn all_output_types() {
        // Supported output types should parse and validate successfully.
        for (otype, extra) in [
            ("otlp", "endpoint: http://x:4317"),
            ("http", "endpoint: http://x"),
            ("stdout", ""),
            ("\"null\"", ""),
            ("elasticsearch", "endpoint: http://x"),
            ("loki", "endpoint: http://x"),
            ("arrow_ipc", "endpoint: http://x"),
            ("file", "path: /tmp/x.ndjson"),
            ("tcp", "endpoint: 127.0.0.1:5140"),
            ("udp", "endpoint: 127.0.0.1:5140"),
        ] {
            let output = if extra.is_empty() {
                format!("type: {otype}")
            } else {
                format!("type: {otype}\n{extra}")
            };
            let yaml = single_pipeline_yaml("type: file\npath: /tmp/x.log", &output);
            Config::load_str(yaml).unwrap_or_else(|e| panic!("failed for {otype}: {e}"));
        }
    }

    #[test]
    fn otlp_input_accepts_experimental_protobuf_decode_mode() {
        let yaml = single_pipeline_yaml(
            "type: otlp\nlisten: 127.0.0.1:4318\nprotobuf_decode_mode: projected_fallback",
            "type: stdout",
        );
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
        let yaml = single_pipeline_yaml(
            "type: otlp\nlisten: 127.0.0.1:4318\nresource_prefix: resource.attributes.",
            "type: stdout",
        );
        let err = Config::load_str(yaml).expect_err("resource_prefix is no longer supported");
        let msg = err.to_string();
        assert!(
            msg.contains("resource_prefix") || msg.contains("unknown field"),
            "expected serde rejection of resource_prefix, got: {msg}"
        );
    }

    #[test]
    fn otlp_tls_requires_cert_and_key_together() {
        let yaml = single_pipeline_yaml(
            "type: otlp\nlisten: 127.0.0.1:4318\ntls:\n  cert_file: /tmp/server.pem",
            "type: stdout",
        );
        let err = Config::load_str(yaml).expect_err("partial otlp TLS config must fail");
        assert!(
            err.to_string()
                .contains("otlp tls requires both tls.cert_file and tls.key_file"),
            "expected OTLP cert/key pairing validation: {err}"
        );
    }

    #[test]
    fn otlp_mtls_requires_client_ca() {
        let yaml = single_pipeline_yaml(
            "type: otlp\nlisten: 127.0.0.1:4318\ntls:\n  cert_file: /tmp/server.pem\n  key_file: /tmp/server.key\n  require_client_auth: true",
            "type: stdout",
        );
        let err = Config::load_str(yaml).expect_err("otlp mTLS without client CA must fail");
        assert!(
            err.to_string()
                .contains("otlp tls require_client_auth requires tls.client_ca_file"),
            "expected OTLP mTLS client CA validation failure: {err}"
        );
    }

    #[test]
    fn s3_compression_uses_typed_values() {
        let yaml = single_pipeline_yaml(
            "type: s3\ns3:\n  bucket: log-bucket\n  compression: zstd",
            "type: stdout",
        );
        let cfg = Config::load_str(yaml).expect("typed s3 compression should parse");
        let InputTypeConfig::S3(config) = &cfg.pipelines["default"].inputs[0].type_config else {
            panic!("expected s3 input");
        };
        assert_eq!(config.s3.compression, Some(S3CompressionConfig::Zstd));
    }

    #[test]
    fn s3_compression_rejects_aliases() {
        let yaml = single_pipeline_yaml(
            "type: s3\ns3:\n  bucket: log-bucket\n  compression: zst",
            "type: stdout",
        );
        let err = Config::load_str(yaml).expect_err("s3 compression aliases are not accepted");
        assert!(
            err.to_string().contains("unknown variant `zst`"),
            "expected typed s3 compression parse failure: {err}"
        );
    }

    #[test]
    fn non_otlp_input_rejects_protobuf_decode_mode() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/app.log\nprotobuf_decode_mode: projected_fallback",
            "type: stdout",
        );
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
                single_pipeline_yaml(&format!("type: stdin\nformat: {format}"), "type: stdout");
            let cfg = Config::load_str(yaml)
                .unwrap_or_else(|e| panic!("failed for stdin format {format}: {e}"));
            let input = &cfg.pipelines["default"].inputs[0];
            assert_eq!(input.input_type(), InputType::Stdin);
        }
    }

    #[test]
    fn stdin_input_rejects_unsupported_format() {
        let yaml = single_pipeline_yaml("type: stdin\nformat: logfmt", "type: stdout");
        assert_config_err!(
            yaml,
            "stdin input only supports format auto, cri, json, or raw"
        );
    }

    #[test]
    fn stdin_input_rejects_file_only_fields() {
        let yaml = single_pipeline_yaml("type: stdin\npath: /tmp/app.log", "type: stdout");
        assert_config_err!(yaml, "unknown field `path`");
    }

    #[test]
    fn sensor_aliases_are_rejected() {
        for itype in [
            "linux_sensor_beta",
            "macos_sensor_beta",
            "windows_sensor_beta",
            "macos_endpointsecurity_sensor",
        ] {
            let yaml = single_pipeline_yaml(
                &format!("type: {itype}\nsensor_beta:\n  poll_interval_ms: 1000"),
                "type: stdout",
            );
            let err = Config::load_str(yaml).expect_err("legacy sensor type alias should fail");
            let msg = err.to_string();
            assert!(
                msg.contains("unknown variant"),
                "expected unknown variant for {itype}: {msg}"
            );
        }

        let block_yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor_beta:\n  poll_interval_ms: 1000",
            "type: stdout",
        );
        let err = Config::load_str(&block_yaml).expect_err("legacy sensor block alias should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field `sensor_beta`"),
            "expected unknown field for sensor_beta block: {msg}"
        );
    }

    #[test]
    fn file_input_accepts_tuning_knobs() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/test.log\npoll_interval_ms: 100\nread_buf_size: 1048576\nper_file_read_budget_bytes: 2097152",
            "type: stdout",
        );
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
            let yaml = single_pipeline_yaml(
                &format!("type: file\npath: /tmp/test.log\n{field}: {value}"),
                "type: stdout",
            );
            let err = Config::load_str(yaml).unwrap_err().to_string();
            assert!(
                err.contains(&format!("'{field}' must be at least 1")),
                "expected error about positive {field}, got: {err}"
            );
        }

        // Duration field (poll_interval_ms) is rejected at parse time via PositiveMillis.
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/test.log\npoll_interval_ms: 0",
            "type: stdout",
        );
        let err = Config::load_str(yaml).unwrap_err().to_string();
        assert!(
            err.contains("invalid value") || err.contains("positive"),
            "expected zero-rejection error for poll_interval_ms, got: {err}"
        );
    }

    #[test]
    fn sensor_rejects_format_configuration() {
        let yaml = single_pipeline_yaml("type: linux_ebpf_sensor\nformat: raw", "type: stdout");
        assert_config_err!(yaml, "sensor inputs do not support 'format'");
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
                let input = if extra.is_empty() {
                    format!("type: {in_type}\n{field}")
                } else {
                    format!("type: {in_type}\n{extra}\n{field}")
                };
                let yaml = single_pipeline_yaml(&input, "type: stdout");
                let err = Config::load_str(yaml).unwrap_err().to_string();
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
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log\nsensor:\n  poll_interval_ms: 1000",
            "type: stdout",
        );
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("sensor") || err.to_string().contains("unknown field"),
            "expected serde rejection of sensor for file input: {err}"
        );
    }

    #[test]
    fn arrow_ipc_rejects_format_override() {
        let yaml = single_pipeline_yaml(
            "type: arrow_ipc\nlisten: 0.0.0.0:4319\nformat: raw",
            "type: stdout",
        );
        assert_config_err!(yaml, "'format' is not supported for arrow_ipc inputs");
    }

    #[test]
    fn sensor_rejects_zero_poll_interval() {
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  poll_interval_ms: 0",
            "type: stdout",
        );
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
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  control_reload_interval_ms: 0",
            "type: stdout",
        );
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
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  control_path: \"   \"",
            "type: stdout",
        );
        assert_config_err!(yaml, "sensor.control_path must not be empty");
    }

    #[test]
    fn sensor_rejects_unknown_enabled_family() {
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  enabled_families: [process, made_up_family]",
            "type: stdout",
        );
        assert_config_err!(yaml, "unknown sensor family 'made_up_family'");
    }

    #[test]
    fn linux_sensor_accepts_event_type_filters() {
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  include_event_types: [exec, tcp_connect]\n  exclude_event_types: [tcp_accept]",
            "type: stdout",
        );
        Config::load_str(yaml).expect("known linux sensor event types should validate");
    }

    #[test]
    fn linux_sensor_rejects_unknown_event_type_filter() {
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  include_event_types: [process_exec]",
            "type: stdout",
        );
        assert_config_err!(yaml, "unknown sensor event type 'process_exec'");
    }

    #[test]
    fn linux_sensor_rejects_blank_event_type_filter() {
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  exclude_event_types: [\"  \"]",
            "type: stdout",
        );
        assert_config_err!(yaml, "sensor.exclude_event_types entries must not be empty");
    }

    #[test]
    fn linux_sensor_trims_whitespace_padded_event_types() {
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  include_event_types: [\" exec \", \"  tcp_connect\"]\n  exclude_event_types: [\"exit  \"]",
            "type: stdout",
        );
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
        let yaml = single_pipeline_yaml(
            "type: host_metrics\nsensor:\n  include_event_types: [exec]",
            "type: stdout",
        );
        assert_config_err!(
            yaml,
            "sensor.include_event_types and sensor.exclude_event_types are only supported for linux_ebpf_sensor inputs"
        );
    }

    #[test]
    fn host_metrics_accepts_string_max_process_rows_per_poll() {
        let yaml = single_pipeline_yaml(
            "type: host_metrics\nsensor:\n  max_process_rows_per_poll: \"1024\"",
            "type: stdout",
        );
        let cfg = Config::load_str(yaml).expect("string max_process_rows_per_poll should parse");
        let InputTypeConfig::HostMetrics(config) = &cfg.pipelines["default"].inputs[0].type_config
        else {
            panic!("expected host_metrics input");
        };
        assert_eq!(
            config
                .sensor
                .as_ref()
                .and_then(|sensor| sensor.max_process_rows_per_poll),
            Some(1024)
        );
    }

    #[test]
    fn max_open_files_zero_rejected() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log\nmax_open_files: 0",
            "type: stdout",
        );
        assert_config_err!(yaml, "max_open_files must be at least 1");
    }

    #[test]
    fn max_open_files_one_accepted() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log\nmax_open_files: 1",
            "type: stdout",
        );
        Config::load_str(yaml).expect("max_open_files: 1 should be valid");
    }

    #[test]
    fn max_open_files_absent_accepted() {
        let yaml = single_pipeline_yaml("type: file\npath: /var/log/test.log", "type: stdout");
        Config::load_str(yaml).expect("absent max_open_files should be valid");
    }

    #[test]
    fn adaptive_fast_polls_max_zero_accepted_for_file() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log\nadaptive_fast_polls_max: 0",
            "type: stdout",
        );
        Config::load_str(yaml).expect("adaptive_fast_polls_max: 0 should be valid for file");
    }

    #[test]
    fn adaptive_fast_polls_max_custom_accepted_for_file() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log\nadaptive_fast_polls_max: 12",
            "type: stdout",
        );
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
    fn tcp_input_accepts_max_clients() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        max_clients: 2048
    outputs:
      - type: "null"
"#;
        let config = Config::load_str(yaml).expect("tcp input must accept max_clients");
        match &config.pipelines["test"].inputs[0].type_config {
            InputTypeConfig::Tcp(tcp) => {
                assert_eq!(tcp.max_clients, Some(2048));
            }
            _ => panic!("Expected TCP input config"),
        }
    }

    #[test]
    fn tcp_input_rejects_max_clients_zero() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        max_clients: 0
    outputs:
      - type: "null"
"#;
        assert_config_err!(yaml, "max_clients cannot be 0");
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
        assert_config_err!(yaml, "requires both tls.cert_file and tls.key_file");
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
        assert_config_err!(yaml, "require_client_auth requires tls.client_ca_file");
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
        assert_config_err!(
            yaml,
            "client_ca_file requires tls.require_client_auth: true"
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
