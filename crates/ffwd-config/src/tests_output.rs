//! Output type tests: type acceptance, field rejection, null output handling,
//! auth, elasticsearch, Loki, file output, V2 config, and example parsing.

#[cfg(test)]
mod tests {
    use crate::test_yaml::single_pipeline_yaml;
    use crate::*;
    use std::fs;

    fn elasticsearch_request_mode(output: &OutputConfigV2) -> Option<ElasticsearchRequestMode> {
        match output {
            OutputConfigV2::Elasticsearch(config) => config.request_mode,
            _ => None,
        }
    }

    #[test]
    fn legacy_output_aliases_are_rejected() {
        for alias in ["file_out", "tcp_out", "udp_out"] {
            let extra = match alias {
                "file_out" => "path: /tmp/out.ndjson",
                "tcp_out" | "udp_out" => "endpoint: 127.0.0.1:5140",
                _ => unreachable!(),
            };
            let yaml = single_pipeline_yaml(
                "type: file\npath: /tmp/x.log",
                &format!("type: {alias}\n{extra}"),
            );
            let err = Config::load_str(yaml).expect_err("legacy alias should fail");
            assert!(
                err.to_string().contains("unknown variant"),
                "expected unknown variant error for {alias}: {err}"
            );
        }
    }

    #[test]
    fn http_output_is_rejected() {
        // In the pipelines form, http output without an endpoint is rejected.
        let yaml = single_pipeline_yaml("type: file\npath: /tmp/x.log", "type: http");
        assert_config_err!(yaml, "endpoint");
    }

    #[test]
    fn http_output_rejects_non_json_format() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log",
            "type: http\nendpoint: http://localhost:9200\nformat: text",
        );
        let err = Config::load_str(yaml).expect_err("http output must stay json-only");
        let msg = err.to_string();
        assert!(
            msg.contains("http output only supports format json"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn udp_output_rejects_zero_max_datagram_size() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log",
            "type: udp\nendpoint: 127.0.0.1:9001\nmax_datagram_size_bytes: 0",
        );
        let err = Config::load_str(yaml).expect_err("zero datagram size should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("udp.max_datagram_size_bytes must be at least 1"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn output_rejects_unwired_sink_knobs() {
        let cases = [
            (
                "tcp encoding",
                "type: tcp\nendpoint: 127.0.0.1:9000\nencoding: json",
                "encoding",
            ),
            (
                "udp max datagram",
                "type: udp\nendpoint: 127.0.0.1:9001\nmax_datagram_size_bytes: 1400",
                "max_datagram_size_bytes",
            ),
            (
                "arrow ipc batch size",
                "type: arrow_ipc\nendpoint: http://localhost:4317\nbatch_size: 1024",
                "batch_size",
            ),
        ];

        for (name, output_body, field) in cases {
            let yaml = single_pipeline_yaml("type: file\npath: /tmp/x.log", output_body);
            let err = match Config::load_str(yaml) {
                Ok(_) => panic!("{name}: unwired field '{field}' should fail validation"),
                Err(err) => err,
            };
            let msg = err.to_string();
            assert!(
                msg.contains("does not support") && msg.contains(field),
                "{name}: unexpected error: {msg}"
            );
        }
    }

    #[test]
    fn file_output_requires_path() {
        let yaml = single_pipeline_yaml("type: file\npath: /var/log/test.log", "type: file");
        assert_config_err!(yaml, "file output requires 'path'");
    }

    #[test]
    fn file_output_accepts_path() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log",
            "type: file\npath: /tmp/out.ndjson",
        );
        let cfg = Config::load_str(yaml).unwrap();
        assert_eq!(
            cfg.pipelines["default"].outputs[0].output_type(),
            OutputType::File
        );
    }

    #[test]
    fn file_output_empty_path_rejected() {
        // Regression test for #1663: path: "" passed --validate but failed at startup.
        let yaml = single_pipeline_yaml("type: file\npath: /tmp/x.log", "type: file\npath: \"\"");
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("path' must not be empty") || msg.contains("path must not be empty"),
            "expected 'path must not be empty' in error, got: {msg}"
        );
    }

    #[test]
    fn file_output_rejects_console_format() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log",
            "type: file\npath: /tmp/out.ndjson\nformat: console",
        );
        assert_config_err!(yaml, "file output only supports format json or text");
    }

    #[test]
    fn stdout_output_accepts_console_format() {
        // console is a valid stdout format — build_sink_factory maps it to
        // StdoutFormat::Console, so validation must not reject it (#1465 regression fix).
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log",
            "type: stdout\nformat: console",
        );
        Config::load_str(yaml).expect("stdout with format: console should be valid");
    }

    #[test]
    fn file_output_rejects_unwired_compression() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log",
            "type: file\npath: /tmp/out.ndjson\ncompression: zstd",
        );
        let err = Config::load_str(yaml).expect_err("file output compression is not wired");
        assert!(
            err.to_string()
                .contains("does not support 'compression' yet"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn loki_output_rejects_unwired_compression() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log",
            "type: loki\nendpoint: http://localhost:3100\ncompression: gzip",
        );
        let err = Config::load_str(yaml).expect_err("loki output compression is not wired");
        assert!(
            err.to_string()
                .contains("does not support 'compression' yet"),
            "unexpected error: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Auth tests
    // -----------------------------------------------------------------------

    #[test]
    fn auth_bearer_token() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            "type: otlp\nendpoint: http://localhost:4318/v1/logs\nauth:\n  bearer_token: \"my-secret-token\"",
        );
        let cfg = Config::load_str(yaml).expect("auth bearer_token");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth().expect("auth present");
        assert_eq!(auth.bearer_token.as_deref(), Some("my-secret-token"));
        assert!(auth.headers.is_empty());
    }

    #[test]
    fn auth_custom_headers() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            "type: otlp\nendpoint: http://localhost:4318/v1/logs\nauth:\n  headers:\n    X-API-Key: \"supersecret\"\n    X-Tenant: \"acme\"",
        );
        let cfg = Config::load_str(yaml).expect("auth custom headers");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth().expect("auth present");
        assert_eq!(auth.bearer_token, None);
        assert_eq!(
            auth.headers.get("X-API-Key").map(String::as_str),
            Some("supersecret")
        );
        assert_eq!(
            auth.headers.get("X-Tenant").map(String::as_str),
            Some("acme")
        );
    }

    #[test]
    fn auth_env_var_bearer_token() {
        // SAFETY: test is not run concurrently with other tests that modify this var.
        unsafe { std::env::set_var("FFWD_TEST_TOKEN", "env-bearer-token") };
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            "type: otlp\nendpoint: http://localhost:4318/v1/logs\nauth:\n  bearer_token: \"${FFWD_TEST_TOKEN}\"",
        );
        let cfg = Config::load_str(yaml).expect("auth env var bearer");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth().expect("auth present");
        assert_eq!(auth.bearer_token.as_deref(), Some("env-bearer-token"));
        // SAFETY: this test is not run concurrently with other tests that
        // depend on the same environment variable.
        unsafe { std::env::remove_var("FFWD_TEST_TOKEN") };
    }

    #[test]
    fn auth_absent_is_none() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            "type: otlp\nendpoint: http://localhost:4318/v1/logs",
        );
        let cfg = Config::load_str(yaml).expect("no auth");
        let pipe = &cfg.pipelines["default"];
        assert!(pipe.outputs[0].auth().is_none());
    }

    // -----------------------------------------------------------------------
    // Elasticsearch tests
    // -----------------------------------------------------------------------

    #[test]
    fn elasticsearch_request_mode_accepts_streaming() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            "type: elasticsearch\nendpoint: http://localhost:9200\nrequest_mode: streaming",
        );
        let cfg = Config::load_str(yaml).expect("streaming request_mode should validate");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            elasticsearch_request_mode(&pipe.outputs[0]),
            Some(ElasticsearchRequestMode::Streaming)
        );
    }

    #[test]
    fn elasticsearch_request_mode_rejects_unknown_value() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            "type: elasticsearch\nendpoint: http://localhost:9200\nrequest_mode: fancy",
        );
        assert_config_err!(yaml, "fancy", "buffered", "streaming");
    }

    #[test]
    fn elasticsearch_streaming_rejects_gzip() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            "type: elasticsearch\nendpoint: http://localhost:9200\ncompression: gzip\nrequest_mode: streaming",
        );
        assert_config_err!(yaml, "does not support gzip");
    }

    #[test]
    fn non_elasticsearch_request_mode_is_rejected() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            "type: otlp\nendpoint: http://localhost:4318/v1/logs\nrequest_mode: streaming",
        );
        assert_config_err!(yaml, "unknown field", "request_mode");
    }

    // -----------------------------------------------------------------------
    // Null output tests
    // -----------------------------------------------------------------------

    #[test]
    fn unquoted_type_null_is_rejected_simple_layout() {
        // Unquoted `type: null` is YAML null — must be rejected, not silently
        // routed to the Null sink.
        let yaml = r"
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: null
";
        assert_config_err!(yaml, "invalid type: unit value");
    }

    #[test]
    fn empty_output_type_with_endpoint_is_rejected() {
        let yaml = r"
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type:
        endpoint: https://collector:4318
";
        assert_config_err!(yaml, "invalid type: unit value");
    }

    #[test]
    fn unquoted_type_null_is_rejected_advanced_list_layout() {
        // Same as above but in the advanced pipelines layout.
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: null
";
        assert_config_err!(yaml, "invalid type: unit value", "pipelines.app.outputs[0]");
    }

    #[test]
    fn quoted_type_null_creates_null_sink() {
        // `type: "null"` (quoted string) creates the Null sink intentionally.
        let yaml = single_pipeline_yaml("type: file\npath: /tmp/x.log", "type: \"null\"");
        let cfg = Config::load_str(yaml).expect("type: \"null\" quoted");
        assert_eq!(
            cfg.pipelines["default"].outputs[0].output_type(),
            OutputType::Null
        );
    }

    #[test]
    fn quoted_null_output_rejects_endpoint() {
        let yaml = r#"
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: "null"
        endpoint: https://collector:4318
"#;
        assert_config_err!(yaml, "unknown field `endpoint`", "expected `name`");
    }

    #[test]
    fn quoted_null_output_rejects_unrelated_output_fields() {
        for (field, value) in [
            ("auth", "\n    bearer_token: secret"),
            ("batch_size", "100"),
            ("compression", "gzip"),
            ("format", "json"),
            ("headers", "\n    x-test: value"),
            ("index", "logs"),
            ("path", "/tmp/out.log"),
            ("protocol", "grpc"),
            ("request_timeout_ms", "1000"),
            ("tls", "\n    ca_file: /tmp/ca.pem"),
        ] {
            let yaml = single_pipeline_yaml(
                "type: file\npath: /tmp/x.log",
                &format!("type: \"null\"\n{field}: {value}"),
            );
            let err = Config::load_str(yaml).unwrap_err();
            let msg = err.to_string();
            let unknown_field = format!("unknown field `{field}`");
            assert!(
                msg.contains(&unknown_field) && msg.contains("expected `name`"),
                "explicit null output with {field} must be rejected: {msg}"
            );
        }
    }

    #[test]
    fn whole_output_null_is_rejected() {
        let yaml = r"
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/x.log
";
        assert_config_err!(yaml, "has no outputs");
    }

    #[test]
    fn missing_output_type_is_rejected() {
        let yaml = r"
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - {}
";
        assert_config_err!(yaml, "missing configuration field");
    }

    #[test]
    fn empty_string_output_type_is_rejected() {
        let yaml = "pipelines:\n  default:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: \"\"\n";
        assert_config_err!(yaml, "unknown variant ``");
    }

    #[test]
    fn null_output_type_field_is_rejected() {
        // `type: ~` (YAML null as type value) must not silently become the Null
        // sink — that would cause silent data loss.
        let yaml = "pipelines:\n  default:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: ~\n";
        assert_config_err!(yaml, "invalid type: unit value");
    }

    // -----------------------------------------------------------------------
    // Output-side field rejection
    // -----------------------------------------------------------------------

    #[test]
    fn non_elasticsearch_output_rejects_index() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        index: my-index
";
        assert_config_err!(yaml, "index");
    }

    #[test]
    fn non_otlp_output_rejects_protocol() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: http://localhost:9200
        protocol: grpc
";
        assert_config_err!(yaml, "protocol");
    }

    #[test]
    fn stdout_output_rejects_compression() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        compression: zstd
";
        assert_config_err!(yaml, "compression");
    }

    #[test]
    fn stdout_output_rejects_auth() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        auth:
          bearer_token: "secret"
"#;
        assert_config_err!(yaml, "auth");
    }

    #[test]
    fn stdout_output_rejects_path() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        path: /tmp/out.log
";
        assert_config_err!(yaml, "path");
    }

    // -----------------------------------------------------------------------
    // Loki-only field rejection
    // -----------------------------------------------------------------------

    #[test]
    fn non_loki_output_rejects_tenant_id() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        tenant_id: my-tenant
";
        assert_config_err!(yaml, "unknown field", "tenant_id");
    }

    #[test]
    fn non_loki_output_rejects_static_labels() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        static_labels:
          env: prod
";
        assert_config_err!(yaml, "unknown field", "static_labels");
    }

    #[test]
    fn non_loki_output_rejects_label_columns() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        label_columns:
          - container_name
";
        assert_config_err!(yaml, "unknown field", "label_columns");
    }

    // -----------------------------------------------------------------------
    // Arrow IPC output
    // -----------------------------------------------------------------------

    /// Regression: arrow_ipc output with `compression: gzip` must be rejected.
    /// Only `zstd` and `none` are supported. Before the fix, gzip was silently
    /// accepted and would fail at runtime.
    #[test]
    fn arrow_ipc_output_rejects_gzip_compression() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: arrow_ipc
        endpoint: http://localhost:4317
        compression: gzip
";
        assert_config_err!(
            yaml,
            "arrow_ipc output only supports 'zstd' or 'none'",
            "'gzip'"
        );
    }

    #[test]
    fn arrow_ipc_output_accepts_none_compression() {
        let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: arrow_ipc
        endpoint: http://localhost:4317
        compression: none
";
        Config::load_str(yaml).expect("arrow_ipc output should accept none compression");
    }

    // -----------------------------------------------------------------------
    // OutputConfigV2 direct deserialization tests
    // -----------------------------------------------------------------------

    #[test]
    fn output_config_v2_rejects_irrelevant_variant_fields() {
        let yaml = r"
type: otlp
endpoint: http://localhost:4318/v1/logs
format: json
";

        let err = serde_yaml_ng::from_str::<OutputConfigV2>(yaml).unwrap_err();
        assert!(
            err.to_string().contains("format"),
            "strict v2 otlp config should reject format, got: {err}"
        );
    }

    #[test]
    fn output_config_v2_rejects_invalid_enum_values_at_parse_time() {
        let cases = [
            (
                "type: otlp\nendpoint: http://localhost:4318/v1/logs\nprotocol: websocket\n",
                "websocket",
                &["http", "grpc"][..],
            ),
            (
                "type: otlp\nendpoint: http://localhost:4318/v1/logs\ncompression: lz4\n",
                "lz4",
                &["gzip", "zstd", "none"][..],
            ),
            (
                "type: elasticsearch\nendpoint: http://localhost:9200\nrequest_mode: fancy\n",
                "fancy",
                &["buffered", "streaming"][..],
            ),
        ];

        for (yaml, bad_value, expected_variants) in cases {
            let err = serde_yaml_ng::from_str::<OutputConfigV2>(yaml).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains(bad_value)
                    && expected_variants
                        .iter()
                        .all(|variant| msg.contains(variant)),
                "expected enum parse rejection mentioning {bad_value} and valid variants {expected_variants:?}: {msg}"
            );
        }
    }

    #[test]
    fn output_config_v2_variants_normalize_to_output_types() {
        let cases = [
            (
                "type: otlp\nendpoint: http://localhost:4318/v1/logs\n",
                OutputType::Otlp,
            ),
            (
                "type: http\nendpoint: http://localhost:8080/ingest\n",
                OutputType::Http,
            ),
            (
                "type: elasticsearch\nendpoint: http://localhost:9200\nindex: logs\n",
                OutputType::Elasticsearch,
            ),
            (
                "type: loki\nendpoint: http://localhost:3100/loki/api/v1/push\n",
                OutputType::Loki,
            ),
            ("type: stdout\nformat: json\n", OutputType::Stdout),
            ("type: file\npath: /tmp/ff.log\n", OutputType::File),
            ("type: \"null\"\n", OutputType::Null),
            ("type: tcp\nendpoint: 127.0.0.1:9000\n", OutputType::Tcp),
            ("type: udp\nendpoint: 127.0.0.1:9001\n", OutputType::Udp),
            (
                "type: arrow_ipc\nendpoint: http://localhost:4317\n",
                OutputType::ArrowIpc,
            ),
        ];

        for (yaml, expected_type) in cases {
            let output = serde_yaml_ng::from_str::<OutputConfigV2>(yaml).unwrap();
            assert_eq!(output.output_type(), expected_type);
        }
    }

    #[test]
    fn example_outputs_parse_as_v2() {
        use serde::Deserialize;
        use serde_yaml_ng::Value;

        let examples_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/use-cases");
        let mut checked = 0usize;

        for entry in fs::read_dir(&examples_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
                continue;
            }

            let yaml = fs::read_to_string(&path).unwrap();
            let value: Value = serde_yaml_ng::from_str(&yaml)
                .unwrap_or_else(|err| panic!("{} should parse as YAML: {err}", path.display()));
            for output_value in collect_output_values(&value) {
                OutputConfigV2::deserialize(output_value.clone()).unwrap_or_else(|err| {
                    panic!("{} output should parse as v2: {err}", path.display())
                });
                checked += 1;
            }
        }

        assert!(checked > 0, "expected example configs to contain outputs");
    }

    fn collect_output_values(value: &serde_yaml_ng::Value) -> Vec<serde_yaml_ng::Value> {
        let Some(root) = value.as_mapping() else {
            return Vec::new();
        };
        let mut outputs = Vec::new();

        if let Some(output) = root.get(serde_yaml_ng::Value::String("output".to_string())) {
            outputs.push(output.clone());
        }

        if let Some(pipelines) = root
            .get(serde_yaml_ng::Value::String("pipelines".to_string()))
            .and_then(serde_yaml_ng::Value::as_mapping)
        {
            for pipeline in pipelines.values() {
                let Some(pipeline) = pipeline.as_mapping() else {
                    continue;
                };
                if let Some(output_value) =
                    pipeline.get(serde_yaml_ng::Value::String("outputs".to_string()))
                {
                    match output_value {
                        serde_yaml_ng::Value::Sequence(values) => outputs.extend_from_slice(values),
                        value => outputs.push(value.clone()),
                    }
                }
            }
        }

        outputs
    }
}
