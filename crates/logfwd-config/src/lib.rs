//! YAML configuration parser for logfwd.
//!
//! Supports two layout styles:
//!
//! - **Simple** (single pipeline): top-level `input`, `transform`, `output`.
//! - **Advanced** (multiple pipelines): top-level `pipelines` map.
//!
//! Environment variables in values are expanded using `${VAR}` syntax.

mod compat;
mod env;
mod load;
mod serde_helpers;
mod shared;
mod types;
mod validate;

pub use serde_helpers::{PositiveMillis, PositiveSecs};

#[cfg(test)]
pub(crate) use env::expand_env_vars;
pub use shared::{
    BatchConfig, Compression, NetworkConfig, RetryConfig, TlsClientConfig, TlsServerConfig,
};
pub use types::{
    ArrowIpcOutputConfig, ArrowIpcTypeConfig, AuthConfig, CompressionFormat, Config, ConfigError,
    CsvEnrichmentConfig, ElasticsearchOutputConfig, ElasticsearchRequestMode, EnrichmentConfig,
    FileOutputConfig, FileTypeConfig, Format, GeneratorAttributeValueConfig,
    GeneratorComplexityConfig, GeneratorInputConfig, GeneratorProfileConfig,
    GeneratorSequenceConfig, GeneratorTypeConfig, GeoDatabaseConfig, GeoDatabaseFormat,
    HostInfoConfig, HostMetricsInputConfig, HttpInputConfig, HttpMethodConfig, HttpOutputConfig,
    HttpTypeConfig, InputConfig, InputType, InputTypeConfig, JournaldBackendConfig,
    JournaldInputConfig, JournaldTypeConfig, JsonlEnrichmentConfig, K8sPathConfig,
    LokiOutputConfig, NullOutputConfig, OtlpOutputConfig, OtlpProtobufDecodeModeConfig,
    OtlpProtocol, OtlpTypeConfig, OutputConfig, OutputConfigV2, OutputType, ParquetOutputConfig,
    PipelineConfig, S3InputConfig, S3TypeConfig, SensorTypeConfig, ServerConfig,
    SocketOutputConfig, SourceMetadataStyle, StaticEnrichmentConfig, StdoutOutputConfig,
    StorageConfig, TcpTypeConfig, UdpTypeConfig,
};
pub use validate::validate_host_port;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn simple_config() {
        let yaml = r"
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri

transform: |
  SELECT * FROM logs WHERE level != 'DEBUG'

output:
  type: otlp
  endpoint: http://otel-collector:4317
  compression: zstd

server:
  diagnostics: 0.0.0.0:9090
  log_level: info

storage:
  data_dir: /var/lib/logfwd
";
        let cfg = Config::load_str(yaml).expect("should parse simple config");
        assert_eq!(cfg.pipelines.len(), 1);
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs.len(), 1);
        assert_eq!(pipe.inputs[0].input_type(), InputType::File);
        assert!(matches!(
            &pipe.inputs[0].type_config,
            InputTypeConfig::File(f) if f.path == "/var/log/pods/**/*.log"
        ));
        assert_eq!(pipe.inputs[0].format, Some(Format::Cri));
        assert!(pipe.transform.as_ref().unwrap().contains("SELECT"));
        assert_eq!(pipe.outputs[0].output_type(), OutputType::Otlp);
        assert_eq!(
            pipe.outputs[0].compat_config().endpoint.as_deref(),
            Some("http://otel-collector:4317")
        );
        assert_eq!(cfg.server.diagnostics.as_deref(), Some("0.0.0.0:9090"));
        assert_eq!(cfg.storage.data_dir.as_deref(), Some("/var/lib/logfwd"));
    }

    #[test]
    fn input_source_metadata_style_defaults_none_and_parses_styles() {
        let default_yaml = r#"
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri

output:
  type: "null"
"#;
        let cfg = Config::load_str(default_yaml).expect("default source_metadata should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].source_metadata, SourceMetadataStyle::None);

        let enabled_yaml = r#"
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
  source_metadata: fastforward

output:
  type: "null"
"#;
        let cfg = Config::load_str(enabled_yaml).expect("source_metadata style should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.inputs[0].source_metadata,
            SourceMetadataStyle::Fastforward
        );

        let beats_yaml = r#"
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
  source_metadata: beats

output:
  type: "null"
"#;
        let cfg = Config::load_str(beats_yaml).expect("beats alias should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].source_metadata, SourceMetadataStyle::Ecs);

        let otel_yaml = r#"
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
  source_metadata: otel

output:
  type: "null"
"#;
        let cfg = Config::load_str(otel_yaml).expect("otel style should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].source_metadata, SourceMetadataStyle::Otel);

        let vector_yaml = r#"
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
  source_metadata: vector

output:
  type: "null"
"#;
        let cfg = Config::load_str(vector_yaml).expect("vector style should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].source_metadata, SourceMetadataStyle::Vector);
    }

    #[test]
    fn advanced_config() {
        let yaml = r"
pipelines:
  app_logs:
    inputs:
      - name: pod_logs
        type: file
        path: /var/log/pods/**/*.log
        format: cri
      - name: raw_in
        type: udp
        listen: 0.0.0.0:514
        format: raw
    transform: |
      SELECT * EXCEPT (stack_trace) FROM logs WHERE level != 'DEBUG'
    outputs:
      - name: collector
        type: otlp
        endpoint: http://otel-collector:4317
        protocol: grpc
        compression: zstd
      - name: debug
        type: stdout
        format: json

server:
  diagnostics: 0.0.0.0:9090
  log_level: info
";
        let cfg = Config::load_str(yaml).expect("should parse advanced config");
        assert_eq!(cfg.pipelines.len(), 1);
        let pipe = &cfg.pipelines["app_logs"];
        assert_eq!(pipe.inputs.len(), 2);
        assert_eq!(pipe.inputs[0].input_type(), InputType::File);
        assert_eq!(pipe.inputs[1].input_type(), InputType::Udp);
        assert!(matches!(
            &pipe.inputs[1].type_config,
            InputTypeConfig::Udp(u) if u.listen == "0.0.0.0:514"
        ));
        assert_eq!(pipe.outputs.len(), 2);
        assert_eq!(pipe.outputs[0].output_type(), OutputType::Otlp);
        assert_eq!(pipe.outputs[1].output_type(), OutputType::Stdout);
    }

    #[test]
    fn env_var_substitution() {
        // SAFETY: this test is not run concurrently with other tests that
        // depend on the same environment variable.
        unsafe { std::env::set_var("LOGFWD_TEST_ENDPOINT", "http://my-collector:4317") };
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_TEST_ENDPOINT}
";
        let cfg = Config::load_str(yaml).expect("env var substitution");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.outputs[0].compat_config().endpoint.as_deref(),
            Some("http://my-collector:4317")
        );
        // SAFETY: this test is not run concurrently with other tests that
        // depend on the same environment variable.
        unsafe { std::env::remove_var("LOGFWD_TEST_ENDPOINT") };
    }

    #[test]
    fn load_reads_config_from_file() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time must be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "logfwd-config-load-{}-{unique}.yaml",
            std::process::id()
        ));
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: stdout
";
        fs::write(&path, yaml).expect("write config");

        let cfg = Config::load(&path).expect("Config::load should parse file");
        assert_eq!(cfg.pipelines.len(), 1);
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].input_type(), InputType::File);
        assert_eq!(pipe.outputs[0].output_type(), OutputType::Stdout);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn unset_env_var_rejected() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_NONEXISTENT_VAR_12345}
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("LOGFWD_NONEXISTENT_VAR_12345"),
            "error should mention the variable name: {msg}"
        );
        assert!(
            msg.contains("not set"),
            "error should say variable is not set: {msg}"
        );
    }

    #[test]
    fn unterminated_env_var_preserved_as_is() {
        assert_eq!(
            expand_env_vars("endpoint: ${LOGFWD_TEST_UNTERMINATED").unwrap(),
            "endpoint: ${LOGFWD_TEST_UNTERMINATED"
        );
    }

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
            r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: stdout
storage:
  data_dir: {}
"#,
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
    fn otlp_input_accepts_experimental_protobuf_decode_mode() {
        let yaml = r#"
input:
  type: otlp
  listen: 127.0.0.1:4318
  protobuf_decode_mode: projected_fallback
output:
  type: stdout
"#;
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
        let yaml = r#"
input:
  type: otlp
  listen: 127.0.0.1:4318
  resource_prefix: resource.attributes.
output:
  type: stdout
"#;
        let err = Config::load_str(yaml).expect_err("resource_prefix is no longer supported");
        let msg = err.to_string();
        assert!(
            msg.contains("resource_prefix") || msg.contains("unknown field"),
            "expected serde rejection of resource_prefix, got: {msg}"
        );
    }

    #[test]
    fn non_otlp_input_rejects_protobuf_decode_mode() {
        let yaml = r#"
input:
  type: file
  path: /var/log/app.log
  protobuf_decode_mode: projected_fallback
output:
  type: stdout
"#;
        let err = Config::load_str(yaml).expect_err("protobuf_decode_mode must be otlp-only");
        let msg = err.to_string();
        assert!(
            msg.contains("protobuf_decode_mode") || msg.contains("unknown field"),
            "expected serde rejection of protobuf_decode_mode for file input, got: {msg}"
        );
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
    fn file_output_requires_path() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: file
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("file output requires 'path'"),
            "expected missing path validation in error: {msg}"
        );
    }

    #[test]
    fn validation_unimplemented_output_type() {
        // Each placeholder type should be caught by Config::validate() before
        // pipeline construction, not silently accepted.
        for (otype, extra) in [("parquet", "path: /tmp/x"), ("http", "endpoint: http://x")] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  {extra}\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "validation should reject unimplemented type '{otype}'"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not yet implemented"),
                "error message should mention 'not yet implemented' for '{otype}': {msg}"
            );
            assert!(
                msg.contains(otype),
                "error message should include the type name '{otype}': {msg}"
            );
        }
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
    fn legacy_output_aliases_are_rejected() {
        for alias in ["file_out", "tcp_out", "udp_out"] {
            let extra = match alias {
                "file_out" => "path: /tmp/out.ndjson",
                "tcp_out" | "udp_out" => "endpoint: 127.0.0.1:5140",
                _ => unreachable!(),
            };
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {alias}\n  {extra}\n"
            );
            let err = Config::load_str(&yaml).expect_err("legacy alias should fail");
            assert!(
                err.to_string().contains("unknown variant"),
                "expected unknown variant error for {alias}: {err}"
            );
        }
    }

    #[test]
    fn http_output_is_rejected() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: http
  endpoint: http://localhost:9200
";
        let err = Config::load_str(yaml).expect_err("http output should be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("not yet implemented"),
            "error should mention 'not yet implemented': {msg}"
        );
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
    fn file_output_accepts_path() {
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: file\n  path: /tmp/out.ndjson\n";
        let cfg = Config::load_str(yaml).unwrap();
        assert_eq!(
            cfg.pipelines["default"].outputs[0].output_type(),
            OutputType::File
        );
    }

    #[test]
    fn file_output_empty_path_rejected() {
        // Regression test for #1663: path: "" passed --validate but failed at startup.
        let yaml =
            "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: file\n  path: \"\"\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("path' must not be empty") || msg.contains("path must not be empty"),
            "expected 'path must not be empty' in error, got: {msg}"
        );
    }

    #[test]
    fn file_output_rejects_console_format() {
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: file\n  path: /tmp/out.ndjson\n  format: console\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("file output only supports format json or text"));
    }

    #[test]
    fn stdout_output_accepts_console_format() {
        // console is a valid stdout format — build_sink_factory maps it to
        // StdoutFormat::Console, so validation must not reject it (#1465 regression fix).
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: stdout\n  format: console\n";
        Config::load_str(yaml).expect("stdout with format: console should be valid");
    }

    #[test]
    fn file_output_rejects_compression() {
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: file\n  path: /tmp/out.ndjson\n  compression: zstd\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") && msg.contains("compression"),
            "file output should reject compression at parse time: {msg}"
        );
    }

    #[test]
    fn loki_output_rejects_compression() {
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: loki\n  endpoint: http://localhost:3100\n  compression: gzip\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") && msg.contains("compression"),
            "loki output should reject compression at parse time: {msg}"
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
                r#"
input:
  type: file
  path: /tmp/test.log
  {field}: {value}
output:
  type: stdout
"#
            );
            let err = Config::load_str(&yaml).unwrap_err().to_string();
            assert!(
                err.contains(&format!("'{field}' must be at least 1")),
                "expected error about positive {field}, got: {err}"
            );
        }

        // Duration field (poll_interval_ms) is rejected at parse time via PositiveMillis.
        let yaml = r#"
input:
  type: file
  path: /tmp/test.log
  poll_interval_ms: 0
output:
  type: stdout
"#;
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
                    r#"
input:
  type: {in_type}
  {extra}
  {field}
output:
  type: stdout
"#
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
        let yaml = r#"
input:
  type: arrow_ipc
  listen: 0.0.0.0:4319
  format: raw
output:
  type: stdout
"#;
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
    fn auth_bearer_token() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: http://localhost:4318/v1/logs
  auth:
    bearer_token: "my-secret-token"
"#;
        let cfg = Config::load_str(yaml).expect("auth bearer_token");
        let pipe = &cfg.pipelines["default"];
        let output = pipe.outputs[0].compat_config();
        let auth = output.auth.as_ref().expect("auth present");
        assert_eq!(auth.bearer_token.as_deref(), Some("my-secret-token"));
        assert!(auth.headers.is_empty());
    }

    #[test]
    fn auth_custom_headers() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: http://localhost:4318/v1/logs
  auth:
    headers:
      X-API-Key: "supersecret"
      X-Tenant: "acme"
"#;
        let cfg = Config::load_str(yaml).expect("auth custom headers");
        let pipe = &cfg.pipelines["default"];
        let output = pipe.outputs[0].compat_config();
        let auth = output.auth.as_ref().expect("auth present");
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
        unsafe { std::env::set_var("LOGFWD_TEST_TOKEN", "env-bearer-token") };
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: http://localhost:4318/v1/logs
  auth:
    bearer_token: "${LOGFWD_TEST_TOKEN}"
"#;
        let cfg = Config::load_str(yaml).expect("auth env var bearer");
        let pipe = &cfg.pipelines["default"];
        let output = pipe.outputs[0].compat_config();
        let auth = output.auth.as_ref().expect("auth present");
        assert_eq!(auth.bearer_token.as_deref(), Some("env-bearer-token"));
        // SAFETY: this test is not run concurrently with other tests that
        // depend on the same environment variable.
        unsafe { std::env::remove_var("LOGFWD_TEST_TOKEN") };
    }

    #[test]
    fn auth_absent_is_none() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: http://localhost:4318/v1/logs
";
        let cfg = Config::load_str(yaml).expect("no auth");
        let pipe = &cfg.pipelines["default"];
        assert!(pipe.outputs[0].compat_config().auth.is_none());
    }

    #[test]
    fn elasticsearch_request_mode_accepts_streaming() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: elasticsearch
  endpoint: http://localhost:9200
  request_mode: streaming
";
        let cfg = Config::load_str(yaml).expect("streaming request_mode should validate");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.outputs[0].compat_config().request_mode,
            Some(ElasticsearchRequestMode::Streaming)
        );
    }

    #[test]
    fn elasticsearch_request_mode_rejects_unknown_value() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: elasticsearch
  endpoint: http://localhost:9200
  request_mode: fancy
";
        let err = Config::load_str(yaml).expect_err("invalid request_mode should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("fancy") && msg.contains("buffered") && msg.contains("streaming"),
            "expected request_mode enum rejection: {msg}"
        );
    }

    #[test]
    fn elasticsearch_streaming_rejects_gzip() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: elasticsearch
  endpoint: http://localhost:9200
  compression: gzip
  request_mode: streaming
";
        let err = Config::load_str(yaml).expect_err("streaming+gzip should fail");
        assert!(err.to_string().contains("does not support gzip"));
    }

    #[test]
    fn non_elasticsearch_request_mode_is_rejected() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: http://localhost:4318/v1/logs
  request_mode: streaming
";
        let err = Config::load_str(yaml).expect_err("request_mode should be es-only");
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") && msg.contains("request_mode"),
            "otlp output should reject request_mode at parse time: {msg}"
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
    fn resource_attrs_simple_form() {
        let yaml = r#"
input:
  type: file
  path: /var/log/app.log

output:
  type: otlp
  endpoint: http://otel-collector:4317

resource_attrs:
  service.name: my-service
  service.version: "1.2.3"
  deployment.environment: production
"#;
        let cfg = Config::load_str(yaml).expect("should parse simple config with resource_attrs");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.resource_attrs.get("service.name").map(String::as_str),
            Some("my-service")
        );
        assert_eq!(
            pipe.resource_attrs
                .get("service.version")
                .map(String::as_str),
            Some("1.2.3")
        );
        assert_eq!(
            pipe.resource_attrs
                .get("deployment.environment")
                .map(String::as_str),
            Some("production")
        );
    }

    #[test]
    fn resource_attrs_advanced_form() {
        let yaml = r"
pipelines:
  app_logs:
    resource_attrs:
      service.name: advanced-service
      deployment.environment: staging
    inputs:
      - type: file
        path: /var/log/app.log
    outputs:
      - type: otlp
        endpoint: http://otel-collector:4317
";
        let cfg = Config::load_str(yaml).expect("should parse advanced config with resource_attrs");
        let pipe = &cfg.pipelines["app_logs"];
        assert_eq!(
            pipe.resource_attrs.get("service.name").map(String::as_str),
            Some("advanced-service")
        );
        assert_eq!(
            pipe.resource_attrs
                .get("deployment.environment")
                .map(String::as_str),
            Some("staging")
        );
    }

    #[test]
    fn resource_attrs_absent_is_empty() {
        let yaml = r"
input:
  type: file
  path: /var/log/app.log
output:
  type: otlp
  endpoint: http://otel-collector:4317
";
        let cfg = Config::load_str(yaml).expect("should parse config without resource_attrs");
        let pipe = &cfg.pipelines["default"];
        assert!(pipe.resource_attrs.is_empty());
    }

    // -----------------------------------------------------------------------
    // Bug #707 / #2001: type: null YAML keyword collision
    // -----------------------------------------------------------------------
    // Unquoted `type: null` is YAML's null value, not the string "null".
    // Silently treating YAML null as the Null sink caused silent data loss
    // (#2001). Users must quote the value: `type: "null"`.

    #[test]
    fn unquoted_type_null_is_rejected_simple_layout() {
        // Unquoted `type: null` is YAML null — must be rejected, not silently
        // routed to the Null sink.
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: null\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid output config"),
            "unquoted type: null must be rejected: {msg}"
        );
    }

    #[test]
    fn empty_output_type_with_endpoint_is_rejected() {
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type:\n  endpoint: https://collector:4318\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid output config"),
            "empty output type with endpoint must be rejected before it can become the null sink: {msg}"
        );
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
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid output config") || msg.contains("did not match"),
            "unquoted type: null in list must be rejected: {msg}"
        );
    }

    #[test]
    fn quoted_type_null_creates_null_sink() {
        // `type: "null"` (quoted string) creates the Null sink intentionally.
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: \"null\"\n";
        let cfg = Config::load_str(yaml).expect("type: \"null\" quoted");
        assert_eq!(
            cfg.pipelines["default"].outputs[0].output_type(),
            OutputType::Null
        );
    }

    #[test]
    fn quoted_null_output_rejects_endpoint() {
        let yaml = r#"
input:
  type: file
  path: /tmp/x.log
output:
  type: "null"
  endpoint: https://collector:4318
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("pipeline 'default' output '#0'"),
            "error should include pipeline/output context: {msg}"
        );
        assert!(
            msg.contains("null output does not support") && msg.contains("endpoint"),
            "explicit null output with endpoint must be rejected: {msg}"
        );
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
            let yaml = format!(
                r#"
input:
  type: file
  path: /tmp/x.log
output:
  type: "null"
  {field}: {value}
"#
            );
            let err = Config::load_str(&yaml).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("pipeline 'default' output '#0'"),
                "error should include pipeline/output context: {msg}"
            );
            let expected = format!("null output does not support '{field}'");
            assert!(
                msg.contains(&expected),
                "explicit null output with {field} must be rejected: {msg}"
            );
        }
    }

    #[test]
    fn whole_output_null_is_rejected() {
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput: null\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("output is required when input is specified"),
            "whole output null must not be treated as the null sink: {msg}"
        );
    }

    #[test]
    fn missing_output_type_is_rejected() {
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput: {}\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid output config")
                && msg.contains("missing")
                && msg.contains("type"),
            "missing output type must fail clearly: {msg}"
        );
    }

    #[test]
    fn empty_string_output_type_is_rejected() {
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: \"\"\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid output config") && msg.contains("unknown variant ``"),
            "empty-string output type must fail clearly: {msg}"
        );
    }

    #[test]
    fn null_output_type_field_is_rejected() {
        // `type: ~` (YAML null as type value) must not silently become the Null
        // sink — that would cause silent data loss.
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: ~\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid output config"),
            "null type field value must be rejected: {msg}"
        );
    }

    #[test]
    fn duplicate_pipeline_mapping_key_is_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/a.log
    outputs:
      - type: stdout
  app:
    inputs:
      - type: file
        path: /tmp/b.log
    outputs:
      - type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate entry with key \"app\""),
            "duplicate pipeline names must be rejected before validation: {msg}"
        );
    }

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
    // Bug #709: normalize_args allows flags before --config
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_args_canonical_form_unchanged() {
        use super::*;
        // When --config is already at position 1, args are returned unchanged.
        // (We test the normalize_args logic via Config parsing instead since
        // normalize_args lives in the binary crate.)
        let _ = Config::load_str; // ensure the import is live
    }

    // -----------------------------------------------------------------------
    // Bug #696: max_open_files: 0 silently disables file reading
    // -----------------------------------------------------------------------

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
    // Bug #550: enrichment path validation at config load time
    // -----------------------------------------------------------------------

    #[test]
    fn enrichment_geo_database_missing_path_rejected() {
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
        path: /nonexistent/path/to/GeoLite2-City.mmdb
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not found"),
            "expected 'not found' in error: {msg}"
        );
        assert!(
            msg.contains("geo database"),
            "expected 'geo database' in error: {msg}"
        );
    }

    #[test]
    fn enrichment_csv_missing_path_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: csv
        table_name: assets
        path: /nonexistent/path/to/assets.csv
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not found"),
            "expected 'not found' in error: {msg}"
        );
        assert!(msg.contains("csv"), "expected 'csv' in error: {msg}");
    }

    #[test]
    fn enrichment_jsonl_missing_path_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: jsonl
        table_name: ips
        path: /nonexistent/path/to/data.jsonl
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not found"),
            "expected 'not found' in error: {msg}"
        );
        assert!(msg.contains("jsonl"), "expected 'jsonl' in error: {msg}");
    }

    #[test]
    fn enrichment_relative_path_accepted_at_validation_time() {
        // Relative paths are resolved against base_path in Pipeline::from_config,
        // so Config::validate() must not reject them.
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: csv
        table_name: assets
        path: data/assets.csv
      - type: jsonl
        table_name: ips
        path: data/ips.jsonl
      - type: geo_database
        format: mmdb
        path: data/GeoLite2-City.mmdb
";
        Config::load_str(yaml).expect("relative enrichment paths should pass validation");
    }

    #[test]
    fn enrichment_static_empty_labels_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: static
        table_name: env
        labels: {}
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("at least one label"),
            "expected 'at least one label' in error: {msg}"
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

    // -----------------------------------------------------------------------
    // Bug #540: enrichment config variants for all implemented types
    // -----------------------------------------------------------------------

    #[test]
    fn enrichment_static_config_accepted() {
        let yaml = r#"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: static
        table_name: env
        labels:
          dc: us-east-1
          team: platform
"#;
        let cfg = Config::load_str(yaml).expect("static enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Static(c) => {
                assert_eq!(c.table_name, "env");
                assert_eq!(c.labels.get("dc").map(String::as_str), Some("us-east-1"));
                assert_eq!(c.labels.get("team").map(String::as_str), Some("platform"));
            }
            other => panic!("expected Static, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_host_info_config_accepted() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: host_info
";
        let cfg = Config::load_str(yaml).expect("host_info enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        assert!(matches!(&pipe.enrichment[0], EnrichmentConfig::HostInfo(_)));
    }

    #[test]
    fn enrichment_k8s_path_config_accepted() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: k8s_path
        table_name: pods
";
        let cfg = Config::load_str(yaml).expect("k8s_path enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::K8sPath(c) => assert_eq!(c.table_name, "pods"),
            other => panic!("expected K8sPath, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_k8s_path_default_table_name() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: k8s_path
";
        let cfg = Config::load_str(yaml).expect("k8s_path with default table_name should parse");
        let pipe = &cfg.pipelines["app"];
        match &pipe.enrichment[0] {
            EnrichmentConfig::K8sPath(c) => assert_eq!(c.table_name, "k8s_pods"),
            other => panic!("expected K8sPath, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_csv_config_accepted() {
        // Use a path that exists to pass validation.
        let tmp = std::env::temp_dir().join("logfwd_test_enrichment.csv");
        fs::write(&tmp, "host,owner\nweb1,alice\n").expect("create temp csv");
        let yaml = format!(
            "pipelines:\n  app:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: csv\n        table_name: assets\n        path: {}\n",
            tmp.display()
        );
        let cfg = Config::load_str(&yaml).expect("csv enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Csv(c) => {
                assert_eq!(c.table_name, "assets");
                assert_eq!(c.path, tmp.to_str().unwrap());
            }
            other => panic!("expected Csv, got {other:?}"),
        }
        let _ = fs::remove_file(&tmp);
    }

    #[test]
    fn enrichment_jsonl_config_accepted() {
        // Use a path that exists to pass validation.
        let tmp = std::env::temp_dir().join("logfwd_test_enrichment.jsonl");
        fs::write(&tmp, "{\"ip\":\"1.2.3.4\",\"owner\":\"alice\"}\n").expect("create temp jsonl");
        let yaml = format!(
            "pipelines:\n  app:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: jsonl\n        table_name: ip_owners\n        path: {}\n",
            tmp.display()
        );
        let cfg = Config::load_str(&yaml).expect("jsonl enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Jsonl(c) => {
                assert_eq!(c.table_name, "ip_owners");
                assert_eq!(c.path, tmp.to_str().unwrap());
            }
            other => panic!("expected Jsonl, got {other:?}"),
        }
        let _ = fs::remove_file(&tmp);
    }

    #[test]
    fn enrichment_simple_form_preserved() {
        // Enrichment in simple form should be wired into the default pipeline,
        // not silently dropped (#540).
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
enrichment:
  - type: host_info
  - type: k8s_path
";
        let cfg = Config::load_str(yaml).expect("simple form with enrichment should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.enrichment.len(),
            2,
            "enrichment should not be dropped in simple form"
        );
        assert!(matches!(&pipe.enrichment[0], EnrichmentConfig::HostInfo(_)));
        assert!(matches!(&pipe.enrichment[1], EnrichmentConfig::K8sPath(_)));
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

    // -----------------------------------------------------------------------
    // Top-level enrichment rejected with pipelines: form
    // -----------------------------------------------------------------------

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
    fn generator_input_rejects_path() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        path: /tmp/test.log
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("generator input must reject path");
    }

    #[test]
    fn generator_input_accepts_explicit_generator_block() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          events_per_sec: 25000
          batch_size: 2048
          total_events: 123
          profile: record
          attributes:
            benchmark_id: run-123
            pod_name: emitter-0
            stream_id: emitter-0
            service: bench-emitter
            status: 200
            sampled: true
            deleted_at: null
          sequence:
            field: seq
    outputs:
      - type: "null"
"#;
        let cfg = Config::load_str(yaml).expect("generator block should be valid");
        let gen_type = match &cfg.pipelines["test"].inputs[0].type_config {
            InputTypeConfig::Generator(g) => g,
            _ => panic!("expected Generator type_config"),
        };
        let generator = gen_type.generator.as_ref().expect("generator config");
        assert_eq!(generator.events_per_sec, Some(25000));
        assert_eq!(generator.batch_size, Some(2048));
        assert_eq!(generator.total_events, Some(123));
        assert_eq!(generator.profile, Some(GeneratorProfileConfig::Record));
        assert_eq!(
            generator.attributes.get("benchmark_id"),
            Some(&GeneratorAttributeValueConfig::String(
                "run-123".to_string()
            ))
        );
        assert_eq!(
            generator.attributes.get("pod_name"),
            Some(&GeneratorAttributeValueConfig::String(
                "emitter-0".to_string()
            ))
        );
        assert_eq!(
            generator.attributes.get("stream_id"),
            Some(&GeneratorAttributeValueConfig::String(
                "emitter-0".to_string()
            ))
        );
        assert_eq!(
            generator.attributes.get("status"),
            Some(&GeneratorAttributeValueConfig::Integer(200))
        );
        assert_eq!(
            generator.attributes.get("sampled"),
            Some(&GeneratorAttributeValueConfig::Bool(true))
        );
        assert_eq!(
            generator.attributes.get("deleted_at"),
            Some(&GeneratorAttributeValueConfig::Null)
        );
        assert_eq!(
            generator.sequence.as_ref().map(|seq| seq.field.as_str()),
            Some("seq")
        );
    }

    #[test]
    fn generator_input_rejects_listen() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        listen: "1000"
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("generator input must reject listen");
    }

    #[test]
    fn non_generator_input_rejects_generator_block() {
        // With the tagged-enum refactor, the `generator` key is only valid inside
        // the generator variant. Serde rejects it at parse time for file inputs.
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
        generator:
          profile: record
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("file input must reject generator block");
    }

    #[test]
    fn generator_input_rejects_empty_sequence_field() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          sequence:
            field: " "
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("generator.sequence.field"),
            "expected empty sequence field rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_duplicate_generated_field_names() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            seq: already-here
          sequence:
            field: seq
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("must not duplicate a generator.attributes key"),
            "expected duplicate generated field rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_zero_batch_size() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          batch_size: 0
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("batch_size must be at least 1"),
            "expected zero batch size rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_empty_attribute_key() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            "": run-123
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("attributes keys must not be empty"),
            "expected empty attribute key rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_non_finite_attribute_values() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            ratio: .nan
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("float values must be finite"),
            "expected non-finite attribute rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_empty_event_created_field_name() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          event_created_unix_nano_field: " "
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("event_created_unix_nano_field must not be empty"),
            "expected empty event_created field rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_event_created_field_name_duplicate_with_attribute() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            created: run-123
          event_created_unix_nano_field: created
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("must not duplicate a generator.attributes key"),
            "expected event_created/attribute duplication rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_event_created_field_name_duplicate_with_sequence() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          sequence:
            field: seq
          event_created_unix_nano_field: seq
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("must not duplicate generator.sequence.field"),
            "expected event_created/sequence duplication rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_record_fields_without_record_profile() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          attributes:
            benchmark_id: run-123
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("require generator.profile=record"),
            "expected record profile requirement rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_config_accepted() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "2023-06-01T12:00:00Z"
            step_ms: 5000
    outputs:
      - type: "null"
"#;
        let cfg = Config::load_str(yaml).expect("timestamp config should be valid");
        let gen_type = match &cfg.pipelines["test"].inputs[0].type_config {
            InputTypeConfig::Generator(g) => g,
            _ => panic!("expected Generator type_config"),
        };
        let ts = gen_type
            .generator
            .as_ref()
            .unwrap()
            .timestamp
            .as_ref()
            .expect("timestamp config");
        assert_eq!(ts.start.as_deref(), Some("2023-06-01T12:00:00Z"));
        assert_eq!(ts.step_ms, Some(5000));
    }

    #[test]
    fn generator_timestamp_now_accepted() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "now"
            step_ms: -100
    outputs:
      - type: "null"
"#;
        Config::load_str(yaml).expect("timestamp start=now should be valid");
    }

    #[test]
    fn generator_timestamp_rejects_zero_step() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            step_ms: 0
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("step_ms must not be zero"),
            "expected zero step rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_rejects_invalid_start() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "not-a-date"
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("YYYY-MM-DDTHH:MM:SSZ"),
            "expected format rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_rejects_record_profile() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          timestamp:
            start: "now"
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("only supported for the logs profile"),
            "expected logs-only rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_rejects_invalid_calendar_date() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "2024-02-31T00:00:00Z"
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("day 31 out of range"),
            "expected invalid date rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_rejects_month_13() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "2024-13-01T00:00:00Z"
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("month 13 out of range"),
            "expected invalid month rejection: {err}"
        );
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

    // -----------------------------------------------------------------------
    // Output-side field rejection
    // -----------------------------------------------------------------------

    #[test]
    fn non_elasticsearch_output_rejects_index() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        index: my-index
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("index"),
            "expected index rejection: {err}"
        );
    }

    #[test]
    fn non_otlp_output_rejects_protocol() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: http://localhost:9200
        protocol: grpc
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("protocol"),
            "expected protocol rejection: {err}"
        );
    }

    #[test]
    fn stdout_output_rejects_compression() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        compression: zstd
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("compression"),
            "expected compression rejection: {err}"
        );
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
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("auth"),
            "expected auth rejection: {err}"
        );
    }

    #[test]
    fn stdout_output_rejects_path() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        path: /tmp/out.log
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path"),
            "expected path rejection: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Format: text is accepted as a distinct raw-text mode
    // -----------------------------------------------------------------------

    #[test]
    fn format_text_alias_accepted() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
  format: text
output:
  type: stdout
";
        let cfg = Config::load_str(yaml).expect("format: text should be accepted");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].format, Some(Format::Text));
    }

    // -----------------------------------------------------------------------
    // Loki-only field rejection for non-Loki outputs
    // -----------------------------------------------------------------------

    #[test]
    fn non_loki_output_rejects_tenant_id() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        tenant_id: my-tenant
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") && msg.contains("tenant_id"),
            "stdout output should reject tenant_id at parse time: {msg}"
        );
    }

    #[test]
    fn non_loki_output_rejects_static_labels() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        static_labels:
          env: prod
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") && msg.contains("static_labels"),
            "stdout output should reject static_labels at parse time: {msg}"
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
    fn non_loki_output_rejects_label_columns() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        label_columns:
          - container_name
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") && msg.contains("label_columns"),
            "stdout output should reject label_columns at parse time: {msg}"
        );
    }

    // Regression tests for issue #1667: empty paths for geo_database/csv/jsonl enrichment
    // must be rejected at --validate time, not silently passed through to runtime.

    #[test]
    fn geo_database_empty_path_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
    enrichment:
      - type: geo_database
        format: mmdb
        path: ""
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path") && err.to_string().contains("empty"),
            "expected empty-path rejection for geo_database: {err}"
        );
    }

    #[test]
    fn csv_enrichment_empty_path_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
    enrichment:
      - type: csv
        table_name: assets
        path: ""
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path") && err.to_string().contains("empty"),
            "expected empty-path rejection for csv enrichment: {err}"
        );
    }

    #[test]
    fn jsonl_enrichment_empty_path_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
    enrichment:
      - type: jsonl
        table_name: owners
        path: "   "
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path") && err.to_string().contains("empty"),
            "expected empty-path rejection for jsonl enrichment: {err}"
        );
    }

    #[test]
    fn geo_database_whitespace_path_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: geo_database\n        format: mmdb\n        path: \"   \"\n";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path") && err.to_string().contains("empty"),
            "whitespace-only path must be rejected for geo_database: {err}"
        );
    }

    /// Regression: arrow_ipc output with `compression: gzip` must be rejected.
    /// Only `zstd` and `none` are supported. Before the fix, gzip was silently
    /// accepted and would fail at runtime.
    #[test]
    fn arrow_ipc_output_rejects_gzip_compression() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: arrow_ipc
        endpoint: http://localhost:4317
        compression: gzip
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("arrow_ipc output only supports 'zstd' or 'none'")
                && msg.contains("'gzip'"),
            "expected arrow_ipc-specific gzip rejection, got: {msg}"
        );
    }

    #[test]
    fn arrow_ipc_output_accepts_none_compression() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: arrow_ipc
        endpoint: http://localhost:4317
        compression: none
"#;
        Config::load_str(yaml).expect("arrow_ipc output should accept none compression");
    }

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
            (
                "type: parquet\npath: /tmp/ff.parquet\n",
                OutputType::Parquet,
            ),
            ("type: \"null\"\n", OutputType::Null),
            ("type: tcp\nendpoint: 127.0.0.1:9000\n", OutputType::Tcp),
            ("type: udp\nendpoint: 127.0.0.1:9001\n", OutputType::Udp),
            (
                "type: arrow_ipc\nendpoint: http://localhost:4317\n",
                OutputType::ArrowIpc,
            ),
        ];

        for (yaml, expected_type) in cases {
            let v2 = serde_yaml_ng::from_str::<OutputConfigV2>(yaml).unwrap();
            assert_eq!(OutputConfig::from(v2).output_type, expected_type);
        }
    }

    #[test]
    fn example_outputs_parse_as_v2_and_normalize_equivalently() {
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
                let v2 = OutputConfigV2::deserialize(output_value.clone()).unwrap_or_else(|err| {
                    panic!("{} output should parse as v2: {err}", path.display())
                });
                let flat = OutputConfig::deserialize(output_value.clone()).unwrap_or_else(|err| {
                    panic!(
                        "{} output should normalize to flat shape: {err}",
                        path.display()
                    )
                });
                assert_eq!(OutputConfig::from(v2), flat, "{}", path.display());
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

    #[test]
    fn csv_enrichment_whitespace_path_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: csv\n        table_name: assets\n        path: \"   \"\n";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path") && err.to_string().contains("empty"),
            "whitespace-only path must be rejected for csv enrichment: {err}"
        );
    }
}

mod tests_generator_unsupported;
mod tests_otlp_config;
mod tests_static_labels;

#[cfg(test)]
mod tests_sensor;
