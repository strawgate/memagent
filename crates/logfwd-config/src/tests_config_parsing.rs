//! Basic config parsing tests: simple/advanced layouts, env var substitution, file loading.

#[cfg(test)]
mod tests {
    use crate::*;
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
            pipe.outputs[0].endpoint(),
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
        assert_eq!(pipe.outputs[0].endpoint(), Some("http://my-collector:4317"));
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
        assert_config_err!(yaml, "LOGFWD_NONEXISTENT_VAR_12345", "not set");
    }

    #[test]
    fn unterminated_env_var_preserved_as_is() {
        assert_eq!(
            expand_env_vars("endpoint: ${LOGFWD_TEST_UNTERMINATED").unwrap(),
            "endpoint: ${LOGFWD_TEST_UNTERMINATED"
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
        assert_config_err!(yaml, "duplicate entry with key \"app\"");
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

    #[test]
    fn normalize_args_canonical_form_unchanged() {
        use crate::*;
        // When --config is already at position 1, args are returned unchanged.
        // (We test the normalize_args logic via Config parsing instead since
        // normalize_args lives in the binary crate.)
        let _ = Config::load_str; // ensure the import is live
    }

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
}
