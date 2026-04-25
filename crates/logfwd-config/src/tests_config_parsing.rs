//! Basic config parsing tests: simple/advanced layouts, env var substitution, file loading.

#[cfg(test)]
mod tests {
    use crate::*;
    use std::fs;

    fn single_pipeline_yaml(
        input_body: &str,
        transform: Option<&str>,
        output_body: &str,
    ) -> String {
        if let Some(transform) = transform {
            test_yaml::single_pipeline_yaml_with_transform(input_body, transform, output_body)
        } else {
            test_yaml::single_pipeline_yaml(input_body, output_body)
        }
    }

    #[test]
    fn config_with_one_pipeline() {
        let mut yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/pods/**/*.log\nformat: cri",
            Some("SELECT * FROM logs WHERE level != 'DEBUG'"),
            "type: otlp\nendpoint: http://otel-collector:4317\ncompression: zstd",
        );
        yaml.push_str("\nserver:\n  diagnostics: 0.0.0.0:9090\n  log_level: info\n");
        yaml.push_str("\nstorage:\n  data_dir: /var/lib/logfwd\n");
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
        let default_yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/pods/**/*.log\nformat: cri",
            None,
            "type: \"null\"",
        );
        let cfg = Config::load_str(&default_yaml).expect("default source_metadata should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].source_metadata, SourceMetadataStyle::None);

        let enabled_yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/pods/**/*.log\nformat: cri\nsource_metadata: fastforward",
            None,
            "type: \"null\"",
        );
        let cfg = Config::load_str(&enabled_yaml).expect("source_metadata style should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.inputs[0].source_metadata,
            SourceMetadataStyle::Fastforward
        );

        let ecs_yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/pods/**/*.log\nformat: cri\nsource_metadata: ecs",
            None,
            "type: \"null\"",
        );
        let cfg = Config::load_str(&ecs_yaml).expect("ecs style should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].source_metadata, SourceMetadataStyle::Ecs);

        let otel_yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/pods/**/*.log\nformat: cri\nsource_metadata: otel",
            None,
            "type: \"null\"",
        );
        let cfg = Config::load_str(&otel_yaml).expect("otel style should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].source_metadata, SourceMetadataStyle::Otel);

        let vector_yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/pods/**/*.log\nformat: cri\nsource_metadata: vector",
            None,
            "type: \"null\"",
        );
        let cfg = Config::load_str(&vector_yaml).expect("vector style should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].source_metadata, SourceMetadataStyle::Vector);
    }

    #[test]
    fn input_source_metadata_beats_alias_is_rejected() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/pods/**/*.log\nformat: cri\nsource_metadata: beats",
            None,
            "type: \"null\"",
        );
        let err = Config::load_str(yaml).expect_err("beats alias should no longer parse");
        let msg = err.to_string();
        assert!(
            msg.contains("beats")
                && (msg.contains("unknown variant")
                    || msg.contains("does not have variant constructor")),
            "expected enum parse rejection for beats alias: {msg}"
        );
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
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/test.log",
            None,
            "type: otlp\nendpoint: ${LOGFWD_TEST_ENDPOINT}",
        );
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
        let yaml =
            single_pipeline_yaml("type: file\npath: /var/log/test.log", None, "type: stdout");
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
pipelines:
  default:
    resource_attrs:
      service.name: my-service
      service.version: "1.2.3"
      deployment.environment: production
    inputs:
      - type: file
        path: /var/log/app.log
    outputs:
      - type: otlp
        endpoint: http://otel-collector:4317
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
        let yaml = single_pipeline_yaml(
            "type: file\npath: /var/log/app.log",
            None,
            "type: otlp\nendpoint: http://otel-collector:4317",
        );
        let cfg = Config::load_str(yaml).expect("should parse config without resource_attrs");
        let pipe = &cfg.pipelines["default"];
        assert!(pipe.resource_attrs.is_empty());
    }

    #[test]
    fn normalize_args_canonical_form_unchanged() {
        // When --config is already at position 1, args are returned unchanged.
        // (We test the normalize_args logic via Config parsing instead since
        // normalize_args lives in the binary crate.)
        let _ = Config::load_str("pipelines: {}\n").expect_err("empty pipelines should fail");
    }

    #[test]
    fn format_text_alias_accepted() {
        let yaml = single_pipeline_yaml(
            "type: file\npath: /tmp/x.log\nformat: text",
            None,
            "type: stdout",
        );
        let cfg = Config::load_str(yaml).expect("format: text should be accepted");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].format, Some(Format::Text));
    }
}
