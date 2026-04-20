use logfwd_config::Config;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvVarGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        // SAFETY: tests that mutate process environment hold ENV_LOCK.
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: tests that mutate process environment hold ENV_LOCK.
        match &self.previous {
            Some(val) => unsafe { std::env::set_var(self.key, val) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().expect("env lock should not be poisoned")
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("logfwd-{prefix}-{nanos}-{}", std::process::id()))
}

#[test]
fn config_deserialization_error_includes_simple_layout_field_path() {
    let yaml = r#"
input:
  type: generator
output:
  type: stdout
  batch_size: not-a-number
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("config deserialization error"),
        "error should include a deserialization path: {err}"
    );
    assert!(err.contains("output"), "error should name output: {err}");
    assert!(
        err.contains("batch_size"),
        "error should name batch_size: {err}"
    );
}

#[test]
fn config_deserialization_error_includes_pipeline_field_path() {
    let yaml = r#"
pipelines:
  app:
    workers: not-a-number
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("config deserialization error"),
        "error should include a deserialization path: {err}"
    );
    assert!(
        err.contains("pipelines"),
        "error should name pipelines: {err}"
    );
    assert!(err.contains("app"), "error should name pipeline key: {err}");
    assert!(err.contains("workers"), "error should name workers: {err}");
    assert!(
        !err.contains("outputs"),
        "error should point at the direct pipeline field, not outputs: {err}"
    );
}

#[test]
fn raw_yaml_number_for_string_field_is_rejected() {
    let yaml = r#"
input:
  type: file
  path: 123
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("config deserialization error"),
        "error should come from deserialization: {err}"
    );
    assert!(
        err.contains("string"),
        "error should expect a string: {err}"
    );
}

#[test]
fn raw_yaml_bool_for_string_field_is_rejected() {
    let yaml = r#"
input:
  type: http
  listen: 127.0.0.1:8080
  http:
    path: true
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("config deserialization error"),
        "error should come from deserialization: {err}"
    );
    assert!(
        err.contains("string"),
        "error should expect a string: {err}"
    );
}

#[test]
fn raw_yaml_bool_for_numeric_field_is_rejected() {
    let yaml = r#"
pipelines:
  app:
    workers: true
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("config deserialization error"),
        "error should come from deserialization: {err}"
    );
    assert!(
        err.contains("unsigned integer"),
        "error should expect an unsigned integer: {err}"
    );
}

#[test]
fn raw_yaml_float_for_integer_field_is_rejected() {
    let yaml = r#"
pipelines:
  app:
    workers: 1.5
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("config deserialization error"),
        "error should come from deserialization: {err}"
    );
    assert!(
        err.contains("unsigned integer"),
        "error should expect an unsigned integer: {err}"
    );
}

#[test]
fn raw_yaml_number_for_bool_field_is_rejected() {
    let yaml = r#"
input:
  type: http
  listen: 127.0.0.1:8080
  http:
    strict_path: 1
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("config deserialization error"),
        "error should come from deserialization: {err}"
    );
    assert!(
        err.contains("boolean"),
        "error should expect a boolean: {err}"
    );
}

#[test]
fn top_level_dotted_yaml_key_is_rejected_as_unknown_field() {
    let yaml = r#"
server.log_level: debug
input:
  type: generator
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("unknown field"),
        "dotted YAML key should remain literal and be rejected: {err}"
    );
    assert!(
        err.contains("server.log_level"),
        "error should name the literal dotted key: {err}"
    );
}

#[test]
fn generator_attribute_scalar_types_are_preserved() {
    let yaml = r#"
input:
  type: generator
  generator:
    profile: record
    attributes:
      count: 7
      enabled: true
      label: "7"
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("generator attributes should parse");
    let input = &config.pipelines["default"].inputs[0];
    let attributes = match &input.type_config {
        logfwd_config::InputTypeConfig::Generator(generator) => generator
            .generator
            .as_ref()
            .expect("generator config should be present")
            .attributes
            .clone(),
        _ => panic!("expected generator input"),
    };

    assert!(matches!(
        attributes["count"],
        logfwd_config::GeneratorAttributeValueConfig::Integer(7)
    ));
    assert!(matches!(
        attributes["enabled"],
        logfwd_config::GeneratorAttributeValueConfig::Bool(true)
    ));
    assert!(matches!(
        attributes["label"],
        logfwd_config::GeneratorAttributeValueConfig::String(ref value) if value == "7"
    ));
}

#[test]
fn env_generator_attribute_values_remain_strings() {
    let _env_lock = env_lock();
    let _count = EnvVarGuard::set("LOGFWD_ISSUE_1855_GENERATOR_COUNT", "7");
    let _enabled = EnvVarGuard::set("LOGFWD_ISSUE_1855_GENERATOR_ENABLED", "true");

    let yaml = r#"
input:
  type: generator
  generator:
    profile: record
    attributes:
      count: ${LOGFWD_ISSUE_1855_GENERATOR_COUNT}
      enabled: ${LOGFWD_ISSUE_1855_GENERATOR_ENABLED}
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("generator attributes should parse");
    let input = &config.pipelines["default"].inputs[0];
    let attributes = match &input.type_config {
        logfwd_config::InputTypeConfig::Generator(generator) => generator
            .generator
            .as_ref()
            .expect("generator config should be present")
            .attributes
            .clone(),
        _ => panic!("expected generator input"),
    };

    assert!(matches!(
        attributes["count"],
        logfwd_config::GeneratorAttributeValueConfig::String(ref value) if value == "7"
    ));
    assert!(matches!(
        attributes["enabled"],
        logfwd_config::GeneratorAttributeValueConfig::String(ref value) if value == "true"
    ));
}

#[test]
fn env_expansion_preserves_yaml_hash_content() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855", "/var/log/my app #1.log");

    let yaml = r#"
input:
  type: file
  path: ${LOGFWD_ISSUE_1855}
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("config should parse after env expansion");
    let input = &config.pipelines["default"].inputs[0];
    let path = match &input.type_config {
        logfwd_config::InputTypeConfig::File(file) => file.path.as_str(),
        _ => panic!("expected file input"),
    };

    assert_eq!(path, "/var/log/my app #1.log");
}

#[test]
fn effective_yaml_preserves_yaml_hash_content() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_EFFECTIVE", "/var/log/my app #1.log");

    let yaml = r#"
input:
  type: file
  path: ${LOGFWD_ISSUE_1855_EFFECTIVE}
output:
  type: stdout
"#;

    let expanded =
        Config::expand_env_yaml_str(yaml).expect("YAML-aware env expansion should succeed");
    assert!(
        expanded.contains("#1.log"),
        "expanded YAML should preserve hash content: {expanded}"
    );

    let config = Config::load_str(&expanded).expect("expanded YAML should remain parseable");
    let input = &config.pipelines["default"].inputs[0];
    let path = match &input.type_config {
        logfwd_config::InputTypeConfig::File(file) => file.path.as_str(),
        _ => panic!("expected file input"),
    };

    assert_eq!(path, "/var/log/my app #1.log");
}

#[test]
fn env_numeric_string_is_parsed_by_typed_schema() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_WORKERS", "4");

    let yaml = r#"
pipelines:
  test:
    workers: ${LOGFWD_ISSUE_1855_WORKERS}
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let config = Config::load_str(yaml).expect("config should parse env-backed numeric string");
    assert_eq!(config.pipelines["test"].workers, Some(4));

    let expanded = Config::expand_env_yaml_str(yaml).expect("env expansion should succeed");
    let expanded_value: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&expanded).expect("expanded YAML should parse");
    assert_eq!(
        expanded_value["pipelines"]["test"]["workers"].as_str(),
        Some("4"),
        "env substitution should produce string data, not YAML numbers"
    );
}

#[test]
fn env_bool_string_is_parsed_by_typed_schema() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_STRICT_PATH", "false");

    let yaml = r#"
input:
  type: http
  listen: 127.0.0.1:8080
  http:
    strict_path: ${LOGFWD_ISSUE_1855_STRICT_PATH}
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("config should parse env-backed bool");
    let input = &config.pipelines["default"].inputs[0];
    let strict_path = match &input.type_config {
        logfwd_config::InputTypeConfig::Http(http) => {
            http.http.as_ref().and_then(|config| config.strict_path)
        }
        _ => panic!("expected http input"),
    };

    assert_eq!(strict_path, Some(false));
}

#[test]
fn env_bool_string_is_parsed_by_shared_tls_schema() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_TLS_SKIP_VERIFY", "true");

    let yaml = r#"
input:
  type: generator
output:
  type: otlp
  endpoint: http://127.0.0.1:4318
  tls:
    insecure_skip_verify: ${LOGFWD_ISSUE_1855_TLS_SKIP_VERIFY}
"#;

    let config = Config::load_str(yaml).expect("config should parse env-backed TLS bool");
    let logfwd_config::OutputConfigV2::Otlp(output) = &config.pipelines["default"].outputs[0]
    else {
        panic!("expected otlp output");
    };
    let tls = output.tls.as_ref().expect("TLS config should be present");

    assert!(tls.insecure_skip_verify);
}

#[test]
fn quoted_env_expansion_preserves_string_scalars() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_QUOTED_PATH", "1234");

    let yaml = r#"
input:
  type: file
  path: "${LOGFWD_ISSUE_1855_QUOTED_PATH}"
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("quoted env-backed string should parse");
    let input = &config.pipelines["default"].inputs[0];
    let path = match &input.type_config {
        logfwd_config::InputTypeConfig::File(file) => file.path.as_str(),
        _ => panic!("expected file input"),
    };

    assert_eq!(path, "1234");
}

#[test]
fn tagged_quoted_env_expansion_preserves_string_scalars() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_TAGGED_PATH", "true");

    let yaml = r#"
input:
  type: file
  path: !!str "${LOGFWD_ISSUE_1855_TAGGED_PATH}"
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("tagged quoted env-backed string should parse");
    let input = &config.pipelines["default"].inputs[0];
    let path = match &input.type_config {
        logfwd_config::InputTypeConfig::File(file) => file.path.as_str(),
        _ => panic!("expected file input"),
    };

    assert_eq!(path, "true");
}

#[test]
fn tagged_unquoted_env_expansion_preserves_string_scalars() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_TAGGED_UNQUOTED", "123");

    // Env substitution already produces string data; the explicit string tag
    // should preserve that behavior.
    let yaml = r#"
input:
  type: file
  path: !str ${LOGFWD_ISSUE_1855_TAGGED_UNQUOTED}
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("tagged unquoted env-backed string should parse");
    let input = &config.pipelines["default"].inputs[0];
    let path = match &input.type_config {
        logfwd_config::InputTypeConfig::File(file) => file.path.as_str(),
        _ => panic!("expected file input"),
    };

    assert_eq!(path, "123");
}

#[test]
fn explicit_string_yaml_tag_preserves_string_field() {
    let yaml = r#"
input:
  type: file
  path: !!str 123
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("explicit string tag should parse as string");
    let input = &config.pipelines["default"].inputs[0];
    let path = match &input.type_config {
        logfwd_config::InputTypeConfig::File(file) => file.path.as_str(),
        _ => panic!("expected file input"),
    };

    assert_eq!(path, "123");
}

#[test]
fn non_string_core_yaml_tag_for_string_field_is_rejected() {
    let yaml = r#"
input:
  type: file
  path: !!int "123"
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("config deserialization error"),
        "error should come from deserialization: {err}"
    );
    assert!(
        err.contains("string"),
        "error should expect a string: {err}"
    );
}

#[test]
fn custom_yaml_tag_for_string_field_is_rejected() {
    let yaml = r#"
input:
  type: file
  path: !custom "123"
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("unsupported explicit YAML tag"),
        "error should reject unsupported explicit tags: {err}"
    );
}

#[test]
fn custom_yaml_tag_for_mapping_key_is_rejected() {
    let yaml = r#"
input:
  type: generator
output:
  type: stdout
resource_attrs:
  !custom key: value
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("unsupported explicit YAML tag"),
        "error should reject unsupported explicit mapping-key tags: {err}"
    );
}

#[test]
fn anchored_quoted_env_expansion_preserves_string_scalars() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_ANCHORED_PATH", "true");

    let yaml = r#"
input:
  type: file
  path: &path "${LOGFWD_ISSUE_1855_ANCHORED_PATH}"
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("anchored quoted env-backed string should parse");
    let input = &config.pipelines["default"].inputs[0];
    let path = match &input.type_config {
        logfwd_config::InputTypeConfig::File(file) => file.path.as_str(),
        _ => panic!("expected file input"),
    };

    assert_eq!(path, "true");
}

#[test]
fn mixed_env_uses_schema_for_types_and_strings_for_string_fields() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_MIXED", "4");

    let yaml = r#"
pipelines:
  test:
    workers: ${LOGFWD_ISSUE_1855_MIXED}
    resource_attrs:
      note: "${LOGFWD_ISSUE_1855_MIXED}"
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let config = Config::load_str(yaml).expect("mixed env-backed values should parse");

    assert_eq!(config.pipelines["test"].workers, Some(4));
    assert_eq!(
        config.pipelines["test"].resource_attrs.get("note"),
        Some(&"4".to_string())
    );
}

#[test]
fn plain_scalar_apostrophe_is_not_treated_as_quote_boundary() {
    let yaml = r#"
input:
  type: file
  path: /var/log/it's.log
output:
  type: stdout
"#;

    let config = Config::load_str(yaml).expect("plain scalar apostrophe should parse");
    let input = &config.pipelines["default"].inputs[0];
    let path = match &input.type_config {
        logfwd_config::InputTypeConfig::File(file) => file.path.as_str(),
        _ => panic!("expected file input"),
    };

    assert_eq!(path, "/var/log/it's.log");
}

#[test]
fn block_scalar_mixed_indentation_expands() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_BLOCK_FIELD", "message");

    let yaml = r#"
pipelines:
  test:
    transform: |
      {
        "${LOGFWD_ISSUE_1855_BLOCK_FIELD}"
      }
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let config = Config::load_str(yaml).expect("block scalar env placeholder should parse");
    let transform = config.pipelines["test"]
        .transform
        .as_ref()
        .expect("transform should be present");

    assert!(
        transform.contains(r#""message""#),
        "unexpected transform: {transform}"
    );
}

#[test]
fn single_quoted_yaml_escape_survives_env_expansion() {
    let yaml = r#"
pipelines:
  test:
    resource_attrs:
      note: 'it''s'
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let config = Config::load_str(yaml).expect("single-quoted escape should parse");

    assert_eq!(
        config.pipelines["test"].resource_attrs.get("note"),
        Some(&"it's".to_string())
    );
}

#[test]
fn env_expanded_mapping_key_collision_is_rejected() {
    let _env_lock = env_lock();
    let _env_a = EnvVarGuard::set("LOGFWD_ISSUE_1855_KEY_A", "prod");
    let _env_b = EnvVarGuard::set("LOGFWD_ISSUE_1855_KEY_B", "prod");

    let yaml = r#"
pipelines:
  test:
    resource_attrs:
      ${LOGFWD_ISSUE_1855_KEY_A}: one
      ${LOGFWD_ISSUE_1855_KEY_B}: two
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();

    assert!(
        err.contains("duplicate YAML mapping key"),
        "unexpected error: {err}"
    );
}

#[test]
fn env_expansion_applies_to_mapping_keys() {
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("LOGFWD_ISSUE_1855_PIPELINE", "from-env");

    let yaml = r#"
pipelines:
  ${LOGFWD_ISSUE_1855_PIPELINE}:
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let config = Config::load_str(yaml).expect("config should parse env-backed mapping key");
    assert!(config.pipelines.contains_key("from-env"));
}

#[test]
fn issue_1856_allow_same_listen_address_for_different_transports() {
    let yaml = r#"
pipelines:
  p1:
    inputs:
      - type: udp
        listen: 0.0.0.0:9000
    outputs:
      - type: stdout
  p2:
    inputs:
      - type: tcp
        listen: 0.0.0.0:9000
    outputs:
      - type: stdout
"#;

    Config::load_str(yaml).expect("tcp and udp should share an address because transports differ");
}

#[test]
fn issue_1856_reject_duplicate_listen_addresses_for_same_transport() {
    let yaml = r#"
pipelines:
  p1:
    inputs:
      - type: tcp
        listen: 0.0.0.0:9000
    outputs:
      - type: stdout
  p2:
    inputs:
      - type: http
        listen: 0.0.0.0:09000
    outputs:
      - type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("listen address '0.0.0.0:09000' duplicates"),
        "unexpected error: {err}"
    );
}

#[test]
fn issue_1856_reject_duplicate_ipv6_listen_addresses_for_same_transport() {
    let yaml = r#"
pipelines:
  p1:
    inputs:
      - type: tcp
        listen: "[::1]:9000"
    outputs:
      - type: stdout
  p2:
    inputs:
      - type: http
        listen: "[0:0:0:0:0:0:0:1]:9000"
    outputs:
      - type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("listen address '[0:0:0:0:0:0:0:1]:9000' duplicates"),
        "unexpected error: {err}"
    );
}

#[test]
fn issue_1856_allow_duplicate_ephemeral_port_zero_for_same_transport() {
    let yaml = r#"
pipelines:
  p1:
    inputs:
      - type: udp
        listen: 127.0.0.1:0
    outputs:
      - type: stdout
  p2:
    inputs:
      - type: udp
        listen: 127.0.0.1:0
    outputs:
      - type: stdout
"#;

    Config::load_str(yaml).expect("port 0 asks the OS for distinct ephemeral ports");
}

#[test]
fn issue_1857_reject_duplicate_file_output_paths_across_pipelines() {
    let yaml = r#"
pipelines:
  p1:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: /tmp/shared.log
  p2:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: /tmp/shared.log
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("file output path '/tmp/shared.log' duplicates"),
        "unexpected error: {err}"
    );
}

#[test]
fn issue_1857_reject_relative_and_absolute_file_output_aliases() {
    let base = std::env::current_dir().expect("current dir should be available");
    let absolute = base.join("logs/shared.log");
    let yaml = format!(
        r#"
pipelines:
  p1:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: logs/shared.log
  p2:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: {}
"#,
        absolute.display()
    );

    let err = Config::load_str_with_base_path(&yaml, Some(&base))
        .unwrap_err()
        .to_string();
    assert!(err.contains("file output path"), "unexpected error: {err}");
    assert!(err.contains("duplicates"), "unexpected error: {err}");
}

#[test]
fn issue_1857_reject_relative_glob_input_matching_relative_output() {
    let base = std::env::current_dir().expect("current dir should be available");
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: logs/*.log
    outputs:
      - type: file
        path: logs/app.log
"#;

    let err = Config::load_str_with_base_path(yaml, Some(&base))
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("could match file input glob"),
        "unexpected error: {err}"
    );
}

#[test]
fn issue_1858_reject_workers_above_max_bound() {
    let yaml = r#"
pipelines:
  test:
    workers: 5000
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("workers must be in range 1..=1024"),
        "unexpected error: {err}"
    );
}

#[test]
fn issue_1862_reject_tcp_udp_output_formats() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - type: tcp
        endpoint: localhost:9001
        format: json
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("unknown field") && err.contains("format"),
        "tcp output should reject format at parse time: {err}"
    );
}

#[test]
fn issue_1920_reject_duplicate_named_inputs_and_outputs_in_pipeline() {
    let duplicate_input_yaml = r#"
pipelines:
  test:
    inputs:
      - name: in1
        type: generator
      - name: in1
        type: file
        path: /var/log/test.log
    outputs:
      - type: stdout
"#;

    let input_err = Config::load_str(duplicate_input_yaml)
        .unwrap_err()
        .to_string();
    assert!(
        input_err.contains("duplicate input name 'in1'"),
        "unexpected error: {input_err}"
    );

    let duplicate_output_yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - name: out1
        type: stdout
      - name: out1
        type: "null"
"#;

    let output_err = Config::load_str(duplicate_output_yaml)
        .unwrap_err()
        .to_string();
    assert!(
        output_err.contains("duplicate output name 'out1'"),
        "unexpected error: {output_err}"
    );
}

#[test]
fn issue_2349_reject_null_output_endpoint() {
    let yaml = r#"
input:
  type: file
  path: /tmp/x.log
output:
  type: "null"
  endpoint: https://collector:4318
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("unknown field") && err.contains("endpoint"),
        "null output should reject endpoint at parse time: {err}"
    );
}

#[test]
fn issue_2349_reject_named_null_output_format() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - name: discard
        type: "null"
        format: json
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("unknown field") && err.contains("format"),
        "null output should reject format at parse time: {err}"
    );
}

#[test]
fn issue_2035_reject_file_output_when_parent_directory_missing() {
    let missing_parent = unique_temp_dir("missing-parent").join("child");
    let out_path = missing_parent.join("out.ndjson");
    let yaml = format!(
        r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: {}
"#,
        out_path.display()
    );

    let err = Config::load_str(&yaml).unwrap_err().to_string();
    assert!(
        err.contains("file output parent directory") && err.contains("is not usable"),
        "unexpected error: {err}"
    );
}

#[test]
fn issue_2035_reject_relative_file_output_when_base_parent_directory_missing() {
    let base = unique_temp_dir("relative-base");
    fs::create_dir_all(&base).expect("base dir should be created");
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: missing/out.ndjson
"#;

    let err = Config::load_str_with_base_path(yaml, Some(&base))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("file output parent directory") && err.contains("is not usable"),
        "unexpected error: {err}"
    );

    fs::remove_dir_all(&base).expect("base dir cleanup should succeed");
}

#[test]
fn issue_2035_reject_file_output_when_parent_path_is_file() {
    let dir = unique_temp_dir("parent-file");
    fs::create_dir_all(&dir).expect("temp dir should be created");
    let parent_file = dir.join("not-a-dir");
    fs::write(&parent_file, b"not a directory").expect("parent file should be written");
    let out_path = parent_file.join("out.ndjson");
    let yaml = format!(
        r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: {}
"#,
        out_path.display()
    );

    let err = Config::load_str(&yaml).unwrap_err().to_string();
    assert!(
        err.contains("file output parent") && err.contains("is not a directory"),
        "unexpected error: {err}"
    );

    fs::remove_dir_all(&dir).expect("temp dir cleanup should succeed");
}

#[cfg(unix)]
#[test]
fn issue_2035_reject_file_output_when_parent_directory_is_read_only() {
    let dir = unique_temp_dir("readonly-parent");
    fs::create_dir_all(&dir).expect("temp dir should be created");
    let original_perms = fs::metadata(&dir)
        .expect("temp dir metadata should be readable")
        .permissions();
    let mut readonly_perms = original_perms.clone();
    readonly_perms.set_readonly(true);
    fs::set_permissions(&dir, readonly_perms)
        .expect("setting read-only permissions should succeed");

    let out_path = dir.join("out.ndjson");
    let yaml = format!(
        r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: {}
"#,
        out_path.display()
    );

    let err = Config::load_str(&yaml).unwrap_err().to_string();
    assert!(
        err.contains("file output parent") && err.contains("read-only"),
        "unexpected error: {err}"
    );

    fs::set_permissions(&dir, original_perms).expect("permissions reset should succeed");
    fs::remove_dir_all(&dir).expect("temp dir cleanup should succeed");
}

#[test]
fn issue_2035_reject_file_output_when_existing_file_is_read_only() {
    let dir = unique_temp_dir("readonly-file");
    fs::create_dir_all(&dir).expect("temp dir should be created");
    let out_path = dir.join("out.ndjson");
    fs::write(&out_path, b"existing content").expect("output file should be written");

    let mut readonly_perms = fs::metadata(&out_path)
        .expect("file metadata should be readable")
        .permissions();
    readonly_perms.set_readonly(true);
    fs::set_permissions(&out_path, readonly_perms).expect("setting read-only should succeed");

    let yaml = format!(
        r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: {}
"#,
        out_path.display()
    );

    let err = Config::load_str(&yaml).unwrap_err().to_string();
    assert!(
        err.contains("file output path") && err.contains("read-only"),
        "unexpected error: {err}"
    );

    // Reset permissions so cleanup can remove the file.
    let mut writable_perms = fs::metadata(&out_path)
        .expect("file metadata should be readable")
        .permissions();
    writable_perms.set_readonly(false);
    fs::set_permissions(&out_path, writable_perms).expect("permissions reset should succeed");
    fs::remove_dir_all(&dir).expect("temp dir cleanup should succeed");
}

#[test]
fn issue_2035_reject_file_output_when_existing_path_is_directory() {
    let dir = unique_temp_dir("output-is-dir");
    let out_path = dir.join("capture.ndjson");
    fs::create_dir_all(&out_path).expect("output directory should be created");
    let yaml = format!(
        r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: {}
"#,
        out_path.display()
    );

    let err = Config::load_str(&yaml).unwrap_err().to_string();
    assert!(
        err.contains("file output path") && err.contains("is a directory"),
        "unexpected error: {err}"
    );

    fs::remove_dir_all(&dir).expect("temp dir cleanup should succeed");
}

#[test]
fn issue_2035_accept_file_output_when_parent_directory_is_writable() {
    let dir = unique_temp_dir("writable-parent");
    fs::create_dir_all(&dir).expect("temp dir should be created");
    let out_path = dir.join("out.ndjson");
    let yaml = format!(
        r#"
pipelines:
  test:
    inputs:
      - type: generator
    outputs:
      - type: file
        path: {}
"#,
        out_path.display()
    );

    Config::load_str(&yaml).expect("writable output parent should validate");

    fs::remove_dir_all(&dir).expect("temp dir cleanup should succeed");
}

#[test]
fn backslash_escaped_dollar_in_double_quoted_yaml_is_not_expanded() {
    // `\$` is not a valid YAML escape sequence, so serde_yaml_ng will reject it.
    // The important thing is that the env pre-scanner does NOT silently expand
    // `\${VAR}` as if it were `${VAR}` — the user should get a YAML parse error,
    // not a surprise env substitution.
    let _env_lock = env_lock();
    let _env = EnvVarGuard::set("HOME", "/Users/test");

    let yaml = r#"
pipelines:
  test:
    resource_attrs:
      note: "\${HOME}"
    inputs:
      - type: generator
    outputs:
      - type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    // Must be a YAML scanner error, NOT an env-expansion error.
    assert!(
        err.contains("unknown escape character") || err.contains("SCANNER"),
        "expected YAML parse error for invalid \\$ escape, got: {err}"
    );
}

#[test]
fn issue_1957_reject_loki_static_and_dynamic_label_collisions() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
    outputs:
      - type: loki
        endpoint: http://localhost:3100
        static_labels:
          app: my-service
        label_columns:
          - app
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("is defined in both 'label_columns' and 'static_labels'"),
        "unexpected error: {err}"
    );
}

#[test]
fn issue_1958_generator_record_profile_preserves_null_attribute_values() {
    let yaml = r#"
input:
  type: generator
  generator:
    profile: record
    attributes:
      deleted_at: null
output:
  type: stdout
"#;

    Config::load_str(yaml).expect("generator null attributes should remain supported");
}

#[test]
fn issue_2060_reject_unmatched_opening_bracket_in_host_port() {
    let yaml = r#"
input:
  type: udp
  listen: foo[bar:4317
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(err.contains("unmatched '['"), "unexpected error: {err}");
}

#[test]
fn issue_2062_reject_sensor_max_rows_per_poll_zero() {
    let yaml = r#"
input:
  type: host_metrics
  sensor:
    max_rows_per_poll: 0
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("sensor.max_rows_per_poll") && err.contains("at least 1"),
        "unexpected error: {err}"
    );
}

#[test]
fn issue_2178_reject_otlp_max_recv_message_size_bytes_zero() {
    let yaml = r#"
input:
  type: otlp
  listen: 127.0.0.1:4318
  max_recv_message_size_bytes: 0
output:
  type: stdout
"#;

    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("otlp.max_recv_message_size_bytes") && err.contains("at least 1"),
        "unexpected error: {err}"
    );
}
