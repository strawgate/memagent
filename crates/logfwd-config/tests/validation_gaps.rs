use logfwd_config::Config;
use std::sync::{Mutex, MutexGuard};

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvVarGuard {
    key: &'static str,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        // SAFETY: tests that mutate process environment hold ENV_LOCK.
        unsafe { std::env::set_var(key, value) };
        Self { key }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: tests that mutate process environment hold ENV_LOCK.
        unsafe { std::env::remove_var(self.key) };
    }
}

fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().expect("env lock should not be poisoned")
}

#[test]
fn issue_1855_env_expansion_preserves_yaml_hash_content() {
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
fn issue_1855_env_expansion_preserves_non_string_scalars() {
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

    let config = Config::load_str(yaml).expect("config should parse env-backed numeric scalar");
    assert_eq!(config.pipelines["test"].workers, Some(4));
}

#[test]
fn issue_1855_quoted_env_expansion_preserves_string_scalars() {
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
fn issue_1855_mixed_quoted_and_unquoted_env_uses_node_context() {
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

    let config = Config::load_str(yaml).expect("mixed env-backed scalars should parse");

    assert_eq!(config.pipelines["test"].workers, Some(4));
    assert_eq!(
        config.pipelines["test"].resource_attrs.get("note"),
        Some(&"4".to_string())
    );
}

#[test]
fn issue_1855_env_expansion_applies_to_mapping_keys() {
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
        err.contains("tcp output does not support 'format'"),
        "unexpected error: {err}"
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
        type: null
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
