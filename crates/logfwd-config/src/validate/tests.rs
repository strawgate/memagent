use crate::types::Config;

use super::{is_glob_match_possible, validate_endpoint_url, validate_host_port};

fn host_port_error(addr: &str) -> String {
    validate_host_port(addr).unwrap_err().to_string()
}

#[test]
fn validate_host_port_works() {
    assert!(validate_host_port("127.0.0.1:4317").is_ok());
    assert!(validate_host_port("localhost:4317").is_ok());
    assert!(validate_host_port("my-host.internal:8080").is_ok());
    assert!(validate_host_port("[::1]:4317").is_ok());
    assert!(validate_host_port("[2001:db8::1]:80").is_ok());

    assert!(host_port_error(":4317").contains("empty host"));
    assert!(host_port_error("http://localhost:4317").contains("URL"));
    assert!(host_port_error("https://localhost:4317").contains("URL"));
    assert!(host_port_error("foo:bar:4317").contains("multiple colons"));
    assert!(host_port_error("localhost").contains("missing a port"));
    assert!(host_port_error("localhost:").contains("invalid port"));
    assert!(host_port_error("localhost:999999").contains("invalid port"));
    assert!(host_port_error("[::1]").contains("missing a port"));
    assert!(host_port_error("[::1]:").contains("invalid port"));
    assert!(host_port_error("[]:8080").contains("empty"));
    assert!(host_port_error("[::1]]:4317").contains("missing a port"));
    assert!(host_port_error("foo/bar:4317").contains('/'));
    assert!(host_port_error("foo]:4317").contains(']'));
    assert!(host_port_error("foo[bar:4317").contains('['));
}

#[test]
fn validate_bind_addr_works() {
    assert!(super::validate_bind_addr("127.0.0.1:4317").is_ok());
    assert!(super::validate_bind_addr("localhost:4317").is_ok());
    assert!(super::validate_bind_addr("[::1]:4317").is_ok());
    assert!(super::validate_bind_addr("http://localhost:4317").is_err());
}

#[test]
fn endpoint_url_requires_http_or_https_with_host() {
    assert!(validate_endpoint_url("http://localhost:4317").is_ok());
    assert!(validate_endpoint_url("https://example.com/path").is_ok());
    assert!(validate_endpoint_url("HTTP://EXAMPLE.COM").is_ok());

    for bad in [
        "http:///bulk",
        "https://?x=1",
        "http://   ",
        "ftp://example.com",
    ] {
        assert!(
            validate_endpoint_url(bad).is_err(),
            "expected endpoint validation error for {bad}"
        );
    }
}

#[test]
fn endpoint_url_redacts_userinfo_for_malformed_urls() {
    let err = validate_endpoint_url("https://user:secret@/bulk")
        .expect_err("must fail")
        .to_string();
    assert!(
        err.contains("***redacted***") && !err.contains("secret"),
        "malformed endpoint errors must redact userinfo: {err}"
    );
}

#[test]
fn file_output_same_as_input_rejected() {
    let yaml = r"
pipelines:
  looping:
    inputs:
      - type: file
        path: /tmp/logfwd-feedback-test.log
    outputs:
      - type: file
        path: /tmp/logfwd-feedback-test.log
        format: json
";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("feedback loop") || msg.contains("same as file input"),
        "expected feedback-loop rejection, got: {msg}"
    );
}

#[test]
fn file_output_different_from_input_allowed() {
    let yaml = r"
pipelines:
  ok:
    inputs:
      - type: file
        path: /tmp/logfwd-input.log
    outputs:
      - type: file
        path: /tmp/logfwd-output.log
        format: json
";
    Config::load_str(yaml).expect("different input/output paths should be allowed");
}

#[test]
fn file_output_same_as_input_rejected_after_lexical_normalization() {
    let yaml = r"
pipelines:
  looping:
    inputs:
      - type: file
        path: ./tmp/logs/../app.log
    outputs:
      - type: file
        path: tmp/./app.log
        format: json
";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("feedback loop") || msg.contains("same as file input"),
        "expected normalized-path feedback-loop rejection, got: {msg}"
    );
}

#[test]
fn glob_could_match_literal_filename_requires_exact_name() {
    assert!(is_glob_match_possible(
        "/var/log/access.log",
        "/var/log/access.log"
    ));
    assert!(!is_glob_match_possible(
        "/var/log/access.log",
        "/var/log/other.log"
    ));
}

#[test]
fn glob_could_match_wildcard_suffix_pattern() {
    assert!(is_glob_match_possible(
        "/var/log/*.log",
        "/var/log/access.log"
    ));
    assert!(!is_glob_match_possible(
        "/var/log/*.log",
        "/var/log/access.txt"
    ));
}

#[test]
fn glob_could_match_rejects_different_directory() {
    assert!(!is_glob_match_possible("/var/log/*.log", "/tmp/access.log"));
}

#[test]
fn glob_could_match_nested_wildcards_are_conservative() {
    assert!(is_glob_match_possible(
        "/var/log/*test*.log",
        "/var/log/mytest_file.log"
    ));
}

#[test]
fn glob_could_match_recursive_double_star_matches_nested_directories() {
    assert!(is_glob_match_possible(
        "/var/log/**/access.log",
        "/var/log/subdir/access.log"
    ));
    assert!(!is_glob_match_possible(
        "/var/log/**/access.log",
        "/var/log/subdir/error.log"
    ));
}

#[test]
fn glob_could_match_recursive_double_star_directory_only_pattern() {
    assert!(is_glob_match_possible(
        "/var/log/**",
        "/var/log/subdir/app.log"
    ));
    assert!(is_glob_match_possible("/var/log/**", "/var/log/app.log"));
    assert!(!is_glob_match_possible("/var/log/**", "/srv/log/app.log"));
}

#[test]
fn glob_could_match_directory_wildcards_are_conservative() {
    assert!(is_glob_match_possible("/var/*/app.log", "/var/log/app.log"));
    assert!(is_glob_match_possible(
        "/var/log[12]/*.log",
        "/var/log1/a.log"
    ));
    assert!(!is_glob_match_possible(
        "/var/*/app.log",
        "/srv/log/app.log"
    ));
}

#[test]
fn file_input_empty_path_rejected() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: ""
    outputs:
      - type: stdout
"#;
    let err = Config::load_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("path"),
        "expected path rejection: {err}"
    );
    assert!(
        err.to_string().contains("must not be empty"),
        "expected 'must not be empty' message: {err}"
    );
}

#[test]
fn file_input_whitespace_path_rejected() {
    let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: \"   \"\n    outputs:\n      - type: stdout\n";
    let err = Config::load_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("path") && err.to_string().contains("must not be empty"),
        "whitespace-only path must be rejected: {err}"
    );
}

#[test]
fn elasticsearch_empty_index_rejected() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: http://localhost:9200
        index: ""
"#;
    let err = Config::load_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("index"),
        "expected index rejection: {err}"
    );
    assert!(
        err.to_string().contains("must not be empty"),
        "expected 'must not be empty' message: {err}"
    );
}

#[test]
fn elasticsearch_whitespace_index_rejected() {
    let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: elasticsearch\n        endpoint: http://localhost:9200\n        index: \"   \"\n";
    let err = Config::load_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("index") && err.to_string().contains("must not be empty"),
        "whitespace-only index must be rejected: {err}"
    );
}

#[test]
fn elasticsearch_index_prefix_rejected() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: http://localhost:9200
        index: "_bad-index"
"#;
    let err = Config::load_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("has illegal prefix '_'"),
        "expected prefix rejection: {err}"
    );
}

#[test]
fn http_response_body_with_204_is_rejected() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: http
        listen: 127.0.0.1:8081
        format: json
        http:
          path: /ingest
          response_code: 204
          response_body: '{"ok":true}'
    outputs:
      - type: "null"
"#;
    let err = Config::load_str(yaml).expect_err("204 + response_body must fail validation");
    assert!(
        err.to_string()
            .contains("http.response_body is not allowed when http.response_code is 204"),
        "unexpected error: {err}"
    );
}

#[test]
fn http_max_drained_bytes_per_poll_zero_is_rejected() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: http
        listen: 127.0.0.1:8081
        format: json
        http:
          max_drained_bytes_per_poll: 0
    outputs:
      - type: "null"
"#;
    let err = Config::load_str(yaml).expect_err("zero drain cap must fail validation");
    assert!(
        err.to_string()
            .contains("http.max_drained_bytes_per_poll must be at least 1"),
        "unexpected error: {err}"
    );
}

#[test]
fn enrichment_static_empty_table_name_rejected() {
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
        table_name: ''
        labels:
          key: val
";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("table_name must not be empty"),
        "expected 'table_name must not be empty' in error: {msg}"
    );
}

#[test]
fn enrichment_csv_empty_table_name_rejected() {
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
        table_name: ''
        path: relative/path/assets.csv
";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("table_name must not be empty"),
        "expected 'table_name must not be empty' in error: {msg}"
    );
}

#[test]
fn enrichment_jsonl_empty_table_name_rejected() {
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
        table_name: ''
        path: relative/path/ips.jsonl
";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("table_name must not be empty"),
        "expected 'table_name must not be empty' in error: {msg}"
    );
}

#[test]
fn otlp_valid_protocol_accepted() {
    for proto in ["http", "grpc"] {
        let yaml = format!(
            "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: otlp\n        endpoint: http://localhost:4317\n        protocol: {proto}\n"
        );
        Config::load_str(yaml)
            .unwrap_or_else(|e| panic!("protocol '{proto}' should be accepted: {e}"));
    }
}

#[test]
fn otlp_invalid_protocol_rejected() {
    let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: otlp\n        endpoint: http://localhost:4317\n        protocol: websocket\n";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("websocket") && msg.contains("http") && msg.contains("grpc"),
        "expected protocol rejection for 'websocket': {msg}"
    );
}

#[test]
fn otlp_valid_compression_accepted() {
    for comp in ["zstd", "gzip", "none"] {
        let yaml = format!(
            "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: otlp\n        endpoint: http://localhost:4317\n        compression: {comp}\n"
        );
        Config::load_str(yaml)
            .unwrap_or_else(|e| panic!("compression '{comp}' should be accepted for otlp: {e}"));
    }
}

#[test]
fn otlp_invalid_compression_rejected() {
    let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: otlp\n        endpoint: http://localhost:4317\n        compression: lz4\n";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("lz4") && msg.contains("zstd") && msg.contains("gzip"),
        "expected compression rejection for 'lz4': {msg}"
    );
}

#[test]
fn elasticsearch_invalid_compression_rejected() {
    let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: elasticsearch\n        endpoint: http://localhost:9200\n        compression: zstd\n";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("compression") && msg.contains("zstd"),
        "expected compression rejection for 'zstd' on elasticsearch: {msg}"
    );
}

#[test]
fn elasticsearch_valid_compression_accepted() {
    for comp in ["gzip", "none"] {
        let yaml = format!(
            "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: elasticsearch\n        endpoint: http://localhost:9200\n        compression: {comp}\n"
        );
        Config::load_str(yaml).unwrap_or_else(|e| {
            panic!("compression '{comp}' should be accepted for elasticsearch: {e}")
        });
    }
}

#[test]
fn read_buf_size_upper_bound_rejected() {
    let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n        read_buf_size: 5000000\n    outputs:\n      - type: stdout\n";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("read_buf_size") && msg.contains("4194304"),
        "expected read_buf_size upper bound rejection: {msg}"
    );
}

#[test]
fn read_buf_size_at_max_accepted() {
    let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n        read_buf_size: 4194304\n    outputs:\n      - type: stdout\n";
    Config::load_str(yaml).expect("read_buf_size at exactly 4 MiB should be accepted");
}

#[test]
fn read_buf_size_just_over_max_rejected() {
    let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n        read_buf_size: 4194305\n    outputs:\n      - type: stdout\n";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("read_buf_size") && msg.contains("4194304"),
        "expected read_buf_size upper bound rejection: {msg}"
    );
}

#[test]
fn metrics_endpoint_valid_url_accepted() {
    let yaml = "server:\n  metrics_endpoint: http://localhost:4318/v1/metrics\npipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n";
    Config::load_str(yaml).expect("valid metrics_endpoint should be accepted");
}

#[test]
fn metrics_endpoint_invalid_url_rejected() {
    let yaml = "server:\n  metrics_endpoint: not-a-url\npipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("metrics_endpoint"),
        "expected metrics_endpoint rejection: {msg}"
    );
}

#[test]
fn metrics_endpoint_ftp_scheme_rejected() {
    let yaml = "server:\n  metrics_endpoint: ftp://localhost:21/metrics\npipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n";
    let err = Config::load_str(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("metrics_endpoint") && msg.contains("scheme"),
        "expected metrics_endpoint scheme rejection: {msg}"
    );
}

#[test]
fn otlp_accepts_new_options() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        retry_attempts: 3
        retry_initial_backoff_ms: 100
        retry_max_backoff_ms: 1000
        request_timeout_ms: 5000
        batch_size: 2048
        batch_timeout_ms: 1000
        headers:
          X-Custom: value
        tls:
          insecure_skip_verify: true
";
    Config::load_str(yaml).expect("otlp options should be accepted");
}

#[test]
fn non_otlp_rejects_new_options() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        retry_attempts: 3
";
    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("unknown field") && err.contains("retry_attempts"),
        "stdout output should reject retry_attempts at parse time: {err}"
    );
}

#[test]
fn elasticsearch_accepts_tls_and_request_timeout_ms() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: https://localhost:9200
        request_timeout_ms: 5000
        tls:
          insecure_skip_verify: true
";
    Config::load_str(yaml).expect("elasticsearch should accept tls and request_timeout_ms");
}

#[test]
fn loki_accepts_tls_and_request_timeout_ms() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: loki
        endpoint: https://localhost:3100
        request_timeout_ms: 5000
        tls:
          insecure_skip_verify: true
";
    Config::load_str(yaml).expect("loki should accept tls and request_timeout_ms");
}

#[test]
fn elasticsearch_rejects_zero_request_timeout_ms() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: https://localhost:9200
        request_timeout_ms: 0
";
    let _ = Config::load_str(yaml).expect_err("zero request_timeout_ms should be rejected");
}

#[test]
fn otlp_rejects_zero_request_timeout_ms() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        request_timeout_ms: 0
";
    let _ = Config::load_str(yaml).expect_err("zero request_timeout_ms should be rejected");
}

#[test]
fn otlp_rejects_zero_batch_timeout_ms() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        batch_timeout_ms: 0
";
    let _ = Config::load_str(yaml).expect_err("zero batch_timeout_ms should be rejected");
}

#[test]
fn otlp_rejects_initial_backoff_exceeding_max() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        retry_initial_backoff_ms: 5000
        retry_max_backoff_ms: 1000
";
    let err = Config::load_str(yaml).unwrap_err().to_string();
    assert!(
        err.contains("retry_initial_backoff_ms") && err.contains("retry_max_backoff_ms"),
        "expected backoff ordering rejection, got: {err}"
    );
}

#[test]
fn arrow_ipc_accepts_batch_size() {
    let yaml = r"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: arrow_ipc
        endpoint: http://localhost:9000
        batch_size: 512
";
    Config::load_str(yaml).expect("arrow_ipc should accept batch_size");
}

#[test]
fn tcp_tls_accepts_cert_and_key() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
          key_file: /tmp/server.key
    outputs:
      - type: "null"
"#;
    Config::load_str(yaml).expect("tcp tls cert+key should validate");
}

#[test]
fn tcp_tls_rejects_partial_cert_key_pair() {
    let partial = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
    outputs:
      - type: "null"
"#;
    let err = Config::load_str(partial).unwrap_err().to_string();
    assert!(
        err.contains("tls.cert_file") && err.contains("tls.key_file"),
        "expected cert/key pairing validation error, got: {err}"
    );
}

#[test]
fn tcp_mtls_requires_client_ca() {
    let mtls = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
          key_file: /tmp/server.key
          require_client_auth: true
    outputs:
      - type: "null"
"#;
    let err = Config::load_str(mtls).unwrap_err().to_string();
    assert!(
        err.contains("require_client_auth requires tls.client_ca_file"),
        "expected missing client CA rejection, got: {err}"
    );
}

#[test]
fn tcp_mtls_accepts_client_ca_when_auth_required() {
    let mtls = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
          key_file: /tmp/server.key
          client_ca_file: /tmp/ca.crt
          require_client_auth: true
    outputs:
      - type: "null"
"#;
    Config::load_str(mtls).expect("mTLS with client CA should validate");
}

#[test]
fn tcp_client_ca_requires_auth_required() {
    let mtls = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
          key_file: /tmp/server.key
          client_ca_file: /tmp/ca.crt
    outputs:
      - type: "null"
"#;
    let err = Config::load_str(mtls).unwrap_err().to_string();
    assert!(
        err.contains("client_ca_file requires tls.require_client_auth: true"),
        "expected client CA without mTLS rejection, got: {err}"
    );
}

#[test]
fn tcp_client_ca_rejects_blank_path() {
    let mtls = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
          key_file: /tmp/server.key
          client_ca_file: "   "
    outputs:
      - type: "null"
"#;
    let err = Config::load_str(mtls).unwrap_err().to_string();
    assert!(
        err.contains("client_ca_file must not be empty"),
        "expected blank client CA rejection, got: {err}"
    );
}
