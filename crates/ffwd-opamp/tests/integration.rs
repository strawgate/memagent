//! Integration tests for the ffwd-opamp crate.
//!
//! These tests verify the OpAMP client's behavior in isolation (without a
//! real server connection). For end-to-end tests with the Go reference server,
//! see the e2e test suite.

use std::time::Duration;

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use ffwd_opamp::{AgentIdentity, OpampClient, OpampConfig};

fn positive_secs(value: u64) -> ffwd_config::PositiveSecs {
    ffwd_config::PositiveSecs::new(value).expect("test poll interval is non-zero")
}

#[tokio::test]
async fn client_starts_and_shuts_down_cleanly() {
    let (reload_tx, _reload_rx) = tokio::sync::mpsc::channel(1);
    let config = OpampConfig {
        // Point at a non-existent server — the client should still start
        // and shut down gracefully without panicking.
        endpoint: "http://127.0.0.1:19999/v1/opamp".to_string(),
        poll_interval_secs: positive_secs(1),
        ..Default::default()
    };
    let identity = AgentIdentity::resolve(None, None, "ffwd-test", "0.1.0");
    let client = OpampClient::new(config, identity, reload_tx);

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let handle = tokio::spawn(async move { client.run(shutdown_clone, None, None, None).await });

    // Let the client run briefly.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Signal shutdown.
    shutdown.cancel();

    // Client should exit cleanly.
    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("client should shut down within 5s")
        .expect("task should not panic");

    assert!(result.is_ok(), "client should exit without error");
}

#[tokio::test]
async fn state_handle_survives_client_run() {
    let (reload_tx, _reload_rx) = tokio::sync::mpsc::channel(1);
    let config = OpampConfig {
        endpoint: "http://127.0.0.1:19999/v1/opamp".to_string(),
        poll_interval_secs: positive_secs(1),
        ..Default::default()
    };
    let identity = AgentIdentity::resolve(None, None, "ffwd-test", "0.1.0");
    let client = OpampClient::new(config, identity, reload_tx);
    let state_handle = client.state_handle();

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    tokio::spawn(async move {
        let _ = client.run(shutdown_clone, None, None, None).await;
    });

    // Update effective config via the handle while client is running.
    state_handle.set_effective_config("pipelines:\n  test:\n    inputs: []\n");

    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();
}

#[tokio::test]
async fn identity_persists_across_instances() {
    let dir = TempDir::new().expect("create temp dir");

    let id1 = AgentIdentity::resolve(Some("auto"), Some(dir.path()), "ffwd", "0.1.0");
    let id2 = AgentIdentity::resolve(Some("auto"), Some(dir.path()), "ffwd", "0.1.0");

    // Same data dir → same UID.
    assert_eq!(id1.instance_uid, id2.instance_uid);
    assert_eq!(id1.uid_hex(), id2.uid_hex());

    // Hex should be 32 chars (16 bytes as hex).
    assert_eq!(id1.uid_hex().len(), 32);
}

#[tokio::test]
async fn explicit_uuid_overrides_auto() {
    let dir = TempDir::new().expect("create temp dir");
    let fixed_uid = "550e8400-e29b-41d4-a716-446655440000";

    let id = AgentIdentity::resolve(Some(fixed_uid), Some(dir.path()), "ffwd", "0.1.0");
    assert_eq!(id.uid_hex(), "550e8400e29b41d4a716446655440000");
}

#[tokio::test]
async fn remote_config_path_uses_data_dir() {
    let dir = TempDir::new().expect("create temp dir");
    let path = OpampClient::remote_config_path(Some(dir.path()));
    assert_eq!(path, dir.path().join("opamp_remote_config.yaml"));
}

#[tokio::test]
async fn remote_config_path_fallback_to_temp() {
    let path = OpampClient::remote_config_path(None);
    // Should be in system temp dir.
    assert!(
        path.to_string_lossy().contains("ffwd_opamp_remote_config"),
        "path should contain ffwd_opamp_remote_config: {path:?}"
    );
}

/// Test that config with OpAMP section parses correctly.
#[test]
fn config_with_opamp_section_parses() {
    let yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
opamp:
  endpoint: http://localhost:4320/v1/opamp
  api_key: secret123
  instance_uid: auto
  service_name: my-ffwd
  poll_interval_secs: 15
  accept_remote_config: true";

    let config = ffwd_config::Config::load_str(yaml).expect("should parse");
    let opamp = config.opamp.expect("opamp section should be present");
    assert_eq!(opamp.endpoint, "http://localhost:4320/v1/opamp");
    assert_eq!(opamp.api_key, Some("secret123".to_string()));
    assert_eq!(opamp.instance_uid, "auto");
    assert_eq!(opamp.service_name, "my-ffwd");
    assert_eq!(opamp.poll_interval_secs.get(), 15);
    assert!(opamp.accept_remote_config);
}

/// Test that config without OpAMP section still parses (backward compatible).
#[test]
fn config_without_opamp_section_parses() {
    let yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json";

    let config = ffwd_config::Config::load_str(yaml).expect("should parse");
    assert!(config.opamp.is_none());
}

/// Test OpAMP config defaults.
#[test]
fn opamp_config_defaults() {
    let yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
opamp:
  endpoint: http://localhost:4320/v1/opamp";

    let config = ffwd_config::Config::load_str(yaml).expect("should parse");
    let opamp = config.opamp.expect("opamp section should be present");
    assert_eq!(opamp.endpoint, "http://localhost:4320/v1/opamp");
    assert_eq!(opamp.api_key, None);
    assert_eq!(opamp.instance_uid, "auto");
    assert_eq!(opamp.service_name, "ffwd");
    assert_eq!(opamp.poll_interval_secs.get(), 30);
    assert!(opamp.accept_remote_config);
}

#[test]
fn opamp_config_rejects_zero_poll_interval() {
    let yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
opamp:
  endpoint: http://localhost:4320/v1/opamp
  poll_interval_secs: 0";

    let err = ffwd_config::Config::load_str(yaml).expect_err("zero poll interval must fail");
    assert!(
        err.to_string().contains("positive integer"),
        "unexpected error: {err}"
    );
}

#[test]
fn opamp_config_rejects_invalid_endpoint() {
    let yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
opamp:
  endpoint: localhost:4320/v1/opamp";

    let err = ffwd_config::Config::load_str(yaml).expect_err("invalid endpoint must fail");
    assert!(
        err.to_string().contains("opamp.endpoint"),
        "unexpected error: {err}"
    );
}

// ═══════════════════════════════════════════════════════════════
// Property-based tests
// ═══════════════════════════════════════════════════════════════

mod proptests {
    use proptest::prelude::*;

    // Identity is deterministic given the same data_dir.
    proptest! {
        #[test]
        fn identity_deterministic_with_data_dir(
            service_name in "[a-z]{1,20}",
            version in "[0-9]+\\.[0-9]+\\.[0-9]+",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let id1 = ffwd_opamp::AgentIdentity::resolve(
                Some("auto"), Some(dir.path()), &service_name, &version,
            );
            let id2 = ffwd_opamp::AgentIdentity::resolve(
                Some("auto"), Some(dir.path()), &service_name, &version,
            );
            prop_assert_eq!(&id1.instance_uid, &id2.instance_uid);
        }
    }

    // Explicit UIDs are preserved exactly (after dash removal).
    proptest! {
        #[test]
        fn explicit_uid_preserved(
            hex_chars in "[0-9a-f]{32}",
        ) {
            // Format as UUID-style with dashes.
            let uid = format!(
                "{}-{}-{}-{}-{}",
                &hex_chars[0..8], &hex_chars[8..12], &hex_chars[12..16],
                &hex_chars[16..20], &hex_chars[20..32]
            );
            let id = ffwd_opamp::AgentIdentity::resolve(Some(&uid), None, "test", "1.0.0");
            prop_assert_eq!(id.uid_hex(), hex_chars);
        }
    }

    // UID hex output is always 32 characters (16 bytes).
    proptest! {
        #[test]
        fn uid_hex_always_32_chars(
            service in "[a-z]{1,10}",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let id = ffwd_opamp::AgentIdentity::resolve(
                Some("auto"), Some(dir.path()), &service, "0.1.0",
            );
            prop_assert_eq!(id.uid_hex().len(), 32);
        }
    }

    // Remote config path is always under data_dir when provided.
    proptest! {
        #[test]
        fn remote_config_path_under_data_dir(
            suffix in "[a-z]{1,10}",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let sub = dir.path().join(&suffix);
            std::fs::create_dir_all(&sub).expect("create sub dir");
            let path = ffwd_opamp::OpampClient::remote_config_path(Some(&sub));
            prop_assert!(path.starts_with(&sub));
            prop_assert!(path.to_string_lossy().contains("opamp_remote_config"));
        }
    }

    // remote_config_path is always a YAML file (has .yaml extension).
    proptest! {
        #[test]
        fn remote_config_path_is_yaml(
            suffix in "[a-z]{1,10}",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let sub = dir.path().join(&suffix);
            std::fs::create_dir_all(&sub).expect("create sub dir");
            let path = ffwd_opamp::OpampClient::remote_config_path(Some(&sub));
            prop_assert!(
                path.extension().map_or(false, |ext| ext == "yaml"),
                "remote config path must have .yaml extension: {path:?}"
            );
        }
    }

    // remote_config_path with None uses temp dir (never panics).
    proptest! {
        #[test]
        fn remote_config_path_none_never_panics(
            _seed in 0u32..100,
        ) {
            let path = ffwd_opamp::OpampClient::remote_config_path(None);
            prop_assert!(!path.as_os_str().is_empty());
            prop_assert!(path.to_string_lossy().contains("ffwd_opamp_remote_config"));
        }
    }

    // Supervisor-mode contract: remote_config_path NEVER equals any plausible
    // main config path (they must be different files).
    proptest! {
        #[test]
        fn remote_config_path_never_collides_with_main(
            config_name in "(ffwd|config|pipeline)\\.ya?ml",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let main_config = dir.path().join(&config_name);
            let remote = ffwd_opamp::OpampClient::remote_config_path(Some(dir.path()));

            prop_assert_ne!(
                main_config, remote,
                "remote config path must never equal main config path"
            );
        }
    }
}
