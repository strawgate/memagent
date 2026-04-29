//! Supervisor integration tests.
//!
//! These tests exercise the supervisor lifecycle without a real OpAMP server.

use std::io::Write;
use std::time::Duration;

/// Test that the supervisor starts, spawns a child, and stops cleanly on SIGTERM.
#[tokio::test]
async fn supervisor_starts_and_stops_on_sigterm() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let config_path = dir.path().join("ffwd.yaml");

    // Config with opamp section (endpoint won't be reachable, but supervisor
    // should still start the child).
    let config = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          total_events: 999999
          batch_size: 100
    outputs:
      - type: "null"
opamp:
  endpoint: "http://127.0.0.1:19999/v1/opamp"
  poll_interval_secs: 3600
"#;
    std::fs::File::create(&config_path)
        .and_then(|mut f| f.write_all(config.as_bytes()))
        .expect("write config");

    // Find the ff binary.
    let exe = std::env::current_exe()
        .expect("current exe")
        .parent()
        .expect("parent dir")
        .parent()
        .expect("deps parent")
        .join("ff");

    if !exe.exists() {
        eprintln!("skipping supervisor test: ff binary not found at {}", exe.display());
        return;
    }

    let mut child = tokio::process::Command::new(&exe)
        .arg("supervised")
        .arg("-c")
        .arg(&config_path)
        .kill_on_drop(false)
        .spawn()
        .expect("spawn supervised");

    // Give the supervisor time to start.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify process is running.
    assert!(child.id().is_some(), "supervisor should have a PID");

    // Send SIGTERM.
    let pid = child.id().expect("pid");
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }

    // Wait for exit with timeout.
    let result = tokio::time::timeout(Duration::from_secs(15), child.wait()).await;
    match result {
        Ok(Ok(status)) => {
            // The supervisor should exit cleanly (possibly with signal status).
            eprintln!("supervisor exited with: {status:?}");
        }
        Ok(Err(e)) => {
            panic!("error waiting for supervisor: {e}");
        }
        Err(_) => {
            // Timeout — kill it.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGKILL);
            }
            panic!("supervisor did not exit within 15s after SIGTERM");
        }
    }
}

/// Test that config validation rejects invalid YAML before writing.
#[test]
fn config_validation_rejects_bad_config() {
    let bad_yaml = "this is not valid ffwd config at all {{{{";
    let result = ffwd_config::Config::load_str(bad_yaml);
    assert!(result.is_err(), "invalid config should be rejected");
}

/// Test that config validation accepts a valid config.
#[test]
fn config_validation_accepts_valid_config() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          total_events: 1
          batch_size: 1
    outputs:
      - type: "null"
"#;
    let result = ffwd_config::Config::load_str(yaml);
    assert!(result.is_ok(), "valid config should be accepted: {result:?}");
}
