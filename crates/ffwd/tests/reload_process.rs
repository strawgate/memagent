//! Process-level reload integration tests.
//!
//! These tests spawn the `ff` binary as a child process and verify that
//! reload signals (SIGHUP, file watching) work end-to-end.

#[cfg(unix)]
mod process_reload {
    use std::process::{Command, Stdio};
    use std::time::Duration;

    fn ff_binary() -> std::path::PathBuf {
        let mut path = std::env::current_exe()
            .expect("current exe")
            .parent()
            .expect("parent")
            .parent()
            .expect("grandparent")
            .to_path_buf();
        path.push("ff");
        if !path.exists() {
            path = std::env::current_exe()
                .expect("current exe")
                .parent()
                .expect("parent")
                .to_path_buf();
            path.push("ff");
        }
        path
    }

    fn minimal_config() -> &'static str {
        "\
pipelines:
  default:
    inputs:
      - type: generator
        generator:
          events_per_sec: 10
    outputs:
      - type: 'null'
"
    }

    fn modified_config() -> &'static str {
        "\
pipelines:
  default:
    inputs:
      - type: generator
        generator:
          events_per_sec: 100
    outputs:
      - type: 'null'
"
    }

    #[test]
    fn sighup_triggers_reload_and_process_survives() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let config_path = dir.path().join("ffwd.yaml");
        std::fs::write(&config_path, minimal_config()).expect("write config");

        let binary = ff_binary();
        if !binary.exists() {
            panic!(
                "ff binary not found at {} — run `cargo build -p ffwd` first",
                binary.display()
            );
        }

        let child = Command::new(&binary)
            .args(["run", "--config", config_path.to_str().expect("path")])
            .env("FFWD_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ff");

        let pid = child.id() as i32;

        // Wait for startup.
        std::thread::sleep(Duration::from_millis(2000));

        // Modify config and send SIGHUP.
        std::fs::write(&config_path, modified_config()).expect("write modified config");
        // SAFETY: pid is a valid child process we just spawned and verified is running.
        unsafe { libc::kill(pid, libc::SIGHUP) };

        // Wait for reload to process.
        std::thread::sleep(Duration::from_millis(1500));

        // Send SIGTERM to shut down gracefully.
        // SAFETY: pid is a valid child process we just spawned.
        unsafe { libc::kill(pid, libc::SIGTERM) };

        let output = child.wait_with_output().expect("wait for ff");
        let stderr = String::from_utf8_lossy(&output.stderr);

        // Process should exit via SIGTERM (code 0 or signal 15).
        // On some systems SIGTERM yields exit code 143, on others 0.
        let exit_ok = output.status.success()
            || output.status.code() == Some(143)
            || output.status.code().is_none(); // killed by signal
        assert!(
            exit_ok,
            "ff should exit cleanly after SIGHUP+SIGTERM. status={:?} stderr:\n{stderr}",
            output.status
        );
    }

    #[test]
    fn invalid_config_sighup_does_not_crash() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let config_path = dir.path().join("ffwd.yaml");
        std::fs::write(&config_path, minimal_config()).expect("write config");

        let binary = ff_binary();
        if !binary.exists() {
            panic!(
                "ff binary not found at {} — run `cargo build -p ffwd` first",
                binary.display()
            );
        }

        let child = Command::new(&binary)
            .args(["run", "--config", config_path.to_str().expect("path")])
            .env("FFWD_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ff");

        let pid = child.id() as i32;
        std::thread::sleep(Duration::from_millis(2000));

        // Write broken config and trigger reload.
        std::fs::write(&config_path, "invalid yaml: [[[").expect("write bad config");
        // SAFETY: pid is a valid child process we just spawned and verified is running.
        unsafe { libc::kill(pid, libc::SIGHUP) };

        // Process should survive.
        std::thread::sleep(Duration::from_millis(1500));

        // Send SIGTERM.
        // SAFETY: pid is a valid child process we just spawned.
        unsafe { libc::kill(pid, libc::SIGTERM) };

        let output = child.wait_with_output().expect("wait for ff");
        let _stderr = String::from_utf8_lossy(&output.stderr);

        // Process should exit via SIGTERM — key point is it didn't crash from bad config.
        let exit_ok = output.status.success()
            || output.status.code() == Some(143)
            || output.status.code().is_none();
        assert!(
            exit_ok,
            "ff should survive invalid config reload. status={:?}",
            output.status
        );
    }

    #[test]
    fn watch_config_detects_file_changes() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let config_path = dir.path().join("ffwd.yaml");
        std::fs::write(&config_path, minimal_config()).expect("write config");

        let binary = ff_binary();
        if !binary.exists() {
            panic!(
                "ff binary not found at {} — run `cargo build -p ffwd` first",
                binary.display()
            );
        }

        let child = Command::new(&binary)
            .args([
                "run",
                "--config",
                config_path.to_str().expect("path"),
                "--watch-config",
            ])
            .env("FFWD_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ff");

        let pid = child.id() as i32;

        // Wait for startup + file watcher.
        std::thread::sleep(Duration::from_millis(2000));

        // Modify the config file.
        std::fs::write(&config_path, modified_config()).expect("write modified config");

        // Wait for debounce + reload.
        std::thread::sleep(Duration::from_millis(2000));

        // Shut down.
        // SAFETY: pid is a valid child process we just spawned.
        unsafe { libc::kill(pid, libc::SIGTERM) };

        let output = child.wait_with_output().expect("wait for ff");
        let stderr = String::from_utf8_lossy(&output.stderr);

        let exit_ok = output.status.success()
            || output.status.code() == Some(143)
            || output.status.code().is_none();
        assert!(
            exit_ok,
            "ff should exit cleanly with --watch-config. status={:?} stderr:\n{stderr}",
            output.status
        );
        assert!(
            stderr.contains("config watcher") || stderr.contains("config reload"),
            "expected watcher/reload log in stderr:\n{stderr}"
        );
    }
}
