use std::process::Command;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_exclusive_lock() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let config_path = dir.path().join("config.yaml");
    std::fs::write(
        &config_path,
        format!(
            r#"
input:
  type: generator
output:
  type: "null"
storage:
  data_dir: {}
"#,
            data_dir.display()
        ),
    )
    .unwrap();

    // Start the first instance.
    let mut child1 = Command::new("cargo")
        .args([
            "run",
            "-p",
            "logfwd",
            "--",
            "--config",
            config_path.to_str().unwrap(),
        ])
        .spawn()
        .expect("failed to start first logfwd instance");

    // Wait a bit for it to start and acquire the lock.
    std::thread::sleep(Duration::from_secs(10));

    // Try to start a second instance.
    let output2 = Command::new("cargo")
        .args([
            "run",
            "-p",
            "logfwd",
            "--",
            "--config",
            config_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to start second logfwd instance");

    // Clean up first instance — wait after kill to reap the process and prevent zombies.
    let _ = child1.kill();
    let _ = child1.wait();

    let stderr = String::from_utf8_lossy(&output2.stderr);
    assert!(
        stderr.contains("another instance of logfwd is already running"),
        "expected error message about another instance, got stderr: {}\nstdout: {}",
        stderr,
        String::from_utf8_lossy(&output2.stdout)
    );
    assert_eq!(output2.status.code(), Some(2), "expected exit code 2");
}
