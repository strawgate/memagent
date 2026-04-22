use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use tempfile::tempdir;

fn ff_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ff"))
}

fn write_file(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write test file");
}

fn free_localhost_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind localhost ephemeral port")
        .local_addr()
        .expect("read listener addr")
        .port()
}

#[test]
fn cli_help_uses_documented_binary_name() {
    let output = ff_command().arg("--help").output().expect("run --help");
    assert!(output.status.success(), "--help should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Usage: ff [COMMAND]"), "stdout was: {stdout}");
    assert!(stdout.contains("generate-json"), "stdout was: {stdout}");
    assert!(stdout.contains("send"), "stdout was: {stdout}");
}

#[test]
fn generate_json_writes_requested_number_of_lines() {
    let dir = tempdir().expect("tempdir");
    let output_path = dir.path().join("logs.json");

    let output = ff_command()
        .args(["generate-json", "5"])
        .arg(&output_path)
        .output()
        .expect("run generate-json");
    assert!(output.status.success(), "generate-json should succeed");

    let contents = fs::read_to_string(&output_path).expect("read generated file");
    assert_eq!(contents.lines().count(), 5, "generated contents were: {contents}");
}

#[test]
fn validate_and_dry_run_are_read_only_for_file_outputs() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yaml");
    let log_path = dir.path().join("logs.json");
    let output_path = dir.path().join("out.ndjson");

    write_file(
        &config_path,
        &format!(
            "input:\n  type: file\n  path: {}\n  format: json\noutput:\n  type: file\n  path: {}\n  format: json\n",
            log_path.display(),
            output_path.display()
        ),
    );
    write_file(&log_path, "{\"level\":\"INFO\",\"message\":\"ok\"}\n");

    let validate = ff_command()
        .args(["validate", "--config"])
        .arg(&config_path)
        .output()
        .expect("run validate");
    assert!(validate.status.success(), "validate should succeed");
    assert!(
        !output_path.exists(),
        "validate should not create output files"
    );
    let validate_stdout = String::from_utf8_lossy(&validate.stdout);
    assert!(
        validate_stdout.contains("config ok: 1 pipeline(s)"),
        "stdout was: {validate_stdout}"
    );

    let dry_run = ff_command()
        .args(["dry-run", "--config"])
        .arg(&config_path)
        .output()
        .expect("run dry-run");
    assert!(dry_run.status.success(), "dry-run should succeed");
    assert!(
        !output_path.exists(),
        "dry-run should not create output files"
    );
    let dry_run_stdout = String::from_utf8_lossy(&dry_run.stdout);
    assert!(
        dry_run_stdout.contains("dry run ok: 1 pipeline(s)"),
        "stdout was: {dry_run_stdout}"
    );
}

#[test]
fn effective_config_is_read_only() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yaml");
    let log_path = dir.path().join("logs.json");
    let output_path = dir.path().join("out.ndjson");

    write_file(
        &config_path,
        &format!(
            "input:\n  type: file\n  path: {}\n  format: json\noutput:\n  type: file\n  path: {}\n  format: json\n",
            log_path.display(),
            output_path.display()
        ),
    );
    write_file(&log_path, "{\"level\":\"INFO\",\"message\":\"ok\"}\n");

    let output = ff_command()
        .args(["effective-config", "--config"])
        .arg(&config_path)
        .output()
        .expect("run effective-config");
    assert!(output.status.success(), "effective-config should succeed");
    assert!(
        !output_path.exists(),
        "effective-config should not create output files"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("type: file"), "stdout was: {stdout}");
    assert!(stdout.contains("format: json"), "stdout was: {stdout}");
}

#[test]
fn send_and_bare_piped_ff_follow_documented_stdin_workflows() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("destination.yaml");
    write_file(
        &config_path,
        "output:\n  type: stdout\n  format: json\n",
    );
    let stdin_payload = "{\"level\":\"ERROR\",\"message\":\"slow bad\",\"status\":503,\"duration_ms\":80}\n";

    let mut send = ff_command();
    send.args(["send", "--config"])
        .arg(&config_path)
        .args(["--format", "json", "--service", "checkout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut send = send.spawn().expect("spawn ff send");
    send.stdin
        .as_mut()
        .expect("stdin handle")
        .write_all(stdin_payload.as_bytes())
        .expect("write stdin payload");
    let send_output = send.wait_with_output().expect("wait for ff send");
    assert!(send_output.status.success(), "ff send should succeed");
    let send_stdout = String::from_utf8_lossy(&send_output.stdout);
    let send_stderr = String::from_utf8_lossy(&send_output.stderr);
    assert!(
        send_stdout.contains("\"message\":\"slow bad\""),
        "stdout was: {send_stdout}"
    );
    assert!(
        !send_stderr.contains("checkpoint flush failed"),
        "stderr was: {send_stderr}"
    );

    let mut bare = ff_command();
    bare.args(["--format", "json", "--service", "checkout"])
        .env("LOGFWD_CONFIG", &config_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut bare = bare.spawn().expect("spawn bare piped ff");
    bare.stdin
        .as_mut()
        .expect("stdin handle")
        .write_all(stdin_payload.as_bytes())
        .expect("write stdin payload");
    let bare_output = bare.wait_with_output().expect("wait for bare ff");
    assert!(bare_output.status.success(), "bare piped ff should succeed");
    let bare_stdout = String::from_utf8_lossy(&bare_output.stdout);
    let bare_stderr = String::from_utf8_lossy(&bare_output.stderr);
    assert!(
        bare_stdout.contains("\"message\":\"slow bad\""),
        "stdout was: {bare_stdout}"
    );
    assert!(
        !bare_stderr.contains("checkpoint flush failed"),
        "stderr was: {bare_stderr}"
    );
}

#[test]
fn blackhole_accepts_documented_otlp_send_flow() {
    let port = free_localhost_port();
    let bind_addr = format!("127.0.0.1:{port}");

    let mut blackhole = ff_command();
    blackhole
        .arg("blackhole")
        .arg(&bind_addr)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut blackhole = blackhole.spawn().expect("spawn blackhole");

    thread::sleep(Duration::from_millis(300));

    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("destination.yaml");
    write_file(
        &config_path,
        &format!(
            "output:\n  type: otlp\n  endpoint: http://{bind_addr}/v1/logs\n"
        ),
    );

    let mut send = ff_command();
    send.args(["send", "--config"])
        .arg(&config_path)
        .args(["--format", "json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut send = send.spawn().expect("spawn ff send");
    send.stdin
        .as_mut()
        .expect("stdin handle")
        .write_all(b"{\"level\":\"INFO\",\"message\":\"hello\"}\n")
        .expect("write stdin payload");
    let send_output = send.wait_with_output().expect("wait for ff send");

    blackhole.kill().expect("kill blackhole");
    let _ = blackhole.wait();

    assert!(
        send_output.status.success(),
        "otlp send via blackhole should succeed: stdout={} stderr={}",
        String::from_utf8_lossy(&send_output.stdout),
        String::from_utf8_lossy(&send_output.stderr)
    );
}
