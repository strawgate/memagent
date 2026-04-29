//! Supervisor mode: wraps `ff run` as a child process managed via OpAMP.
//!
//! The supervisor:
//! 1. Parses the config to extract the `opamp:` section
//! 2. Spawns `ff run -c <config>` as a child process
//! 3. Runs the OpAMP client, listening for remote config pushes
//! 4. On new config: validates, writes atomically, sends SIGHUP to child
//! 5. Restarts the child on unexpected exits (with backoff)
//! 6. Forwards SIGTERM/SIGINT to child for graceful shutdown

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::cli::CliError;

/// Initial delay before restarting a crashed child.
const RESTART_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
/// Maximum backoff delay.
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Backoff multiplier after each consecutive crash.
const RESTART_BACKOFF_FACTOR: u32 = 2;

/// Entry point for `ff supervised`.
pub(crate) async fn cmd_supervised(config_path: &str) -> Result<(), CliError> {
    let config_path = std::fs::canonicalize(config_path)
        .map_err(|e| CliError::Config(format!("cannot resolve config path {config_path}: {e}")))?;

    // Parse config to extract opamp section.
    let config_yaml = std::fs::read_to_string(&config_path)
        .map_err(|e| CliError::Config(format!("cannot read {}: {e}", config_path.display())))?;
    let config = ffwd_config::Config::load_str(&config_yaml)
        .map_err(|e| CliError::Config(e.to_string()))?;

    let opamp_config = config
        .opamp
        .ok_or_else(|| CliError::Config("supervised mode requires an `opamp:` section in config".to_string()))?;

    let data_dir = config
        .storage
        .data_dir
        .as_deref()
        .map(PathBuf::from);

    tracing::info!(
        config = %config_path.display(),
        endpoint = %opamp_config.endpoint,
        "supervisor: starting"
    );

    let shutdown = CancellationToken::new();

    // Set up signal handlers.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| CliError::Runtime(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|e| CliError::Runtime(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    // Channel for OpAMP to notify us of new remote config.
    let (reload_tx, mut reload_rx) = mpsc::channel::<()>(4);

    // Resolve agent identity.
    let identity = ffwd_opamp::AgentIdentity::resolve(
        Some(&opamp_config.instance_uid),
        data_dir.as_deref(),
        &opamp_config.service_name,
        crate::VERSION,
    );

    // Create OpAMP client.
    let opamp_client = ffwd_opamp::OpampClient::new(
        opamp_config.clone(),
        identity,
        reload_tx,
    );
    let state_handle = opamp_client.state_handle();

    // Report initial effective config.
    state_handle.set_effective_config(&config_yaml);

    // Spawn OpAMP client task.
    let opamp_shutdown = shutdown.clone();
    let opamp_data_dir = data_dir.clone();
    let opamp_handle = tokio::spawn(async move {
        if let Err(e) = opamp_client.run(opamp_shutdown, opamp_data_dir.as_deref()).await {
            tracing::error!(error = %e, "supervisor: opamp client exited with error");
        }
    });

    let config_path_str = config_path.to_string_lossy().to_string();
    let mut backoff = RESTART_BACKOFF_INITIAL;

    // Main supervisor loop: spawn child, handle events until child exits.
    'outer: loop {
        let mut child = spawn_child(&config_path_str)?;
        let child_pid = child.id().ok_or_else(|| {
            CliError::Runtime(std::io::Error::new(
                std::io::ErrorKind::Other,
                "failed to get child PID",
            ))
        })?;
        tracing::info!(pid = child_pid, "supervisor: child started");

        // Inner loop: monitor this child until it exits or we need to break.
        loop {
            let exit_reason = tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(s) => ChildExit::Exited(s),
                        Err(e) => ChildExit::WaitError(e),
                    }
                }
                _ = sigterm.recv() => {
                    tracing::info!("supervisor: received SIGTERM, forwarding to child");
                    ChildExit::Signal(libc::SIGTERM)
                }
                _ = sigint.recv() => {
                    tracing::info!("supervisor: received SIGINT, forwarding to child");
                    ChildExit::Signal(libc::SIGINT)
                }
                _ = reload_rx.recv() => {
                    ChildExit::Reload
                }
            };

            match exit_reason {
                ChildExit::Signal(sig) => {
                    forward_signal(child_pid, sig);
                    shutdown.cancel();
                    wait_or_kill(&mut child, child_pid).await;
                    break 'outer;
                }
                ChildExit::Exited(status) => {
                    let terminated_normally = status.success()
                        || was_terminated_by(status, libc::SIGTERM)
                        || was_terminated_by(status, libc::SIGINT);

                    if terminated_normally {
                        tracing::info!(?status, "supervisor: child exited normally");
                        shutdown.cancel();
                        break 'outer;
                    }

                    tracing::warn!(
                        ?status,
                        backoff_secs = backoff.as_secs(),
                        "supervisor: child exited unexpectedly, restarting after backoff"
                    );
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        _ = sigterm.recv() => {
                            tracing::info!("supervisor: received SIGTERM during backoff");
                            shutdown.cancel();
                            break 'outer;
                        }
                        _ = sigint.recv() => {
                            tracing::info!("supervisor: received SIGINT during backoff");
                            shutdown.cancel();
                            break 'outer;
                        }
                    }
                    backoff = (backoff * RESTART_BACKOFF_FACTOR).min(RESTART_BACKOFF_MAX);
                    // Break inner loop to respawn child in outer loop.
                    break;
                }
                ChildExit::WaitError(e) => {
                    tracing::error!(error = %e, "supervisor: error waiting for child");
                    shutdown.cancel();
                    break 'outer;
                }
                ChildExit::Reload => {
                    // OpAMP pushed new config — validate, write, SIGHUP.
                    handle_remote_config(
                        &config_path,
                        &state_handle,
                        data_dir.as_deref(),
                        child_pid,
                    );
                    // Continue inner loop — same child, just signalled it.
                    continue;
                }
            }
        }
    }

    // Wait for OpAMP task to finish.
    let _ = opamp_handle.await;
    tracing::info!("supervisor: exiting");
    Ok(())
}

// ---------------------------------------------------------------------------
// Child management helpers
// ---------------------------------------------------------------------------

enum ChildExit {
    Exited(std::process::ExitStatus),
    Signal(i32),
    WaitError(std::io::Error),
    Reload,
}

fn spawn_child(config_path: &str) -> Result<tokio::process::Child, CliError> {
    let exe = std::env::current_exe().map_err(|e| {
        CliError::Runtime(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("cannot determine current executable: {e}"),
        ))
    })?;

    tracing::debug!(exe = %exe.display(), config = config_path, "supervisor: spawning child");

    Command::new(&exe)
        .arg("run")
        .arg("-c")
        .arg(config_path)
        .kill_on_drop(false)
        .spawn()
        .map_err(|e| {
            CliError::Runtime(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to spawn child process: {e}"),
            ))
        })
}

fn forward_signal(pid: u32, signal: i32) {
    // Safety: sending a signal to a known child PID.
    let ret = unsafe { libc::kill(pid as libc::pid_t, signal) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(pid, signal, error = %err, "supervisor: failed to send signal to child");
    }
}

/// Wait for a child to exit, escalating to SIGKILL after a timeout.
async fn wait_or_kill(child: &mut tokio::process::Child, pid: u32) {
    match tokio::time::timeout(Duration::from_secs(30), child.wait()).await {
        Ok(Ok(status)) => {
            tracing::info!(?status, "supervisor: child exited after signal");
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "supervisor: error waiting for child after signal");
        }
        Err(_) => {
            tracing::warn!("supervisor: child did not exit within 30s, sending SIGKILL");
            forward_signal(pid, libc::SIGKILL);
            let _ = child.wait().await;
        }
    }
}

fn was_terminated_by(status: std::process::ExitStatus, signal: i32) -> bool {
    use std::os::unix::process::ExitStatusExt;
    status.signal() == Some(signal)
}

// ---------------------------------------------------------------------------
// Config reload helpers
// ---------------------------------------------------------------------------

/// Handle a remote config push from OpAMP.
///
/// Reads the remote config file written by the OpAMP client, validates it,
/// atomically writes it to the main config path, and sends SIGHUP to the child.
fn handle_remote_config(
    config_path: &Path,
    state_handle: &ffwd_opamp::OpampStateHandle,
    data_dir: Option<&Path>,
    child_pid: u32,
) {
    let remote_path = ffwd_opamp::OpampClient::remote_config_path(data_dir);

    let new_yaml = match std::fs::read_to_string(&remote_path) {
        Ok(yaml) => yaml,
        Err(e) => {
            tracing::error!(
                error = %e,
                path = %remote_path.display(),
                "supervisor: failed to read remote config"
            );
            return;
        }
    };

    // Validate the config before writing.
    if let Err(e) = ffwd_config::Config::load_str(&new_yaml) {
        tracing::error!(
            error = %e,
            "supervisor: remote config failed validation, skipping reload"
        );
        return;
    }

    // Atomic write: write to temp file next to target, then rename.
    if let Err(e) = atomic_write_config(config_path, &new_yaml) {
        tracing::error!(
            error = %e,
            path = %config_path.display(),
            "supervisor: failed to write config"
        );
        return;
    }

    tracing::info!(
        path = %config_path.display(),
        "supervisor: wrote updated config, sending SIGHUP to child"
    );

    // Update effective config in OpAMP state.
    state_handle.set_effective_config(&new_yaml);

    // Send SIGHUP to trigger child reload.
    forward_signal(child_pid, libc::SIGHUP);
}

/// Atomically write config by writing to a `.tmp` sibling then renaming.
fn atomic_write_config(target: &Path, content: &str) -> std::io::Result<()> {
    let tmp_path = target.with_extension("yaml.tmp");
    std::fs::write(&tmp_path, content)?;
    std::fs::rename(&tmp_path, target)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_creates_file() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let target = dir.path().join("config.yaml");
        let content = "pipelines:\n  test:\n    inputs: []\n    outputs: []\n";

        atomic_write_config(&target, content).expect("atomic write");
        let read_back = std::fs::read_to_string(&target).expect("read back");
        assert_eq!(read_back, content);

        // Temp file should not remain.
        let tmp = target.with_extension("yaml.tmp");
        assert!(!tmp.exists(), "temp file should be removed by rename");
    }

    #[test]
    fn atomic_write_overwrites_existing() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let target = dir.path().join("config.yaml");

        std::fs::write(&target, "old content").expect("write initial");
        atomic_write_config(&target, "new content").expect("atomic overwrite");

        let read_back = std::fs::read_to_string(&target).expect("read back");
        assert_eq!(read_back, "new content");
    }

    #[test]
    fn config_validation_rejects_invalid_yaml() {
        let result = ffwd_config::Config::load_str("not: valid: config: {{{{");
        assert!(result.is_err(), "invalid YAML should be rejected");
    }

    #[test]
    fn config_validation_accepts_minimal_config() {
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
        assert!(result.is_ok(), "minimal valid config should be accepted: {result:?}");
    }

    #[test]
    fn was_terminated_by_matches_signal() {
        use std::os::unix::process::ExitStatusExt;
        let status = std::process::ExitStatus::from_raw(libc::SIGTERM);
        assert!(was_terminated_by(status, libc::SIGTERM));
        assert!(!was_terminated_by(status, libc::SIGINT));
    }
}
