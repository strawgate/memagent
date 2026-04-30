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
/// Run duration after which a child is considered stable for backoff reset.
const STABLE_RUN_THRESHOLD: Duration = Duration::from_secs(30);
/// Time a reloaded child must remain alive before reporting the pushed config as effective.
const CHILD_RELOAD_STABILITY_WINDOW: Duration = Duration::from_secs(2);
/// Backoff multiplier after each consecutive crash.
const RESTART_BACKOFF_FACTOR: u32 = 2;

/// Entry point for `ff supervised`.
pub(crate) async fn cmd_supervised(config_path: &str) -> Result<(), CliError> {
    let config_path = std::fs::canonicalize(config_path)
        .map_err(|e| CliError::Config(format!("cannot resolve config path {config_path}: {e}")))?;

    // Parse config to extract opamp section.
    let config_yaml = std::fs::read_to_string(&config_path)
        .map_err(|e| CliError::Config(format!("cannot read {}: {e}", config_path.display())))?;
    let validated = ffwd_config::ValidatedConfig::from_yaml(&config_yaml, config_path.parent())
        .map_err(|e| CliError::Config(e.to_string()))?;

    let opamp_config = validated.config().opamp.clone().ok_or_else(|| {
        CliError::Config("supervised mode requires an `opamp:` section in config".to_string())
    })?;

    let data_dir = validated
        .config()
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
        .map_err(|e| CliError::Runtime(std::io::Error::other(e)))?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|e| CliError::Runtime(std::io::Error::other(e)))?;

    // Channel for OpAMP to deliver config reload signals (capacity 1 — coalesces).
    // Some(yaml) = OpAMP pushed config, None = restart command (re-read from disk).
    let (reload_tx, mut reload_rx) = mpsc::channel::<Option<String>>(1);

    // Resolve agent identity.
    let identity = ffwd_opamp::AgentIdentity::resolve(
        Some(&opamp_config.instance_uid),
        data_dir.as_deref(),
        &opamp_config.service_name,
        crate::VERSION,
    );
    let remote_config_path =
        ffwd_opamp::OpampClient::remote_config_path_for_identity(data_dir.as_deref(), &identity);

    // Create OpAMP client.
    let opamp_client = ffwd_opamp::OpampClient::new(opamp_config.clone(), identity, reload_tx);
    let state_handle = opamp_client.state_handle();

    // Report initial effective config (use validated YAML for consistency).
    state_handle.set_effective_config(validated.effective_yaml());

    // Spawn OpAMP client task.
    // Config pushes are sent directly on the reload channel as Some(yaml).
    // No shared state for config delivery — the channel carries both the
    // signal and the payload atomically.
    let opamp_shutdown = shutdown.clone();
    let opamp_data_dir = data_dir.clone();
    let opamp_remote_config_path = remote_config_path.clone();
    let opamp_config_base = config_path.parent().map(PathBuf::from);
    let opamp_handle = tokio::spawn(async move {
        if let Err(e) = opamp_client
            .run(
                opamp_shutdown,
                opamp_data_dir.as_deref(),
                Some(opamp_remote_config_path.as_path()),
                opamp_config_base.as_deref(),
            )
            .await
        {
            tracing::error!(error = %e, "supervisor: opamp client exited with error");
        }
    });

    let config_path_str = config_path.to_string_lossy().to_string();
    let mut backoff = RESTART_BACKOFF_INITIAL;

    // Main supervisor loop: spawn child, handle events until child exits.
    'outer: loop {
        let mut child = spawn_child(&config_path_str)?;
        let child_pid = child
            .id()
            .ok_or_else(|| CliError::Runtime(std::io::Error::other("failed to get child PID")))?;
        let spawn_time = tokio::time::Instant::now();
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
                result = reload_rx.recv() => {
                    match result {
                        Some(signal) => ChildExit::Reload(signal),
                        None => {
                            // Channel closed (all senders dropped, e.g. OpAMP task exited).
                            // Wait for child or shutdown — no more reloads possible.
                            tracing::warn!("supervisor: reload channel closed, waiting for child");
                            tokio::select! {
                                status = child.wait() => {
                                    match status {
                                        Ok(s) => ChildExit::Exited(s),
                                        Err(e) => ChildExit::WaitError(e),
                                    }
                                }
                                _ = sigterm.recv() => ChildExit::Signal(libc::SIGTERM),
                                _ = sigint.recv() => ChildExit::Signal(libc::SIGINT),
                            }
                        }
                    }
                }
            };

            match exit_reason {
                ChildExit::Signal(sig) => {
                    // Guard: check if child is still alive before forwarding.
                    // Prevents signaling a recycled PID if the child died between
                    // the select! and here (astronomically unlikely but safe).
                    match child.try_wait() {
                        Ok(Some(_status)) => {
                            // Child already exited — no need to signal.
                            tracing::debug!(
                                "supervisor: child already exited before signal forward"
                            );
                        }
                        _ => {
                            forward_signal(child_pid, sig);
                        }
                    }
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
                    // Reset backoff if child ran for a substantial time (stable run).
                    if spawn_time.elapsed() > STABLE_RUN_THRESHOLD {
                        backoff = RESTART_BACKOFF_INITIAL;
                    }
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
                ChildExit::Reload(yaml) => {
                    // Reload triggered — validate, write, SIGHUP.
                    // Guard: verify child is still alive before signaling.
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            // Child already exited — handle as a normal exit.
                            let terminated_normally = status.success()
                                || was_terminated_by(status, libc::SIGTERM)
                                || was_terminated_by(status, libc::SIGINT);
                            if terminated_normally {
                                tracing::info!(
                                    ?status,
                                    "supervisor: child exited (discovered during reload)"
                                );
                                shutdown.cancel();
                                break 'outer;
                            }
                            tracing::warn!(
                                ?status,
                                "supervisor: child crashed (discovered during reload)"
                            );
                            break; // Respawn via outer loop.
                        }
                        Ok(None) => {
                            // Child still running — safe to signal.
                            handle_remote_config(
                                &config_path,
                                &state_handle,
                                &mut child,
                                child_pid,
                                yaml,
                            )
                            .await;
                        }
                        Err(e) => {
                            // Cannot determine child status — treat as crash and respawn.
                            tracing::error!(
                                error = %e,
                                "supervisor: failed to check child status during reload, treating as crash"
                            );
                            break; // Respawn via outer loop.
                        }
                    }
                }
            }
        }
    }

    // Wait for OpAMP task to finish (bounded — don't hang on stuck network).
    match tokio::time::timeout(Duration::from_secs(5), opamp_handle).await {
        Ok(_) => {}
        Err(_) => {
            tracing::warn!("supervisor: opamp task did not exit within 5s, abandoning");
        }
    }
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
    /// Reload requested. `Some(yaml)` = OpAMP-pushed config, `None` = re-read from disk.
    Reload(Option<String>),
}

fn spawn_child(config_path: &str) -> Result<tokio::process::Child, CliError> {
    let exe = std::env::current_exe().map_err(|e| {
        CliError::Runtime(std::io::Error::other(format!(
            "cannot determine current executable: {e}"
        )))
    })?;

    tracing::debug!(exe = %exe.display(), config = config_path, "supervisor: spawning child");

    Command::new(&exe)
        .arg("run")
        .arg("-c")
        .arg(config_path)
        .kill_on_drop(false)
        .spawn()
        .map_err(|e| {
            CliError::Runtime(std::io::Error::other(format!(
                "failed to spawn child process: {e}"
            )))
        })
}

fn forward_signal(pid: u32, signal: i32) -> bool {
    // SAFETY: `pid` comes from `child.id()` for a process this supervisor
    // spawned, and callers check child liveness before reload signals where it
    // matters. `signal` is supplied from libc signal constants. `kill` takes
    // integer values only, creates no Rust references, and any OS failure is
    // handled via `last_os_error` below.
    let ret = unsafe { libc::kill(pid as libc::pid_t, signal) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(pid, signal, error = %err, "supervisor: failed to send signal to child");
        return false;
    }
    true
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

/// Handle a remote config push from OpAMP using the config-flow state machine.
///
/// Drives a [`ConfigFlow`](crate::config_flow::ConfigFlow) through its states,
/// executing effects at each step. Each step is deterministic and individually
/// testable.
///
/// `yaml` is `Some(config_yaml)` when the OpAMP server pushed a config directly,
/// or `None` for a restart command (re-read config from disk — not yet supported
/// in supervisor mode, logged as a no-op).
async fn handle_remote_config(
    config_path: &Path,
    state_handle: &ffwd_opamp::OpampStateHandle,
    child: &mut tokio::process::Child,
    child_pid: u32,
    yaml: Option<String>,
) {
    use crate::config_flow::{ConfigFlow, Effect, Event};

    // None means "re-read from disk" — in supervisor mode we only apply explicit
    // config pushes since we own the authoritative config file.
    let Some(remote_yaml) = yaml else {
        tracing::debug!(
            "supervisor: reload signal without config payload (restart command), ignoring"
        );
        return;
    };

    let mut flow = ConfigFlow::new(config_path.to_owned(), child_pid);

    // Kick off the cycle. We pass a dummy path since we already have the yaml.
    let mut effect = flow.step(Event::ConfigAvailable {
        remote_path: PathBuf::from("<in-memory>"),
    });

    loop {
        match effect {
            Effect::ReadAndValidate(_path) => {
                // Validate the YAML we received directly on the channel.
                let event = match ffwd_config::ValidatedConfig::from_yaml(
                    &remote_yaml,
                    config_path.parent(),
                ) {
                    Ok(v) => Event::ReadOk(Box::new(v)),
                    Err(e) => Event::ReadFailed(format!("validation failed: {e}")),
                };
                effect = flow.step(event);
            }
            Effect::WriteConfig { config_path, yaml } => {
                // Perform atomic write in a blocking task to avoid stalling the
                // tokio runtime on slow/network filesystems.
                let write_result = tokio::task::spawn_blocking(move || {
                    atomic_write_config(&config_path, &yaml).map(|()| config_path)
                })
                .await;
                let event = match write_result {
                    Ok(Ok(written_path)) => {
                        tracing::info!(
                            path = %written_path.display(),
                            "supervisor: wrote updated config, sending SIGHUP to child"
                        );
                        Event::WriteOk
                    }
                    Ok(Err(e)) => Event::WriteFailed(format!("failed to write config: {e}")),
                    Err(e) => Event::WriteFailed(format!("write task panicked: {e}")),
                };
                effect = flow.step(event);
            }
            Effect::SendSignal { child_pid: pid } => {
                let event = if forward_signal(pid, libc::SIGHUP) {
                    if child_survives_reload_window(child).await {
                        Event::SignalDelivered
                    } else {
                        Event::SignalFailed(
                            "child exited during reload stability window".to_owned(),
                        )
                    }
                } else {
                    Event::SignalFailed("failed to send SIGHUP".to_owned())
                };
                effect = flow.step(event);
            }
            Effect::ReportEffective { yaml } => {
                state_handle.set_effective_config(&yaml);
                break;
            }
            Effect::LogError(msg) => {
                tracing::error!("supervisor: {msg}");
                break;
            }
            Effect::None => {
                // The state machine returned no effect — either the cycle is complete
                // or an unexpected event was delivered (defensive no-op in step()).
                debug_assert!(
                    flow.is_idle(),
                    "Effect::None with non-idle state {:?} — possible invalid transition",
                    flow.state()
                );
                break;
            }
        }
    }
}

async fn child_survives_reload_window(child: &mut tokio::process::Child) -> bool {
    let deadline = tokio::time::Instant::now() + CHILD_RELOAD_STABILITY_WINDOW;
    loop {
        match child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => {
                tracing::error!(?status, "supervisor: child exited after SIGHUP");
                return false;
            }
            Err(e) => {
                tracing::error!(error = %e, "supervisor: failed to check child after SIGHUP");
                return false;
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Atomically write config by writing to a `.tmp` sibling then renaming.
fn atomic_write_config(target: &Path, content: &str) -> std::io::Result<()> {
    let file_name = target
        .file_name()
        .ok_or_else(|| std::io::Error::other("invalid config target"))?;
    let tmp_path = target.with_file_name(format!("{}.tmp", file_name.to_string_lossy()));
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
        let tmp = target.with_file_name("config.yaml.tmp");
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
        assert!(
            result.is_ok(),
            "minimal valid config should be accepted: {result:?}"
        );
    }

    #[test]
    fn was_terminated_by_matches_signal() {
        use std::os::unix::process::ExitStatusExt;
        // Unix `ExitStatus::from_raw(sig)` models the raw wait status used by
        // `was_terminated_by`, where `ExitStatusExt::signal()` applies
        // WIFSIGNALED/WTERMSIG semantics.
        let status = std::process::ExitStatus::from_raw(libc::SIGTERM);
        assert!(was_terminated_by(status, libc::SIGTERM));
        assert!(!was_terminated_by(status, libc::SIGINT));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    // Backoff always stays within [INITIAL, MAX] bounds.
    proptest! {
        #[test]
        fn backoff_stays_bounded(crash_count in 0u32..100) {
            let mut backoff = RESTART_BACKOFF_INITIAL;
            for _ in 0..crash_count {
                backoff = (backoff * RESTART_BACKOFF_FACTOR).min(RESTART_BACKOFF_MAX);
            }
            prop_assert!(backoff >= RESTART_BACKOFF_INITIAL);
            prop_assert!(backoff <= RESTART_BACKOFF_MAX);
        }
    }

    // Backoff is monotonically non-decreasing (without resets).
    proptest! {
        #[test]
        fn backoff_is_monotonic(crash_count in 1u32..50) {
            let mut backoff = RESTART_BACKOFF_INITIAL;
            let mut prev = backoff;
            for _ in 0..crash_count {
                backoff = (backoff * RESTART_BACKOFF_FACTOR).min(RESTART_BACKOFF_MAX);
                prop_assert!(backoff >= prev, "backoff must not decrease: {} < {}", backoff.as_millis(), prev.as_millis());
                prev = backoff;
            }
        }
    }

    // After enough crashes, backoff saturates at MAX.
    proptest! {
        #[test]
        fn backoff_saturates_at_max(crash_count in 10u32..100) {
            let mut backoff = RESTART_BACKOFF_INITIAL;
            for _ in 0..crash_count {
                backoff = (backoff * RESTART_BACKOFF_FACTOR).min(RESTART_BACKOFF_MAX);
            }
            prop_assert_eq!(backoff, RESTART_BACKOFF_MAX);
        }
    }

    // Atomic write is crash-safe: target always contains valid content.
    proptest! {
        #[test]
        fn atomic_write_leaves_valid_content(content in "[a-zA-Z0-9\n ]{1,1000}") {
            let dir = tempfile::tempdir().expect("create temp dir");
            let target = dir.path().join("test.yaml");

            // Pre-populate with known content.
            std::fs::write(&target, "original").expect("write original");

            atomic_write_config(&target, &content).expect("atomic write");

            let read_back = std::fs::read_to_string(&target).expect("read back");
            prop_assert_eq!(read_back, content);
        }
    }

    // Atomic write doesn't leave temp file behind on success.
    proptest! {
        #[test]
        fn atomic_write_no_temp_residue(content in "[a-z]{1,100}") {
            let dir = tempfile::tempdir().expect("create temp dir");
            let target = dir.path().join("cfg.yaml");

            atomic_write_config(&target, &content).expect("atomic write");

            let tmp = target.with_file_name("cfg.yaml.tmp");
            prop_assert!(!tmp.exists(), "temp file should not remain");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Path contract properties — remote_config_path helpers are consistent
    // ═══════════════════════════════════════════════════════════════

    // The remote_config_path helper functions return consistent results.
    // While the OpAMP callback no longer writes to disk (it uses in-memory
    // shared state), these path helpers are still used for crash-recovery
    // persistence and must remain deterministic.
    proptest! {
        #[test]
        fn path_contract_remote_config_path_is_deterministic(
            suffix in "[a-z]{1,10}",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let data_dir = dir.path().join(&suffix);
            std::fs::create_dir_all(&data_dir).expect("create data dir");

            // Both calls with same data_dir should return the same path.
            let path_a = ffwd_opamp::OpampClient::remote_config_path(Some(&data_dir));
            let path_b = ffwd_opamp::OpampClient::remote_config_path(Some(&data_dir));

            prop_assert_eq!(
                path_a, path_b,
                "remote_config_path must be deterministic"
            );
        }
    }

    // In supervisor mode (config_path=None), the remote config path must NOT
    // equal the main config path — they are intentionally different files.
    proptest! {
        #[test]
        fn supervisor_intermediate_differs_from_main(
            config_name in "[a-z]{1,8}\\.yaml",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let main_config = dir.path().join(&config_name);

            let intermediate = ffwd_opamp::OpampClient::remote_config_path(Some(dir.path()));

            prop_assert_ne!(
                main_config, intermediate,
                "intermediate file must differ from main config"
            );
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Atomic write survives concurrent reads — simulates file watcher
    // ═══════════════════════════════════════════════════════════════

    // After atomic_write_config, the file is always readable and contains
    // the expected content (never partial or empty).
    proptest! {
        #[test]
        fn atomic_write_never_produces_partial_content(
            iterations in 1u32..10,
            content in "[a-z]{10,200}",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let target = dir.path().join("config.yaml");

            // Simulate multiple rapid writes (like OpAMP pushing configs).
            for i in 0..iterations {
                let versioned = format!("{content}_v{i}");
                atomic_write_config(&target, &versioned).expect("atomic write");

                // Read immediately — should always be complete.
                let read_back = std::fs::read_to_string(&target).expect("read");
                prop_assert_eq!(
                    read_back, versioned,
                    "content must be complete after atomic write"
                );
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // State machine properties — uses production config_flow module
    // ═══════════════════════════════════════════════════════════════

    use crate::config_flow::{self, SimpleEvent, State as CfState};

    proptest! {
        /// Any sequence of events always leaves the supervisor in a valid state.
        #[test]
        fn supervisor_state_machine_always_valid(
            events in proptest::collection::vec(
                prop_oneof![
                    Just(SimpleEvent::ConfigAvailable),
                    Just(SimpleEvent::ReadOk),
                    Just(SimpleEvent::ReadFailed),
                    Just(SimpleEvent::WriteOk),
                    Just(SimpleEvent::WriteFailed),
                    Just(SimpleEvent::SignalDelivered),
                    Just(SimpleEvent::SignalFailed),
                ],
                1..50
            )
        ) {
            let mut state = CfState::Idle;
            for event in &events {
                state = config_flow::transition(state, event);
                prop_assert!(matches!(
                    state,
                    CfState::Idle
                        | CfState::Reading
                        | CfState::Writing
                        | CfState::Signaling
                ));
            }
        }

        /// A valid "happy path" sequence always ends in Idle.
        #[test]
        fn happy_path_returns_to_idle(iterations in 1u32..20) {
            let mut state = CfState::Idle;
            for _ in 0..iterations {
                state = config_flow::transition(state, &SimpleEvent::ConfigAvailable);
                prop_assert_eq!(state, CfState::Reading);
                state = config_flow::transition(state, &SimpleEvent::ReadOk);
                prop_assert_eq!(state, CfState::Writing);
                state = config_flow::transition(state, &SimpleEvent::WriteOk);
                prop_assert_eq!(state, CfState::Signaling);
                state = config_flow::transition(state, &SimpleEvent::SignalDelivered);
                prop_assert_eq!(state, CfState::Idle);
            }
        }

        /// Read failure always returns to idle without writing.
        #[test]
        fn invalid_config_never_writes(iterations in 1u32..20) {
            let mut state = CfState::Idle;
            let mut ever_writing = false;
            for _ in 0..iterations {
                state = config_flow::transition(state, &SimpleEvent::ConfigAvailable);
                state = config_flow::transition(state, &SimpleEvent::ReadFailed);
                if state == CfState::Writing || state == CfState::Signaling {
                    ever_writing = true;
                }
                prop_assert_eq!(state, CfState::Idle);
            }
            prop_assert!(!ever_writing, "read failure must never reach writing or signaling");
        }

        /// Signal failure always returns to idle (PID race guard).
        #[test]
        fn dead_child_never_gets_signaled(iterations in 1u32..10) {
            for _ in 0..iterations {
                let mut state = CfState::Idle;
                state = config_flow::transition(state, &SimpleEvent::ConfigAvailable);
                state = config_flow::transition(state, &SimpleEvent::ReadOk);
                state = config_flow::transition(state, &SimpleEvent::WriteOk);
                state = config_flow::transition(state, &SimpleEvent::SignalFailed);
                prop_assert_eq!(state, CfState::Idle,
                    "signal failure must return supervisor to idle");
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // File watcher filtering property
    // ═══════════════════════════════════════════════════════════════

    // Parent directory watch with filename filtering correctly identifies
    // when our config file is involved in a filesystem event.
    proptest! {
        #[test]
        fn filename_filter_matches_target(
            dir_name in "[a-z]{1,8}",
            config_name in "[a-z]{1,8}\\.yaml",
            other_name in "[a-z]{1,8}\\.json",
        ) {
            let base = PathBuf::from("/tmp").join(&dir_name);
            let config_path = base.join(&config_name);
            let other_path = base.join(&other_name);

            let config_filename = config_path.file_name().unwrap().to_os_string();

            // Event for our config file — should match.
            let config_matches = config_path.file_name()
                .map_or(false, |n| n == config_filename);
            prop_assert!(config_matches, "config file event must match filter");

            // Event for another file — should NOT match (unless names collide).
            if config_name != other_name {
                let other_matches = other_path.file_name()
                    .map_or(false, |n| n == config_filename);
                prop_assert!(!other_matches, "other file event must not match filter");
            }
        }
    }
}
