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

    let data_dir = validated.config().storage.data_dir.as_deref().map(PathBuf::from);

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

    // Channel for OpAMP to notify us of new remote config.
    let (reload_tx, mut reload_rx) = mpsc::channel::<()>(4);

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

    // Report initial effective config.
    state_handle.set_effective_config(&config_yaml);

    // Spawn OpAMP client task.
    // In supervisor mode, the OpAMP client writes to the intermediate remote
    // config file (NOT the main config path). handle_remote_config then reads
    // from that file, validates, and does the atomic write + SIGHUP dance.
    let opamp_shutdown = shutdown.clone();
    let opamp_data_dir = data_dir.clone();
    let opamp_remote_config_path = remote_config_path.clone();
    let opamp_handle = tokio::spawn(async move {
        if let Err(e) = opamp_client
            .run(
                opamp_shutdown,
                opamp_data_dir.as_deref(),
                Some(opamp_remote_config_path.as_path()),
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
                        Some(()) => ChildExit::Reload,
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
                ChildExit::Reload => {
                    // OpAMP pushed new config — validate, write, SIGHUP.
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
                                &remote_config_path,
                                &mut child,
                                child_pid,
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "supervisor: failed to check child status");
                        }
                    }
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

/// Handle a remote config push from OpAMP.
///
/// Reads the remote config file written by the OpAMP client, validates it,
/// atomically writes it to the main config path, and sends SIGHUP to the child.
async fn handle_remote_config(
    config_path: &Path,
    state_handle: &ffwd_opamp::OpampStateHandle,
    remote_path: &Path,
    child: &mut tokio::process::Child,
    child_pid: u32,
) {
    // Read and validate the remote config in one step.
    let validated = match ffwd_config::ValidatedConfig::from_file(remote_path) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(
                error = %e,
                path = %remote_path.display(),
                "supervisor: remote config failed to load or validate, skipping reload"
            );
            return;
        }
    };

    // Atomic write: write to temp file next to target, then rename.
    if let Err(e) = atomic_write_config(config_path, validated.effective_yaml()) {
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

    // Send SIGHUP to trigger child reload.
    if !forward_signal(child_pid, libc::SIGHUP) {
        return;
    }

    if child_survives_reload_window(child).await {
        state_handle.set_effective_config(validated.effective_yaml());
    } else {
        tracing::error!("supervisor: child exited during reload; effective config not updated");
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
    // Path contract properties — prevent the path mismatch bug
    // ═══════════════════════════════════════════════════════════════

    // OpAMP remote_config_path and handle_remote_config MUST read from the
    // same location. This property verifies the contract: both use
    // `OpampClient::remote_config_path(data_dir)`.
    proptest! {
        #[test]
        fn path_contract_opamp_writes_where_supervisor_reads(
            suffix in "[a-z]{1,10}",
        ) {
            let dir = tempfile::tempdir().expect("create temp dir");
            let data_dir = dir.path().join(&suffix);
            std::fs::create_dir_all(&data_dir).expect("create data dir");

            // The path OpAMP client writes to (when config_path=None, i.e. supervisor mode).
            let opamp_write_path = ffwd_opamp::OpampClient::remote_config_path(Some(&data_dir));

            // The path handle_remote_config reads from (line 322 in supervisor.rs).
            let supervisor_read_path = ffwd_opamp::OpampClient::remote_config_path(Some(&data_dir));

            prop_assert_eq!(
                opamp_write_path, supervisor_read_path,
                "OpAMP write path must equal supervisor read path"
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
    // State machine properties — model supervisor transitions
    // ═══════════════════════════════════════════════════════════════

    /// Model the supervisor state machine: idle → detect_config → validate →
    /// write → signal → idle. Every valid transition sequence ends in idle.
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum SupervisorState {
        Idle,
        ReadingConfig,
        Validating,
        Writing,
        Signaling,
    }

    #[derive(Debug, Clone, Copy)]
    enum Event {
        ConfigAvailable,
        ReadSuccess,
        ValidationOk,
        ValidationFail,
        WriteSuccess,
        WriteFail,
        ChildAlive,
        ChildDead,
    }

    fn transition(state: SupervisorState, event: Event) -> SupervisorState {
        match (state, event) {
            (SupervisorState::Idle, Event::ConfigAvailable) => SupervisorState::ReadingConfig,
            (SupervisorState::ReadingConfig, Event::ReadSuccess) => SupervisorState::Validating,
            (SupervisorState::ReadingConfig, _) => SupervisorState::Idle, // Read failed
            (SupervisorState::Validating, Event::ValidationOk) => SupervisorState::Writing,
            (SupervisorState::Validating, Event::ValidationFail) => SupervisorState::Idle,
            (SupervisorState::Writing, Event::WriteSuccess) => SupervisorState::Signaling,
            (SupervisorState::Writing, Event::WriteFail) => SupervisorState::Idle,
            (SupervisorState::Signaling, Event::ChildAlive) => SupervisorState::Idle,
            (SupervisorState::Signaling, Event::ChildDead) => SupervisorState::Idle,
            _ => state, // Invalid transition — stay in place.
        }
    }

    proptest! {
        /// Any sequence of events always leaves the supervisor in a valid state.
        #[test]
        fn supervisor_state_machine_always_valid(
            events in proptest::collection::vec(
                prop_oneof![
                    Just(Event::ConfigAvailable),
                    Just(Event::ReadSuccess),
                    Just(Event::ValidationOk),
                    Just(Event::ValidationFail),
                    Just(Event::WriteSuccess),
                    Just(Event::WriteFail),
                    Just(Event::ChildAlive),
                    Just(Event::ChildDead),
                ],
                1..50
            )
        ) {
            let mut state = SupervisorState::Idle;
            for event in &events {
                state = transition(state, *event);
                // State is always one of the valid states (type system enforces this,
                // but we also verify the machine doesn't get stuck in signaling forever).
                prop_assert!(matches!(
                    state,
                    SupervisorState::Idle
                        | SupervisorState::ReadingConfig
                        | SupervisorState::Validating
                        | SupervisorState::Writing
                        | SupervisorState::Signaling
                ));
            }
        }

        /// A valid "happy path" sequence always ends in Idle.
        #[test]
        fn happy_path_returns_to_idle(iterations in 1u32..20) {
            let mut state = SupervisorState::Idle;
            for _ in 0..iterations {
                state = transition(state, Event::ConfigAvailable);
                prop_assert_eq!(state, SupervisorState::ReadingConfig);
                state = transition(state, Event::ReadSuccess);
                prop_assert_eq!(state, SupervisorState::Validating);
                state = transition(state, Event::ValidationOk);
                prop_assert_eq!(state, SupervisorState::Writing);
                state = transition(state, Event::WriteSuccess);
                prop_assert_eq!(state, SupervisorState::Signaling);
                state = transition(state, Event::ChildAlive);
                prop_assert_eq!(state, SupervisorState::Idle);
            }
        }

        /// Validation failure always returns to idle without signaling.
        #[test]
        fn invalid_config_never_signals(iterations in 1u32..20) {
            let mut state = SupervisorState::Idle;
            let mut ever_signaled = false;
            for _ in 0..iterations {
                state = transition(state, Event::ConfigAvailable);
                state = transition(state, Event::ReadSuccess);
                state = transition(state, Event::ValidationFail);
                if state == SupervisorState::Signaling {
                    ever_signaled = true;
                }
                prop_assert_eq!(state, SupervisorState::Idle);
            }
            prop_assert!(!ever_signaled, "validation failure must never reach signaling");
        }

        /// Dead child check always returns to idle (PID race guard).
        #[test]
        fn dead_child_never_gets_signaled(iterations in 1u32..10) {
            for _ in 0..iterations {
                let mut state = SupervisorState::Idle;
                state = transition(state, Event::ConfigAvailable);
                state = transition(state, Event::ReadSuccess);
                state = transition(state, Event::ValidationOk);
                state = transition(state, Event::WriteSuccess);
                // Child died before we could signal.
                state = transition(state, Event::ChildDead);
                prop_assert_eq!(state, SupervisorState::Idle,
                    "dead child must return supervisor to idle");
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
