//! OpAMP client implementation.
//!
//! Wraps `otel-opamp-rs` to connect to an OpAMP server, report identity/health,
//! and trigger config reload when remote configuration is received.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use otel_opamp_rs::api::{Api, ApiCallbacks, ApiClientError, ConnectionSettings};
use otel_opamp_rs::opamp::spec::{AgentConfigFile, AgentConfigMap, AgentToServer, ServerToAgent};

use crate::error::OpampError;
use crate::identity::AgentIdentity;
use ffwd_config::OpampConfig;

/// Shared state between the OpAMP callbacks and the main client loop.
struct SharedState {
    /// The current effective configuration YAML.
    effective_config: String,
    /// Whether the last remote config was applied successfully.
    last_config_applied: bool,
}

/// A lightweight handle for updating OpAMP state from outside the client task.
///
/// Obtained via [`OpampClient::state_handle`] before calling [`OpampClient::run`].
#[derive(Clone)]
pub struct OpampStateHandle {
    state: Arc<Mutex<SharedState>>,
}

impl OpampStateHandle {
    /// Update the effective configuration reported to the OpAMP server.
    pub fn set_effective_config(&self, yaml: &str) {
        match self.state.lock() {
            Ok(mut state) => {
                state.effective_config = yaml.to_string();
                state.last_config_applied = true;
            }
            Err(e) => {
                tracing::warn!(error = %e, "opamp: state mutex poisoned, cannot update effective config");
            }
        }
    }
}

/// OpAMP client that connects to a server and manages remote configuration.
///
/// # Usage
///
/// ```ignore
/// let client = OpampClient::new(opamp_config, identity, reload_tx);
/// // Spawn in a background task:
/// tokio::spawn(async move { client.run(shutdown, data_dir, config_path, base_path).await });
/// ```
pub struct OpampClient {
    config: OpampConfig,
    identity: AgentIdentity,
    reload_tx: tokio::sync::mpsc::Sender<Option<String>>,
    state: Arc<Mutex<SharedState>>,
}

impl OpampClient {
    /// Create a new OpAMP client.
    ///
    /// - `config`: OpAMP configuration (endpoint, poll interval, etc.)
    /// - `identity`: Agent identity (instance UID, version, service name)
    /// - `reload_tx`: Channel to deliver config reload signals. `Some(yaml)` for
    ///   OpAMP-pushed configs, `None` for restart commands (re-read from disk).
    pub fn new(
        config: OpampConfig,
        identity: AgentIdentity,
        reload_tx: tokio::sync::mpsc::Sender<Option<String>>,
    ) -> Self {
        let state = Arc::new(Mutex::new(SharedState {
            effective_config: String::new(),
            last_config_applied: true,
        }));

        Self {
            config,
            identity,
            reload_tx,
            state,
        }
    }

    /// Update the effective configuration (used internally and in tests).
    ///
    /// Production callers should use [`OpampStateHandle::set_effective_config`] instead.
    #[cfg(test)]
    pub(crate) fn set_effective_config(&self, yaml: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.effective_config = yaml.to_string();
            state.last_config_applied = true;
        }
    }

    /// Get a lightweight handle for updating OpAMP state from outside the client task.
    ///
    /// Call this before [`OpampClient::run`] since `run` consumes `self`.
    pub fn state_handle(&self) -> OpampStateHandle {
        OpampStateHandle {
            state: Arc::clone(&self.state),
        }
    }

    /// Get the path where remote config should be written.
    ///
    /// Returns `<data_dir>/opamp_remote_config.yaml` if a data directory is
    /// configured, or a temp file path otherwise.
    pub fn remote_config_path(data_dir: Option<&Path>) -> PathBuf {
        data_dir.map_or_else(
            || {
                std::env::temp_dir().join(format!(
                    "ffwd_opamp_remote_config_{}.yaml",
                    std::process::id()
                ))
            },
            |d| d.join("opamp_remote_config.yaml"),
        )
    }

    /// Get the path where remote config should be staged for a specific agent.
    ///
    /// When no `data_dir` is configured, the fallback temp path includes the
    /// OpAMP instance UID so multiple supervised agents on one host cannot share
    /// a staging file by accident.
    pub fn remote_config_path_for_identity(
        data_dir: Option<&Path>,
        identity: &AgentIdentity,
    ) -> PathBuf {
        data_dir.map_or_else(
            || {
                std::env::temp_dir().join(format!(
                    "ffwd_opamp_remote_config_{}.yaml",
                    identity.uid_hex()
                ))
            },
            |d| d.join("opamp_remote_config.yaml"),
        )
    }

    /// Run the OpAMP client loop until shutdown is signalled.
    ///
    /// - `shutdown`: Cancellation token to stop the client
    /// - `data_dir`: Data directory for state persistence (reserved for future use)
    /// - `config_path`: Fallback path used to derive the validation base directory
    ///   when `config_base_path` is not provided (uses the parent directory).
    /// - `config_base_path`: Base directory for resolving relative paths during
    ///   config validation. In supervised mode this should be the child's real
    ///   config directory (not the staging file's parent).
    pub async fn run(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        _data_dir: Option<&Path>,
        config_path: Option<&Path>,
        config_base_path: Option<&Path>,
    ) -> Result<(), OpampError> {
        tracing::info!(
            endpoint = %self.config.endpoint,
            instance_uid = %self.identity.uid_hex(),
            service = %self.identity.service_name,
            "opamp: starting client"
        );

        let poll_interval = Duration::from_secs(self.config.poll_interval_secs.get());
        let state = Arc::clone(&self.state);
        let accept_remote_config = self.config.accept_remote_config;
        let reload_tx = self.reload_tx.clone();

        // Derive validation base path: prefer explicit config_base_path (supervised mode),
        // fall back to config_path's parent, then data_dir.
        let validation_base_path = config_base_path.map(PathBuf::from).or_else(|| {
            config_path
                .and_then(|p| Path::new(p).parent())
                .map(PathBuf::from)
        });
        let mut handler = OpampHandler {
            state,
            accept_remote_config,
            reload_tx,
            validation_base_path,
        };

        let connection_settings = ConnectionSettings {
            server_endpoint: self.config.endpoint.clone(),
            api_key: self.config.api_key.clone().unwrap_or_default(),
            name: self.identity.service_name.clone(),
            version: self.identity.version.clone(),
            instance_id: self.identity.uid_hex(),
            ..Default::default()
        };

        let mut api = Api::new(connection_settings, Box::new(&mut handler));

        // Timeout for individual poll requests — prevents hanging on unresponsive servers.
        let poll_timeout = Duration::from_secs(30);

        // Initial poll: report identity/status to server immediately on connect
        // rather than waiting for the first poll_interval to elapse.
        match tokio::time::timeout(poll_timeout, api.poll()).await {
            Ok(()) => {}
            Err(_) => {
                tracing::warn!("opamp: initial poll timed out after {poll_timeout:?}");
            }
        }

        // Main polling loop.
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!("opamp: shutting down");
                    break;
                }
                () = tokio::time::sleep(poll_interval) => {
                    match tokio::time::timeout(poll_timeout, api.poll()).await {
                        Ok(()) => {}
                        Err(_) => {
                            tracing::warn!("opamp: poll timed out after {poll_timeout:?}");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Implements the `ApiCallbacks` trait from otel-opamp-rs.
struct OpampHandler {
    state: Arc<Mutex<SharedState>>,
    accept_remote_config: bool,
    reload_tx: tokio::sync::mpsc::Sender<Option<String>>,
    /// Base directory for validating config (resolving relative paths).
    /// In supervised mode this is the child's config dir, not the staging dir.
    validation_base_path: Option<PathBuf>,
}

impl ApiCallbacks for &mut OpampHandler {
    fn get_configuration(&mut self) -> Result<Option<AgentConfigMap>, ApiClientError> {
        let state = self
            .state
            .lock()
            .map_err(|e| ApiClientError::new(500, &format!("lock poisoned: {e}")))?;

        if state.effective_config.is_empty() {
            return Ok(None);
        }

        let mut config_map = AgentConfigMap::default();
        let body = AgentConfigFile {
            body: state.effective_config.as_bytes().to_vec(),
            content_type: "text/yaml".to_string(),
        };
        config_map.config_map.insert("ffwd.yaml".to_string(), body);

        Ok(Some(config_map))
    }

    fn get_features(&mut self) -> (u64, u64) {
        // Report capabilities based on configuration.
        // Bit 0x01 = ReportsStatus, 0x02 = AcceptsRemoteConfig, 0x04 = ReportsEffectiveConfig
        let mut capabilities: u64 = 0x01 | 0x04; // Always report status + effective config
        if self.accept_remote_config {
            capabilities |= 0x02;
        }
        let flags: u64 = 0;
        (capabilities, flags)
    }

    fn on_loop(&mut self) -> Result<Option<AgentToServer>, ApiClientError> {
        Ok(None)
    }

    fn on_error(&mut self, inbound: &ServerToAgent) {
        tracing::error!("opamp: server error: {:?}", inbound.error_response);
    }

    fn on_health_check(
        &mut self,
        _inbound: &ServerToAgent,
    ) -> Result<Option<AgentToServer>, ApiClientError> {
        Ok(None)
    }

    fn on_command(
        &mut self,
        inbound: &ServerToAgent,
    ) -> Result<Option<AgentToServer>, ApiClientError> {
        if let Some(ref command) = inbound.command {
            tracing::info!(command_type = command.r#type, "opamp: received command");
            // Command type 1 = Restart → re-read config from disk
            if command.r#type == 1 {
                tracing::info!("opamp: restart command received, triggering reload");
                let _ = self.reload_tx.try_send(None);
            }
        }
        Ok(None)
    }

    fn on_agent_remote_config(
        &mut self,
        inbound: &ServerToAgent,
    ) -> Result<Option<AgentToServer>, ApiClientError> {
        if !self.accept_remote_config {
            tracing::info!(
                "opamp: remote config received but accept_remote_config=false, ignoring"
            );
            return Ok(None);
        }

        let Some(ref remote_config) = inbound.remote_config else {
            return Ok(None);
        };
        let Some(ref config_map) = remote_config.config else {
            return Ok(None);
        };

        // Look for the ffwd.yaml entry (or take the first one).
        let config_body = config_map
            .config_map
            .get("ffwd.yaml")
            .or_else(|| config_map.config_map.values().next());

        let Some(body) = config_body else {
            return Ok(None);
        };

        let yaml = match String::from_utf8(body.body.clone()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "opamp: remote config is not valid UTF-8"
                );
                return Ok(None);
            }
        };
        tracing::info!(
            bytes = body.body.len(),
            "opamp: received remote configuration"
        );

        // Validate before signaling reload. Use the config base path (child's
        // config dir in supervised mode) rather than the staging file's parent.
        // Wrapped in catch_unwind to prevent a validation panic from killing the
        // entire OpAMP task (which would permanently disable remote config).
        let base_path = self.validation_base_path.as_deref();
        let validation_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ffwd_config::ValidatedConfig::from_yaml(&yaml, base_path)
        }));
        match validation_result {
            Ok(Ok(_validated)) => {
                // Send the validated YAML directly on the channel — no shared state,
                // no split-brain race. The consumer gets the config atomically with
                // the reload signal. Persistence to disk happens in the consumer
                // after the config is committed (for crash recovery).
                tracing::info!("opamp: validated remote config, triggering reload");
                if self.reload_tx.try_send(Some(yaml)).is_ok() {
                    // Only mark as not-applied once the signal is successfully queued.
                    if let Ok(mut state) = self.state.lock() {
                        state.last_config_applied = false;
                    }
                } else {
                    // Channel full: a reload is already in progress. This config is
                    // intentionally dropped — the server will re-push on the next poll
                    // because last_config_applied remains true (server sees config not
                    // yet acknowledged). This is safe debounce behavior.
                    tracing::debug!(
                        "opamp: reload channel full, config will be re-delivered on next poll"
                    );
                }
            }
            Ok(Err(e)) => {
                tracing::error!(
                    error = %e,
                    "opamp: received invalid remote config, rejecting"
                );
            }
            Err(_panic) => {
                tracing::error!(
                    "opamp: config validation panicked — rejecting config (task continues)"
                );
            }
        }
        Ok(None)
    }

    fn on_connection_settings_offers(
        &mut self,
        _inbound: &ServerToAgent,
    ) -> Result<Option<AgentToServer>, ApiClientError> {
        Ok(None)
    }

    fn on_packages_available(
        &mut self,
        _inbound: &ServerToAgent,
    ) -> Result<Option<AgentToServer>, ApiClientError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_creation() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let config = OpampConfig {
            endpoint: "http://localhost:4320/v1/opamp".to_string(),
            ..Default::default()
        };
        let identity = AgentIdentity::resolve(
            Some("550e8400-e29b-41d4-a716-446655440000"),
            None,
            "ffwd",
            "0.1.0",
        );
        let client = OpampClient::new(config, identity, tx);
        assert!(
            client
                .state
                .lock()
                .expect("lock")
                .effective_config
                .is_empty()
        );
    }

    #[test]
    fn set_effective_config() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let config = OpampConfig {
            endpoint: "http://localhost:4320/v1/opamp".to_string(),
            ..Default::default()
        };
        let identity = AgentIdentity::resolve(None, None, "ffwd", "0.1.0");
        let client = OpampClient::new(config, identity, tx);

        client.set_effective_config("pipelines:\n  default:\n    inputs: []\n");
        let state = client.state.lock().expect("lock");
        assert!(state.effective_config.contains("pipelines:"));
        assert!(state.last_config_applied);
    }

    #[test]
    fn remote_config_path_with_data_dir() {
        let dir = Path::new("/tmp/ffwd-test");
        let path = OpampClient::remote_config_path(Some(dir));
        assert_eq!(path, dir.join("opamp_remote_config.yaml"));
    }

    #[test]
    fn remote_config_path_without_data_dir() {
        let path = OpampClient::remote_config_path(None);
        assert!(path.to_string_lossy().contains("ffwd_opamp_remote_config"));
    }
}
