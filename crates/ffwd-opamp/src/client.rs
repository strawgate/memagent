//! OpAMP client implementation.
//!
//! Wraps `otel-opamp-rs` to connect to an OpAMP server, report identity/health,
//! and trigger config reload when remote configuration is received.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use otel_opamp_rs::api::{Api, ApiCallbacks, ApiClientError, ConnectionSettings};
use otel_opamp_rs::opamp::spec::{AgentConfigFile, AgentConfigMap, AgentToServer, ServerToAgent};

use ffwd_config::OpampConfig;
use crate::error::OpampError;
use crate::identity::AgentIdentity;

/// Shared state between the OpAMP callbacks and the main client loop.
struct SharedState {
    /// The current effective configuration YAML.
    effective_config: String,
    /// Whether the last remote config was applied successfully.
    last_config_applied: bool,
    /// The last remote config received from the server (if any).
    pending_remote_config: Option<String>,
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
        if let Ok(mut state) = self.state.lock() {
            state.effective_config = yaml.to_string();
            state.last_config_applied = true;
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
/// tokio::spawn(async move { client.run(shutdown).await });
/// ```
pub struct OpampClient {
    config: OpampConfig,
    identity: AgentIdentity,
    reload_tx: tokio::sync::mpsc::Sender<()>,
    state: Arc<Mutex<SharedState>>,
}

impl OpampClient {
    /// Create a new OpAMP client.
    ///
    /// - `config`: OpAMP configuration (endpoint, poll interval, etc.)
    /// - `identity`: Agent identity (instance UID, version, service name)
    /// - `reload_tx`: Channel to trigger config reload in the bootstrap loop
    pub fn new(
        config: OpampConfig,
        identity: AgentIdentity,
        reload_tx: tokio::sync::mpsc::Sender<()>,
    ) -> Self {
        let state = Arc::new(Mutex::new(SharedState {
            effective_config: String::new(),
            last_config_applied: true,
            pending_remote_config: None,
        }));

        Self {
            config,
            identity,
            reload_tx,
            state,
        }
    }

    /// Update the effective configuration reported to the OpAMP server.
    ///
    /// Should be called after each successful reload with the new config YAML.
    pub fn set_effective_config(&self, yaml: &str) {
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
            || std::env::temp_dir().join("ffwd_opamp_remote_config.yaml"),
            |d| d.join("opamp_remote_config.yaml"),
        )
    }

    /// Run the OpAMP client loop until shutdown is signalled.
    ///
    /// This is the main entry point — spawn this as a background task.
    pub async fn run(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        data_dir: Option<&Path>,
    ) -> Result<(), OpampError> {
        tracing::info!(
            endpoint = %self.config.endpoint,
            instance_uid = %self.identity.uid_hex(),
            service = %self.identity.service_name,
            "opamp: starting client"
        );

        let poll_interval = Duration::from_secs(self.config.poll_interval_secs);
        let state = Arc::clone(&self.state);
        let accept_remote_config = self.config.accept_remote_config;
        let reload_tx = self.reload_tx.clone();
        let remote_config_path = Self::remote_config_path(data_dir);

        // Create the OpAMP API callbacks handler.
        let mut handler = OpampHandler {
            state,
            accept_remote_config,
            reload_tx,
            remote_config_path,
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

        // Main polling loop.
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!("opamp: shutting down");
                    break;
                }
                () = tokio::time::sleep(poll_interval) => {
                    api.poll().await;
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
    reload_tx: tokio::sync::mpsc::Sender<()>,
    remote_config_path: PathBuf,
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
        // Report capabilities: AcceptsRemoteConfig | ReportsEffectiveConfig | ReportsHealth
        let capabilities: u64 = 0x01 | 0x02 | 0x04;
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
            // Command type 1 = Restart
            if command.r#type == 1 {
                tracing::info!("opamp: restart command received, triggering reload");
                let _ = self.reload_tx.try_send(());
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

        let yaml = String::from_utf8_lossy(&body.body).to_string();
        tracing::info!(
            bytes = body.body.len(),
            "opamp: received remote configuration"
        );

        // Validate before writing.
        match ffwd_config::Config::load_str(&yaml) {
            Ok(_) => {
                // Write to disk so the bootstrap reload loop can pick it up.
                if let Err(e) = std::fs::write(&self.remote_config_path, &yaml) {
                    tracing::error!(
                        error = %e,
                        path = %self.remote_config_path.display(),
                        "opamp: failed to write remote config"
                    );
                    return Ok(None);
                }
                tracing::info!(
                    path = %self.remote_config_path.display(),
                    "opamp: wrote remote config, triggering reload"
                );
                let _ = self.reload_tx.try_send(());

                if let Ok(mut state) = self.state.lock() {
                    state.pending_remote_config = Some(yaml);
                    state.last_config_applied = false;
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "opamp: received invalid remote config, rejecting"
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
        let dir = std::path::Path::new("/tmp/ffwd-test");
        let path = OpampClient::remote_config_path(Some(dir));
        assert_eq!(path, dir.join("opamp_remote_config.yaml"));
    }

    #[test]
    fn remote_config_path_without_data_dir() {
        let path = OpampClient::remote_config_path(None);
        assert!(path.to_string_lossy().contains("ffwd_opamp_remote_config"));
    }
}
