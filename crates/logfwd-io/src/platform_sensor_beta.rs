//! Cross-platform beta sensor input.
//!
//! This source is intentionally lightweight: it provides a stable beta input
//! shape and explicit platform targeting while deeper platform-native sensor
//! integrations are under active development.

use std::io;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use logfwd_types::diagnostics::ComponentHealth;
use serde::Serialize;

use crate::input::{InputEvent, InputSource};

/// Platform target for a beta sensor input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformSensorTarget {
    Linux,
    Macos,
    Windows,
}

impl PlatformSensorTarget {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Macos => "macos",
            Self::Windows => "windows",
        }
    }
}

/// Runtime options for platform beta inputs.
#[derive(Debug, Clone, Copy)]
pub struct PlatformSensorBetaConfig {
    /// Emit periodic heartbeats when the source is idle.
    pub emit_heartbeat: bool,
    /// Heartbeat cadence.
    pub poll_interval: Duration,
}

impl Default for PlatformSensorBetaConfig {
    fn default() -> Self {
        Self {
            emit_heartbeat: true,
            poll_interval: Duration::from_millis(10_000),
        }
    }
}

/// Beta input source for per-platform sensor bring-up.
#[derive(Debug)]
pub struct PlatformSensorBetaInput {
    machine: Option<PlatformSensorMachine>,
}

#[derive(Debug)]
enum PlatformSensorMachine {
    Init(PlatformSensorState<InitState>),
    Running(PlatformSensorState<RunningState>),
}

#[derive(Debug)]
struct PlatformSensorCommon {
    name: String,
    target: PlatformSensorTarget,
    host_platform: &'static str,
    cfg: PlatformSensorBetaConfig,
}

#[derive(Debug)]
struct PlatformSensorState<S> {
    common: PlatformSensorCommon,
    state: S,
}

#[derive(Debug)]
struct InitState;

#[derive(Debug)]
struct RunningState {
    last_emit: Instant,
}

impl PlatformSensorState<InitState> {
    fn start(self) -> io::Result<(PlatformSensorState<RunningState>, InputEvent)> {
        let event = self.common.build_data_event("startup")?;
        let running = PlatformSensorState {
            common: self.common,
            state: RunningState {
                last_emit: Instant::now(),
            },
        };
        Ok((running, event))
    }
}

impl PlatformSensorState<RunningState> {
    fn maybe_emit_heartbeat(&mut self) -> io::Result<Option<InputEvent>> {
        if !self.common.cfg.emit_heartbeat
            || self.state.last_emit.elapsed() < self.common.cfg.poll_interval
        {
            return Ok(None);
        }

        let event = self.common.build_data_event("heartbeat")?;
        self.state.last_emit = Instant::now();
        Ok(Some(event))
    }
}

impl PlatformSensorCommon {
    fn build_data_event(&self, sensor_event: &'static str) -> io::Result<InputEvent> {
        let event = SensorControlEvent {
            timestamp_unix_nano: now_unix_nano(),
            level: "INFO",
            message: "platform sensor beta heartbeat",
            event_family: "sensor_control",
            sensor_event,
            sensor_status: "beta",
            sensor_target_platform: self.target.as_str(),
            sensor_host_platform: self.host_platform,
            sensor_name: &self.name,
            sensor_beta: true,
        };

        let mut bytes = serde_json::to_vec(&event)
            .map_err(|e| io::Error::other(format!("json encode: {e}")))?;
        bytes.push(b'\n');

        let accounted_bytes = bytes.len() as u64;
        Ok(InputEvent::Data {
            bytes,
            source_id: None,
            accounted_bytes,
        })
    }
}

impl PlatformSensorBetaInput {
    /// Create a beta platform sensor source.
    ///
    /// Returns an error when `target` does not match the current host platform.
    pub fn new(
        name: impl Into<String>,
        target: PlatformSensorTarget,
        cfg: PlatformSensorBetaConfig,
    ) -> io::Result<Self> {
        let host_platform = current_host_platform().as_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "platform sensor beta inputs are unsupported on this host",
            )
        })?;

        if target.as_str() != host_platform {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} sensor beta input can only run on {} hosts (current host: {})",
                    target.as_str(),
                    target.as_str(),
                    host_platform
                ),
            ));
        }

        Ok(Self {
            machine: Some(PlatformSensorMachine::Init(PlatformSensorState {
                common: PlatformSensorCommon {
                    name: name.into(),
                    target,
                    host_platform,
                    cfg,
                },
                state: InitState,
            })),
        })
    }
}

impl InputSource for PlatformSensorBetaInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let machine = self
            .machine
            .take()
            .ok_or_else(|| io::Error::other("platform sensor beta state missing"))?;

        match machine {
            PlatformSensorMachine::Init(init) => {
                let (running, event) = init.start()?;
                self.machine = Some(PlatformSensorMachine::Running(running));
                Ok(vec![event])
            }
            PlatformSensorMachine::Running(mut running) => {
                let maybe_event = running.maybe_emit_heartbeat()?;
                self.machine = Some(PlatformSensorMachine::Running(running));
                Ok(maybe_event.into_iter().collect())
            }
        }
    }

    fn name(&self) -> &str {
        match self
            .machine
            .as_ref()
            .expect("platform sensor beta state should always be present")
        {
            PlatformSensorMachine::Init(init) => &init.common.name,
            PlatformSensorMachine::Running(running) => &running.common.name,
        }
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }
}

#[derive(Serialize)]
struct SensorControlEvent<'a> {
    timestamp_unix_nano: u64,
    level: &'static str,
    message: &'static str,
    event_family: &'static str,
    sensor_event: &'static str,
    sensor_status: &'static str,
    sensor_target_platform: &'static str,
    sensor_host_platform: &'static str,
    sensor_name: &'a str,
    sensor_beta: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostPlatform {
    Linux,
    Macos,
    Windows,
    Unsupported,
}

impl HostPlatform {
    const fn as_str(self) -> Option<&'static str> {
        match self {
            Self::Linux => Some("linux"),
            Self::Macos => Some("macos"),
            Self::Windows => Some("windows"),
            Self::Unsupported => None,
        }
    }
}

fn now_unix_nano() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn current_host_platform() -> HostPlatform {
    if cfg!(target_os = "linux") {
        HostPlatform::Linux
    } else if cfg!(target_os = "macos") {
        HostPlatform::Macos
    } else if cfg!(target_os = "windows") {
        HostPlatform::Windows
    } else {
        HostPlatform::Unsupported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host_target() -> PlatformSensorTarget {
        #[cfg(target_os = "linux")]
        {
            PlatformSensorTarget::Linux
        }
        #[cfg(target_os = "macos")]
        {
            PlatformSensorTarget::Macos
        }
        #[cfg(target_os = "windows")]
        {
            PlatformSensorTarget::Windows
        }
    }

    fn non_host_target() -> PlatformSensorTarget {
        #[cfg(target_os = "linux")]
        {
            PlatformSensorTarget::Macos
        }
        #[cfg(target_os = "macos")]
        {
            PlatformSensorTarget::Windows
        }
        #[cfg(target_os = "windows")]
        {
            PlatformSensorTarget::Linux
        }
    }

    #[test]
    fn rejects_non_matching_platform_target() {
        let err = PlatformSensorBetaInput::new(
            "beta",
            non_host_target(),
            PlatformSensorBetaConfig::default(),
        )
        .expect_err("non-matching target must fail");
        assert!(err.to_string().contains("can only run on"));
    }

    #[test]
    fn emits_startup_event_on_first_poll() {
        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig::default(),
        )
        .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        assert_eq!(events.len(), 1);
        let payload = match &events[0] {
            InputEvent::Data { bytes, .. } => std::str::from_utf8(bytes).expect("utf8"),
            _ => panic!("expected Data event"),
        };
        assert!(payload.contains("\"sensor_event\":\"startup\""));
        assert!(payload.contains("\"sensor_status\":\"beta\""));
    }

    #[test]
    fn heartbeat_disabled_emits_only_startup() {
        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                emit_heartbeat: false,
                poll_interval: Duration::from_millis(1),
            },
        )
        .expect("host target should be valid");

        assert_eq!(input.poll().expect("startup poll").len(), 1);
        assert!(input.poll().expect("second poll").is_empty());
    }
}
