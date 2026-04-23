impl HostMetricsInput {
    /// Create a platform sensor source.
    ///
    /// Returns an error when `target` does not match the current host platform.
    pub fn new(
        name: impl Into<String>,
        target: HostMetricsTarget,
        cfg: HostMetricsConfig,
    ) -> io::Result<Self> {
        let name = name.into();
        let host_platform = current_host_platform().as_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "platform sensor inputs are unsupported on this host",
            )
        })?;

        if target.as_str() != host_platform {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} sensor input can only run on {} hosts (current host: {})",
                    target.as_str(),
                    target.as_str(),
                    host_platform
                ),
            ));
        }
        let control = ControlState {
            generation: 1,
            enabled_families: parse_enabled_families(cfg.enabled_families.as_deref(), target)?,
            source: ControlSource::StaticConfig,
            emit_signal_rows: cfg.emit_signal_rows,
        };

        Ok(Self {
            name: name.clone(),
            machine: Some(HostMetricsMachine::Init(HostMetricsState {
                common: HostMetricsCommon {
                    name,
                    target,
                    host_platform,
                    cfg,
                    schema: sensor_schema(),
                    system: System::new(),
                    networks: Networks::new_with_refreshed_list(),
                    process_container_ids: HashMap::default(),
                    poll_count: 0,
                },
                state: InitState { control },
            })),
        })
    }
}

impl InputSource for HostMetricsInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let machine = self
            .machine
            .take()
            .ok_or_else(|| io::Error::other("platform sensor state missing"))?;

        let (next_machine, result) = match machine {
            HostMetricsMachine::Init(init) => match init.start() {
                Ok((running, events)) => (HostMetricsMachine::Running(running), Ok(events)),
                Err(err) => {
                    let (init, err) = *err;
                    (HostMetricsMachine::Init(init), Err(err))
                }
            },
            HostMetricsMachine::Running(mut running) => {
                let rows = running.poll_rows();
                let result = if rows.is_empty() {
                    Ok(Vec::new())
                } else {
                    running
                        .common
                        .build_batch_event(rows)
                        .map(|event| vec![event])
                };
                (HostMetricsMachine::Running(running), result)
            }
        };
        self.machine = Some(next_machine);
        result
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        match self.machine.as_ref() {
            Some(HostMetricsMachine::Init(_)) => ComponentHealth::Starting,
            Some(HostMetricsMachine::Running(running)) => running.state.health,
            None => ComponentHealth::Failed,
        }
    }
}

fn parse_enabled_families(
    configured: Option<&[String]>,
    target: HostMetricsTarget,
) -> io::Result<Vec<SignalFamily>> {
    let mut out = Vec::new();
    let Some(configured) = configured else {
        out.extend_from_slice(target_signal_families(target));
        return Ok(out);
    };

    for name in configured {
        let normalized = name.trim();
        if normalized.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sensor family names must not be empty",
            ));
        }
        let family = SignalFamily::parse(normalized)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if !target_signal_families(target).contains(&family) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "family '{}' is not available for {} targets",
                    family.as_str(),
                    target.as_str()
                ),
            ));
        }
        if !out.contains(&family) {
            out.push(family);
        }
    }

    Ok(out)
}

fn read_control_file(path: &Path) -> io::Result<Option<ControlFileConfig>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let parsed = serde_json::from_slice::<ControlFileConfig>(&bytes).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("control file '{}' is not valid JSON: {e}", path.display()),
                )
            })?;
            Ok(Some(parsed))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn enabled_families_csv(control: &ControlState) -> String {
    control
        .enabled_families
        .iter()
        .map(|family| family.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(target_os = "linux")]
fn extract_container_id(pid: u32) -> Option<String> {
    let path = format!("/proc/{pid}/cgroup");
    let content = std::fs::read_to_string(path).ok()?;
    for line in content.lines() {
        for segment in line.rsplit('/') {
            let cleaned = segment
                .strip_prefix("docker-")
                .or_else(|| segment.strip_prefix("cri-containerd-"))
                .or_else(|| segment.strip_prefix("crio-"))
                .unwrap_or(segment);
            let cleaned = cleaned.strip_suffix(".scope").unwrap_or(cleaned);
            if cleaned.len() == 64 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                return Some(cleaned.to_string());
            }
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn extract_container_id(_pid: u32) -> Option<String> {
    None
}
