struct SensorRow {
    timestamp_unix_nano: u64,
    event_family: String,
    event_kind: String,
    signal_family: Option<String>,
    signal_status: String,
    control_generation: u64,
    control_source: String,
    control_path: Option<String>,
    enabled_families: Option<String>,
    effective_emit_signal_rows: Option<bool>,
    message: String,

    // Process fields
    process_pid: Option<u32>,
    process_parent_pid: Option<u32>,
    process_name: Option<String>,
    process_cmd: Option<String>,
    process_exe: Option<String>,
    process_status: Option<String>,
    process_cpu_usage: Option<f32>,
    process_memory_bytes: Option<u64>,
    process_virtual_memory_bytes: Option<u64>,
    process_start_time_unix_sec: Option<u64>,
    process_disk_read_delta_bytes: Option<u64>,
    process_disk_write_delta_bytes: Option<u64>,
    process_user_id: Option<u32>,
    process_effective_user_id: Option<u32>,
    process_group_id: Option<u32>,
    process_effective_group_id: Option<u32>,
    process_cwd: Option<String>,
    process_session_id: Option<u32>,
    process_run_time_secs: Option<u64>,
    process_open_files: Option<u64>,
    process_thread_count: Option<u64>,
    process_container_id: Option<String>,

    // Network fields
    network_interface: Option<String>,
    network_received_delta_bytes: Option<u64>,
    network_transmitted_delta_bytes: Option<u64>,
    network_received_total_bytes: Option<u64>,
    network_transmitted_total_bytes: Option<u64>,
    network_packets_received_delta: Option<u64>,
    network_packets_transmitted_delta: Option<u64>,
    network_errors_received_delta: Option<u64>,
    network_errors_transmitted_delta: Option<u64>,
    network_mac_address: Option<String>,
    network_ip_addresses: Option<String>,
    network_mtu: Option<u64>,

    // System-level fields
    system_total_memory_bytes: Option<u64>,
    system_used_memory_bytes: Option<u64>,
    system_available_memory_bytes: Option<u64>,
    system_total_swap_bytes: Option<u64>,
    system_used_swap_bytes: Option<u64>,
    system_cpu_usage_percent: Option<f32>,
    system_uptime_secs: Option<u64>,
    system_cgroup_memory_limit: Option<u64>,
    system_cgroup_memory_current: Option<u64>,

    // Disk I/O fields
    disk_io_read_delta_bytes: Option<u64>,
    disk_io_write_delta_bytes: Option<u64>,
    disk_io_read_total_bytes: Option<u64>,
    disk_io_write_total_bytes: Option<u64>,
}

// Control files are operator-tunable runtime state, not a fixed schema. Tolerate
// unknown fields so operators can stage new knobs or leave stale ones behind
// (for example `emit_heartbeat`, which became a no-op) without the reader
// aborting mid-poll.
#[derive(Debug, Deserialize)]
struct ControlFileConfig {
    generation: Option<u64>,
    enabled_families: Option<Vec<String>>,
    emit_signal_rows: Option<bool>,
}

impl HostMetricsState<InitState> {
    fn start(mut self) -> Result<InitStartOk, InitStartErr> {
        let now = Instant::now();
        let mut rows = vec![self.common.control_row(
            &self.state.control,
            "startup",
            "sensor startup complete",
            "ok",
        )];
        rows.extend(self.common.capability_rows(&self.state.control));
        self.common
            .run_collection_cycle(&self.state.control, &mut rows);
        rows.extend(self.common.signal_sample_rows(
            &self.state.control,
            "startup_sample",
            "initial signal snapshot",
        ));

        let event = match self.common.build_batch_event(rows) {
            Ok(event) => event,
            Err(err) => return Err(Box::new((self, err))),
        };
        let last_control_check = now
            .checked_sub(self.common.cfg.control_reload_interval)
            .unwrap_or(now);
        let running = HostMetricsState {
            common: self.common,
            state: RunningState {
                last_emit: now,
                last_control_check,
                control: self.state.control,
                health: ComponentHealth::Healthy,
            },
        };
        Ok((running, vec![event]))
    }
}

impl HostMetricsState<RunningState> {
    fn poll_rows(&mut self) -> Vec<SensorRow> {
        let mut rows = Vec::new();

        if let Some(reload_rows) = self.try_reload_control() {
            rows.extend(reload_rows);
        }

        if self.state.last_emit.elapsed() >= self.common.cfg.poll_interval {
            self.common
                .run_collection_cycle(&self.state.control, &mut rows);

            if self.state.control.emit_signal_rows {
                rows.extend(self.common.signal_sample_rows(
                    &self.state.control,
                    "sample",
                    "periodic signal snapshot",
                ));
            }
            self.state.last_emit = Instant::now();
        }

        rows
    }

    fn try_reload_control(&mut self) -> Option<Vec<SensorRow>> {
        let path = self.common.cfg.control_path.as_ref()?;
        if self.state.last_control_check.elapsed() < self.common.cfg.control_reload_interval {
            return None;
        }
        self.state.last_control_check = Instant::now();

        match read_control_file(path) {
            Ok(None) => {
                self.state.health = ComponentHealth::Healthy;
                None
            }
            Ok(Some(file_cfg)) => {
                let mut next = self.state.control.clone();
                if let Some(enabled) = file_cfg.enabled_families {
                    let parsed = match parse_enabled_families(Some(&enabled), self.common.target) {
                        Ok(v) => v,
                        Err(e) => {
                            self.state.health = ComponentHealth::Degraded;
                            return Some(vec![self.common.control_row(
                                &self.state.control,
                                "control_reload_failed",
                                &format!("invalid enabled_families in control file: {e}"),
                                "error",
                            )]);
                        }
                    };
                    next.enabled_families = parsed;
                }
                if let Some(v) = file_cfg.emit_signal_rows {
                    next.emit_signal_rows = v;
                }
                next.source = ControlSource::ControlFile;
                let generation_changed = file_cfg
                    .generation
                    .is_some_and(|generation| generation != self.state.control.generation);

                let changed = next.enabled_families != self.state.control.enabled_families
                    || next.emit_signal_rows != self.state.control.emit_signal_rows
                    || generation_changed;

                if !changed {
                    self.state.health = ComponentHealth::Healthy;
                    return None;
                }

                next.generation = file_cfg
                    .generation
                    .unwrap_or_else(|| self.state.control.generation.saturating_add(1));
                self.state.control = next.clone();
                self.state.health = ComponentHealth::Healthy;

                let mut rows = vec![self.common.control_row(
                    &next,
                    "control_reload_applied",
                    "applied control file settings",
                    "ok",
                )];
                rows.extend(self.common.capability_rows(&next));
                rows.extend(self.common.signal_sample_rows(
                    &next,
                    "control_reload_sample",
                    "signal snapshot after control reload",
                ));
                Some(rows)
            }
            Err(e) => {
                self.state.health = ComponentHealth::Degraded;
                Some(vec![self.common.control_row(
                    &self.state.control,
                    "control_reload_failed",
                    &format!("failed to load control file: {e}"),
                    "error",
                )])
            }
        }
    }
}
