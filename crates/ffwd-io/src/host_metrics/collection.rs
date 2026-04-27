impl HostMetricsCommon {
    fn base_row(
        &self,
        control: &ControlState,
        event_family: &str,
        event_kind: &str,
        signal_status: &str,
        message: &str,
    ) -> SensorRow {
        SensorRow {
            timestamp_unix_nano: get_unix_nanos_now(),
            sensor_name: self.name.clone(),
            sensor_target_platform: self.target.as_str().to_string(),
            sensor_host_platform: self.host_platform.to_string(),
            event_family: event_family.to_string(),
            event_kind: event_kind.to_string(),
            signal_family: None,
            signal_status: signal_status.to_string(),
            control_generation: control.generation,
            control_source: control.source.as_str().to_string(),
            control_path: self
                .cfg
                .control_path
                .as_ref()
                .map(|path| path.display().to_string()),
            enabled_families: Some(enabled_families_csv(control)),
            effective_emit_signal_rows: Some(control.emit_signal_rows),
            message: message.to_string(),
            process_pid: None,
            process_parent_pid: None,
            process_name: None,
            process_cmd: None,
            process_exe: None,
            process_status: None,
            process_cpu_usage: None,
            process_memory_bytes: None,
            process_virtual_memory_bytes: None,
            process_start_time_unix_sec: None,
            process_disk_read_delta_bytes: None,
            process_disk_write_delta_bytes: None,
            process_user_id: None,
            process_effective_user_id: None,
            process_group_id: None,
            process_effective_group_id: None,
            process_cwd: None,
            process_session_id: None,
            process_run_time_secs: None,
            process_open_files: None,
            process_thread_count: None,
            process_container_id: None,
            network_interface: None,
            network_received_delta_bytes: None,
            network_transmitted_delta_bytes: None,
            network_received_total_bytes: None,
            network_transmitted_total_bytes: None,
            network_packets_received_delta: None,
            network_packets_transmitted_delta: None,
            network_errors_received_delta: None,
            network_errors_transmitted_delta: None,
            network_mac_address: None,
            network_ip_addresses: None,
            network_mtu: None,
            system_total_memory_bytes: None,
            system_used_memory_bytes: None,
            system_available_memory_bytes: None,
            system_total_swap_bytes: None,
            system_used_swap_bytes: None,
            system_cpu_usage_percent: None,
            system_uptime_secs: None,
            system_cgroup_memory_limit: None,
            system_cgroup_memory_current: None,
            disk_io_read_delta_bytes: None,
            disk_io_write_delta_bytes: None,
            disk_io_read_total_bytes: None,
            disk_io_write_total_bytes: None,
        }
    }

    fn control_row(
        &self,
        control: &ControlState,
        event_kind: &str,
        message: &str,
        signal_status: &str,
    ) -> SensorRow {
        self.base_row(
            control,
            "sensor_control",
            event_kind,
            signal_status,
            message,
        )
    }

    fn capability_rows(&self, control: &ControlState) -> Vec<SensorRow> {
        let enabled: std::collections::HashSet<_> =
            control.enabled_families.iter().copied().collect();
        target_signal_families(self.target)
            .iter()
            .map(|family| {
                let is_enabled = enabled.contains(family);
                let mut row = self.base_row(
                    control,
                    "sensor_control",
                    "capability",
                    if is_enabled { "enabled" } else { "disabled" },
                    &if is_enabled {
                        format!("{} family enabled", family.as_str())
                    } else {
                        format!("{} family disabled", family.as_str())
                    },
                );
                row.signal_family = Some(family.as_str().to_string());
                row
            })
            .collect()
    }

    fn signal_sample_rows(
        &self,
        control: &ControlState,
        event_kind: &str,
        message: &str,
    ) -> Vec<SensorRow> {
        if !control.emit_signal_rows {
            return Vec::new();
        }

        control
            .enabled_families
            .iter()
            .filter(|family| {
                !matches!(
                    family,
                    SignalFamily::Process | SignalFamily::Network | SignalFamily::File
                )
            })
            .map(|family| {
                let mut row = self.base_row(
                    control,
                    family.as_str(),
                    event_kind,
                    "ok",
                    &format!("{} ({})", message, family.as_str()),
                );
                row.signal_family = Some(family.as_str().to_string());
                row
            })
            .collect()
    }

    fn run_collection_cycle(&mut self, control: &ControlState, out: &mut Vec<SensorRow>) -> usize {
        let enabled: std::collections::HashSet<_> =
            control.enabled_families.iter().copied().collect();

        // Families that have real data collection.
        let collection_families = [
            SignalFamily::Process,
            SignalFamily::Network,
            SignalFamily::File,
        ];
        let active_count = collection_families
            .iter()
            .filter(|f| enabled.contains(f))
            .count();
        if active_count == 0 {
            return 0;
        }

        // Always emit system health regardless of family budget.
        self.emit_system_rows(control, out);

        // Only refresh the subsystems we actually need.
        let needs_processes =
            enabled.contains(&SignalFamily::Process) || enabled.contains(&SignalFamily::File);
        if needs_processes {
            self.system.refresh_processes_specifics(
                ProcessesToUpdate::All,
                true,
                ProcessRefreshKind::nothing()
                    .with_cpu()
                    .with_memory()
                    .with_disk_usage()
                    .with_cmd(UpdateKind::OnlyIfNotSet)
                    .with_exe(UpdateKind::OnlyIfNotSet)
                    .with_user(UpdateKind::OnlyIfNotSet)
                    .with_cwd(UpdateKind::OnlyIfNotSet),
            );
        }
        if enabled.contains(&SignalFamily::Network) {
            self.networks.refresh(true);
        }

        // Split budget evenly across active families, rotating the remainder
        // so no single family is permanently starved (#1935).
        let poll_count = self.poll_count;
        self.poll_count = self.poll_count.wrapping_add(1);

        let mut emitted = 0usize;
        let mut family_idx = 0usize;

        if enabled.contains(&SignalFamily::Process) {
            emitted += self.emit_process_rows(
                control,
                out,
                rotated_family_budget(
                    self.cfg.max_rows_per_poll,
                    active_count,
                    poll_count,
                    family_idx,
                ),
            );
            family_idx += 1;
        }
        if enabled.contains(&SignalFamily::Network) {
            emitted += self.emit_network_rows(
                control,
                out,
                rotated_family_budget(
                    self.cfg.max_rows_per_poll,
                    active_count,
                    poll_count,
                    family_idx,
                ),
                &self.cfg,
            );
            family_idx += 1;
        }
        if enabled.contains(&SignalFamily::File) {
            emitted += self.emit_disk_io_rows(
                control,
                out,
                rotated_family_budget(
                    self.cfg.max_rows_per_poll,
                    active_count,
                    poll_count,
                    family_idx,
                ),
            );
        }

        emitted
    }

    fn emit_process_rows(
        &mut self,
        control: &ControlState,
        out: &mut Vec<SensorRow>,
        limit: usize,
    ) -> usize {
        let max_process_rows_per_poll = self
            .cfg
            .max_process_rows_per_poll
            .map_or(DEFAULT_MAX_PROCESS_ROWS_PER_POLL, NonZeroUsize::get);
        let limit = limit.min(max_process_rows_per_poll);
        if limit == 0 {
            return 0;
        }

        let processes = self.system.processes();
        let mut procs = BinaryHeap::with_capacity(limit.min(processes.len()));
        for (pid, process) in processes {
            let selected = SelectedProcess {
                pid: pid.as_u32(),
                process,
            };
            if procs.len() < limit {
                procs.push(selected);
            } else if procs
                .peek()
                .is_some_and(|largest| selected.pid < largest.pid)
            {
                procs.pop();
                procs.push(selected);
            }
        }
        let mut procs = procs.into_vec();
        procs.sort_unstable_by_key(|selected| selected.pid);

        let active_pids: std::collections::HashSet<u32> =
            procs.iter().map(|selected| selected.pid).collect();
        self.process_container_ids
            .retain(|pid, _| active_pids.contains(pid));

        let mut emitted = 0usize;
        for selected in procs {
            let pid = selected.pid;
            let process = selected.process;
            let disk_usage = process.disk_usage();
            let mut row = self.base_row(control, "process", "snapshot", "ok", "process snapshot");
            row.signal_family = Some("process".to_string());
            row.process_pid = Some(pid);
            row.process_parent_pid = process.parent().map(sysinfo::Pid::as_u32);
            row.process_name = Some(os_to_string(process.name()));
            row.process_cmd = Some(os_vec_to_string(process.cmd()));
            row.process_exe = process.exe().map(|p| p.to_string_lossy().to_string());
            row.process_status = Some(format!("{:?}", process.status()));
            row.process_cpu_usage = Some(process.cpu_usage());
            row.process_memory_bytes = Some(process.memory());
            row.process_virtual_memory_bytes = Some(process.virtual_memory());
            row.process_start_time_unix_sec = Some(process.start_time());
            row.process_disk_read_delta_bytes = Some(disk_usage.read_bytes);
            row.process_disk_write_delta_bytes = Some(disk_usage.written_bytes);
            #[cfg(unix)]
            {
                row.process_user_id = process.user_id().map(|uid| **uid);
                row.process_effective_user_id = process.effective_user_id().map(|uid| **uid);
                row.process_group_id = process.group_id().map(|gid| *gid);
                row.process_effective_group_id = process.effective_group_id().map(|gid| *gid);
            }
            row.process_cwd = process.cwd().map(|p| p.to_string_lossy().to_string());
            row.process_session_id = process.session_id().map(sysinfo::Pid::as_u32);
            row.process_run_time_secs = Some(process.run_time());
            row.process_open_files = process.open_files().map(|n| n as u64);
            row.process_thread_count = process.tasks().map(|t| t.len() as u64);
            row.process_container_id =
                cached_container_id(&mut self.process_container_ids, pid, process.start_time());
            out.push(row);
            emitted += 1;
        }

        emitted
    }

}

fn rotated_family_budget(
    max_rows_per_poll: usize,
    active_count: usize,
    poll_count: usize,
    family_idx: usize,
) -> usize {
    let per_family_budget = max_rows_per_poll / active_count;
    let remainder = max_rows_per_poll % active_count;
    let start_idx = poll_count % active_count;
    let extra = usize::from((family_idx + active_count - start_idx) % active_count < remainder);
    per_family_budget + extra
}

fn cached_container_id(
    cache: &mut HashMap<u32, CachedContainerId>,
    pid: u32,
    process_start_time_unix_sec: u64,
) -> Option<String> {
    cached_container_id_with_lookup(cache, pid, process_start_time_unix_sec, extract_container_id)
}

fn cached_container_id_with_lookup(
    cache: &mut HashMap<u32, CachedContainerId>,
    pid: u32,
    process_start_time_unix_sec: u64,
    mut lookup: impl FnMut(u32) -> Option<String>,
) -> Option<String> {
    let entry = cache.entry(pid).or_insert_with(|| CachedContainerId {
        process_start_time_unix_sec,
        container_id: lookup(pid),
    });

    if entry.process_start_time_unix_sec != process_start_time_unix_sec {
        *entry = CachedContainerId {
            process_start_time_unix_sec,
            container_id: lookup(pid),
        };
    }

    entry.container_id.clone()
}
