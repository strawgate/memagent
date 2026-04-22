impl HostMetricsCommon {
    fn emit_network_rows(
        &self,
        control: &ControlState,
        out: &mut Vec<SensorRow>,
        limit: usize,
    ) -> usize {
        let mut ifaces: Vec<_> = self.networks.iter().collect();
        ifaces.sort_unstable_by_key(|(a, _)| *a);

        let mut emitted = 0usize;
        for (iface, data) in ifaces.into_iter().take(limit) {
            let mut row = self.base_row(control, "network", "snapshot", "ok", "network snapshot");
            row.signal_family = Some("network".to_string());
            row.network_interface = Some(iface.clone());
            row.network_received_delta_bytes = Some(data.received());
            row.network_transmitted_delta_bytes = Some(data.transmitted());
            row.network_received_total_bytes = Some(data.total_received());
            row.network_transmitted_total_bytes = Some(data.total_transmitted());
            row.network_packets_received_delta = Some(data.packets_received());
            row.network_packets_transmitted_delta = Some(data.packets_transmitted());
            row.network_errors_received_delta = Some(data.errors_on_received());
            row.network_errors_transmitted_delta = Some(data.errors_on_transmitted());
            row.network_mac_address = Some(data.mac_address().to_string());
            row.network_ip_addresses = {
                let ips: Vec<String> = data
                    .ip_networks()
                    .iter()
                    .map(|ip| ip.addr.to_string())
                    .collect();
                if ips.is_empty() {
                    None
                } else {
                    Some(ips.join(","))
                }
            };
            row.network_mtu = Some(data.mtu());
            out.push(row);
            emitted += 1;
        }

        emitted
    }

    fn emit_system_rows(&mut self, control: &ControlState, out: &mut Vec<SensorRow>) -> usize {
        self.system.refresh_memory();
        self.system.refresh_cpu_usage();

        let mut row = self.base_row(
            control,
            "system",
            "snapshot",
            "ok",
            "system health snapshot",
        );
        row.signal_family = Some("system".to_string());
        row.system_total_memory_bytes = Some(self.system.total_memory());
        row.system_used_memory_bytes = Some(self.system.used_memory());
        row.system_available_memory_bytes = Some(self.system.available_memory());
        row.system_total_swap_bytes = Some(self.system.total_swap());
        row.system_used_swap_bytes = Some(self.system.used_swap());
        row.system_cpu_usage_percent = Some(self.system.global_cpu_usage());
        row.system_uptime_secs = Some(System::uptime());

        if let Some(limits) = self.system.cgroup_limits() {
            row.system_cgroup_memory_limit = Some(limits.total_memory);
            row.system_cgroup_memory_current =
                Some(limits.total_memory.saturating_sub(limits.free_memory));
        }

        out.push(row);
        1
    }

    fn emit_disk_io_rows(
        &self,
        control: &ControlState,
        out: &mut Vec<SensorRow>,
        limit: usize,
    ) -> usize {
        let mut procs: Vec<(&sysinfo::Pid, &Process)> = self
            .system
            .processes()
            .iter()
            .filter(|(_, process)| {
                let usage = process.disk_usage();
                usage.read_bytes > 0 || usage.written_bytes > 0
            })
            .collect();
        procs.sort_unstable_by(|(a_pid, a_proc), (b_pid, b_proc)| {
            let a = a_proc.disk_usage().written_bytes + a_proc.disk_usage().read_bytes;
            let b = b_proc.disk_usage().written_bytes + b_proc.disk_usage().read_bytes;
            b.cmp(&a).then_with(|| a_pid.as_u32().cmp(&b_pid.as_u32()))
        });

        let mut emitted = 0usize;
        for (pid, process) in procs.into_iter().take(limit) {
            let usage = process.disk_usage();
            let mut row = self.base_row(control, "disk_io", "snapshot", "ok", "disk I/O snapshot");
            row.signal_family = Some("file".to_string());
            row.process_pid = Some(pid.as_u32());
            row.process_name = Some(os_to_string(process.name()));
            row.disk_io_read_delta_bytes = Some(usage.read_bytes);
            row.disk_io_write_delta_bytes = Some(usage.written_bytes);
            row.disk_io_read_total_bytes = Some(usage.total_read_bytes);
            row.disk_io_write_total_bytes = Some(usage.total_written_bytes);
            out.push(row);
            emitted += 1;
        }

        emitted
    }

    fn build_batch_event(&self, rows: Vec<SensorRow>) -> io::Result<InputEvent> {
        if rows.is_empty() {
            return Err(io::Error::other("cannot build sensor batch from zero rows"));
        }

        let len = rows.len();
        let mut timestamp_unix_nano = Vec::with_capacity(len);
        let mut sensor_name = Vec::with_capacity(len);
        let mut sensor_target_platform = Vec::with_capacity(len);
        let mut sensor_host_platform = Vec::with_capacity(len);
        let mut event_family = Vec::with_capacity(len);
        let mut event_kind = Vec::with_capacity(len);
        let mut signal_family: Vec<Option<String>> = Vec::with_capacity(len);
        let mut signal_status = Vec::with_capacity(len);
        let mut control_generation = Vec::with_capacity(len);
        let mut control_source = Vec::with_capacity(len);
        let mut control_path: Vec<Option<String>> = Vec::with_capacity(len);
        let mut enabled_families: Vec<Option<String>> = Vec::with_capacity(len);
        let mut effective_emit_signal_rows: Vec<Option<bool>> = Vec::with_capacity(len);
        let mut message = Vec::with_capacity(len);

        let mut process_pid: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_parent_pid: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_name: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_cmd: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_exe: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_status: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_cpu_usage: Vec<Option<f32>> = Vec::with_capacity(len);
        let mut process_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_virtual_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_start_time_unix_sec: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_disk_read_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_disk_write_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_user_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_effective_user_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_group_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_effective_group_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_cwd: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_session_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_run_time_secs: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_open_files: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_thread_count: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_container_id: Vec<Option<String>> = Vec::with_capacity(len);

        let mut network_interface: Vec<Option<String>> = Vec::with_capacity(len);
        let mut network_received_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_transmitted_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_received_total_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_transmitted_total_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_packets_received_delta: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_packets_transmitted_delta: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_errors_received_delta: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_errors_transmitted_delta: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_mac_address: Vec<Option<String>> = Vec::with_capacity(len);
        let mut network_ip_addresses: Vec<Option<String>> = Vec::with_capacity(len);
        let mut network_mtu: Vec<Option<u64>> = Vec::with_capacity(len);

        let mut system_total_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_used_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_available_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_total_swap_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_used_swap_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_cpu_usage_percent: Vec<Option<f32>> = Vec::with_capacity(len);
        let mut system_uptime_secs: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_cgroup_memory_limit: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_cgroup_memory_current: Vec<Option<u64>> = Vec::with_capacity(len);

        let mut disk_io_read_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut disk_io_write_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut disk_io_read_total_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut disk_io_write_total_bytes: Vec<Option<u64>> = Vec::with_capacity(len);

        let mut accounted_bytes = 0_u64;

        for row in rows {
            timestamp_unix_nano.push(row.timestamp_unix_nano);
            sensor_name.push(self.name.clone());
            sensor_target_platform.push(self.target.as_str().to_string());
            sensor_host_platform.push(self.host_platform.to_string());
            event_family.push(row.event_family);
            event_kind.push(row.event_kind);
            signal_family.push(row.signal_family);
            signal_status.push(row.signal_status);
            control_generation.push(row.control_generation);
            control_source.push(row.control_source);
            control_path.push(row.control_path);
            enabled_families.push(row.enabled_families);
            effective_emit_signal_rows.push(row.effective_emit_signal_rows);
            accounted_bytes = accounted_bytes.saturating_add(row.message.len() as u64);
            message.push(row.message);

            process_pid.push(row.process_pid);
            process_parent_pid.push(row.process_parent_pid);
            process_name.push(row.process_name);
            process_cmd.push(row.process_cmd);
            process_exe.push(row.process_exe);
            process_status.push(row.process_status);
            process_cpu_usage.push(row.process_cpu_usage);
            process_memory_bytes.push(row.process_memory_bytes);
            process_virtual_memory_bytes.push(row.process_virtual_memory_bytes);
            process_start_time_unix_sec.push(row.process_start_time_unix_sec);
            process_disk_read_delta_bytes.push(row.process_disk_read_delta_bytes);
            process_disk_write_delta_bytes.push(row.process_disk_write_delta_bytes);
            process_user_id.push(row.process_user_id);
            process_effective_user_id.push(row.process_effective_user_id);
            process_group_id.push(row.process_group_id);
            process_effective_group_id.push(row.process_effective_group_id);
            process_cwd.push(row.process_cwd);
            process_session_id.push(row.process_session_id);
            process_run_time_secs.push(row.process_run_time_secs);
            process_open_files.push(row.process_open_files);
            process_thread_count.push(row.process_thread_count);
            process_container_id.push(row.process_container_id);

            network_interface.push(row.network_interface);
            network_received_delta_bytes.push(row.network_received_delta_bytes);
            network_transmitted_delta_bytes.push(row.network_transmitted_delta_bytes);
            network_received_total_bytes.push(row.network_received_total_bytes);
            network_transmitted_total_bytes.push(row.network_transmitted_total_bytes);
            network_packets_received_delta.push(row.network_packets_received_delta);
            network_packets_transmitted_delta.push(row.network_packets_transmitted_delta);
            network_errors_received_delta.push(row.network_errors_received_delta);
            network_errors_transmitted_delta.push(row.network_errors_transmitted_delta);
            network_mac_address.push(row.network_mac_address);
            network_ip_addresses.push(row.network_ip_addresses);
            network_mtu.push(row.network_mtu);

            system_total_memory_bytes.push(row.system_total_memory_bytes);
            system_used_memory_bytes.push(row.system_used_memory_bytes);
            system_available_memory_bytes.push(row.system_available_memory_bytes);
            system_total_swap_bytes.push(row.system_total_swap_bytes);
            system_used_swap_bytes.push(row.system_used_swap_bytes);
            system_cpu_usage_percent.push(row.system_cpu_usage_percent);
            system_uptime_secs.push(row.system_uptime_secs);
            system_cgroup_memory_limit.push(row.system_cgroup_memory_limit);
            system_cgroup_memory_current.push(row.system_cgroup_memory_current);

            disk_io_read_delta_bytes.push(row.disk_io_read_delta_bytes);
            disk_io_write_delta_bytes.push(row.disk_io_write_delta_bytes);
            disk_io_read_total_bytes.push(row.disk_io_read_total_bytes);
            disk_io_write_total_bytes.push(row.disk_io_write_total_bytes);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(timestamp_unix_nano)),
            Arc::new(StringArray::from(sensor_name)),
            Arc::new(StringArray::from(sensor_target_platform)),
            Arc::new(StringArray::from(sensor_host_platform)),
            Arc::new(StringArray::from(event_family)),
            Arc::new(StringArray::from(event_kind)),
            Arc::new(StringArray::from(signal_family)),
            Arc::new(StringArray::from(signal_status)),
            Arc::new(UInt64Array::from(control_generation)),
            Arc::new(StringArray::from(control_source)),
            Arc::new(StringArray::from(control_path)),
            Arc::new(StringArray::from(enabled_families)),
            Arc::new(BooleanArray::from(effective_emit_signal_rows)),
            Arc::new(StringArray::from(message)),
            Arc::new(UInt32Array::from(process_pid)),
            Arc::new(UInt32Array::from(process_parent_pid)),
            Arc::new(StringArray::from(process_name)),
            Arc::new(StringArray::from(process_cmd)),
            Arc::new(StringArray::from(process_exe)),
            Arc::new(StringArray::from(process_status)),
            Arc::new(Float32Array::from(process_cpu_usage)),
            Arc::new(UInt64Array::from(process_memory_bytes)),
            Arc::new(UInt64Array::from(process_virtual_memory_bytes)),
            Arc::new(UInt64Array::from(process_start_time_unix_sec)),
            Arc::new(UInt64Array::from(process_disk_read_delta_bytes)),
            Arc::new(UInt64Array::from(process_disk_write_delta_bytes)),
            Arc::new(UInt32Array::from(process_user_id)),
            Arc::new(UInt32Array::from(process_effective_user_id)),
            Arc::new(UInt32Array::from(process_group_id)),
            Arc::new(UInt32Array::from(process_effective_group_id)),
            Arc::new(StringArray::from(process_cwd)),
            Arc::new(UInt32Array::from(process_session_id)),
            Arc::new(UInt64Array::from(process_run_time_secs)),
            Arc::new(UInt64Array::from(process_open_files)),
            Arc::new(UInt64Array::from(process_thread_count)),
            Arc::new(StringArray::from(process_container_id)),
            Arc::new(StringArray::from(network_interface)),
            Arc::new(UInt64Array::from(network_received_delta_bytes)),
            Arc::new(UInt64Array::from(network_transmitted_delta_bytes)),
            Arc::new(UInt64Array::from(network_received_total_bytes)),
            Arc::new(UInt64Array::from(network_transmitted_total_bytes)),
            Arc::new(UInt64Array::from(network_packets_received_delta)),
            Arc::new(UInt64Array::from(network_packets_transmitted_delta)),
            Arc::new(UInt64Array::from(network_errors_received_delta)),
            Arc::new(UInt64Array::from(network_errors_transmitted_delta)),
            Arc::new(StringArray::from(network_mac_address)),
            Arc::new(StringArray::from(network_ip_addresses)),
            Arc::new(UInt64Array::from(network_mtu)),
            Arc::new(UInt64Array::from(system_total_memory_bytes)),
            Arc::new(UInt64Array::from(system_used_memory_bytes)),
            Arc::new(UInt64Array::from(system_available_memory_bytes)),
            Arc::new(UInt64Array::from(system_total_swap_bytes)),
            Arc::new(UInt64Array::from(system_used_swap_bytes)),
            Arc::new(Float32Array::from(system_cpu_usage_percent)),
            Arc::new(UInt64Array::from(system_uptime_secs)),
            Arc::new(UInt64Array::from(system_cgroup_memory_limit)),
            Arc::new(UInt64Array::from(system_cgroup_memory_current)),
            Arc::new(UInt64Array::from(disk_io_read_delta_bytes)),
            Arc::new(UInt64Array::from(disk_io_write_delta_bytes)),
            Arc::new(UInt64Array::from(disk_io_read_total_bytes)),
            Arc::new(UInt64Array::from(disk_io_write_total_bytes)),
        ];

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|e| io::Error::other(format!("build sensor batch: {e}")))?;

        Ok(InputEvent::Batch {
            batch,
            source_id: None,
            accounted_bytes,
        })
    }
}

}
