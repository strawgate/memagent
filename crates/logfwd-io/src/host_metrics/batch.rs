macro_rules! define_sensor_column_buffers {
    ($(($field:ident, $ty:ty, $array:ident, $data_type:expr, $nullable:expr))+) => {
        struct SensorColumnBuffers {
            $(
                $field: Vec<$ty>,
            )+
        }

        impl SensorColumnBuffers {
            fn with_capacity(len: usize) -> Self {
                Self {
                    $(
                        $field: Vec::with_capacity(len),
                    )+
                }
            }

            fn push_row(&mut self, row: SensorRow) {
                $(
                    self.$field.push(row.$field);
                )+
            }

            fn into_arrays(self) -> Vec<ArrayRef> {
                vec![
                    $(Arc::new($array::from(self.$field)) as ArrayRef),+
                ]
            }
        }
    };
}

sensor_row_columns!(define_sensor_column_buffers);

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

        let mut columns = SensorColumnBuffers::with_capacity(rows.len());
        let mut accounted_bytes = 0_u64;

        for row in rows {
            accounted_bytes = accounted_bytes.saturating_add(row.message.len() as u64);
            columns.push_row(row);
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns.into_arrays())
            .map_err(|e| io::Error::other(format!("build sensor batch: {e}")))?;

        Ok(InputEvent::Batch {
            batch,
            source_id: None,
            accounted_bytes,
        })
    }
}
