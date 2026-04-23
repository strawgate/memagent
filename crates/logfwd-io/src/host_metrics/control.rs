macro_rules! define_sensor_row {
    ($(($field:ident, $ty:ty, $array:ident, $data_type:expr, $nullable:expr))+) => {
        #[derive(Debug)]
        struct SensorRow {
            $(
                $field: $ty,
            )+
        }
    };
}

sensor_row_columns!(define_sensor_row);

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

fn next_control_generation(current_generation: u64, file_generation: Option<u64>) -> u64 {
    // A missing generation means "advance from the currently active control
    // state". This lets operators tweak control-file behavior without having to
    // hand-manage a monotonically increasing counter on every edit.
    file_generation.unwrap_or_else(|| current_generation.saturating_add(1))
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
                    self.state.control.source = next.source;
                    self.state.health = ComponentHealth::Healthy;
                    return None;
                }

                next.generation =
                    next_control_generation(self.state.control.generation, file_cfg.generation);
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
