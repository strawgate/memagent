import re

with open('crates/logfwd-config/src/validate.rs', 'r') as f:
    content = f.read()

validation_logic = """                            // Reject eBPF-specific fields on host_metrics inputs.
                            if matches!(input.input_type(), InputType::HostMetrics) {
                                if s.sensor
                                    .as_ref()
                                    .and_then(|cfg| cfg.ebpf_binary_path.as_ref())
                                    .is_some()
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': sensor.ebpf_binary_path is not supported for host_metrics inputs"
                                    )));
                                }
                                if s.sensor
                                    .as_ref()
                                    .and_then(|cfg| cfg.max_events_per_poll)
                                    .is_some()
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': sensor.max_events_per_poll is not supported for host_metrics inputs"
                                    )));
                                }
                            }
                            if s.sensor.as_ref().and_then(|cfg| cfg.ring_buffer_size_kb) == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': sensor.ring_buffer_size_kb must be at least 1"
                                )));
                            }
                            if s.sensor.as_ref().and_then(|cfg| cfg.poll_interval_ms) == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': sensor.poll_interval_ms must be at least 1"
                                )));
                            }"""

content = re.sub(
    r'                            // Reject eBPF-specific fields on host_metrics inputs\.\n                            if matches!\(input.input_type\(\), InputType::HostMetrics\) \{.*?\n                            \}',
    validation_logic,
    content,
    flags=re.DOTALL
)

with open('crates/logfwd-config/src/validate.rs', 'w') as f:
    f.write(content)
