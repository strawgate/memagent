with open("crates/logfwd-config/src/tests_sensor.rs", "w") as f:
    f.write("""use crate::InputTypeConfig;

#[test]
fn test_sensor_config_new_fields() {
    let yaml = r#"
type: linux_ebpf_sensor
sensor:
  include_process_names: ["nginx*", "redis"]
  exclude_process_names: ["bash"]
  include_event_types: ["process_exec"]
  exclude_event_types: ["tcp_connect"]
  ring_buffer_size_kb: 4096
  poll_interval_ms: 100
"#;

    let config: InputTypeConfig = serde_yaml_ng::from_str(yaml).expect("parse failed");

    match config {
        InputTypeConfig::LinuxEbpfSensor(cfg) => {
            let sensor = cfg.sensor.expect("sensor block missing");
            assert_eq!(sensor.include_process_names.unwrap(), vec!["nginx*", "redis"]);
            assert_eq!(sensor.exclude_process_names.unwrap(), vec!["bash"]);
            assert_eq!(sensor.include_event_types.unwrap(), vec!["process_exec"]);
            assert_eq!(sensor.exclude_event_types.unwrap(), vec!["tcp_connect"]);
            assert_eq!(sensor.ring_buffer_size_kb.unwrap(), 4096);
            assert_eq!(sensor.poll_interval_ms.unwrap(), 100);
        }
        _ => panic!("wrong variant"),
    }
}
""")

with open("crates/logfwd-config/src/lib.rs", "r") as f:
    content = f.read()
if "mod tests_sensor;" not in content:
    with open("crates/logfwd-config/src/lib.rs", "a") as f:
        f.write("\n#[cfg(test)]\nmod tests_sensor;\n")
