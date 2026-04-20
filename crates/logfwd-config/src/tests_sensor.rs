use crate::InputTypeConfig;

#[test]
fn test_sensor_config_new_fields() {
    let yaml = r#"
type: linux_ebpf_sensor
sensor:
  include_process_names: ["nginx*", "redis"]
  exclude_process_names: ["bash"]
  include_event_types: ["exec"]
  exclude_event_types: ["tcp_connect"]
  ring_buffer_size_kb: 4096
  poll_interval_ms: 100
"#;

    let config: InputTypeConfig = serde_yaml_ng::from_str(yaml).expect("parse failed");

    match config {
        InputTypeConfig::LinuxEbpfSensor(cfg) => {
            let sensor = cfg.sensor.expect("sensor block missing");
            assert_eq!(
                sensor.include_process_names.unwrap(),
                vec!["nginx*", "redis"]
            );
            assert_eq!(sensor.exclude_process_names.unwrap(), vec!["bash"]);
            assert_eq!(sensor.include_event_types.unwrap(), vec!["exec"]);
            assert_eq!(sensor.exclude_event_types.unwrap(), vec!["tcp_connect"]);
            assert_eq!(sensor.ring_buffer_size_kb.unwrap(), 4096);
            assert_eq!(sensor.poll_interval_ms, crate::PositiveMillis::new(100));
        }
        _ => panic!("wrong variant"),
    }
}
