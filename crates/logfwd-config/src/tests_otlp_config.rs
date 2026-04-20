#[cfg(test)]
mod tests {
    use crate::{Config, InputTypeConfig};

    #[test]
    fn test_otlp_config_all_fields_parsed() {
        let yaml = "
pipelines:
  test:
    inputs:
      - type: otlp
        listen: 0.0.0.0:4318
        max_recv_message_size_bytes: 8388608
        grpc_keepalive_time_ms: 10000
        grpc_max_concurrent_streams: 100
        tls:
          cert_file: /path/to/cert
          key_file: /path/to/key
          client_ca_file: /path/to/ca
          require_client_auth: true
    outputs:
      - type: loki
        endpoint: http://localhost:3100
";
        let config = Config::load_str(yaml).unwrap();
        let pipeline = config.pipelines.get("test").unwrap();
        let input = &pipeline.inputs[0];

        match &input.type_config {
            InputTypeConfig::Otlp(otlp) => {
                assert_eq!(otlp.listen, "0.0.0.0:4318");
                assert_eq!(otlp.max_recv_message_size_bytes, Some(8388608));
                assert_eq!(
                    otlp.grpc_keepalive_time_ms,
                    crate::PositiveMillis::new(10000)
                );
                assert_eq!(otlp.grpc_max_concurrent_streams, Some(100));

                let tls = otlp.tls.as_ref().unwrap();
                assert_eq!(tls.cert_file.as_deref(), Some("/path/to/cert"));
                assert_eq!(tls.key_file.as_deref(), Some("/path/to/key"));
                assert_eq!(tls.client_ca_file.as_deref(), Some("/path/to/ca"));
                assert!(tls.require_client_auth);
            }
            _ => panic!("Expected OTLP input config"),
        }
    }
}
