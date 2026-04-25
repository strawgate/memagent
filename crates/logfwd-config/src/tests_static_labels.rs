#[cfg(test)]
mod tests {
    use crate::Config;

    #[test]
    fn test_static_labels_empty_rejected() {
        let yaml = "
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
    outputs:
      - type: loki
        endpoint: http://localhost:3100
        static_labels:
          ok_key: ok_value
          bad_key: \"\"
";
        assert_config_err!(yaml, "keys and values must not be empty");

        let yaml2 = "
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
    outputs:
      - type: loki
        endpoint: http://localhost:3100
        static_labels:
          \"\": ok_value
";
        assert_config_err!(yaml2, "keys and values must not be empty");
    }
}
