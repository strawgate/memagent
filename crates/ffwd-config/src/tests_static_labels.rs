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
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("keys and values must not be empty"),
            "{}",
            err
        );

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
        let err2 = Config::load_str(yaml2).unwrap_err();
        assert!(
            err2.to_string()
                .contains("keys and values must not be empty"),
            "{}",
            err2
        );
    }
}
