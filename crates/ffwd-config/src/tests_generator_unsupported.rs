#[cfg(test)]
mod tests {
    use crate::Config;

    #[test]
    fn test_generator_unsupported_rejected() {
        let yaml = "
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            my_list: [1, 2, 3]
    outputs:
      - type: loki
        endpoint: http://localhost:3100
";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("has an unsupported type"),
            "{}",
            err
        );
    }
}
