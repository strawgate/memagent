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
        assert_config_err!(yaml, "has an unsupported type");
    }
}
