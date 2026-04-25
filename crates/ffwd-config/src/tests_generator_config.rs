//! Generator input detailed tests: block parsing, field validation, attribute
//! constraints, sequence, timestamp, and profile requirements.

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn generator_input_rejects_path() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        path: /tmp/test.log
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("generator input must reject path");
    }

    #[test]
    fn generator_input_accepts_explicit_generator_block() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          events_per_sec: 25000
          batch_size: 2048
          total_events: 123
          profile: record
          attributes:
            benchmark_id: run-123
            pod_name: emitter-0
            stream_id: emitter-0
            service: bench-emitter
            status: 200
            sampled: true
            deleted_at: null
          sequence:
            field: seq
    outputs:
      - type: "null"
"#;
        let cfg = Config::load_str(yaml).expect("generator block should be valid");
        let gen_type = match &cfg.pipelines["test"].inputs[0].type_config {
            InputTypeConfig::Generator(g) => g,
            _ => panic!("expected Generator type_config"),
        };
        let generator = gen_type.generator.as_ref().expect("generator config");
        assert_eq!(generator.events_per_sec, Some(25000));
        assert_eq!(generator.batch_size, Some(2048));
        assert_eq!(generator.total_events, Some(123));
        assert_eq!(generator.profile, Some(GeneratorProfileConfig::Record));
        assert_eq!(
            generator.attributes.get("benchmark_id"),
            Some(&GeneratorAttributeValueConfig::String(
                "run-123".to_string()
            ))
        );
        assert_eq!(
            generator.attributes.get("pod_name"),
            Some(&GeneratorAttributeValueConfig::String(
                "emitter-0".to_string()
            ))
        );
        assert_eq!(
            generator.attributes.get("stream_id"),
            Some(&GeneratorAttributeValueConfig::String(
                "emitter-0".to_string()
            ))
        );
        assert_eq!(
            generator.attributes.get("status"),
            Some(&GeneratorAttributeValueConfig::Integer(200))
        );
        assert_eq!(
            generator.attributes.get("sampled"),
            Some(&GeneratorAttributeValueConfig::Bool(true))
        );
        assert_eq!(
            generator.attributes.get("deleted_at"),
            Some(&GeneratorAttributeValueConfig::Null)
        );
        assert_eq!(
            generator.sequence.as_ref().map(|seq| seq.field.as_str()),
            Some("seq")
        );
    }

    #[test]
    fn generator_input_rejects_listen() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        listen: "1000"
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("generator input must reject listen");
    }

    #[test]
    fn non_generator_input_rejects_generator_block() {
        // With the tagged-enum refactor, the `generator` key is only valid inside
        // the generator variant. Serde rejects it at parse time for file inputs.
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
        generator:
          profile: record
    outputs:
      - type: "null"
"#;
        let _ = Config::load_str(yaml).expect_err("file input must reject generator block");
    }

    #[test]
    fn generator_input_rejects_empty_sequence_field() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          sequence:
            field: " "
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("generator.sequence.field"),
            "expected empty sequence field rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_duplicate_generated_field_names() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            seq: already-here
          sequence:
            field: seq
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("must not duplicate a generator.attributes key"),
            "expected duplicate generated field rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_zero_batch_size() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          batch_size: 0
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("batch_size must be at least 1"),
            "expected zero batch size rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_empty_attribute_key() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            "": run-123
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("attributes keys must not be empty"),
            "expected empty attribute key rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_non_finite_attribute_values() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            ratio: .nan
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("float values must be finite"),
            "expected non-finite attribute rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_empty_event_created_field_name() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          event_created_unix_nano_field: " "
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("event_created_unix_nano_field must not be empty"),
            "expected empty event_created field rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_event_created_field_name_duplicate_with_attribute() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          attributes:
            created: run-123
          event_created_unix_nano_field: created
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("must not duplicate a generator.attributes key"),
            "expected event_created/attribute duplication rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_event_created_field_name_duplicate_with_sequence() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          sequence:
            field: seq
          event_created_unix_nano_field: seq
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("must not duplicate generator.sequence.field"),
            "expected event_created/sequence duplication rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_record_fields_without_record_profile() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          attributes:
            benchmark_id: run-123
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("require generator.profile=record"),
            "expected record profile requirement rejection: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Generator timestamp tests
    // -----------------------------------------------------------------------

    #[test]
    fn generator_timestamp_config_accepted() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "2023-06-01T12:00:00Z"
            step_ms: 5000
    outputs:
      - type: "null"
"#;
        let cfg = Config::load_str(yaml).expect("timestamp config should be valid");
        let gen_type = match &cfg.pipelines["test"].inputs[0].type_config {
            InputTypeConfig::Generator(g) => g,
            _ => panic!("expected Generator type_config"),
        };
        let ts = gen_type
            .generator
            .as_ref()
            .unwrap()
            .timestamp
            .as_ref()
            .expect("timestamp config");
        assert_eq!(ts.start.as_deref(), Some("2023-06-01T12:00:00Z"));
        assert_eq!(ts.step_ms, Some(5000));
    }

    #[test]
    fn generator_timestamp_now_accepted() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "now"
            step_ms: -100
    outputs:
      - type: "null"
"#;
        Config::load_str(yaml).expect("timestamp start=now should be valid");
    }

    #[test]
    fn generator_timestamp_rejects_zero_step() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            step_ms: 0
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("step_ms must not be zero"),
            "expected zero step rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_rejects_invalid_start() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "not-a-date"
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("YYYY-MM-DDTHH:MM:SSZ"),
            "expected format rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_rejects_record_profile() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          profile: record
          timestamp:
            start: "now"
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("only supported for the logs profile"),
            "expected logs-only rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_rejects_invalid_calendar_date() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "2024-02-31T00:00:00Z"
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("day 31 out of range"),
            "expected invalid date rejection: {err}"
        );
    }

    #[test]
    fn generator_timestamp_rejects_month_13() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          timestamp:
            start: "2024-13-01T00:00:00Z"
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("month 13 out of range"),
            "expected invalid month rejection: {err}"
        );
    }
}
