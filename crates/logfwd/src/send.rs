//! Send command config building: YAML manipulation for `ff send`.

use crate::cli::{CliError, SendFormat};
use crate::commands::resolve_config_path;

pub(crate) fn build_stdin_send_config_yaml(
    config_yaml: &str,
    format: Option<SendFormat>,
    service: Option<&str>,
    resources: &[String],
) -> Result<String, CliError> {
    let mut value: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(config_yaml).map_err(|e| CliError::Config(e.to_string()))?;
    let mapping = value.as_mapping_mut().ok_or_else(|| {
        CliError::Config("send destination config must be a YAML mapping".to_owned())
    })?;

    for unsupported in ["input", "pipelines"] {
        if mapping.contains_key(yaml_string(unsupported)) {
            return Err(CliError::Config(format!(
                "`ff send` expects a destination-only config; remove top-level `{unsupported}` or use `ff run`"
            )));
        }
    }
    let has_output = mapping.contains_key(yaml_string("output"));
    let has_outputs = mapping.contains_key(yaml_string("outputs"));
    if has_output && has_outputs {
        return Err(CliError::Config(
            "`ff send` destination config must define only one of top-level `output` or `outputs`"
                .to_owned(),
        ));
    }
    if !has_output && !has_outputs {
        return Err(CliError::Config(
            "`ff send` destination config must define top-level `output` or `outputs`".to_owned(),
        ));
    }

    let mut input = serde_yaml_ng::Mapping::new();
    input.insert(yaml_string("type"), yaml_string("stdin"));
    if let Some(format) = format {
        let format = format.as_config_format().to_string();
        input.insert(yaml_string("format"), yaml_string(&format));
    }

    merge_send_resource_attrs(mapping, service, resources)?;

    if has_outputs {
        rewrite_send_config_with_outputs(mapping, input);
    } else {
        mapping.insert(yaml_string("input"), serde_yaml_ng::Value::Mapping(input));
    }

    serde_yaml_ng::to_string(&value).map_err(|e| CliError::Config(e.to_string()))
}

fn rewrite_send_config_with_outputs(
    mapping: &mut serde_yaml_ng::Mapping,
    input: serde_yaml_ng::Mapping,
) {
    let outputs = mapping
        .remove(yaml_string("outputs"))
        .expect("internal invariant violated: `ff send` rewrite requires top-level `outputs`");

    let mut pipeline = serde_yaml_ng::Mapping::new();
    pipeline.insert(
        yaml_string("inputs"),
        serde_yaml_ng::Value::Sequence(vec![serde_yaml_ng::Value::Mapping(input)]),
    );
    pipeline.insert(yaml_string("outputs"), outputs);

    for key in ["transform", "enrichment", "resource_attrs"] {
        if let Some(value) = mapping.remove(yaml_string(key)) {
            pipeline.insert(yaml_string(key), value);
        }
    }

    let mut pipelines = serde_yaml_ng::Mapping::new();
    pipelines.insert(
        yaml_string("default"),
        serde_yaml_ng::Value::Mapping(pipeline),
    );
    mapping.insert(
        yaml_string("pipelines"),
        serde_yaml_ng::Value::Mapping(pipelines),
    );
}

fn merge_send_resource_attrs(
    mapping: &mut serde_yaml_ng::Mapping,
    service: Option<&str>,
    resources: &[String],
) -> Result<(), CliError> {
    if service.is_none() && resources.is_empty() {
        return Ok(());
    }

    let key = yaml_string("resource_attrs");
    if !mapping.contains_key(&key) {
        mapping.insert(
            key.clone(),
            serde_yaml_ng::Value::Mapping(serde_yaml_ng::Mapping::new()),
        );
    }
    let attrs = mapping
        .get_mut(&key)
        .and_then(serde_yaml_ng::Value::as_mapping_mut)
        .ok_or_else(|| {
            CliError::Config(
                "top-level `resource_attrs` must be a mapping when using send overrides".to_owned(),
            )
        })?;

    if let Some(service) = service {
        attrs.insert(yaml_string("service.name"), yaml_string(service));
    }
    for raw in resources {
        let (name, value) = parse_key_value(raw, "--resource")?;
        attrs.insert(yaml_string(name), yaml_string(value));
    }

    Ok(())
}

fn parse_key_value<'a>(raw: &'a str, flag: &str) -> Result<(&'a str, &'a str), CliError> {
    let (name, value) = raw.split_once('=').ok_or_else(|| {
        CliError::Config(format!("{flag} must be in KEY=VALUE form, got `{raw}`"))
    })?;
    let name = name.trim();
    let value = value.trim();
    if name.is_empty() {
        return Err(CliError::Config(format!(
            "{flag} key must not be empty, got `{raw}`"
        )));
    }
    Ok((name, value))
}

fn yaml_string(value: &str) -> serde_yaml_ng::Value {
    serde_yaml_ng::Value::String(value.to_owned())
}

pub(crate) fn resolve_send_config_path(config_path: Option<&str>) -> Result<String, CliError> {
    resolve_config_path(config_path).map_err(|err| match err {
        CliError::Config(_) => CliError::Config(
            "no destination config file found (use `ff send --config <file>` or set LOGFWD_CONFIG)"
                .to_owned(),
        ),
        CliError::Runtime(e) => CliError::Runtime(e),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdin_send_config_injects_input_and_merges_resources() {
        let yaml = r"
resource_attrs:
  env: prod
output:
  type: stdout
";
        let resource = vec![" deployment = blue ".to_owned()];
        let generated =
            build_stdin_send_config_yaml(yaml, Some(SendFormat::Json), Some("checkout"), &resource)
                .expect("send config should render");
        let config =
            logfwd_config::Config::load_str(&generated).expect("generated config should parse");
        let pipeline = &config.pipelines["default"];
        let input = &pipeline.inputs[0];

        assert_eq!(input.input_type(), logfwd_config::InputType::Stdin);
        assert_eq!(input.format, Some(logfwd_config::Format::Json));
        assert_eq!(
            pipeline
                .resource_attrs
                .get("service.name")
                .map(String::as_str),
            Some("checkout")
        );
        assert_eq!(
            pipeline
                .resource_attrs
                .get("deployment")
                .map(String::as_str),
            Some("blue")
        );
        assert_eq!(
            pipeline.resource_attrs.get("env").map(String::as_str),
            Some("prod")
        );
    }

    #[test]
    fn stdin_send_config_accepts_plural_outputs() {
        let yaml = r"
resource_attrs:
  env: prod
transform: |
  SELECT * FROM logs
enrichment:
  - type: static
    table_name: labels
    labels:
      env: dogfood
outputs:
  - type: stdout
    format: json
  - type: file
    path: ./sent.ndjson
";
        let generated =
            build_stdin_send_config_yaml(yaml, Some(SendFormat::Raw), Some("checkout"), &[])
                .expect("send config should render");
        let config =
            logfwd_config::Config::load_str(&generated).expect("generated config should parse");
        let pipeline = &config.pipelines["default"];

        assert_eq!(
            pipeline.inputs[0].input_type(),
            logfwd_config::InputType::Stdin
        );
        assert_eq!(pipeline.inputs[0].format, Some(logfwd_config::Format::Raw));
        assert_eq!(pipeline.outputs.len(), 2);
        assert_eq!(
            pipeline
                .resource_attrs
                .get("service.name")
                .map(String::as_str),
            Some("checkout")
        );
        assert_eq!(
            pipeline.resource_attrs.get("env").map(String::as_str),
            Some("prod")
        );
        assert_eq!(pipeline.transform.as_deref(), Some("SELECT * FROM logs\n"));
        match &pipeline.enrichment[..] {
            [logfwd_config::EnrichmentConfig::Static(static_cfg)] => {
                assert_eq!(static_cfg.table_name, "labels");
                assert_eq!(
                    static_cfg.labels.get("env").map(String::as_str),
                    Some("dogfood")
                );
            }
            other => panic!("expected one static enrichment config, got {other:?}"),
        }
    }

    #[test]
    fn stdin_send_config_rejects_runtime_inputs() {
        for (field, yaml) in [
            (
                "input",
                "input:\n  type: file\n  path: /tmp/app.log\noutput:\n  type: stdout\n",
            ),
            (
                "pipelines",
                "pipelines:\n  default:\n    input:\n      type: file\n      path: /tmp/app.log\n    output:\n      type: stdout\n",
            ),
        ] {
            let err = build_stdin_send_config_yaml(yaml, None, None, &[])
                .expect_err("send config should reject runtime input shape");
            assert!(
                err.to_string().contains(field),
                "error should mention {field}: {err}"
            );
        }
    }

    #[test]
    fn stdin_send_config_requires_destination_output() {
        let err = build_stdin_send_config_yaml("server: {}\n", None, None, &[])
            .expect_err("send config should require output");
        assert!(
            err.to_string()
                .contains("must define top-level `output` or `outputs`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn stdin_send_config_rejects_output_and_outputs_together() {
        let yaml = r"
output:
  type: stdout
outputs:
  - type: stdout
";
        let err = build_stdin_send_config_yaml(yaml, None, None, &[])
            .expect_err("send config should reject ambiguous destination output shape");
        assert!(
            err.to_string()
                .contains("only one of top-level `output` or `outputs`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn stdin_send_config_rejects_malformed_resource_override() {
        let err = build_stdin_send_config_yaml(
            "output:\n  type: stdout\n",
            None,
            None,
            &["deployment".to_owned()],
        )
        .expect_err("send config should reject malformed resource override");
        assert!(
            err.to_string().contains("--resource"),
            "unexpected error: {err}"
        );
    }
}
