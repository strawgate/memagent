use logfwd_config::PipelineConfig;

/// Typed definition of a single pipeline to compile.
///
/// Carries a borrowed reference to the parsed [`PipelineConfig`] along with the
/// pipeline's user-facing name so that the topology compiler can produce
/// actionable error messages.
#[derive(Debug, Clone)]
pub struct PipelineSpec<'a> {
    /// User-facing pipeline name (must be non-empty).
    pub name: &'a str,
    /// Parsed pipeline configuration (inputs, transform, outputs).
    pub config: &'a PipelineConfig,
}

/// Validated, typed DAG for a single pipeline.
///
/// Produced by [`compile_topology`] after invariant checks pass. In Phase 1
/// this is a flat summary; future phases will carry the full execution graph.
#[derive(Debug)]
pub struct CompiledTopology {
    /// Pipeline name (copied from the spec).
    pub name: String,
    /// Number of input sources in the pipeline.
    pub input_count: usize,
    /// Number of SQL transform steps (0 or 1 today).
    pub transform_count: usize,
    /// Number of output sinks in the pipeline.
    pub output_count: usize,
}

/// Compiles a typed DAG from configuration without starting workers.
///
/// Returns an error if the spec violates any structural invariants (empty name,
/// no inputs, no outputs).
pub fn compile_topology(spec: &PipelineSpec<'_>) -> Result<CompiledTopology, String> {
    if spec.name.is_empty() {
        return Err("pipeline name must not be empty".to_owned());
    }

    let input_count = spec.config.inputs.len();
    if input_count == 0 {
        return Err(format!(
            "pipeline '{}': at least one input is required",
            spec.name
        ));
    }

    let output_count = spec.config.outputs.len();
    if output_count == 0 {
        return Err(format!(
            "pipeline '{}': at least one output is required",
            spec.name
        ));
    }

    let transform_count = usize::from(spec.config.transform.is_some());

    Ok(CompiledTopology {
        name: spec.name.to_string(),
        input_count,
        transform_count,
        output_count,
    })
}

#[cfg(test)]
mod tests {
    use super::{PipelineSpec, compile_topology};
    use logfwd_config::{
        GeneratorTypeConfig, InputConfig, InputTypeConfig, OutputConfigV2, PipelineConfig,
        StdoutOutputConfig,
    };

    fn build_pipeline_config(
        inputs: Vec<InputConfig>,
        outputs: Vec<OutputConfigV2>,
        transform: Option<String>,
    ) -> PipelineConfig {
        PipelineConfig {
            inputs,
            outputs,
            transform,
            workers: None,
            enrichment: Vec::new(),
            resource_attrs: std::collections::HashMap::new(),
            batch_target_bytes: None,
            batch_timeout_ms: None,
            poll_interval_ms: None,
        }
    }

    fn build_generator_input() -> InputConfig {
        InputConfig {
            name: Some("in".to_string()),
            format: None,
            sql: None,
            source_metadata: Default::default(),
            type_config: InputTypeConfig::Generator(GeneratorTypeConfig::default()),
        }
    }

    fn build_stdout_output() -> OutputConfigV2 {
        OutputConfigV2::Stdout(StdoutOutputConfig {
            name: Some("out".to_string()),
            format: None,
        })
    }

    #[test]
    fn compile_topology_rejects_empty_pipeline_name() {
        let config = build_pipeline_config(
            vec![build_generator_input()],
            vec![build_stdout_output()],
            None,
        );
        let spec = PipelineSpec {
            name: "",
            config: &config,
        };

        let err = compile_topology(&spec).expect_err("empty name should be rejected");
        assert_eq!(err, "pipeline name must not be empty");
    }

    #[test]
    fn compile_topology_rejects_missing_inputs() {
        let config = build_pipeline_config(Vec::new(), vec![build_stdout_output()], None);
        let spec = PipelineSpec {
            name: "main",
            config: &config,
        };

        let err = compile_topology(&spec).expect_err("missing inputs should be rejected");
        assert_eq!(err, "pipeline 'main': at least one input is required");
    }

    #[test]
    fn compile_topology_rejects_missing_outputs() {
        let config = build_pipeline_config(vec![build_generator_input()], Vec::new(), None);
        let spec = PipelineSpec {
            name: "main",
            config: &config,
        };

        let err = compile_topology(&spec).expect_err("missing outputs should be rejected");
        assert_eq!(err, "pipeline 'main': at least one output is required");
    }

    #[test]
    fn compile_topology_counts_nodes_with_optional_transform() {
        let config = build_pipeline_config(
            vec![build_generator_input(), build_generator_input()],
            vec![build_stdout_output(), build_stdout_output()],
            Some("SELECT * FROM logs".to_string()),
        );
        let spec = PipelineSpec {
            name: "main",
            config: &config,
        };

        let compiled = compile_topology(&spec).expect("valid topology compiles");
        assert_eq!(compiled.name, "main");
        assert_eq!(compiled.input_count, 2);
        assert_eq!(compiled.transform_count, 1);
        assert_eq!(compiled.output_count, 2);
    }

    #[test]
    fn compile_topology_counts_zero_transforms_when_transform_is_absent() {
        let config = build_pipeline_config(
            vec![build_generator_input(), build_generator_input()],
            vec![build_stdout_output(), build_stdout_output()],
            None,
        );
        let spec = PipelineSpec {
            name: "main",
            config: &config,
        };

        let compiled = compile_topology(&spec).expect("valid topology compiles");
        assert_eq!(compiled.name, "main");
        assert_eq!(compiled.input_count, 2);
        assert_eq!(compiled.transform_count, 0);
        assert_eq!(compiled.output_count, 2);
    }
}
