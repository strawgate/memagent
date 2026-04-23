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
    use super::*;

    /// Parse a YAML config and extract the single pipeline.
    fn load_pipeline(yaml: &str) -> logfwd_config::PipelineConfig {
        let cfg = logfwd_config::Config::load_str(yaml).expect("valid yaml");
        cfg.pipelines.into_values().next().expect("one pipeline")
    }

    #[test]
    fn empty_name_is_rejected() {
        let config = load_pipeline(
            "pipelines:\n  app:\n    inputs:\n      - type: stdin\n    outputs:\n      type: stdout",
        );
        let spec = PipelineSpec {
            name: "",
            config: &config,
        };
        assert!(compile_topology(&spec).is_err());
    }

    #[test]
    fn valid_pipeline_without_transform() {
        let config = load_pipeline(
            "pipelines:\n  app:\n    inputs:\n      - type: stdin\n    outputs:\n      type: stdout",
        );
        let spec = PipelineSpec {
            name: "app",
            config: &config,
        };
        let topo = compile_topology(&spec).unwrap();
        assert_eq!(topo.input_count, 1);
        assert_eq!(topo.transform_count, 0);
        assert_eq!(topo.output_count, 1);
    }

    #[test]
    fn valid_pipeline_with_transform() {
        let config = load_pipeline(
            "pipelines:\n  app:\n    inputs:\n      - type: stdin\n    transform: \"SELECT * FROM logs\"\n    outputs:\n      type: stdout",
        );
        let spec = PipelineSpec {
            name: "app",
            config: &config,
        };
        let topo = compile_topology(&spec).unwrap();
        assert_eq!(topo.transform_count, 1);
    }
}
