use logfwd_config::PipelineConfig;

/// PipelineSpec represents the typed definition of a single pipeline to compile.
#[derive(Debug, Clone)]
pub struct PipelineSpec<'a> {
    pub name: &'a str,
    pub config: &'a PipelineConfig,
}

/// CompiledTopology represents the validated and typed DAG for a pipeline.
#[derive(Debug)]
pub struct CompiledTopology {
    pub name: String,
    pub input_count: usize,
    pub transform_count: usize,
    pub output_count: usize,
}

/// Compiles a typed DAG from configuration without starting workers.
pub fn compile_topology(spec: &PipelineSpec) -> Result<CompiledTopology, String> {
    let input_count = spec.config.inputs.len();
    let output_count = spec.config.outputs.len();
    let transform_count = usize::from(spec.config.transform.is_some());

    Ok(CompiledTopology {
        name: spec.name.to_string(),
        input_count,
        transform_count,
        output_count,
    })
}
