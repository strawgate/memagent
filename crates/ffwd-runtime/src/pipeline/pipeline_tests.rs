fn single_pipeline_yaml(input: &str, output: &str) -> String {
    single_pipeline_yaml_with_transform(input, None, output)
}

fn single_pipeline_yaml_with_transform(
    input: &str,
    transform: Option<&str>,
    output: &str,
) -> String {
    fn push_list_item(out: &mut String, block: &str) {
        let mut lines = block.trim().lines();
        if let Some(line) = lines.next() {
            out.push_str("      - ");
            out.push_str(line);
            out.push('\n');
        }
        for line in lines {
            out.push_str("        ");
            out.push_str(line);
            out.push('\n');
        }
    }

    let mut yaml = String::from("pipelines:\n  default:\n    inputs:\n");
    push_list_item(&mut yaml, input);
    if let Some(transform) = transform {
        yaml.push_str("    transform: ");
        yaml.push_str(transform);
        yaml.push('\n');
    }
    yaml.push_str("    outputs:\n");
    push_list_item(&mut yaml, output);
    yaml
}

include!("pipeline_tests/config_and_factory.rs");
include!("pipeline_tests/processing.rs");
include!("pipeline_tests/shutdown.rs");
include!("pipeline_tests/machine.rs");
include!("pipeline_tests/failpoints.rs");
