pub(crate) fn single_pipeline_yaml(input_body: &str, output_body: &str) -> String {
    single_pipeline_yaml_with_sections(input_body, None, output_body, None)
}

pub(crate) fn single_pipeline_yaml_with_transform(
    input_body: &str,
    transform: &str,
    output_body: &str,
) -> String {
    single_pipeline_yaml_with_sections(input_body, Some(transform), output_body, None)
}

pub(crate) fn single_pipeline_yaml_with_extra(
    input_body: &str,
    output_body: &str,
    pipeline_extra: &str,
) -> String {
    single_pipeline_yaml_with_sections(input_body, None, output_body, Some(pipeline_extra))
}

pub(crate) fn append_root_sections(mut yaml: String, root_extra: &str) -> String {
    if !root_extra.trim().is_empty() {
        if !yaml.ends_with('\n') {
            yaml.push('\n');
        }
        yaml.push_str(root_extra.trim_start_matches('\n'));
        if !yaml.ends_with('\n') {
            yaml.push('\n');
        }
    }
    yaml
}

fn single_pipeline_yaml_with_sections(
    input_body: &str,
    transform: Option<&str>,
    output_body: &str,
    pipeline_extra: Option<&str>,
) -> String {
    let mut yaml = String::from("pipelines:\n  default:\n    inputs:\n");
    push_block_sequence(&mut yaml, 6, input_body);
    if let Some(transform) = transform {
        yaml.push_str("    transform: |\n");
        for line in transform.lines() {
            yaml.push_str("      ");
            yaml.push_str(line);
            yaml.push('\n');
        }
    }
    yaml.push_str("    outputs:\n");
    push_block_sequence(&mut yaml, 6, output_body);
    if let Some(extra) = pipeline_extra {
        push_pipeline_extra(&mut yaml, extra);
    }
    yaml
}

fn push_block_sequence(yaml: &mut String, indent: usize, body: &str) {
    let prefix = " ".repeat(indent);
    let rest_prefix = " ".repeat(indent + 2);
    let lines: Vec<&str> = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let base_indent = lines
        .iter()
        .map(|line| line.len() - line.trim_start().len())
        .min()
        .unwrap_or(0);
    for (idx, line) in lines.iter().enumerate() {
        let line = line.get(base_indent..).unwrap_or_else(|| line.trim_start());
        if idx == 0 {
            yaml.push_str(&prefix);
            yaml.push_str("- ");
            yaml.push_str(line);
            yaml.push('\n');
        } else {
            yaml.push_str(&rest_prefix);
            yaml.push_str(line);
            yaml.push('\n');
        }
    }
}

fn push_pipeline_extra(yaml: &mut String, extra: &str) {
    for line in extra.lines().filter(|line| !line.trim().is_empty()) {
        yaml.push_str("    ");
        yaml.push_str(line);
        yaml.push('\n');
    }
}
