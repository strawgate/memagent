use crate::env::expand_env_vars;
use crate::types::{
    Config, ConfigError, EnrichmentConfig, InputConfig, OutputConfig, PipelineConfig, ServerConfig,
    StorageConfig,
};
use serde::Deserialize;
use serde_yaml_ng::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

/// Process-unique seed embedded in placeholder markers so that no static user
/// string can collide with a marker. Derived from the address of a private
/// static, which varies per process (ASLR).
static PLACEHOLDER_SEED: LazyLock<u64> = LazyLock::new(|| {
    static ANCHOR: u8 = 0;
    let addr = std::ptr::addr_of!(ANCHOR) as u64;
    // Mix bits so the value looks opaque rather than a raw address.
    addr.wrapping_mul(0x517c_c1b7_2722_0a95)
});

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    input: Option<InputConfig>,
    transform: Option<String>,
    output: Option<OutputConfig>,
    #[serde(default)]
    enrichment: Vec<EnrichmentConfig>,
    #[serde(default)]
    resource_attrs: HashMap<String, String>,
    pipelines: Option<HashMap<String, PipelineConfig>>,
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    storage: StorageConfig,
}

impl Config {
    /// Load configuration from a YAML file path.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)?;
        Self::load_str_with_base_path(&raw, path.parent())
    }

    /// Parse configuration from a YAML string.
    pub fn load_str(yaml: &str) -> Result<Self, ConfigError> {
        Self::load_str_with_base_path(yaml, None)
    }

    /// Parse configuration from YAML with a base path for relative-path validation.
    pub fn load_str_with_base_path(
        yaml: &str,
        base_path: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let (marked_yaml, quoted_placeholders) = mark_quoted_exact_env_placeholders(yaml);
        let mut value: Value = serde_yaml_ng::from_str(&marked_yaml)?;
        expand_env_vars_in_yaml_value(&mut value, &quoted_placeholders)?;
        let raw: RawConfig = serde_yaml_ng::from_value(value)?;
        Self::from_raw(raw, base_path)
    }

    /// Expand `${VAR}` environment variables in raw YAML without parsing.
    pub fn expand_env_str(yaml: &str) -> Result<String, ConfigError> {
        expand_env_vars(yaml)
    }

    fn from_raw(raw: RawConfig, base_path: Option<&Path>) -> Result<Self, ConfigError> {
        let pipelines = match (raw.pipelines, raw.input, raw.output) {
            (Some(p), None, None) => {
                if !raw.enrichment.is_empty() {
                    return Err(ConfigError::Validation(
                        "top-level `enrichment` is not supported when using `pipelines:` form \
                         — move enrichment configuration inside each pipeline"
                            .into(),
                    ));
                }
                if !raw.resource_attrs.is_empty() {
                    return Err(ConfigError::Validation(
                        "top-level `resource_attrs` cannot be used with `pipelines:`; \
                         move resource_attrs into each pipeline"
                            .into(),
                    ));
                }
                if raw.transform.is_some() {
                    return Err(ConfigError::Validation(
                        "top-level `transform` cannot be used with `pipelines:`; \
                         move transform SQL into each pipeline"
                            .into(),
                    ));
                }
                p
            }
            (None, Some(input), Some(output)) => {
                let pipeline = PipelineConfig {
                    inputs: vec![input],
                    transform: raw.transform,
                    outputs: vec![output],
                    enrichment: raw.enrichment,
                    resource_attrs: raw.resource_attrs,
                    workers: None,
                    batch_target_bytes: None,
                    batch_timeout_ms: None,
                    poll_interval_ms: None,
                };
                let mut map = HashMap::new();
                map.insert("default".to_string(), pipeline);
                map
            }
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                return Err(ConfigError::Validation(
                    "cannot mix top-level input/output with pipelines".into(),
                ));
            }
            (None, None, None) => {
                return Err(ConfigError::Validation(
                    "config must define either input/output or pipelines".into(),
                ));
            }
            (None, Some(_), None) => {
                return Err(ConfigError::Validation(
                    "output is required when input is specified".into(),
                ));
            }
            (None, None, Some(_)) => {
                return Err(ConfigError::Validation(
                    "input is required when output is specified".into(),
                ));
            }
        };

        let cfg = Config {
            pipelines,
            server: raw.server,
            storage: raw.storage,
        };
        cfg.validate_with_base_path(base_path)?;
        Ok(cfg)
    }
}

fn expand_env_vars_in_yaml_value(
    value: &mut Value,
    quoted_placeholders: &HashMap<String, String>,
) -> Result<(), ConfigError> {
    match value {
        Value::String(text) => {
            if let Some(original) = quoted_placeholders.get(text.as_str()) {
                *text = expand_env_vars(original)?;
                return Ok(());
            }

            let original = text.clone();
            let expanded = expand_env_vars(&original)?;
            if is_exact_env_placeholder(&original) {
                *value = coerce_expanded_yaml_scalar(&expanded);
            } else {
                *text = expanded;
            }
        }
        Value::Sequence(items) => {
            for item in items {
                expand_env_vars_in_yaml_value(item, quoted_placeholders)?;
            }
        }
        Value::Mapping(map) => {
            let old = std::mem::take(map);
            for (mut key, mut val) in old {
                expand_env_vars_in_yaml_value(&mut key, quoted_placeholders)?;
                expand_env_vars_in_yaml_value(&mut val, quoted_placeholders)?;
                if map.insert(key, val).is_some() {
                    return Err(ConfigError::Validation(
                        "environment variable expansion produced duplicate YAML mapping key".into(),
                    ));
                }
            }
        }
        Value::Tagged(tagged) => {
            expand_env_vars_in_yaml_value(&mut tagged.value, quoted_placeholders)?;
        }
        _ => {}
    }

    Ok(())
}

fn is_exact_env_placeholder(text: &str) -> bool {
    let Some(name) = text
        .strip_prefix("${")
        .and_then(|rest| rest.strip_suffix('}'))
    else {
        return false;
    };
    !name.is_empty() && !name.contains("${") && !name.contains('}')
}

fn coerce_expanded_yaml_scalar(text: &str) -> Value {
    match serde_yaml_ng::from_str::<Value>(text) {
        Ok(value @ (Value::Null | Value::Bool(_) | Value::Number(_))) => value,
        _ => Value::String(text.to_owned()),
    }
}

fn mark_quoted_exact_env_placeholders(yaml: &str) -> (String, HashMap<String, String>) {
    let mut marked = String::with_capacity(yaml.len());
    let mut placeholders = HashMap::new();
    let mut cursor = 0usize;
    let mut chars = yaml.char_indices().peekable();

    while let Some((start, ch)) = chars.next() {
        let quote @ ('\'' | '"') = ch else {
            continue;
        };
        if !is_yaml_quoted_scalar_start(yaml, start) {
            continue;
        }

        let Some((end, text)) = scan_yaml_quoted_scalar(yaml, start, quote) else {
            continue;
        };
        while chars.peek().is_some_and(|(idx, _)| *idx < end) {
            chars.next();
        }

        marked.push_str(&yaml[cursor..start]);
        if is_exact_env_placeholder(&text) {
            // The marker must not collide with user strings. We embed a
            // process-unique seed so that no static literal can match.
            let marker = format!(
                "__LOGFWD_QEP_{seed}_{n}__",
                seed = *PLACEHOLDER_SEED,
                n = placeholders.len(),
            );
            marked.push(quote);
            marked.push_str(&marker);
            marked.push(quote);
            placeholders.insert(marker, text);
        } else {
            marked.push_str(&yaml[start..end]);
        }
        cursor = end;
    }
    marked.push_str(&yaml[cursor..]);

    (marked, placeholders)
}

fn is_yaml_quoted_scalar_start(yaml: &str, quote_start: usize) -> bool {
    let line_start = yaml[..quote_start]
        .rfind('\n')
        .map_or(0, |idx| idx.saturating_add(1));

    // Reject quotes that appear inside a YAML block scalar (| or >).
    // Block scalar content lines are indented beyond the indicator line.
    // Walk backwards from the current line to find the nearest non-blank,
    // non-comment line with lower indentation that might introduce a block
    // scalar.
    if is_inside_block_scalar(yaml, line_start) {
        return false;
    }

    let mut before_quote = yaml[line_start..quote_start].trim_end();
    loop {
        if before_quote
            .chars()
            .rev()
            .find(|ch| !ch.is_whitespace())
            .is_none_or(|ch| matches!(ch, ':' | '-' | ',' | '[' | '{' | '?'))
        {
            return true;
        }

        let Some((token_start, token)) = trailing_yaml_token(before_quote) else {
            return false;
        };
        if !is_yaml_tag_or_anchor_token(token) {
            return false;
        }
        before_quote = before_quote[..token_start].trim_end();
    }
}

/// Returns `true` when `line_start` falls inside a YAML block scalar.
///
/// A block scalar is introduced by `|` or `>` (optionally followed by
/// chomping/indentation indicators) as the last significant token on a line.
/// Every subsequent line whose indentation is strictly greater than the
/// indicator line is part of the scalar body.
fn is_inside_block_scalar(yaml: &str, line_start: usize) -> bool {
    let current_indent = line_indent(yaml, line_start);

    // Walk backwards through preceding lines.
    let mut search_end = line_start.saturating_sub(1);
    while search_end > 0 {
        let prev_line_start = yaml[..search_end].rfind('\n').map_or(0, |idx| idx + 1);
        let prev_line = &yaml[prev_line_start..search_end];
        let prev_indent = count_leading_spaces(prev_line);

        // Skip blank lines (they can appear inside block scalars).
        if prev_line.trim().is_empty() {
            search_end = prev_line_start.saturating_sub(1);
            if prev_line_start == 0 {
                break;
            }
            continue;
        }

        // If this line has lower indentation, it could be the block indicator.
        if prev_indent < current_indent {
            let trimmed = strip_yaml_inline_comment(prev_line).trim_end();
            // Check for block scalar indicator at end of line: `|`, `>`,
            // or with chomping/indentation indicators like `|+`, `|-`, `|2`.
            if let Some(last_segment) = trimmed.split_whitespace().last()
                && is_block_scalar_indicator(last_segment)
            {
                return block_scalar_body_reaches_line(
                    yaml,
                    search_end + 1,
                    line_start,
                    prev_indent,
                );
            }
            // Also check if the whole trimmed line ends with the indicator
            // (handles `key: |` where the last space-separated token is `|`).
            if is_block_scalar_indicator(trimmed) {
                return block_scalar_body_reaches_line(
                    yaml,
                    search_end + 1,
                    line_start,
                    prev_indent,
                );
            }
            // Mixed-indentation block bodies can include lines with lower
            // indentation than the current line. Keep scanning upward; if we
            // later find an indicator, validate that every intervening
            // non-blank line remains indented beyond that indicator.
        }

        // Same or higher indentation: this line is a peer or content line,
        // keep searching upward.
        search_end = prev_line_start.saturating_sub(1);
        if prev_line_start == 0 {
            break;
        }
    }

    false
}

fn block_scalar_body_reaches_line(
    yaml: &str,
    body_start: usize,
    line_start: usize,
    indicator_indent: usize,
) -> bool {
    let mut cursor = body_start;
    while cursor < line_start {
        let line_end = yaml[cursor..line_start]
            .find('\n')
            .map_or(line_start, |idx| cursor + idx);
        let line = &yaml[cursor..line_end];
        if !line.trim().is_empty() && count_leading_spaces(line) <= indicator_indent {
            return false;
        }
        cursor = line_end.saturating_add(1);
    }
    true
}

fn strip_yaml_inline_comment(line: &str) -> &str {
    for (idx, ch) in line.char_indices() {
        if ch == '#'
            && (idx == 0
                || line[..idx]
                    .chars()
                    .next_back()
                    .is_some_and(char::is_whitespace))
        {
            return &line[..idx];
        }
    }
    line
}

fn is_block_scalar_indicator(s: &str) -> bool {
    let s = s.trim_end();
    let Some(rest) = s.strip_prefix(['|', '>']) else {
        return false;
    };

    let mut has_indent = false;
    let mut has_chomp = false;
    for ch in rest.chars() {
        match ch {
            '0'..='9' if !has_indent => has_indent = true,
            '+' | '-' if !has_chomp => has_chomp = true,
            _ => return false,
        }
    }

    true
}

fn trailing_yaml_token(s: &str) -> Option<(usize, &str)> {
    let trimmed = s.trim_end();
    if trimmed.is_empty() {
        return None;
    }
    let token_start = trimmed
        .char_indices()
        .rev()
        .find_map(|(idx, ch)| ch.is_whitespace().then_some(idx + ch.len_utf8()))
        .unwrap_or(0);
    Some((token_start, &trimmed[token_start..]))
}

fn is_yaml_tag_or_anchor_token(token: &str) -> bool {
    token.starts_with('!') || (token.starts_with('&') && token.len() > 1)
}

fn line_indent(yaml: &str, line_start: usize) -> usize {
    count_leading_spaces(&yaml[line_start..])
}

fn count_leading_spaces(s: &str) -> usize {
    s.chars().take_while(|ch| *ch == ' ').count()
}

fn scan_yaml_quoted_scalar(yaml: &str, quote_start: usize, quote: char) -> Option<(usize, String)> {
    let mut text = String::new();
    let body_start = quote_start + quote.len_utf8();
    let mut chars = yaml[body_start..].char_indices().peekable();

    while let Some((rel_idx, ch)) = chars.next() {
        let idx = body_start + rel_idx;
        if quote == '\'' && ch == '\'' {
            if chars.peek().is_some_and(|(_, next)| *next == '\'') {
                chars.next();
                text.push('\'');
                continue;
            }
            return Some((idx + ch.len_utf8(), text));
        }
        if quote == '"' && ch == '"' {
            return Some((idx + ch.len_utf8(), text));
        }
        if quote == '"' && ch == '\\' {
            if let Some((_, escaped)) = chars.next() {
                text.push('\\');
                text.push(escaped);
            }
            continue;
        }
        text.push(ch);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::mark_quoted_exact_env_placeholders;

    #[test]
    fn quoted_exact_env_placeholder_is_marked() {
        let (marked, placeholders) =
            mark_quoted_exact_env_placeholders(r#"path: "${LOGFWD_TEST_PATH}""#);

        assert_eq!(placeholders.len(), 1);
        let (marker, original) = placeholders.iter().next().unwrap();
        assert_eq!(original, "${LOGFWD_TEST_PATH}");
        // The marker is embedded in double quotes in the YAML.
        let expected = format!(r#"path: "{marker}""#);
        assert_eq!(marked, expected);
    }

    #[test]
    fn escaped_double_quoted_env_placeholder_is_not_marked() {
        let yaml = r#"path: "\${LOGFWD_TEST_PATH}""#;

        let (marked, placeholders) = mark_quoted_exact_env_placeholders(yaml);

        assert_eq!(marked, yaml);
        assert!(placeholders.is_empty());
    }

    #[test]
    fn tagged_quoted_env_placeholder_is_marked() {
        let (marked, placeholders) =
            mark_quoted_exact_env_placeholders(r#"path: !!str "${LOGFWD_TEST_PATH}""#);

        assert_eq!(placeholders.len(), 1);
        let (marker, original) = placeholders.iter().next().unwrap();
        assert_eq!(original, "${LOGFWD_TEST_PATH}");
        let expected = format!(r#"path: !!str "{marker}""#);
        assert_eq!(marked, expected);
    }

    #[test]
    fn non_specific_tagged_quoted_env_placeholder_is_marked() {
        let (marked, placeholders) =
            mark_quoted_exact_env_placeholders(r#"path: ! "${LOGFWD_TEST_PATH}""#);

        assert_eq!(placeholders.len(), 1);
        let (marker, original) = placeholders.iter().next().unwrap();
        assert_eq!(original, "${LOGFWD_TEST_PATH}");
        let expected = format!(r#"path: ! "{marker}""#);
        assert_eq!(marked, expected);
    }

    #[test]
    fn anchored_quoted_env_placeholder_is_marked() {
        let (marked, placeholders) =
            mark_quoted_exact_env_placeholders(r#"path: &p "${LOGFWD_TEST_PATH}""#);

        assert_eq!(placeholders.len(), 1);
        let (marker, original) = placeholders.iter().next().unwrap();
        assert_eq!(original, "${LOGFWD_TEST_PATH}");
        let expected = format!(r#"path: &p "{marker}""#);
        assert_eq!(marked, expected);
    }

    #[test]
    fn digit_first_block_scalar_indicator_hides_content_quotes() {
        let yaml = "note: |2+\n  \"${LOGFWD_TEST_PATH}\"\n";
        let quote_start = yaml
            .find('"')
            .expect("fixture should contain a quoted block line");

        assert!(!super::is_yaml_quoted_scalar_start(yaml, quote_start));
    }

    #[test]
    fn commented_block_scalar_indicator_hides_content_quotes() {
        let yaml = "note: | # keep this readable\n  \"${LOGFWD_TEST_PATH}\"\n";
        let quote_start = yaml
            .find('"')
            .expect("fixture should contain a quoted block line");

        assert!(!super::is_yaml_quoted_scalar_start(yaml, quote_start));
    }
}
