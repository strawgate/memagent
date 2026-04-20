use std::fmt::Write as _;

use logfwd_config::{Config, OutputConfig, OutputType};

/// Fully rendered generated config plus its validated in-memory config.
///
/// The CLI facade uses this bundle to print startup banners and hand the
/// validated config to the runtime without rebuilding the YAML a second time.
pub struct GeneratedConfig {
    /// Parsed config ready to hand to the runtime.
    pub config: Config,
    /// YAML snapshot used for diagnostics and startup banners.
    pub yaml: String,
}

/// Devour presets shared with the CLI facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevourModeSpec {
    /// OTLP blackhole receiver.
    Otlp,
    /// Plain HTTP blackhole receiver.
    Http,
    /// Elasticsearch bulk-compatible blackhole receiver.
    ElasticsearchBulk,
    /// Raw TCP blackhole receiver.
    Tcp,
    /// Raw UDP blackhole receiver.
    Udp,
}

/// Build, render, and validate the blast-generated config.
pub fn build_blast_generated_config(
    output_type: OutputType,
    endpoint: Option<&str>,
    auth_bearer_token: Option<&str>,
    auth_header: &[String],
    workers: usize,
    batch_lines: usize,
    diagnostics_addr: &str,
) -> Result<GeneratedConfig, String> {
    let output_cfg =
        resolve_blast_output_config(output_type, endpoint, auth_bearer_token, auth_header)?;
    let yaml = render_blast_yaml(&output_cfg, workers, batch_lines, diagnostics_addr);
    let config = Config::load_str(&yaml).map_err(|e| format!("build generated config: {e}"))?;
    Ok(GeneratedConfig { config, yaml })
}

/// Build, render, and validate the devour-generated config.
pub fn build_devour_generated_config(
    mode: DevourModeSpec,
    listen: &str,
    diagnostics_addr: &str,
) -> Result<GeneratedConfig, String> {
    let yaml = render_devour_yaml(mode, listen, diagnostics_addr);
    let config = Config::load_str(&yaml).map_err(|e| format!("build generated config: {e}"))?;
    Ok(GeneratedConfig { config, yaml })
}

/// Resolve blast output settings from CLI selections.
pub fn resolve_blast_output_config(
    output_type: OutputType,
    endpoint: Option<&str>,
    auth_bearer_token: Option<&str>,
    auth_header: &[String],
) -> Result<OutputConfig, String> {
    let mut output_cfg = OutputConfig {
        output_type: output_type.clone(),
        ..OutputConfig::default()
    };

    if matches!(
        output_type,
        OutputType::Otlp
            | OutputType::Http
            | OutputType::Elasticsearch
            | OutputType::Loki
            | OutputType::ArrowIpc
            | OutputType::Tcp
            | OutputType::Udp
    ) {
        let endpoint =
            endpoint.ok_or_else(|| "blast requires --endpoint for this destination".to_owned())?;
        output_cfg.endpoint = Some(endpoint.to_owned());
    }

    if auth_bearer_token.is_some() || !auth_header.is_empty() {
        if !matches!(
            output_type,
            OutputType::Otlp
                | OutputType::Http
                | OutputType::Elasticsearch
                | OutputType::Loki
                | OutputType::ArrowIpc
        ) {
            return Err(
                "--auth-bearer-token/--auth-header are only supported for otlp, http, elasticsearch, loki, and arrow_ipc destinations"
                    .to_owned(),
            );
        }

        let mut auth = output_cfg.auth.clone().unwrap_or_default();
        if let Some(token) = auth_bearer_token {
            auth.bearer_token = Some(token.to_owned());
        }
        for spec in auth_header {
            let (key, value) = split_header(spec)?;
            auth.headers.insert(key.to_string(), value.to_string());
        }
        output_cfg.auth = Some(auth);
    }

    Ok(output_cfg)
}

/// Render the blast-generated YAML.
pub fn render_blast_yaml(
    output_cfg: &OutputConfig,
    workers: usize,
    batch_lines: usize,
    diagnostics_addr: &str,
) -> String {
    let mut yaml = String::new();
    yaml.push_str("pipelines:\n");
    yaml.push_str("  blast:\n");
    let _ = writeln!(yaml, "    workers: {workers}");
    yaml.push_str("    inputs:\n");
    yaml.push_str("      - type: generator\n");
    yaml.push_str("        format: json\n");
    yaml.push_str("        generator:\n");
    let _ = writeln!(yaml, "          batch_size: {batch_lines}");
    yaml.push_str("          events_per_sec: 0\n");
    yaml.push_str("    outputs:\n");
    let _ = writeln!(yaml, "      - type: {}", output_cfg.output_type);

    if let Some(endpoint) = &output_cfg.endpoint {
        let _ = writeln!(yaml, "        endpoint: {}", yaml_quote(endpoint));
    }
    if let Some(auth) = &output_cfg.auth {
        yaml.push_str("        auth:\n");
        if let Some(token) = &auth.bearer_token {
            let _ = writeln!(yaml, "          bearer_token: {}", yaml_quote(token));
        }
        if !auth.headers.is_empty() {
            yaml.push_str("          headers:\n");
            let mut keys: Vec<_> = auth.headers.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = auth.headers.get(key) {
                    let _ = writeln!(
                        yaml,
                        "            {}: {}",
                        yaml_quote(key),
                        yaml_quote(value)
                    );
                }
            }
        }
    }

    yaml.push_str("server:\n");
    let _ = writeln!(yaml, "  diagnostics: {}", yaml_quote(diagnostics_addr));
    yaml
}

/// Render the devour-generated YAML.
pub fn render_devour_yaml(mode: DevourModeSpec, listen: &str, diagnostics_addr: &str) -> String {
    let input = match mode {
        DevourModeSpec::Otlp => format!(
            "input:\n  type: otlp\n  listen: {}\n  format: json\n",
            yaml_quote(listen)
        ),
        DevourModeSpec::Http => format!(
            "input:\n  type: http\n  listen: {}\n  format: json\n  http:\n    path: '/'\n    strict_path: false\n    method: POST\n    response_code: 204\n",
            yaml_quote(listen)
        ),
        DevourModeSpec::ElasticsearchBulk => format!(
            "input:\n  type: http\n  listen: {}\n  format: json\n  http:\n    path: '/_bulk'\n    strict_path: false\n    method: POST\n    response_code: 200\n    response_body: {}\n",
            yaml_quote(listen),
            yaml_quote("{\"took\":0,\"errors\":false,\"items\":[]}")
        ),
        DevourModeSpec::Tcp => format!(
            "input:\n  type: tcp\n  listen: {}\n  format: json\n",
            yaml_quote(listen)
        ),
        DevourModeSpec::Udp => format!(
            "input:\n  type: udp\n  listen: {}\n  format: json\n",
            yaml_quote(listen)
        ),
    };

    format!(
        "{input}output:\n  type: \"null\"\nserver:\n  diagnostics: {}\n",
        yaml_quote(diagnostics_addr),
    )
}

/// Split a `KEY=VALUE` header assignment for auth/header plumbing.
pub fn split_header(spec: &str) -> Result<(&str, &str), String> {
    let Some((key, value)) = spec.split_once('=') else {
        return Err(format!(
            "invalid --auth-header '{spec}'; expected KEY=VALUE"
        ));
    };
    if key.is_empty() {
        return Err(format!(
            "invalid --auth-header '{spec}'; key must not be empty"
        ));
    }
    Ok((key, value))
}

/// Quote a string for YAML output, preserving control characters.
pub fn yaml_quote(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0c}' => escaped.push_str("\\f"),
            ch if ch.is_control() => {
                let _ =
                    std::fmt::Write::write_fmt(&mut escaped, format_args!("\\u{:04x}", ch as u32));
            }
            ch => escaped.push(ch),
        }
    }
    escaped.push('"');
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_blast_output_config_http_requires_endpoint() {
        let err = resolve_blast_output_config(OutputType::Http, None, None, &[])
            .expect_err("http blast destination should require endpoint");
        assert!(
            err.contains("blast requires --endpoint"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_blast_output_config_http_preserves_endpoint() {
        let cfg = resolve_blast_output_config(
            OutputType::Http,
            Some("http://127.0.0.1:8080/ingest"),
            None,
            &[],
        )
        .expect("http blast destination should preserve endpoint");
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("http://127.0.0.1:8080/ingest")
        );
    }

    #[test]
    fn resolve_blast_output_config_http_allows_auth() {
        let headers = vec!["X-API-Key=secret".to_owned()];
        let cfg = resolve_blast_output_config(
            OutputType::Http,
            Some("http://127.0.0.1:8080/ingest"),
            Some("token-123"),
            &headers,
        )
        .expect("http blast destination should allow auth");

        let auth = cfg.auth.expect("auth should be present");
        assert_eq!(auth.bearer_token.as_deref(), Some("token-123"));
        assert_eq!(
            auth.headers.get("X-API-Key").map(String::as_str),
            Some("secret")
        );
    }
}
