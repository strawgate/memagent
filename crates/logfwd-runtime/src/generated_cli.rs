use std::fmt::Write as _;

use logfwd_config::{
    ArrowIpcOutputConfig, AuthConfig, Config, ElasticsearchOutputConfig, LokiOutputConfig,
    NullOutputConfig, OtlpOutputConfig, OutputConfigV2, OutputType, StdoutOutputConfig,
    TcpOutputConfig, UdpOutputConfig,
};

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
) -> Result<OutputConfigV2, String> {
    if matches!(output_type, OutputType::Http | OutputType::File) {
        return Err(format!("unsupported blast destination '{output_type}'"));
    }

    let endpoint = if output_type.is_endpoint_required() {
        Some(
            endpoint
                .ok_or_else(|| "blast requires --endpoint for this destination".to_owned())?
                .to_owned(),
        )
    } else {
        None
    };

    let auth = if auth_bearer_token.is_some() || !auth_header.is_empty() {
        if !output_type.is_auth_supported() {
            return Err(
                "--auth-bearer-token/--auth-header are only supported for otlp, http, elasticsearch, loki, and arrow_ipc destinations"
                    .to_owned(),
            );
        }

        let mut auth = AuthConfig::default();
        if let Some(token) = auth_bearer_token {
            auth.bearer_token = Some(token.to_owned());
        }
        for spec in auth_header {
            let (key, value) = split_header(spec)?;
            auth.headers.insert(key.to_string(), value.to_string());
        }
        Some(auth)
    } else {
        None
    };

    Ok(match output_type {
        OutputType::Otlp => OutputConfigV2::Otlp(OtlpOutputConfig {
            endpoint,
            auth,
            ..Default::default()
        }),
        OutputType::Elasticsearch => OutputConfigV2::Elasticsearch(ElasticsearchOutputConfig {
            endpoint,
            auth,
            ..Default::default()
        }),
        OutputType::Loki => OutputConfigV2::Loki(LokiOutputConfig {
            endpoint,
            auth,
            ..Default::default()
        }),
        OutputType::Stdout => OutputConfigV2::Stdout(StdoutOutputConfig::default()),
        OutputType::Null => OutputConfigV2::Null(NullOutputConfig::default()),
        OutputType::Tcp => OutputConfigV2::Tcp(TcpOutputConfig {
            endpoint,
            ..Default::default()
        }),
        OutputType::Udp => OutputConfigV2::Udp(UdpOutputConfig {
            endpoint,
            ..Default::default()
        }),
        OutputType::ArrowIpc => OutputConfigV2::ArrowIpc(ArrowIpcOutputConfig {
            endpoint,
            auth,
            ..Default::default()
        }),
        _ => return Err(format!("unsupported blast destination '{output_type}'")),
    })
}

/// Render the blast-generated YAML.
pub fn render_blast_yaml(
    output_cfg: &OutputConfigV2,
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
    let _ = writeln!(
        yaml,
        "      - type: {}",
        yaml_quote(&output_cfg.output_type().to_string())
    );

    if let Some(endpoint) = output_cfg.endpoint() {
        let _ = writeln!(yaml, "        endpoint: {}", yaml_quote(endpoint));
    }
    if let Some(auth) = output_cfg.auth() {
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
    fn resolve_blast_output_config_otlp_requires_endpoint() {
        let err = resolve_blast_output_config(OutputType::Otlp, None, None, &[])
            .expect_err("otlp blast destination should require endpoint");
        assert!(
            err.contains("blast requires --endpoint"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_blast_output_config_otlp_preserves_endpoint() {
        let cfg = resolve_blast_output_config(
            OutputType::Otlp,
            Some("http://127.0.0.1:8080/ingest"),
            None,
            &[],
        )
        .expect("otlp blast destination should preserve endpoint");
        assert_eq!(cfg.endpoint(), Some("http://127.0.0.1:8080/ingest"));
    }

    #[test]
    fn resolve_blast_output_config_otlp_allows_auth() {
        let headers = vec!["X-API-Key=secret".to_owned()];
        let cfg = resolve_blast_output_config(
            OutputType::Otlp,
            Some("http://127.0.0.1:8080/ingest"),
            Some("token-123"),
            &headers,
        )
        .expect("otlp blast destination should allow auth");

        let auth = cfg.auth().expect("auth should be present");
        assert_eq!(auth.bearer_token.as_deref(), Some("token-123"));
        assert_eq!(
            auth.headers.get("X-API-Key").map(String::as_str),
            Some("secret")
        );
    }

    #[test]
    fn resolve_blast_output_config_rejects_unimplemented_destinations() {
        for output_type in [OutputType::Http, OutputType::File] {
            let err =
                resolve_blast_output_config(output_type.clone(), Some("http://example"), None, &[])
                    .expect_err("unimplemented blast destination should be rejected");
            assert!(
                err.contains(&format!("unsupported blast destination '{output_type}'")),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn render_blast_yaml_quotes_null_output_type() {
        let yaml = render_blast_yaml(
            &OutputConfigV2::Null(NullOutputConfig::default()),
            1,
            1,
            "127.0.0.1:0",
        );

        assert!(yaml.contains("      - type: \"null\""));
        Config::load_str(&yaml).expect("quoted null blast YAML should parse");
    }
}
