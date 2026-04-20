use logfwd_config::Config;
use wasm_bindgen::prelude::*;

// ── Template data ──────────────────────────────────────────────────────────
// Mirrored from crates/logfwd/src/config_templates.rs.
// Keep in sync when adding new templates.

/// A single editable field in a template's config panel.
/// `default` is the YAML value that appears in `snippet` for this field.
/// Substitution matches `"key: default"` in the snippet and replaces with `"key: <user value>"`.
/// `options` non-empty → render as a `<select>` dropdown; values are the literal YAML strings.
#[derive(serde::Serialize)]
struct FieldDef {
    key: &'static str,
    label: &'static str,
    #[serde(rename = "default")]
    default_val: &'static str,
    placeholder: &'static str,
    options: &'static [&'static str],
}

#[derive(serde::Serialize)]
struct InputTemplate {
    id: &'static str,
    label: &'static str,
    description: &'static str,
    snippet: &'static str,
    fields: &'static [FieldDef],
}

#[derive(serde::Serialize)]
struct OutputTemplate {
    id: &'static str,
    label: &'static str,
    description: &'static str,
    snippet: &'static str,
    fields: &'static [FieldDef],
}

#[derive(serde::Serialize)]
struct UseCaseTemplate {
    id: &'static str,
    title: &'static str,
    description: &'static str,
    input_id: &'static str,
    output_id: &'static str,
    transform: &'static str,
}

const INPUT_TEMPLATES: &[InputTemplate] = &[
    InputTemplate {
        id: "file_json",
        label: "File (JSON logs)",
        description: "Tail JSON log files on disk.",
        snippet: "input:\n  type: file\n  path: /var/log/app/*.json\n  format: json\n",
        fields: &[
            FieldDef {
                key: "path",
                label: "Path",
                default_val: "/var/log/app/*.json",
                placeholder: "/var/log/app/*.json",
                options: &[],
            },
            FieldDef {
                key: "format",
                label: "Format",
                default_val: "json",
                placeholder: "json",
                options: &["json", "raw", "auto"],
            },
        ],
    },
    InputTemplate {
        id: "file_cri",
        label: "File (Kubernetes CRI)",
        description: "Tail Kubernetes container logs from node filesystems.",
        snippet: "input:\n  type: file\n  path: /var/log/containers/*.log\n  format: cri\n",
        fields: &[FieldDef {
            key: "path",
            label: "Path",
            default_val: "/var/log/containers/*.log",
            placeholder: "/var/log/containers/*.log",
            options: &[],
        }],
    },
    InputTemplate {
        id: "file_raw",
        label: "File (raw lines)",
        description: "Tail plain-text log files, one event per line.",
        snippet: "input:\n  type: file\n  path: /var/log/app/*.log\n  format: raw\n",
        fields: &[
            FieldDef {
                key: "path",
                label: "Path",
                default_val: "/var/log/app/*.log",
                placeholder: "/var/log/app/*.log",
                options: &[],
            },
            FieldDef {
                key: "format",
                label: "Format",
                default_val: "raw",
                placeholder: "raw",
                options: &["raw", "json", "auto"],
            },
        ],
    },
    InputTemplate {
        id: "udp_raw",
        label: "UDP listener",
        description: "Receive raw log lines over UDP (e.g. syslog).",
        snippet: "input:\n  type: udp\n  listen: 0.0.0.0:5514\n  format: raw\n",
        fields: &[
            FieldDef {
                key: "listen",
                label: "Listen",
                default_val: "0.0.0.0:5514",
                placeholder: "0.0.0.0:5514",
                options: &[],
            },
            FieldDef {
                key: "format",
                label: "Format",
                default_val: "raw",
                placeholder: "raw",
                options: &["raw", "json"],
            },
        ],
    },
    InputTemplate {
        id: "tcp_json",
        label: "TCP listener (JSON)",
        description: "Accept newline-delimited JSON logs over TCP.",
        snippet: "input:\n  type: tcp\n  listen: 0.0.0.0:9000\n  format: json\n",
        fields: &[
            FieldDef {
                key: "listen",
                label: "Listen",
                default_val: "0.0.0.0:9000",
                placeholder: "0.0.0.0:9000",
                options: &[],
            },
            FieldDef {
                key: "format",
                label: "Format",
                default_val: "json",
                placeholder: "json",
                options: &["json", "raw"],
            },
        ],
    },
    InputTemplate {
        id: "otlp_receiver",
        label: "OTLP receiver",
        description: "Receive logs via OpenTelemetry Protocol (OTLP/HTTP).",
        snippet: "input:\n  type: otlp\n  listen: 0.0.0.0:4318\n",
        fields: &[FieldDef {
            key: "listen",
            label: "Listen",
            default_val: "0.0.0.0:4318",
            placeholder: "0.0.0.0:4318",
            options: &[],
        }],
    },
    InputTemplate {
        id: "http_json",
        label: "HTTP endpoint (JSON)",
        description: "Accept JSON log batches over HTTP POST.",
        snippet: "input:\n  type: http\n  listen: 0.0.0.0:8080\n",
        fields: &[FieldDef {
            key: "listen",
            label: "Listen",
            default_val: "0.0.0.0:8080",
            placeholder: "0.0.0.0:8080",
            options: &[],
        }],
    },
    InputTemplate {
        id: "journald",
        label: "systemd journald",
        description: "Read logs from the systemd journal.",
        snippet: "input:\n  type: journald\n",
        fields: &[],
    },
    InputTemplate {
        id: "generator",
        label: "Generator",
        description: "Synthetic log generator for testing and benchmarking.",
        snippet: "input:\n  type: generator\n  generator:\n    events_per_sec: 1000\n    complexity: simple\n",
        fields: &[
            FieldDef {
                key: "events_per_sec",
                label: "Events / sec",
                default_val: "1000",
                placeholder: "1000",
                options: &[],
            },
            FieldDef {
                key: "complexity",
                label: "Complexity",
                default_val: "simple",
                placeholder: "simple",
                options: &["simple", "complex"],
            },
        ],
    },
];

const OUTPUT_TEMPLATES: &[OutputTemplate] = &[
    OutputTemplate {
        id: "otlp",
        label: "OTLP collector",
        description: "Send logs to an OpenTelemetry collector via OTLP/HTTP.",
        snippet: "output:\n  type: otlp\n  endpoint: http://localhost:4318/v1/logs\n  compression: none\n",
        fields: &[
            FieldDef {
                key: "endpoint",
                label: "Endpoint",
                default_val: "http://localhost:4318/v1/logs",
                placeholder: "http://otel-collector:4318/v1/logs",
                options: &[],
            },
            FieldDef {
                key: "compression",
                label: "Compression",
                default_val: "none",
                placeholder: "none",
                options: &["none", "gzip", "zstd"],
            },
        ],
    },
    OutputTemplate {
        id: "elasticsearch",
        label: "Elasticsearch",
        description: "Index logs in Elasticsearch.",
        snippet: "output:\n  type: elasticsearch\n  endpoint: http://localhost:9200\n  index: logs\n  compression: none\n",
        fields: &[
            FieldDef {
                key: "endpoint",
                label: "Endpoint",
                default_val: "http://localhost:9200",
                placeholder: "http://es:9200",
                options: &[],
            },
            FieldDef {
                key: "index",
                label: "Index",
                default_val: "logs",
                placeholder: "logs",
                options: &[],
            },
            FieldDef {
                key: "compression",
                label: "Compression",
                default_val: "none",
                placeholder: "none",
                options: &["none", "gzip"],
            },
        ],
    },
    OutputTemplate {
        id: "loki",
        label: "Grafana Loki",
        description: "Push logs to Grafana Loki.",
        snippet: "output:\n  type: loki\n  endpoint: http://localhost:3100\n  static_labels:\n    service: myapp\n  label_columns:\n    - level\n",
        fields: &[
            FieldDef {
                key: "endpoint",
                label: "Endpoint",
                default_val: "http://localhost:3100",
                placeholder: "http://loki:3100",
                options: &[],
            },
            FieldDef {
                key: "service",
                label: "Service label",
                default_val: "myapp",
                placeholder: "myapp",
                options: &[],
            },
        ],
    },
    OutputTemplate {
        id: "file",
        label: "NDJSON file",
        description: "Write logs as newline-delimited JSON to a file.",
        snippet: "output:\n  type: file\n  path: /var/log/out.ndjson\n",
        fields: &[FieldDef {
            key: "path",
            label: "Path",
            default_val: "/var/log/out.ndjson",
            placeholder: "/var/log/out.ndjson",
            options: &[],
        }],
    },
    OutputTemplate {
        id: "stdout",
        label: "stdout",
        description: "Print logs to the terminal. Great for testing.",
        snippet: "output:\n  type: stdout\n",
        fields: &[],
    },
    OutputTemplate {
        id: "null",
        label: "null sink",
        description: "Discard all logs. Useful for benchmarking.",
        snippet: "output:\n  type: \"null\"\n",
        fields: &[],
    },
];

const USE_CASE_TEMPLATES: &[UseCaseTemplate] = &[
    UseCaseTemplate {
        id: "k8s_to_otlp",
        title: "Kubernetes → OTLP",
        description: "Collect Kubernetes pod logs and ship to an OTLP collector.",
        input_id: "file_cri",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "nginx_to_loki",
        title: "nginx → Loki",
        description: "Tail nginx access logs and push to Grafana Loki.",
        input_id: "file_json",
        output_id: "loki",
        transform: "SELECT * FROM logs WHERE status >= 400",
    },
    UseCaseTemplate {
        id: "nginx_to_es",
        title: "nginx → Elasticsearch",
        description: "Index nginx access logs in Elasticsearch.",
        input_id: "file_json",
        output_id: "elasticsearch",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "syslog_to_otlp",
        title: "syslog UDP → OTLP",
        description: "Receive syslog over UDP and forward to OTLP.",
        input_id: "udp_raw",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "otlp_passthrough",
        title: "OTLP passthrough",
        description: "Receive OTLP logs and re-emit to a downstream collector.",
        input_id: "otlp_receiver",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
];

// ── WASM exports ───────────────────────────────────────────────────────────

/// Returns the list of available input templates as a JSON array.
#[wasm_bindgen]
pub fn get_input_templates() -> JsValue {
    serde_wasm_bindgen_or_json(INPUT_TEMPLATES)
}

/// Returns the list of available output templates as a JSON array.
#[wasm_bindgen]
pub fn get_output_templates() -> JsValue {
    serde_wasm_bindgen_or_json(OUTPUT_TEMPLATES)
}

/// Returns the list of use-case starter presets as a JSON array.
#[wasm_bindgen]
pub fn get_use_case_templates() -> JsValue {
    serde_wasm_bindgen_or_json(USE_CASE_TEMPLATES)
}

/// Assemble a config YAML from template IDs and an optional SQL transform.
///
/// Returns the YAML string, or throws a JS error if the IDs are not found.
#[wasm_bindgen]
pub fn render_config(
    input_id: &str,
    output_id: &str,
    transform_sql: &str,
) -> Result<String, JsValue> {
    let input = INPUT_TEMPLATES
        .iter()
        .find(|t| t.id == input_id)
        .ok_or_else(|| JsValue::from_str(&format!("unknown input template: {input_id}")))?;

    let output = OUTPUT_TEMPLATES
        .iter()
        .find(|t| t.id == output_id)
        .ok_or_else(|| JsValue::from_str(&format!("unknown output template: {output_id}")))?;

    let mut out = String::from("# Generated by logfwd config builder\n");
    out.push_str("# Fill in paths, endpoints, and credentials for your environment.\n\n");
    out.push_str(input.snippet);
    out.push('\n');
    if !transform_sql.trim().is_empty() {
        out.push_str("transform: |\n");
        for line in transform_sql.lines() {
            out.push_str("  ");
            out.push_str(line);
            out.push('\n');
        }
        out.push('\n');
    }
    out.push_str(output.snippet);
    out.push_str("\n# Optional: diagnostics server\n# server:\n#   diagnostics: 127.0.0.1:9191\n");
    Ok(out)
}

/// Validate a logfwd config YAML string.
///
/// Returns `undefined` on success, or throws a JS error with the validation
/// message on failure.
#[wasm_bindgen]
pub fn validate_config(yaml: &str) -> Result<(), JsValue> {
    Config::load_str(yaml)
        .map(|_| ())
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Returns the snippet for a single input template by ID.
#[wasm_bindgen]
pub fn get_input_snippet(id: &str) -> Option<String> {
    INPUT_TEMPLATES
        .iter()
        .find(|t| t.id == id)
        .map(|t| t.snippet.to_string())
}

/// Returns the snippet for a single output template by ID.
#[wasm_bindgen]
pub fn get_output_snippet(id: &str) -> Option<String> {
    OUTPUT_TEMPLATES
        .iter()
        .find(|t| t.id == id)
        .map(|t| t.snippet.to_string())
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn serde_wasm_bindgen_or_json<T: serde::Serialize + ?Sized>(value: &T) -> JsValue {
    // Serialize to JSON string then parse — avoids needing serde-wasm-bindgen.
    // All callers pass compile-time static data; serialization cannot fail.
    let json = serde_json::to_string(value).expect("static template data serialization failed");
    js_sys::JSON::parse(&json).expect("static template data produced invalid JSON")
}
