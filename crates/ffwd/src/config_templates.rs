use std::fmt::Write as _;

pub(crate) type InputTemplate = ffwd_config::docspec::TemplateDoc;
pub(crate) type OutputTemplate = ffwd_config::docspec::TemplateDoc;

/// A full end-to-end starter scenario (input, transform, output).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UseCaseTemplate {
    /// Stable template identifier.
    pub(crate) id: &'static str,
    /// Short scenario title.
    pub(crate) title: &'static str,
    /// One-line scenario description.
    pub(crate) description: &'static str,
    /// Input body for the scenario's single pipeline item.
    pub(crate) input_snippet: &'static str,
    /// Output body for the scenario's single pipeline item.
    pub(crate) output_snippet: &'static str,
    /// SQL transform used by the scenario.
    pub(crate) transform: &'static str,
}

/// Input presets currently surfaced by `ff wizard`.
pub(crate) const INPUT_TEMPLATES: &[InputTemplate] = ffwd_config::docspec::INPUT_TEMPLATES;

/// Output presets currently surfaced by `ff wizard`.
pub(crate) const OUTPUT_TEMPLATES: &[OutputTemplate] = ffwd_config::docspec::OUTPUT_TEMPLATES;

/// Opinionated end-to-end presets for `ff wizard`.
pub(crate) const USE_CASE_TEMPLATES: &[UseCaseTemplate] = &[
    UseCaseTemplate {
        id: "redis_to_otlp",
        title: "Redis logs to OTLP",
        description: "Tails Redis JSON logs and sends to an OTLP collector.",
        input_snippet: "type: file\npath: /var/log/redis/redis*.json\nformat: json\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "postgresql_to_otlp",
        title: "PostgreSQL logs to OTLP",
        description: "Parses PostgreSQL JSON logs and ships to OTLP.",
        input_snippet: "type: file\npath: /var/log/postgresql/*.json\nformat: json\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "mysql_to_otlp",
        title: "MySQL logs to OTLP",
        description: "Tails MySQL JSON logs and forwards to OTLP.",
        input_snippet: "type: file\npath: /var/log/mysql/*.json\nformat: json\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "mongodb_to_otlp",
        title: "MongoDB logs to OTLP",
        description: "Parses MongoDB JSON logs and forwards to OTLP.",
        input_snippet: "type: file\npath: /var/log/mongodb/mongod*.json\nformat: json\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "kafka_to_otlp",
        title: "Kafka logs to OTLP",
        description: "Tails Kafka broker logs and forwards to OTLP.",
        input_snippet: "type: file\npath: /var/log/kafka/*.log\nformat: raw\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "rabbitmq_to_otlp",
        title: "RabbitMQ logs to OTLP",
        description: "Tails RabbitMQ logs and forwards to OTLP.",
        input_snippet: "type: file\npath: /var/log/rabbitmq/*.log\nformat: raw\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "nginx_access_to_loki",
        title: "Nginx access logs to Loki",
        description: "Ingests Nginx access logs and sends to Loki with labels.",
        input_snippet: "type: file\npath: /var/log/nginx/access.log\nformat: raw\n",
        output_snippet: "type: loki\nendpoint: https://loki:3100\nstatic_labels:\n  app: nginx\n  stream: access\nlabel_columns:\n  - status\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "nginx_error_to_otlp",
        title: "Nginx error logs to OTLP",
        description: "Ingests Nginx error logs and forwards to OTLP.",
        input_snippet: "type: file\npath: /var/log/nginx/error.log\nformat: raw\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "apache_to_elasticsearch",
        title: "Apache logs to Elasticsearch",
        description: "Ships Apache logs to Elasticsearch for search and dashboards.",
        input_snippet: "type: file\npath: /var/log/apache2/*.log\nformat: raw\n",
        output_snippet: "type: elasticsearch\nendpoint: https://elasticsearch:9200\nindex: apache-logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "systemd_journal_export_to_otlp",
        title: "System logs (journal export) to OTLP",
        description: "Reads journal-exported files and forwards to OTLP.",
        input_snippet: "type: file\npath: /var/log/journal-export/*.log\nformat: raw\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "auth_log_to_loki",
        title: "Linux auth.log to Loki",
        description: "Collects login/authentication events to Loki.",
        input_snippet: "type: file\npath: /var/log/auth.log\nformat: raw\n",
        output_snippet: "type: loki\nendpoint: https://loki:3100\nstatic_labels:\n  app: linux-auth\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "docker_json_to_otlp",
        title: "Docker JSON logs to OTLP",
        description: "Tails Docker's json-file logs and forwards to OTLP.",
        input_snippet: "type: file\npath: /var/lib/docker/containers/*/*.log\nformat: json\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "kubernetes_containers_to_otlp",
        title: "Kubernetes container logs to OTLP",
        description: "Reads CRI logs from Kubernetes nodes and forwards to OTLP.",
        input_snippet: "type: file\npath: /var/log/containers/*.log\nformat: cri\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "kubernetes_containers_to_loki",
        title: "Kubernetes container logs to Loki",
        description: "Reads CRI logs from Kubernetes nodes and ships to Loki.",
        input_snippet: "type: file\npath: /var/log/containers/*.log\nformat: cri\n",
        output_snippet: "type: loki\nendpoint: https://loki:3100\nstatic_labels:\n  cluster: prod\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "syslog_udp_to_otlp",
        title: "Network raw logs (UDP) to OTLP",
        description: "Receives newline-delimited raw logs over UDP and forwards to OTLP.",
        input_snippet: "type: udp\nlisten: 0.0.0.0:5514\nformat: raw\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "syslog_tcp_to_elasticsearch",
        title: "Network raw logs (TCP) to Elasticsearch",
        description: "Receives newline-delimited raw logs over TCP and indexes them in Elasticsearch.",
        input_snippet: "type: tcp\nlisten: 0.0.0.0:1514\nformat: raw\n",
        output_snippet: "type: elasticsearch\nendpoint: https://elasticsearch:9200\nindex: syslog\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "otlp_in_to_loki",
        title: "OTLP in to Loki",
        description: "Receives OTLP logs and forwards to Loki.",
        input_snippet: "type: otlp\nlisten: 0.0.0.0:4318\n",
        output_snippet: "type: loki\nendpoint: https://loki:3100\nstatic_labels:\n  source: otlp\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "otlp_in_to_elasticsearch",
        title: "OTLP in to Elasticsearch",
        description: "Receives OTLP logs and indexes in Elasticsearch.",
        input_snippet: "type: otlp\nlisten: 0.0.0.0:4318\n",
        output_snippet: "type: elasticsearch\nendpoint: https://elasticsearch:9200\nindex: otlp-logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "app_json_errors_only",
        title: "App JSON logs (errors only) to OTLP",
        description: "Filters JSON app logs to warn/error before sending.",
        input_snippet: "type: file\npath: /srv/app/logs/*.json\nformat: json\n",
        output_snippet: "type: otlp\nendpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs WHERE level IN ('warn', 'error', 'WARN', 'ERROR')",
    },
    UseCaseTemplate {
        id: "security_audit_to_file",
        title: "Security audit logs to local file",
        description: "Collects audit logs and writes normalized NDJSON locally.",
        input_snippet: "type: file\npath: /var/log/audit/audit.log\nformat: raw\n",
        output_snippet: "type: file\npath: ./security-audit.ndjson\n",
        transform: "SELECT * FROM logs",
    },
];

fn push_pipeline_list_item_body(out: &mut String, section_name: &str, snippet_body: &str) {
    let lines = normalized_snippet_lines(snippet_body);
    if let Some((first, rest)) = lines.split_first() {
        let _ = writeln!(out, "    {section_name}:");
        let _ = writeln!(out, "      - {first}");
        for line in rest {
            let _ = writeln!(out, "        {line}");
        }
    }
}

fn normalized_snippet_lines(snippet_body: &str) -> Vec<&str> {
    let lines: Vec<&str> = snippet_body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let base_indent = lines
        .iter()
        .map(|line| line.len() - line.trim_start().len())
        .min()
        .unwrap_or(0);
    lines
        .into_iter()
        .map(|line| line.get(base_indent..).unwrap_or_else(|| line.trim_start()))
        .collect()
}

/// Render a complete config from an input/output template pair and SQL text.
pub(crate) fn render_config(
    input: &InputTemplate,
    output: &OutputTemplate,
    transform_sql: &str,
) -> String {
    let mut out = String::new();
    out.push_str("# Generated by ff wizard\n");
    out.push_str("# Fill in paths/endpoints/credentials for your environment.\n\n");
    out.push_str("pipelines:\n  default:\n");
    push_pipeline_list_item_body(&mut out, "inputs", input.snippet.trim_end());
    if !transform_sql.trim().is_empty() {
        out.push_str("    transform: |\n");
        for line in transform_sql.lines() {
            let _ = writeln!(out, "      {line}");
        }
    }
    push_pipeline_list_item_body(&mut out, "outputs", output.snippet.trim_end());
    out.push_str("\n# Optional: diagnostics server\n");
    out.push_str("# server:\n#   diagnostics: 127.0.0.1:9191\n");
    out
}

/// Render a use-case preset, optionally with a custom SQL transform.
pub(crate) fn render_use_case(use_case: &UseCaseTemplate, sql: &str) -> String {
    let mut out = String::new();
    out.push_str("# ff example\n");
    let _ = writeln!(out, "# Use case: {}", use_case.title);
    let _ = writeln!(out, "# {}", use_case.description);
    out.push('\n');
    out.push_str("pipelines:\n  default:\n");
    push_pipeline_list_item_body(&mut out, "inputs", use_case.input_snippet.trim_end());
    out.push_str("    transform: |\n");
    for line in sql.lines() {
        let _ = writeln!(out, "      {line}");
    }
    push_pipeline_list_item_body(&mut out, "outputs", use_case.output_snippet.trim_end());
    out.push_str("\n# Optional: diagnostics\n# server:\n#   diagnostics: 127.0.0.1:9191\n");
    out
}
