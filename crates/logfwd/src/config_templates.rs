use std::fmt::Write as _;

/// A reusable input snippet presented by the interactive wizard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InputTemplate {
    /// Stable template identifier.
    pub(crate) id: &'static str,
    /// Human-friendly label shown in prompts.
    pub(crate) label: &'static str,
    /// One-line description shown below the label in prompts.
    pub(crate) description: &'static str,
    /// YAML fragment injected into the generated config.
    pub(crate) snippet: &'static str,
}

/// A reusable output snippet presented by the interactive wizard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutputTemplate {
    /// Stable template identifier.
    pub(crate) id: &'static str,
    /// Human-friendly label shown in prompts.
    pub(crate) label: &'static str,
    /// One-line description shown below the label in prompts.
    pub(crate) description: &'static str,
    /// YAML fragment injected into the generated config.
    pub(crate) snippet: &'static str,
}

/// A full end-to-end starter scenario (input, transform, output).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UseCaseTemplate {
    /// Stable template identifier.
    pub(crate) id: &'static str,
    /// Short scenario title.
    pub(crate) title: &'static str,
    /// One-line scenario description.
    pub(crate) description: &'static str,
    /// Input YAML snippet for the scenario.
    pub(crate) input: &'static str,
    /// Output YAML snippet for the scenario.
    pub(crate) output: &'static str,
    /// SQL transform used by the scenario.
    pub(crate) transform: &'static str,
}

/// Input presets currently surfaced by `ff wizard`.
pub(crate) const INPUT_TEMPLATES: &[InputTemplate] = &[
    InputTemplate {
        id: "file_json",
        label: "File tailing (JSON logs)",
        description: "Watch JSON log files on disk and stream new lines as they appear.",
        snippet: "input:\n  type: file\n  path: /var/log/app/*.json\n  format: json\n",
    },
    InputTemplate {
        id: "file_cri",
        label: "File tailing (Kubernetes CRI)",
        description: "Tail Kubernetes container logs in CRI format from node filesystems.",
        snippet: "input:\n  type: file\n  path: /var/log/containers/*.log\n  format: cri\n",
    },
    InputTemplate {
        id: "udp_raw",
        label: "UDP listener (newline-delimited raw logs)",
        description: "Receive raw log lines over UDP, e.g. syslog or custom agents.",
        snippet: "input:\n  type: udp\n  listen: 0.0.0.0:5514\n  format: raw\n",
    },
    InputTemplate {
        id: "tcp_json",
        label: "TCP listener (newline JSON)",
        description: "Accept newline-delimited JSON logs over a TCP socket.",
        snippet: "input:\n  type: tcp\n  listen: 0.0.0.0:9000\n  format: json\n",
    },
    InputTemplate {
        id: "otlp_receiver",
        label: "OTLP log receiver",
        description: "Receive logs via the OpenTelemetry Protocol (OTLP/HTTP).",
        snippet: "input:\n  type: otlp\n  listen: 0.0.0.0:4318\n",
    },
];

/// Output presets currently surfaced by `ff wizard`.
pub(crate) const OUTPUT_TEMPLATES: &[OutputTemplate] = &[
    OutputTemplate {
        id: "otlp",
        label: "OTLP collector",
        description: "Send logs to an OpenTelemetry collector via OTLP/HTTP.",
        snippet: "output:\n  type: otlp\n  endpoint: http://localhost:4318/v1/logs\n",
    },
    OutputTemplate {
        id: "loki",
        label: "Grafana Loki",
        description: "Push logs to Grafana Loki for querying with LogQL.",
        snippet: "output:\n  type: loki\n  endpoint: http://localhost:3100\n  static_labels:\n    service: logfwd\n  label_columns:\n    - level\n    - app\n",
    },
    OutputTemplate {
        id: "elasticsearch",
        label: "Elasticsearch",
        description: "Index logs in Elasticsearch for full-text search.",
        snippet: "output:\n  type: elasticsearch\n  endpoint: http://localhost:9200\n  index: logs\n",
    },
    OutputTemplate {
        id: "stdout",
        label: "stdout (for testing)",
        description: "Print logs to the terminal. Great for trying things out.",
        snippet: "output:\n  type: stdout\n",
    },
    OutputTemplate {
        id: "file",
        label: "NDJSON file",
        description: "Write logs as newline-delimited JSON to a local file.",
        snippet: "output:\n  type: file\n  path: ./out.ndjson\n",
    },
    OutputTemplate {
        id: "null",
        label: "null sink (drop data)",
        description: "Discard all logs. Useful for benchmarking or dry runs.",
        snippet: "output:\n  type: \"null\"\n",
    },
];

/// Opinionated end-to-end presets for `ff wizard`.
pub(crate) const USE_CASE_TEMPLATES: &[UseCaseTemplate] = &[
    UseCaseTemplate {
        id: "redis_to_otlp",
        title: "Redis logs to OTLP",
        description: "Tails Redis JSON logs and sends to an OTLP collector.",
        input: "input:\n  type: file\n  path: /var/log/redis/redis*.json\n  format: json\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "postgresql_to_otlp",
        title: "PostgreSQL logs to OTLP",
        description: "Parses PostgreSQL JSON logs and ships to OTLP.",
        input: "input:\n  type: file\n  path: /var/log/postgresql/*.json\n  format: json\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "mysql_to_otlp",
        title: "MySQL logs to OTLP",
        description: "Tails MySQL JSON logs and forwards to OTLP.",
        input: "input:\n  type: file\n  path: /var/log/mysql/*.json\n  format: json\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "mongodb_to_otlp",
        title: "MongoDB logs to OTLP",
        description: "Parses MongoDB JSON logs and forwards to OTLP.",
        input: "input:\n  type: file\n  path: /var/log/mongodb/mongod*.json\n  format: json\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "kafka_to_otlp",
        title: "Kafka logs to OTLP",
        description: "Tails Kafka broker logs and forwards to OTLP.",
        input: "input:\n  type: file\n  path: /var/log/kafka/*.log\n  format: raw\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "rabbitmq_to_otlp",
        title: "RabbitMQ logs to OTLP",
        description: "Tails RabbitMQ logs and forwards to OTLP.",
        input: "input:\n  type: file\n  path: /var/log/rabbitmq/*.log\n  format: raw\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "nginx_access_to_loki",
        title: "Nginx access logs to Loki",
        description: "Ingests Nginx access logs and sends to Loki with labels.",
        input: "input:\n  type: file\n  path: /var/log/nginx/access.log\n  format: raw\n",
        output: "output:\n  type: loki\n  endpoint: https://loki:3100\n  static_labels:\n    app: nginx\n    stream: access\n  label_columns:\n    - status\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "nginx_error_to_otlp",
        title: "Nginx error logs to OTLP",
        description: "Ingests Nginx error logs and forwards to OTLP.",
        input: "input:\n  type: file\n  path: /var/log/nginx/error.log\n  format: raw\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "apache_to_elasticsearch",
        title: "Apache logs to Elasticsearch",
        description: "Ships Apache logs to Elasticsearch for search and dashboards.",
        input: "input:\n  type: file\n  path: /var/log/apache2/*.log\n  format: raw\n",
        output: "output:\n  type: elasticsearch\n  endpoint: https://elasticsearch:9200\n  index: apache-logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "systemd_journal_export_to_otlp",
        title: "System logs (journal export) to OTLP",
        description: "Reads journal-exported files and forwards to OTLP.",
        input: "input:\n  type: file\n  path: /var/log/journal-export/*.log\n  format: raw\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "auth_log_to_loki",
        title: "Linux auth.log to Loki",
        description: "Collects login/authentication events to Loki.",
        input: "input:\n  type: file\n  path: /var/log/auth.log\n  format: raw\n",
        output: "output:\n  type: loki\n  endpoint: https://loki:3100\n  static_labels:\n    app: linux-auth\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "docker_json_to_otlp",
        title: "Docker JSON logs to OTLP",
        description: "Tails Docker's json-file logs and forwards to OTLP.",
        input: "input:\n  type: file\n  path: /var/lib/docker/containers/*/*.log\n  format: json\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "kubernetes_containers_to_otlp",
        title: "Kubernetes container logs to OTLP",
        description: "Reads CRI logs from Kubernetes nodes and forwards to OTLP.",
        input: "input:\n  type: file\n  path: /var/log/containers/*.log\n  format: cri\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "kubernetes_containers_to_loki",
        title: "Kubernetes container logs to Loki",
        description: "Reads CRI logs from Kubernetes nodes and ships to Loki.",
        input: "input:\n  type: file\n  path: /var/log/containers/*.log\n  format: cri\n",
        output: "output:\n  type: loki\n  endpoint: https://loki:3100\n  static_labels:\n    cluster: prod\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "syslog_udp_to_otlp",
        title: "Network raw logs (UDP) to OTLP",
        description: "Receives newline-delimited raw logs over UDP and forwards to OTLP.",
        input: "input:\n  type: udp\n  listen: 0.0.0.0:5514\n  format: raw\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "syslog_tcp_to_elasticsearch",
        title: "Network raw logs (TCP) to Elasticsearch",
        description: "Receives newline-delimited raw logs over TCP and indexes them in Elasticsearch.",
        input: "input:\n  type: tcp\n  listen: 0.0.0.0:1514\n  format: raw\n",
        output: "output:\n  type: elasticsearch\n  endpoint: https://elasticsearch:9200\n  index: syslog\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "otlp_in_to_loki",
        title: "OTLP in to Loki",
        description: "Receives OTLP logs and forwards to Loki.",
        input: "input:\n  type: otlp\n  listen: 0.0.0.0:4318\n",
        output: "output:\n  type: loki\n  endpoint: https://loki:3100\n  static_labels:\n    source: otlp\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "otlp_in_to_elasticsearch",
        title: "OTLP in to Elasticsearch",
        description: "Receives OTLP logs and indexes in Elasticsearch.",
        input: "input:\n  type: otlp\n  listen: 0.0.0.0:4318\n",
        output: "output:\n  type: elasticsearch\n  endpoint: https://elasticsearch:9200\n  index: otlp-logs\n",
        transform: "SELECT * FROM logs",
    },
    UseCaseTemplate {
        id: "app_json_errors_only",
        title: "App JSON logs (errors only) to OTLP",
        description: "Filters JSON app logs to warn/error before sending.",
        input: "input:\n  type: file\n  path: /srv/app/logs/*.json\n  format: json\n",
        output: "output:\n  type: otlp\n  endpoint: https://otel-collector:4318/v1/logs\n",
        transform: "SELECT * FROM logs WHERE level IN ('warn', 'error', 'WARN', 'ERROR')",
    },
    UseCaseTemplate {
        id: "security_audit_to_file",
        title: "Security audit logs to local file",
        description: "Collects audit logs and writes normalized NDJSON locally.",
        input: "input:\n  type: file\n  path: /var/log/audit/audit.log\n  format: raw\n",
        output: "output:\n  type: file\n  path: ./security-audit.ndjson\n",
        transform: "SELECT * FROM logs",
    },
];

/// Render a complete config from an input/output template pair and SQL text.
pub(crate) fn render_config(
    input: &InputTemplate,
    output: &OutputTemplate,
    transform_sql: &str,
) -> String {
    let mut out = String::new();
    out.push_str("# Generated by ff wizard\n");
    out.push_str("# Fill in paths/endpoints/credentials for your environment.\n\n");
    out.push_str(input.snippet);
    out.push('\n');
    if !transform_sql.trim().is_empty() {
        out.push_str("transform: |\n");
        for line in transform_sql.lines() {
            let _ = writeln!(out, "  {line}");
        }
        out.push('\n');
    }
    out.push_str(output.snippet);
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
    out.push_str(use_case.input);
    out.push('\n');
    out.push_str("transform: |\n");
    for line in sql.lines() {
        let _ = writeln!(out, "  {line}");
    }
    out.push('\n');
    out.push_str(use_case.output);
    out.push_str("\n# Optional: diagnostics\n# server:\n#   diagnostics: 127.0.0.1:9191\n");
    out
}
