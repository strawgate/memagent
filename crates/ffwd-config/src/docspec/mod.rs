use std::fmt::Write as _;

use serde::Serialize;

/// Public support level for a documented config surface.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::SupportLevel;
///
/// let level = SupportLevel::Stable;
/// assert!(matches!(level, SupportLevel::Stable));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SupportLevel {
    /// Supported and intended for production use.
    Stable,
    /// Available for evaluation, but still changing quickly.
    Experimental,
    /// Implemented and usable, but still settling.
    Beta,
    /// Mentioned in the schema surface, but not implemented yet.
    NotYetSupported,
    /// Internal-only surface that should not appear in public docs.
    Hidden,
}

/// A single editable field in a starter template.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::BuilderFieldDoc;
///
/// let field = BuilderFieldDoc {
///     key: "path",
///     label: "Path",
///     default_value: "/var/log/app.log",
///     placeholder: "/var/log/app.log",
///     options: &[],
/// };
/// assert_eq!(field.key, "path");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct BuilderFieldDoc {
    /// Stable field identifier used by builder clients.
    pub key: &'static str,
    /// Human-facing field label.
    pub label: &'static str,
    /// Default value presented to the user.
    #[serde(rename = "default")]
    pub default_value: &'static str,
    /// Example value shown before editing.
    pub placeholder: &'static str,
    /// Fixed choices, when the field is an enum-like selector.
    pub options: &'static [&'static str],
}

/// A documented starter template for an input or output.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::{BuilderFieldDoc, SupportLevel, TemplateDoc};
///
/// let fields = [BuilderFieldDoc {
///     key: "listen",
///     label: "Listen",
///     default_value: "0.0.0.0:9000",
///     placeholder: "0.0.0.0:9000",
///     options: &[],
/// }];
/// let template = TemplateDoc {
///     id: "tcp_json",
///     type_tag: "tcp",
///     aliases: &[],
///     label: "TCP listener (JSON)",
///     description: "Accept newline-delimited JSON logs over TCP.",
///     support: SupportLevel::Stable,
///     snippet: "type: tcp\\nlisten: 0.0.0.0:9000\\n",
///     fields: &fields,
/// };
/// assert_eq!(template.type_tag, "tcp");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TemplateDoc {
    /// Stable template identifier used by the docs builder and CLI wizard.
    pub id: &'static str,
    /// The config `type:` tag emitted by this template.
    pub type_tag: &'static str,
    /// Legacy aliases still accepted by the parser, if any.
    pub aliases: &'static [&'static str],
    /// Human-facing label shown in pickers.
    pub label: &'static str,
    /// One-line description shown in pickers and docs.
    pub description: &'static str,
    /// Public support level for the underlying config surface.
    pub support: SupportLevel,
    /// YAML starter snippet for one pipeline item body.
    pub snippet: &'static str,
    /// Small editable subset exposed by the docs builder UI.
    pub fields: &'static [BuilderFieldDoc],
}

/// A documented config component type for support/inventory tables.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::{ComponentTypeDoc, SupportLevel};
///
/// let component = ComponentTypeDoc {
///     type_tag: "stdout",
///     support: SupportLevel::Stable,
///     description: "Print to stdout.",
/// };
/// assert_eq!(component.type_tag, "stdout");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComponentTypeDoc {
    /// The config `type:` value used in YAML.
    pub type_tag: &'static str,
    /// Public support level shown in generated tables.
    pub support: SupportLevel,
    /// Short description used in generated docs.
    pub description: &'static str,
}

/// An end-to-end starter scenario combining an input, output, and optional transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct UseCaseDoc {
    pub id: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub input_id: &'static str,
    pub output_id: &'static str,
    pub transform: &'static str,
}

/// Starter templates exposed for documented input configurations.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::INPUT_TEMPLATES;
///
/// assert!(INPUT_TEMPLATES.iter().any(|template| template.id == "file_json"));
/// ```
pub const INPUT_TEMPLATES: &[TemplateDoc] = &[
    TemplateDoc {
        id: "file_json",
        type_tag: "file",
        aliases: &[],
        label: "File (JSON logs)",
        description: "Tail JSON log files on disk.",
        support: SupportLevel::Stable,
        snippet: "type: file\npath: /var/log/app/*.json\nformat: json\n",
        fields: &[
            BuilderFieldDoc {
                key: "path",
                label: "Path",
                default_value: "/var/log/app/*.json",
                placeholder: "/var/log/app/*.json",
                options: &[],
            },
            BuilderFieldDoc {
                key: "format",
                label: "Format",
                default_value: "json",
                placeholder: "json",
                options: &["json", "raw", "auto"],
            },
        ],
    },
    TemplateDoc {
        id: "file_cri",
        type_tag: "file",
        aliases: &[],
        label: "File (Kubernetes CRI)",
        description: "Tail Kubernetes container logs from node filesystems.",
        support: SupportLevel::Stable,
        snippet: "type: file\npath: /var/log/containers/*.log\nformat: cri\n",
        fields: &[BuilderFieldDoc {
            key: "path",
            label: "Path",
            default_value: "/var/log/containers/*.log",
            placeholder: "/var/log/containers/*.log",
            options: &[],
        }],
    },
    TemplateDoc {
        id: "file_raw",
        type_tag: "file",
        aliases: &[],
        label: "File (raw lines)",
        description: "Tail plain-text log files, one event per line.",
        support: SupportLevel::Stable,
        snippet: "type: file\npath: /var/log/app/*.log\nformat: raw\n",
        fields: &[
            BuilderFieldDoc {
                key: "path",
                label: "Path",
                default_value: "/var/log/app/*.log",
                placeholder: "/var/log/app/*.log",
                options: &[],
            },
            BuilderFieldDoc {
                key: "format",
                label: "Format",
                default_value: "raw",
                placeholder: "raw",
                options: &["raw", "json", "auto"],
            },
        ],
    },
    TemplateDoc {
        id: "udp_raw",
        type_tag: "udp",
        aliases: &[],
        label: "UDP listener",
        description: "Receive raw log lines over UDP (e.g. syslog).",
        support: SupportLevel::Stable,
        snippet: "type: udp\nlisten: 0.0.0.0:5514\nformat: raw\n",
        fields: &[
            BuilderFieldDoc {
                key: "listen",
                label: "Listen",
                default_value: "0.0.0.0:5514",
                placeholder: "0.0.0.0:5514",
                options: &[],
            },
            BuilderFieldDoc {
                key: "format",
                label: "Format",
                default_value: "raw",
                placeholder: "raw",
                options: &["raw", "json"],
            },
        ],
    },
    TemplateDoc {
        id: "tcp_json",
        type_tag: "tcp",
        aliases: &[],
        label: "TCP listener (JSON)",
        description: "Accept newline-delimited JSON logs over TCP.",
        support: SupportLevel::Stable,
        snippet: "type: tcp\nlisten: 0.0.0.0:9000\nformat: json\n",
        fields: &[
            BuilderFieldDoc {
                key: "listen",
                label: "Listen",
                default_value: "0.0.0.0:9000",
                placeholder: "0.0.0.0:9000",
                options: &[],
            },
            BuilderFieldDoc {
                key: "format",
                label: "Format",
                default_value: "json",
                placeholder: "json",
                options: &["json", "raw"],
            },
        ],
    },
    TemplateDoc {
        id: "otlp_receiver",
        type_tag: "otlp",
        aliases: &[],
        label: "OTLP receiver",
        description: "Receive logs via OpenTelemetry Protocol (OTLP/HTTP).",
        support: SupportLevel::Stable,
        snippet: "type: otlp\nlisten: 0.0.0.0:4318\n",
        fields: &[BuilderFieldDoc {
            key: "listen",
            label: "Listen",
            default_value: "0.0.0.0:4318",
            placeholder: "0.0.0.0:4318",
            options: &[],
        }],
    },
    TemplateDoc {
        id: "http_json",
        type_tag: "http",
        aliases: &[],
        label: "HTTP endpoint (JSON)",
        description: "Accept JSON log batches over HTTP POST.",
        support: SupportLevel::Stable,
        snippet: "type: http\nlisten: 0.0.0.0:8080\n",
        fields: &[BuilderFieldDoc {
            key: "listen",
            label: "Listen",
            default_value: "0.0.0.0:8080",
            placeholder: "0.0.0.0:8080",
            options: &[],
        }],
    },
    TemplateDoc {
        id: "journald",
        type_tag: "journald",
        aliases: &[],
        label: "systemd journald",
        description: "Read structured journal entries from systemd journald.",
        support: SupportLevel::Beta,
        snippet: "type: journald\n",
        fields: &[
            BuilderFieldDoc {
                key: "include_units",
                label: "Include units",
                default_value: "",
                placeholder: "nginx, redis",
                options: &[],
            },
            BuilderFieldDoc {
                key: "exclude_units",
                label: "Exclude units",
                default_value: "",
                placeholder: "",
                options: &[],
            },
            BuilderFieldDoc {
                key: "priorities",
                label: "Priorities",
                default_value: "",
                placeholder: "err, warning",
                options: &[],
            },
            BuilderFieldDoc {
                key: "cursor_path",
                label: "Cursor path",
                default_value: "",
                placeholder: "/var/lib/ffwd/journald.cursor",
                options: &[],
            },
            BuilderFieldDoc {
                key: "current_boot_only",
                label: "Current boot only",
                default_value: "true",
                placeholder: "true",
                options: &["true", "false"],
            },
            BuilderFieldDoc {
                key: "since_now",
                label: "Since now",
                default_value: "false",
                placeholder: "false",
                options: &["true", "false"],
            },
            BuilderFieldDoc {
                key: "backend",
                label: "Backend",
                default_value: "auto",
                placeholder: "auto",
                options: &["auto", "native", "subprocess"],
            },
        ],
    },
    TemplateDoc {
        id: "generator",
        type_tag: "generator",
        aliases: &[],
        label: "Generator",
        description: "Synthetic log generator for testing and benchmarking.",
        support: SupportLevel::Stable,
        snippet: "type: generator\ngenerator:\n  events_per_sec: 1000\n  complexity: simple\n",
        fields: &[
            BuilderFieldDoc {
                key: "events_per_sec",
                label: "Events / sec",
                default_value: "1000",
                placeholder: "1000",
                options: &[],
            },
            BuilderFieldDoc {
                key: "complexity",
                label: "Complexity",
                default_value: "simple",
                placeholder: "simple",
                options: &["simple", "complex"],
            },
        ],
    },
];

/// Starter templates exposed for documented output configurations.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::OUTPUT_TEMPLATES;
///
/// assert!(OUTPUT_TEMPLATES.iter().any(|template| template.id == "stdout"));
/// ```
pub const OUTPUT_TEMPLATES: &[TemplateDoc] = &[
    TemplateDoc {
        id: "otlp",
        type_tag: "otlp",
        aliases: &[],
        label: "OTLP collector",
        description: "Send logs to an OpenTelemetry collector via OTLP/HTTP.",
        support: SupportLevel::Stable,
        snippet: "type: otlp\nendpoint: http://localhost:4318/v1/logs\ncompression: none\n",
        fields: &[
            BuilderFieldDoc {
                key: "endpoint",
                label: "Endpoint",
                default_value: "http://localhost:4318/v1/logs",
                placeholder: "http://otel-collector:4318/v1/logs",
                options: &[],
            },
            BuilderFieldDoc {
                key: "compression",
                label: "Compression",
                default_value: "none",
                placeholder: "none",
                options: &["none", "gzip", "zstd"],
            },
        ],
    },
    TemplateDoc {
        id: "elasticsearch",
        type_tag: "elasticsearch",
        aliases: &[],
        label: "Elasticsearch",
        description: "Index logs in Elasticsearch.",
        support: SupportLevel::Stable,
        snippet: "type: elasticsearch\nendpoint: http://localhost:9200\nindex: logs\ncompression: none\n",
        fields: &[
            BuilderFieldDoc {
                key: "endpoint",
                label: "Endpoint",
                default_value: "http://localhost:9200",
                placeholder: "http://es:9200",
                options: &[],
            },
            BuilderFieldDoc {
                key: "index",
                label: "Index",
                default_value: "logs",
                placeholder: "logs",
                options: &[],
            },
            BuilderFieldDoc {
                key: "compression",
                label: "Compression",
                default_value: "none",
                placeholder: "none",
                options: &["none", "gzip"],
            },
        ],
    },
    TemplateDoc {
        id: "loki",
        type_tag: "loki",
        aliases: &[],
        label: "Grafana Loki",
        description: "Push logs to Grafana Loki.",
        support: SupportLevel::Stable,
        snippet: "type: loki\nendpoint: http://localhost:3100\nstatic_labels:\n  service: myapp\nlabel_columns:\n  - level\n",
        fields: &[
            BuilderFieldDoc {
                key: "endpoint",
                label: "Endpoint",
                default_value: "http://localhost:3100",
                placeholder: "http://loki:3100",
                options: &[],
            },
            BuilderFieldDoc {
                key: "service",
                label: "Service label",
                default_value: "myapp",
                placeholder: "myapp",
                options: &[],
            },
        ],
    },
    TemplateDoc {
        id: "file",
        type_tag: "file",
        aliases: &[],
        label: "NDJSON file",
        description: "Write logs as newline-delimited JSON to a file.",
        support: SupportLevel::Stable,
        snippet: "type: file\npath: ./out.ndjson\n",
        fields: &[BuilderFieldDoc {
            key: "path",
            label: "Path",
            default_value: "./out.ndjson",
            placeholder: "./out.ndjson",
            options: &[],
        }],
    },
    TemplateDoc {
        id: "stdout",
        type_tag: "stdout",
        aliases: &[],
        label: "stdout",
        description: "Print logs to the terminal. Great for testing.",
        support: SupportLevel::Stable,
        snippet: "type: stdout\n",
        fields: &[],
    },
    TemplateDoc {
        id: "null",
        type_tag: "null",
        aliases: &[],
        label: "null sink",
        description: "Discard all logs. Useful for benchmarking.",
        support: SupportLevel::Stable,
        snippet: "type: \"null\"\n",
        fields: &[],
    },
];

/// Opinionated end-to-end starter scenarios for `ff wizard`.
///
/// Each entry references an input template and an output template by their
/// stable identifiers (`input_id` / `output_id`).  The YAML snippets are
/// resolved at render time by looking up the corresponding [`INPUT_TEMPLATES`]
/// and [`OUTPUT_TEMPLATES`] entries.
///
/// This definition is shared between `ffwd-config` (runtime wizard), the
/// WASM config builder, and the generated docs.  A compile-time check
/// (`use_case_templates_valid`) ensures every `input_id` and `output_id`
/// resolves to an existing template.
pub const USE_CASE_TEMPLATES: &[UseCaseDoc] = &[
    UseCaseDoc {
        id: "redis_to_otlp",
        title: "Redis logs to OTLP",
        description: "Tails Redis JSON logs and sends to an OTLP collector.",
        input_id: "file_json",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "postgresql_to_otlp",
        title: "PostgreSQL logs to OTLP",
        description: "Parses PostgreSQL JSON logs and ships to OTLP.",
        input_id: "file_json",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "mysql_to_otlp",
        title: "MySQL logs to OTLP",
        description: "Tails MySQL JSON logs and forwards to OTLP.",
        input_id: "file_json",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "mongodb_to_otlp",
        title: "MongoDB logs to OTLP",
        description: "Parses MongoDB JSON logs and forwards to OTLP.",
        input_id: "file_json",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "kafka_to_otlp",
        title: "Kafka logs to OTLP",
        description: "Tails Kafka broker logs and forwards to OTLP.",
        input_id: "file_raw",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "rabbitmq_to_otlp",
        title: "RabbitMQ logs to OTLP",
        description: "Tails RabbitMQ logs and forwards to OTLP.",
        input_id: "file_raw",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "nginx_access_to_loki",
        title: "Nginx access logs to Loki",
        description: "Ingests Nginx access logs and sends to Loki with labels.",
        input_id: "file_raw",
        output_id: "loki",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "nginx_error_to_otlp",
        title: "Nginx error logs to OTLP",
        description: "Ingests Nginx error logs and forwards to OTLP.",
        input_id: "file_raw",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "apache_to_elasticsearch",
        title: "Apache logs to Elasticsearch",
        description: "Ships Apache logs to Elasticsearch for search and dashboards.",
        input_id: "file_raw",
        output_id: "elasticsearch",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "systemd_journal_export_to_otlp",
        title: "System logs (journal export) to OTLP",
        description: "Reads journal-exported files and forwards to OTLP.",
        input_id: "file_raw",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "auth_log_to_loki",
        title: "Linux auth.log to Loki",
        description: "Collects login/authentication events to Loki.",
        input_id: "file_raw",
        output_id: "loki",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "docker_json_to_otlp",
        title: "Docker JSON logs to OTLP",
        description: "Tails Docker's json-file logs and forwards to OTLP.",
        input_id: "file_json",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "kubernetes_containers_to_otlp",
        title: "Kubernetes container logs to OTLP",
        description: "Reads CRI logs from Kubernetes nodes and forwards to OTLP.",
        input_id: "file_cri",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "kubernetes_containers_to_loki",
        title: "Kubernetes container logs to Loki",
        description: "Reads CRI logs from Kubernetes nodes and ships to Loki.",
        input_id: "file_cri",
        output_id: "loki",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "syslog_udp_to_otlp",
        title: "Network raw logs (UDP) to OTLP",
        description: "Receives newline-delimited raw logs over UDP and forwards to OTLP.",
        input_id: "udp_raw",
        output_id: "otlp",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "syslog_tcp_to_elasticsearch",
        title: "Network JSON logs (TCP) to Elasticsearch",
        description: "Receives newline-delimited JSON logs over TCP and indexes them in Elasticsearch.",
        input_id: "tcp_json",
        output_id: "elasticsearch",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "otlp_in_to_loki",
        title: "OTLP to Loki",
        description: "Receives OTLP logs and forwards to Loki.",
        input_id: "otlp_receiver",
        output_id: "loki",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "otlp_in_to_elasticsearch",
        title: "OTLP to Elasticsearch",
        description: "Receives OTLP logs and indexes in Elasticsearch.",
        input_id: "otlp_receiver",
        output_id: "elasticsearch",
        transform: "SELECT * FROM logs",
    },
    UseCaseDoc {
        id: "app_json_errors_only",
        title: "App JSON logs (errors only) to OTLP",
        description: "Filters JSON app logs to warn/error before sending.",
        input_id: "file_json",
        output_id: "otlp",
        transform: "SELECT * FROM logs WHERE level IN ('warn', 'error', 'WARN', 'ERROR')",
    },
    UseCaseDoc {
        id: "security_audit_to_file",
        title: "Security audit logs to local file",
        description: "Collects audit logs and writes normalized NDJSON locally.",
        input_id: "file_raw",
        output_id: "file",
        transform: "SELECT * FROM logs",
    },
];

/// Look up a use-case preset by its stable identifier.
///
/// Returns `None` when `id` is not registered in [`USE_CASE_TEMPLATES`].
pub fn use_case_template(id: &str) -> Option<&'static UseCaseDoc> {
    USE_CASE_TEMPLATES.iter().find(|template| template.id == id)
}

/// Input component inventory used for generated support tables.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::INPUT_TYPE_DOCS;
///
/// assert!(INPUT_TYPE_DOCS.iter().any(|entry| entry.type_tag == "file"));
/// ```
pub const INPUT_TYPE_DOCS: &[ComponentTypeDoc] = &[
    ComponentTypeDoc {
        type_tag: "file",
        support: SupportLevel::Stable,
        description: "Tail files matching a glob pattern.",
    },
    ComponentTypeDoc {
        type_tag: "s3",
        support: SupportLevel::Stable,
        description: "Read objects from AWS S3 or an S3-compatible endpoint.",
    },
    ComponentTypeDoc {
        type_tag: "stdin",
        support: SupportLevel::Stable,
        description: "Read piped stdin until EOF, then drain outputs and exit.",
    },
    ComponentTypeDoc {
        type_tag: "generator",
        support: SupportLevel::Stable,
        description: "Emit synthetic JSON-like records from an in-process source.",
    },
    ComponentTypeDoc {
        type_tag: "udp",
        support: SupportLevel::Stable,
        description: "Receive log lines over UDP.",
    },
    ComponentTypeDoc {
        type_tag: "tcp",
        support: SupportLevel::Stable,
        description: "Accept log lines over TCP.",
    },
    ComponentTypeDoc {
        type_tag: "otlp",
        support: SupportLevel::Stable,
        description: "Receive OTLP logs over a bound listen address.",
    },
    ComponentTypeDoc {
        type_tag: "http",
        support: SupportLevel::Stable,
        description: "Receive newline-delimited payloads via HTTP `POST`.",
    },
    ComponentTypeDoc {
        type_tag: "linux_ebpf_sensor",
        support: SupportLevel::Stable,
        description: "Linux eBPF sensor input (Arrow-native control + signal rows).",
    },
    ComponentTypeDoc {
        type_tag: "macos_es_sensor",
        support: SupportLevel::Stable,
        description: "macOS EndpointSecurity sensor input (Arrow-native control + signal rows).",
    },
    ComponentTypeDoc {
        type_tag: "macos_log",
        support: SupportLevel::Stable,
        description: "Read macOS unified log entries from the `log stream` command.",
    },
    ComponentTypeDoc {
        type_tag: "windows_ebpf_sensor",
        support: SupportLevel::Stable,
        description: "Windows eBPF sensor input (Arrow-native control + signal rows).",
    },
    ComponentTypeDoc {
        type_tag: "journald",
        support: SupportLevel::Beta,
        description: "Read structured journal entries from systemd journald.",
    },
    ComponentTypeDoc {
        type_tag: "host_metrics",
        support: SupportLevel::Stable,
        description: "Host metrics input — process snapshots, CPU, memory, network stats via sysinfo (Arrow-native).",
    },
    ComponentTypeDoc {
        type_tag: "arrow_ipc",
        support: SupportLevel::Stable,
        description: "Receive Arrow IPC stream batches via HTTP `POST /v1/arrow`.",
    },
];

/// Output component inventory used for generated support tables.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::OUTPUT_TYPE_DOCS;
///
/// assert!(OUTPUT_TYPE_DOCS.iter().any(|entry| entry.type_tag == "stdout"));
/// ```
pub const OUTPUT_TYPE_DOCS: &[ComponentTypeDoc] = &[
    ComponentTypeDoc {
        type_tag: "otlp",
        support: SupportLevel::Stable,
        description: "OTLP protobuf over HTTP or gRPC.",
    },
    ComponentTypeDoc {
        type_tag: "http",
        support: SupportLevel::Stable,
        description: "POST newline-delimited JSON rows to an HTTP endpoint.",
    },
    ComponentTypeDoc {
        type_tag: "stdout",
        support: SupportLevel::Stable,
        description: "Print to stdout (JSON, console, or text).",
    },
    ComponentTypeDoc {
        type_tag: "elasticsearch",
        support: SupportLevel::Stable,
        description: "Elasticsearch Bulk API with index/compression/request-mode controls.",
    },
    ComponentTypeDoc {
        type_tag: "loki",
        support: SupportLevel::Stable,
        description: "Grafana Loki push API with label grouping.",
    },
    ComponentTypeDoc {
        type_tag: "file",
        support: SupportLevel::Stable,
        description: "Write NDJSON or text to a local file.",
    },
    ComponentTypeDoc {
        type_tag: "null",
        support: SupportLevel::Stable,
        description: "Drop records intentionally for tests and benchmark baselines.",
    },
    ComponentTypeDoc {
        type_tag: "tcp",
        support: SupportLevel::Stable,
        description: "Send records to a TCP endpoint.",
    },
    ComponentTypeDoc {
        type_tag: "udp",
        support: SupportLevel::Stable,
        description: "Send records to a UDP endpoint.",
    },
    ComponentTypeDoc {
        type_tag: "arrow_ipc",
        support: SupportLevel::Stable,
        description: "Send Arrow IPC payloads to an HTTP endpoint.",
    },
];

/// Look up an input starter template by its stable template identifier.
///
/// Returns `None` when `id` is not registered in [`INPUT_TEMPLATES`].
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::input_template;
///
/// assert_eq!(input_template("file_json").map(|template| template.type_tag), Some("file"));
/// assert!(input_template("missing").is_none());
/// ```
pub fn input_template(id: &str) -> Option<&'static TemplateDoc> {
    INPUT_TEMPLATES.iter().find(|template| template.id == id)
}

/// Look up an output starter template by its stable template identifier.
///
/// Returns `None` when `id` is not registered in [`OUTPUT_TEMPLATES`].
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::output_template;
///
/// assert_eq!(output_template("stdout").map(|template| template.type_tag), Some("stdout"));
/// assert!(output_template("missing").is_none());
/// ```
pub fn output_template(id: &str) -> Option<&'static TemplateDoc> {
    OUTPUT_TEMPLATES.iter().find(|template| template.id == id)
}

/// Render a Markdown support table for public component types.
///
/// Hidden entries are omitted. Public support levels are rendered as
/// human-facing status labels such as `Implemented` and `Not yet supported`.
///
/// # Examples
///
/// ```
/// use ffwd_config::docspec::{render_component_type_table, ComponentTypeDoc, SupportLevel};
///
/// let docs = [ComponentTypeDoc {
///     type_tag: "stdout",
///     support: SupportLevel::Stable,
///     description: "Print to stdout.",
/// }];
/// let table = render_component_type_table(&docs);
/// assert!(table.contains("| `stdout` | Implemented | Print to stdout. |"));
/// ```
pub fn render_component_type_table(entries: &[ComponentTypeDoc]) -> String {
    let mut out =
        String::from("| Value | Status | Description |\n|-------|--------|-------------|\n");
    for entry in entries {
        if entry.support == SupportLevel::Hidden {
            continue;
        }
        let status = match entry.support {
            SupportLevel::Stable => "Implemented",
            SupportLevel::Experimental => "Experimental",
            SupportLevel::Beta => "Beta",
            SupportLevel::NotYetSupported => "Not yet supported",
            SupportLevel::Hidden => "Hidden",
        };
        let _ = writeln!(
            out,
            "| `{}` | {} | {} |",
            entry.type_tag, status, entry.description
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        INPUT_TEMPLATES, INPUT_TYPE_DOCS, OUTPUT_TEMPLATES, OUTPUT_TYPE_DOCS, USE_CASE_TEMPLATES,
        input_template, output_template, render_component_type_table, use_case_template,
    };

    #[test]
    fn otlp_output_template_uses_http_logs_path() {
        let otlp = output_template("otlp").expect("otlp template");
        assert!(otlp.snippet.contains("/v1/logs"));
    }

    #[test]
    fn input_templates_have_unique_ids() {
        let mut ids: Vec<&str> = INPUT_TEMPLATES.iter().map(|template| template.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), INPUT_TEMPLATES.len());
    }

    #[test]
    fn output_templates_have_unique_ids() {
        let mut ids: Vec<&str> = OUTPUT_TEMPLATES
            .iter()
            .map(|template| template.id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), OUTPUT_TEMPLATES.len());
    }

    #[test]
    fn template_lookup_finds_known_templates() {
        assert!(input_template("file_json").is_some());
        assert!(output_template("stdout").is_some());
    }

    #[test]
    fn use_case_templates_have_unique_ids() {
        let mut ids: Vec<&str> = USE_CASE_TEMPLATES.iter().map(|t| t.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), USE_CASE_TEMPLATES.len());
    }

    #[test]
    fn use_case_template_ids_resolve() {
        for uc in USE_CASE_TEMPLATES {
            assert!(
                input_template(uc.input_id).is_some(),
                "use case '{}' references unknown input_id '{}'",
                uc.id,
                uc.input_id
            );
            assert!(
                output_template(uc.output_id).is_some(),
                "use case '{}' references unknown output_id '{}'",
                uc.id,
                uc.output_id
            );
        }
    }

    #[test]
    fn every_template_type_is_present_in_component_inventory() {
        for template in INPUT_TEMPLATES {
            let entry = INPUT_TYPE_DOCS
                .iter()
                .find(|entry| entry.type_tag == template.type_tag)
                .unwrap_or_else(|| {
                    panic!(
                        "missing input type doc for template type {}",
                        template.type_tag
                    )
                });
            assert_eq!(
                entry.support, template.support,
                "input support mismatch for template type {}",
                template.type_tag
            );
        }
        for template in OUTPUT_TEMPLATES {
            let entry = OUTPUT_TYPE_DOCS
                .iter()
                .find(|entry| entry.type_tag == template.type_tag)
                .unwrap_or_else(|| {
                    panic!(
                        "missing output type doc for template type {}",
                        template.type_tag
                    )
                });
            assert_eq!(
                entry.support, template.support,
                "output support mismatch for template type {}",
                template.type_tag
            );
        }
    }

    #[test]
    fn reference_doc_generated_support_tables_are_current() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crates dir")
            .parent()
            .expect("workspace root")
            .to_path_buf();
        let reference_path = root.join("book/src/content/docs/configuration/reference.mdx");
        let reference = std::fs::read_to_string(&reference_path).expect("read reference.mdx");

        let input_expected = render_component_type_table(INPUT_TYPE_DOCS);
        let output_expected = render_component_type_table(OUTPUT_TYPE_DOCS);

        assert_block_matches(
            &reference,
            "input-types",
            &input_expected,
            reference_path.to_string_lossy().as_ref(),
        );
        assert_block_matches(
            &reference,
            "output-types",
            &output_expected,
            reference_path.to_string_lossy().as_ref(),
        );
    }

    fn assert_block_matches(document: &str, name: &str, expected: &str, path: &str) {
        let begin = format!("{{/* BEGIN GENERATED: {name} */}}");
        let end = format!("{{/* END GENERATED: {name} */}}");
        let start = document
            .find(&begin)
            .unwrap_or_else(|| panic!("missing {begin} marker in {path}"));
        let end_index = document[start..].find(&end).map_or_else(
            || panic!("missing {end} marker in {path}"),
            |offset| start + offset,
        );
        let actual = document[start + begin.len()..end_index]
            .trim_matches('\n')
            .trim();
        let expected = expected.trim_matches('\n').trim();
        assert_eq!(
            actual, expected,
            "{path}: generated block '{name}' is stale; update it to match the docspec registry"
        );
    }
}
