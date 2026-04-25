//! CLI argument definitions, error type, and terminal style helpers.

use std::env;
use std::ffi::OsString;
use std::io::{self, IsTerminal};

use clap::{Args, Parser, Subcommand, ValueEnum, error::ErrorKind};

use crate::VERSION;

// ---------------------------------------------------------------------------
// CliError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) enum CliError {
    Config(String),
    Runtime(io::Error),
}

impl CliError {
    pub(crate) fn exit_code(&self) -> i32 {
        match self {
            Self::Config(_) => crate::EXIT_CONFIG,
            Self::Runtime(_) => crate::EXIT_RUNTIME,
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "{msg}"),
            Self::Runtime(err) => write!(f, "{err}"),
        }
    }
}

impl From<io::Error> for CliError {
    fn from(value: io::Error) -> Self {
        Self::Runtime(value)
    }
}

impl From<ffwd_runtime::bootstrap::RuntimeError> for CliError {
    fn from(value: ffwd_runtime::bootstrap::RuntimeError) -> Self {
        match value {
            ffwd_runtime::bootstrap::RuntimeError::Config(msg) => Self::Config(msg),
            ffwd_runtime::bootstrap::RuntimeError::Io(err) => Self::Runtime(err),
        }
    }
}

// ---------------------------------------------------------------------------
// Color support (respects NO_COLOR, checks stderr TTY)
// ---------------------------------------------------------------------------

pub(crate) fn use_color() -> bool {
    env::var_os("NO_COLOR").is_none() && io::stderr().is_terminal()
}

pub(crate) fn use_json_logs_for_stderr(is_terminal: bool) -> bool {
    !is_terminal
}

pub(crate) fn wizard_uses_interactive_terminals(
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
) -> bool {
    stdin_is_terminal && stdout_is_terminal
}

macro_rules! style {
    ($color:expr, $bold:expr) => {
        if use_color() {
            concat!("\x1b[", $bold, ";", $color, "m")
        } else {
            ""
        }
    };
}

pub(crate) fn green() -> &'static str {
    style!("32", "0")
}
pub(crate) fn red() -> &'static str {
    style!("31", "1")
}
pub(crate) fn yellow() -> &'static str {
    style!("33", "0")
}
pub(crate) fn bold() -> &'static str {
    if use_color() { "\x1b[1m" } else { "" }
}
pub(crate) fn dim() -> &'static str {
    if use_color() { "\x1b[2m" } else { "" }
}
pub(crate) fn reset() -> &'static str {
    if use_color() { "\x1b[0m" } else { "" }
}

// ---------------------------------------------------------------------------
// Clap types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum CompletionShell {
    Bash,
    Elvish,
    Fish,
    #[value(name = "powershell", alias = "pwsh", alias = "power-shell")]
    PowerShell,
    Zsh,
    Nushell,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum BlastDestination {
    #[value(alias = "elasticsearch_otlp")]
    Otlp,
    #[value(alias = "elasticsearch_bulk")]
    Elasticsearch,
    Loki,
    #[value(alias = "arrow_ipc", alias = "arrow")]
    ArrowIpc,
    Udp,
    Tcp,
    Null,
}

impl BlastDestination {
    pub(crate) fn as_output_type(self) -> ffwd_config::OutputType {
        match self {
            Self::Otlp => ffwd_config::OutputType::Otlp,
            Self::Elasticsearch => ffwd_config::OutputType::Elasticsearch,
            Self::Loki => ffwd_config::OutputType::Loki,
            Self::ArrowIpc => ffwd_config::OutputType::ArrowIpc,
            Self::Udp => ffwd_config::OutputType::Udp,
            Self::Tcp => ffwd_config::OutputType::Tcp,
            Self::Null => ffwd_config::OutputType::Null,
        }
    }

    pub(crate) fn parse(input: &str) -> Option<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "otlp" | "elasticsearch_otlp" => Some(Self::Otlp),
            "elasticsearch" | "elasticsearch_bulk" => Some(Self::Elasticsearch),
            "loki" => Some(Self::Loki),
            "arrow_ipc" | "arrow-ipc" | "arrow" => Some(Self::ArrowIpc),
            "udp" => Some(Self::Udp),
            "tcp" => Some(Self::Tcp),
            "null" => Some(Self::Null),
            _ => None,
        }
    }

    pub(crate) fn should_require_endpoint(self) -> bool {
        !matches!(self, Self::Null)
    }

    pub(crate) fn default_endpoint(self) -> &'static str {
        match self {
            Self::Otlp => "http://127.0.0.1:4318/v1/logs",
            Self::Elasticsearch => "http://127.0.0.1:9200",
            Self::Loki => "http://127.0.0.1:3100/loki/api/v1/push",
            Self::ArrowIpc => "http://127.0.0.1:18081/v1/arrow",
            Self::Udp => "127.0.0.1:15514",
            Self::Tcp => "127.0.0.1:15140",
            Self::Null => "",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum DevourMode {
    #[value(alias = "elasticsearch_otlp")]
    Otlp,
    Http,
    #[value(name = "elasticsearch_bulk", alias = "elasticsearch")]
    ElasticsearchBulk,
    Tcp,
    Udp,
}

impl DevourMode {
    pub(crate) fn default_listen(self) -> &'static str {
        match self {
            Self::Otlp => "127.0.0.1:4318",
            Self::Http => "127.0.0.1:8080",
            Self::ElasticsearchBulk => "127.0.0.1:9200",
            Self::Tcp => "127.0.0.1:15140",
            Self::Udp => "127.0.0.1:15514",
        }
    }

    pub(crate) fn target_hint(self, listen: &str) -> String {
        match self {
            Self::Otlp => format!("http://{listen}/v1/logs"),
            Self::Http => format!("http://{listen}/"),
            Self::ElasticsearchBulk => format!("http://{listen}/_bulk"),
            Self::Tcp | Self::Udp => listen.to_string(),
        }
    }

    pub(crate) fn spec(self) -> ffwd_runtime::generated_cli::DevourModeSpec {
        match self {
            Self::Otlp => ffwd_runtime::generated_cli::DevourModeSpec::Otlp,
            Self::Http => ffwd_runtime::generated_cli::DevourModeSpec::Http,
            Self::ElasticsearchBulk => {
                ffwd_runtime::generated_cli::DevourModeSpec::ElasticsearchBulk
            }
            Self::Tcp => ffwd_runtime::generated_cli::DevourModeSpec::Tcp,
            Self::Udp => ffwd_runtime::generated_cli::DevourModeSpec::Udp,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct BlastArgs {
    /// Destination type to blast.
    #[arg(long, value_enum)]
    pub(crate) destination: Option<BlastDestination>,

    /// Destination endpoint URL/address (required for all destinations except `null`).
    #[arg(long)]
    pub(crate) endpoint: Option<String>,

    /// Bearer token auth.
    #[arg(long)]
    pub(crate) auth_bearer_token: Option<String>,

    /// Extra header, repeatable: --auth-header 'Authorization=ApiKey xyz'.
    #[arg(long, value_name = "KEY=VALUE")]
    pub(crate) auth_header: Vec<String>,

    /// Worker count.
    #[arg(long, default_value_t = 2)]
    pub(crate) workers: usize,

    /// Lines per generated batch.
    #[arg(long, default_value_t = 5_000)]
    pub(crate) batch_lines: usize,

    /// Optional run duration in seconds (default: run until stopped).
    #[arg(long)]
    pub(crate) duration_secs: Option<u64>,

    /// Diagnostics server bind address for generated config.
    #[arg(long, default_value = "127.0.0.1:0")]
    pub(crate) diagnostics_addr: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct DevourArgs {
    /// Receiver mode to emulate.
    #[arg(long, value_enum, default_value_t = DevourMode::Otlp)]
    pub(crate) mode: DevourMode,

    /// Bind address (defaults depend on mode).
    #[arg(long)]
    pub(crate) listen: Option<String>,

    /// Optional run duration in seconds (default: run until stopped).
    #[arg(long)]
    pub(crate) duration_secs: Option<u64>,

    /// Diagnostics server bind address for generated config.
    #[arg(long, default_value = "127.0.0.1:0")]
    pub(crate) diagnostics_addr: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum SendFormat {
    Auto,
    Cri,
    Json,
    Raw,
}

impl SendFormat {
    pub(crate) fn as_config_format(self) -> ffwd_config::Format {
        match self {
            Self::Auto => ffwd_config::Format::Auto,
            Self::Cri => ffwd_config::Format::Cri,
            Self::Json => ffwd_config::Format::Json,
            Self::Raw => ffwd_config::Format::Raw,
        }
    }
}

#[derive(Debug, Clone, Args, Default)]
pub(crate) struct SendArgs {
    #[arg(
        short = 'c',
        long = "config",
        value_name = "FILE",
        help = "Destination YAML config file"
    )]
    pub(crate) config: Option<String>,

    /// Input format for stdin.
    #[arg(long, value_enum)]
    pub(crate) format: Option<SendFormat>,

    /// Set `service.name` on emitted records.
    #[arg(long)]
    pub(crate) service: Option<String>,

    /// Add or override a resource attribute, repeatable: --resource key=value.
    #[arg(long, value_name = "KEY=VALUE")]
    pub(crate) resource: Vec<String>,
}

#[derive(Debug, Parser)]
#[command(
    name = "ff",
    about = "Fast log forwarder with SQL transforms",
    long_about = "Fast log forwarder with SQL transforms",
    version = VERSION,
    long_version = crate::LONG_VERSION,
    next_line_help = true,
    after_help = crate::CLI_AFTER_HELP
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Commands {
    /// Run pipeline from YAML config.
    Run {
        #[arg(
            short = 'c',
            long = "config",
            value_name = "FILE",
            help = "Path to YAML config file"
        )]
        config: Option<String>,
    },
    /// Validate config and exit.
    Validate {
        #[arg(
            short = 'c',
            long = "config",
            value_name = "FILE",
            help = "Path to YAML config file"
        )]
        config: Option<String>,
    },
    /// Build pipelines without running.
    DryRun {
        #[arg(
            short = 'c',
            long = "config",
            value_name = "FILE",
            help = "Path to YAML config file"
        )]
        config: Option<String>,
    },
    /// Read stdin, send it to the configured destination, drain, and exit.
    Send(SendArgs),
    /// Blast generated data into a destination sink via the normal runtime pipeline.
    Blast(BlastArgs),
    /// Run a blackhole receiver via the normal runtime pipeline.
    Devour(DevourArgs),
    /// Start OTLP blackhole receiver for testing.
    Blackhole {
        #[arg(
            value_name = "BIND_ADDR",
            help = "Bind address (default: 127.0.0.1:4318)"
        )]
        bind_addr: Option<String>,
    },
    /// Generate synthetic JSON log data.
    GenerateJson {
        #[arg(value_name = "NUM_LINES", help = "Number of lines to generate")]
        num_lines: usize,
        #[arg(
            value_name = "OUTPUT_FILE",
            help = "Path to write generated JSON lines"
        )]
        output_file: String,
    },
    /// Validate and print effective runnable config.
    EffectiveConfig {
        #[arg(
            short = 'c',
            long = "config",
            value_name = "FILE",
            help = "Path to YAML config file"
        )]
        config: Option<String>,
    },
    /// Generate starter config in current directory (ffwd.yaml).
    Init,
    /// Interactive config wizard — pick a use-case or build your own pipeline.
    Wizard,
    /// Print shell completions.
    Completions {
        #[arg(value_name = "SHELL", help = "Target shell")]
        shell: CompletionShell,
    },
}

// ---------------------------------------------------------------------------
// CLI parsing helpers
// ---------------------------------------------------------------------------

pub(crate) fn parse_cli_from<I, T>(args: I, stdin_is_terminal: bool) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    match Cli::try_parse_from(args.clone()) {
        Ok(cli) => Ok(cli),
        Err(err) => {
            if stdin_is_terminal || !should_retry_parse_as_send(err.kind()) {
                return Err(err);
            }
            Cli::try_parse_from(rewrite_args_as_send(args))
        }
    }
}

fn should_retry_parse_as_send(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::UnknownArgument | ErrorKind::InvalidSubcommand
    )
}

fn rewrite_args_as_send(args: Vec<OsString>) -> Vec<OsString> {
    let mut rewritten = Vec::with_capacity(args.len() + 1);
    if let Some((program, rest)) = args.split_first() {
        rewritten.push(program.clone());
        rewritten.push(OsString::from("send"));
        rewritten.extend_from_slice(rest);
    } else {
        rewritten.push(OsString::from("ff"));
        rewritten.push(OsString::from("send"));
    }
    rewritten
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use clap::error::ErrorKind;

    use super::*;

    #[test]
    fn clap_parses_validate_subcommand() {
        let cli = Cli::try_parse_from(["ff", "validate", "--config", "foo.yaml"])
            .expect("parser should accept validate subcommand");
        match cli.command.expect("command") {
            Commands::Validate { config } => assert_eq!(config.as_deref(), Some("foo.yaml")),
            other => panic!("expected validate command, got {other:?}"),
        }
    }

    #[test]
    fn clap_parses_effective_config_with_optional_config_flag() {
        let with_path = Cli::try_parse_from(["ff", "effective-config", "--config", "foo.yaml"])
            .expect("parser should accept effective-config with path");
        match with_path.command.expect("command") {
            Commands::EffectiveConfig { config } => assert_eq!(config.as_deref(), Some("foo.yaml")),
            other => panic!("expected effective-config command, got {other:?}"),
        }

        let without_path = Cli::try_parse_from(["ff", "effective-config"])
            .expect("parser should accept effective-config without path");
        match without_path.command.expect("command") {
            Commands::EffectiveConfig { config } => assert!(config.is_none()),
            other => panic!("expected effective-config command, got {other:?}"),
        }
    }

    #[test]
    fn clap_parses_blackhole_default_form() {
        let cli = Cli::try_parse_from(["ff", "blackhole"])
            .expect("parser should accept blackhole without addr");
        match cli.command.expect("command") {
            Commands::Blackhole { bind_addr } => assert!(bind_addr.is_none()),
            other => panic!("expected blackhole command, got {other:?}"),
        }
    }

    #[test]
    fn send_command_with_overrides_parses_successfully() {
        let cli = Cli::try_parse_from([
            "ff",
            "send",
            "--config",
            "dest.yaml",
            "--format",
            "json",
            "--service",
            "checkout",
            "--resource",
            "deployment=blue",
        ])
        .expect("parser should accept send command");
        match cli.command.expect("command") {
            Commands::Send(args) => {
                assert_eq!(args.config.as_deref(), Some("dest.yaml"));
                assert!(matches!(args.format, Some(SendFormat::Json)));
                assert_eq!(args.service.as_deref(), Some("checkout"));
                assert_eq!(args.resource, ["deployment=blue"]);
            }
            other => panic!("expected send command, got {other:?}"),
        }
    }

    #[test]
    fn piped_bare_send_with_overrides_parses_successfully() {
        let cli = parse_cli_from(
            [
                "ff",
                "--config",
                "dest.yaml",
                "--format",
                "raw",
                "--service",
                "checkout",
                "--resource",
                "deployment=blue",
            ],
            false,
        )
        .expect("parser should retry piped bare args as send command");
        match cli.command.expect("command") {
            Commands::Send(args) => {
                assert_eq!(args.config.as_deref(), Some("dest.yaml"));
                assert!(matches!(args.format, Some(SendFormat::Raw)));
                assert_eq!(args.service.as_deref(), Some("checkout"));
                assert_eq!(args.resource, ["deployment=blue"]);
            }
            other => panic!("expected send command, got {other:?}"),
        }
    }

    #[test]
    fn interactive_bare_send_overrides_remain_invalid() {
        let err = parse_cli_from(["ff", "--format", "raw"], true)
            .expect_err("interactive top-level send args should stay invalid");
        assert!(matches!(err.kind(), ErrorKind::UnknownArgument));
    }

    #[test]
    fn clap_completions_supports_nushell_and_powershell_alias() {
        let nu =
            Cli::try_parse_from(["ff", "completions", "nushell"]).expect("nushell should parse");
        match nu.command.expect("command") {
            Commands::Completions { shell } => assert!(matches!(shell, CompletionShell::Nushell)),
            other => panic!("expected completions command, got {other:?}"),
        }

        let pwsh =
            Cli::try_parse_from(["ff", "completions", "pwsh"]).expect("pwsh alias should parse");
        match pwsh.command.expect("command") {
            Commands::Completions { shell } => {
                assert!(matches!(shell, CompletionShell::PowerShell))
            }
            other => panic!("expected completions command, got {other:?}"),
        }
    }

    #[test]
    fn clap_parses_generate_json_subcommand() {
        let cli = Cli::try_parse_from(["ff", "generate-json", "5", "out.json"])
            .expect("generate-json should parse");
        match cli.command.expect("command") {
            Commands::GenerateJson {
                num_lines,
                output_file,
            } => {
                assert_eq!(num_lines, 5);
                assert_eq!(output_file, "out.json");
            }
            other => panic!("expected generate-json command, got {other:?}"),
        }
    }

    #[test]
    fn clap_supports_help_subcommand() {
        let err = Cli::try_parse_from(["ff", "help"])
            .expect_err("help subcommand should trigger help display");
        assert_eq!(err.kind(), ErrorKind::DisplayHelp);
    }

    #[test]
    fn clap_parses_blast_and_devour_aliases() {
        let blast = Cli::try_parse_from([
            "ff",
            "blast",
            "--destination",
            "elasticsearch_otlp",
            "--endpoint",
            "http://127.0.0.1:4318/v1/logs",
        ])
        .expect("blast alias should parse");
        match blast.command.expect("command") {
            Commands::Blast(args) => {
                assert!(matches!(args.destination, Some(BlastDestination::Otlp)))
            }
            other => panic!("expected blast command, got {other:?}"),
        }

        let blast_arrow = Cli::try_parse_from([
            "ff",
            "blast",
            "--destination",
            "arrow_ipc",
            "--endpoint",
            "http://127.0.0.1:18081",
        ])
        .expect("arrow_ipc alias should parse");
        match blast_arrow.command.expect("command") {
            Commands::Blast(args) => {
                assert!(matches!(args.destination, Some(BlastDestination::ArrowIpc)))
            }
            other => panic!("expected blast command, got {other:?}"),
        }

        let devour = Cli::try_parse_from(["ff", "devour", "--mode", "elasticsearch"])
            .expect("devour elasticsearch alias should parse");
        match devour.command.expect("command") {
            Commands::Devour(args) => assert!(matches!(args.mode, DevourMode::ElasticsearchBulk)),
            other => panic!("expected devour command, got {other:?}"),
        }
    }

    #[test]
    fn clap_parses_devour_defaults() {
        let cli = Cli::try_parse_from(["ff", "devour"]).expect("devour should parse");
        match cli.command.expect("command") {
            Commands::Devour(args) => {
                assert!(matches!(args.mode, DevourMode::Otlp));
                assert!(args.listen.is_none());
                assert!(args.duration_secs.is_none());
            }
            other => panic!("expected devour command, got {other:?}"),
        }
    }

    #[test]
    fn blast_destination_http_is_rejected_by_cli_parser() {
        let err = Cli::try_parse_from([
            "ff",
            "blast",
            "--destination",
            "http",
            "--endpoint",
            "http://127.0.0.1:8080",
        ])
        .expect_err("http destination should be rejected");
        assert_eq!(err.kind(), ErrorKind::InvalidValue);
    }

    #[test]
    fn blast_duration_defaults_to_none() {
        let cli = Cli::try_parse_from([
            "ff",
            "blast",
            "--destination",
            "otlp",
            "--endpoint",
            "http://127.0.0.1:4318/v1/logs",
        ])
        .expect("blast with explicit destination should parse");
        match cli.command.expect("command") {
            Commands::Blast(args) => assert!(args.duration_secs.is_none()),
            other => panic!("expected blast command, got {other:?}"),
        }
    }

    #[test]
    fn blast_arrow_ipc_default_endpoint_is_http_url() {
        assert_eq!(
            BlastDestination::ArrowIpc.default_endpoint(),
            "http://127.0.0.1:18081/v1/arrow"
        );
    }

    #[test]
    fn json_logs_are_used_only_when_stderr_is_not_a_tty() {
        assert!(!use_json_logs_for_stderr(true));
        assert!(use_json_logs_for_stderr(false));
    }

    #[test]
    fn wizard_requires_interactive_stdin_and_stdout() {
        assert!(!wizard_uses_interactive_terminals(false, false));
        assert!(!wizard_uses_interactive_terminals(true, false));
        assert!(!wizard_uses_interactive_terminals(false, true));
        assert!(wizard_uses_interactive_terminals(true, true));
    }
}
