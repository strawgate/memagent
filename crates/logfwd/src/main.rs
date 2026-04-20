#![allow(clippy::print_stdout, clippy::print_stderr)]
// Binary crate: user-facing CLI output is intentional.

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(unix, not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::env;
use std::ffi::OsString;
use std::io::{self, IsTerminal, Write};
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum, error::ErrorKind};
use opentelemetry::metrics::MeterProvider;

mod config_templates;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("LOGFWD_GIT_HASH"),
    env!("LOGFWD_GIT_DIRTY"),
    " ",
    env!("LOGFWD_BUILD_DATE"),
    ", ",
    env!("LOGFWD_TARGET"),
    ", ",
    env!("LOGFWD_PROFILE"),
    ")"
);
const CLI_AFTER_HELP: &str = r"Examples:
  ff run --config config.yaml
  cat app.log | ff
  kubectl logs pod/app | ff --format json --service checkout
  ff validate --config config.yaml
  ff dry-run --config config.yaml
  ff effective-config --config config.yaml
  ff blackhole
  ff blast --destination otlp --endpoint http://127.0.0.1:4318/v1/logs
  ff devour --mode otlp --listen 127.0.0.1:4318
  ff generate-json 10000 test.json
  ff wizard
  ff completions bash

Environment:
  LOGFWD_CONFIG    Config file path (auto-discovered if not set)
  LOGFWD_LOG       Set log filter (for example LOGFWD_LOG=debug)
  RUST_LOG         Fallback if LOGFWD_LOG is not set

Config Search Order:
  1. --config <path>
  2. $LOGFWD_CONFIG
  3. ./logfwd.yaml
  4. ~/.config/logfwd/config.yaml
  5. /etc/logfwd/config.yaml

Exit Codes:
  0 success
  1 configuration error
  2 runtime error";

// Exit codes.
const EXIT_OK: i32 = 0;
const EXIT_CONFIG: i32 = 1;
const EXIT_RUNTIME: i32 = 2;

#[derive(Debug)]
enum CliError {
    Config(String),
    Runtime(io::Error),
}

impl CliError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Config(_) => EXIT_CONFIG,
            Self::Runtime(_) => EXIT_RUNTIME,
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

impl From<logfwd_runtime::bootstrap::RuntimeError> for CliError {
    fn from(value: logfwd_runtime::bootstrap::RuntimeError) -> Self {
        match value {
            logfwd_runtime::bootstrap::RuntimeError::Config(msg) => Self::Config(msg),
            logfwd_runtime::bootstrap::RuntimeError::Io(err) => Self::Runtime(err),
        }
    }
}

// ---------------------------------------------------------------------------
// Color support (respects NO_COLOR, checks stderr TTY)
// ---------------------------------------------------------------------------

fn use_color() -> bool {
    env::var_os("NO_COLOR").is_none() && io::stderr().is_terminal()
}

fn use_json_logs_for_stderr(is_terminal: bool) -> bool {
    !is_terminal
}

fn wizard_uses_interactive_terminals(stdin_is_terminal: bool, stdout_is_terminal: bool) -> bool {
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

fn green() -> &'static str {
    style!("32", "0")
}
fn red() -> &'static str {
    style!("31", "1")
}
fn yellow() -> &'static str {
    style!("33", "0")
}
fn bold() -> &'static str {
    if use_color() { "\x1b[1m" } else { "" }
}
fn dim() -> &'static str {
    if use_color() { "\x1b[2m" } else { "" }
}
fn reset() -> &'static str {
    if use_color() { "\x1b[0m" } else { "" }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CompletionShell {
    Bash,
    Elvish,
    Fish,
    #[value(name = "powershell", alias = "pwsh", alias = "power-shell")]
    PowerShell,
    Zsh,
    Nushell,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BlastDestination {
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
    fn as_output_type(self) -> logfwd_config::OutputType {
        match self {
            Self::Otlp => logfwd_config::OutputType::Otlp,
            Self::Elasticsearch => logfwd_config::OutputType::Elasticsearch,
            Self::Loki => logfwd_config::OutputType::Loki,
            Self::ArrowIpc => logfwd_config::OutputType::ArrowIpc,
            Self::Udp => logfwd_config::OutputType::Udp,
            Self::Tcp => logfwd_config::OutputType::Tcp,
            Self::Null => logfwd_config::OutputType::Null,
        }
    }

    fn parse(input: &str) -> Option<Self> {
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

    fn should_require_endpoint(self) -> bool {
        !matches!(self, Self::Null)
    }

    fn default_endpoint(self) -> &'static str {
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
enum DevourMode {
    #[value(alias = "elasticsearch_otlp")]
    Otlp,
    Http,
    #[value(name = "elasticsearch_bulk", alias = "elasticsearch")]
    ElasticsearchBulk,
    Tcp,
    Udp,
}

impl DevourMode {
    fn default_listen(self) -> &'static str {
        match self {
            Self::Otlp => "127.0.0.1:4318",
            Self::Http => "127.0.0.1:8080",
            Self::ElasticsearchBulk => "127.0.0.1:9200",
            Self::Tcp => "127.0.0.1:15140",
            Self::Udp => "127.0.0.1:15514",
        }
    }

    fn target_hint(self, listen: &str) -> String {
        match self {
            Self::Otlp => format!("http://{listen}/v1/logs"),
            Self::Http => format!("http://{listen}/"),
            Self::ElasticsearchBulk => format!("http://{listen}/_bulk"),
            Self::Tcp | Self::Udp => listen.to_string(),
        }
    }

    fn spec(self) -> logfwd_runtime::generated_cli::DevourModeSpec {
        match self {
            Self::Otlp => logfwd_runtime::generated_cli::DevourModeSpec::Otlp,
            Self::Http => logfwd_runtime::generated_cli::DevourModeSpec::Http,
            Self::ElasticsearchBulk => {
                logfwd_runtime::generated_cli::DevourModeSpec::ElasticsearchBulk
            }
            Self::Tcp => logfwd_runtime::generated_cli::DevourModeSpec::Tcp,
            Self::Udp => logfwd_runtime::generated_cli::DevourModeSpec::Udp,
        }
    }
}

#[derive(Debug, Clone, Args)]
struct BlastArgs {
    /// Destination type to blast.
    #[arg(long, value_enum)]
    destination: Option<BlastDestination>,

    /// Destination endpoint URL/address (required for all destinations except `null`).
    #[arg(long)]
    endpoint: Option<String>,

    /// Bearer token auth.
    #[arg(long)]
    auth_bearer_token: Option<String>,

    /// Extra header, repeatable: --auth-header 'Authorization=ApiKey xyz'.
    #[arg(long, value_name = "KEY=VALUE")]
    auth_header: Vec<String>,

    /// Worker count.
    #[arg(long, default_value_t = 2)]
    workers: usize,

    /// Lines per generated batch.
    #[arg(long, default_value_t = 5_000)]
    batch_lines: usize,

    /// Optional run duration in seconds (default: run until stopped).
    #[arg(long)]
    duration_secs: Option<u64>,

    /// Diagnostics server bind address for generated config.
    #[arg(long, default_value = "127.0.0.1:0")]
    diagnostics_addr: String,
}

#[derive(Debug, Clone, Args)]
struct DevourArgs {
    /// Receiver mode to emulate.
    #[arg(long, value_enum, default_value_t = DevourMode::Otlp)]
    mode: DevourMode,

    /// Bind address (defaults depend on mode).
    #[arg(long)]
    listen: Option<String>,

    /// Optional run duration in seconds (default: run until stopped).
    #[arg(long)]
    duration_secs: Option<u64>,

    /// Diagnostics server bind address for generated config.
    #[arg(long, default_value = "127.0.0.1:0")]
    diagnostics_addr: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SendFormat {
    Auto,
    Cri,
    Json,
    Raw,
}

impl SendFormat {
    fn as_config_format(self) -> logfwd_config::Format {
        match self {
            Self::Auto => logfwd_config::Format::Auto,
            Self::Cri => logfwd_config::Format::Cri,
            Self::Json => logfwd_config::Format::Json,
            Self::Raw => logfwd_config::Format::Raw,
        }
    }
}

#[derive(Debug, Clone, Args, Default)]
struct SendArgs {
    #[arg(
        short = 'c',
        long = "config",
        value_name = "FILE",
        help = "Destination YAML config file"
    )]
    config: Option<String>,

    /// Input format for stdin.
    #[arg(long, value_enum)]
    format: Option<SendFormat>,

    /// Set `service.name` on emitted records.
    #[arg(long)]
    service: Option<String>,

    /// Add or override a resource attribute, repeatable: --resource key=value.
    #[arg(long, value_name = "KEY=VALUE")]
    resource: Vec<String>,
}

#[derive(Debug, Parser)]
#[command(
    name = "ff",
    about = "Fast log forwarder with SQL transforms",
    long_about = "Fast log forwarder with SQL transforms",
    version = VERSION,
    long_version = LONG_VERSION,
    next_line_help = true,
    after_help = CLI_AFTER_HELP
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
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
    /// Generate starter config in current directory (logfwd.yaml).
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
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let code = main_inner();
    if code != 0 {
        std::process::exit(code);
    }
}

#[tokio::main]
async fn main_inner() -> i32 {
    let stdin_is_terminal = io::stdin().is_terminal();
    let cli = match parse_cli_from(env::args_os(), stdin_is_terminal) {
        Ok(cli) => cli,
        Err(err) => {
            let code = match err.kind() {
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => EXIT_OK,
                _ => EXIT_CONFIG,
            };
            let _ = err.print();
            return code;
        }
    };

    let result = if let Some(command) = cli.command {
        run_command(command).await
    } else if !stdin_is_terminal {
        run_command(Commands::Send(SendArgs::default())).await
    } else {
        if let Some(path) = discover_config() {
            eprintln!(
                "{}hint{}: found config at {}{}{}",
                dim(),
                reset(),
                bold(),
                path.display(),
                reset(),
            );
            eprintln!(
                "{}      run {}ff run --config {}{}",
                dim(),
                bold(),
                path.display(),
                reset()
            );
            eprintln!();
        } else {
            eprintln!(
                "{}hint{}: no config found — try {}ff init{} or {}ff wizard{} to get started",
                dim(),
                reset(),
                bold(),
                reset(),
                bold(),
                reset(),
            );
            eprintln!();
        }
        let mut cmd = Cli::command();
        let _ = cmd.print_help();
        println!();
        return EXIT_OK;
    };

    match result {
        Ok(()) => EXIT_OK,
        Err(e) => {
            eprintln!("{}error{}: {e}", red(), reset());
            e.exit_code()
        }
    }
}

async fn run_command(command: Commands) -> Result<(), CliError> {
    match command {
        Commands::Run { config } => {
            let config_path = resolve_config_path(config.as_deref())?;
            cmd_run(&config_path, false, false).await
        }
        Commands::Validate { config } => {
            let config_path = resolve_config_path(config.as_deref())?;
            cmd_run(&config_path, true, false).await
        }
        Commands::DryRun { config } => {
            let config_path = resolve_config_path(config.as_deref())?;
            cmd_run(&config_path, false, true).await
        }
        Commands::Send(args) => cmd_send(args).await,
        Commands::Blast(args) => cmd_blast(args).await,
        Commands::Devour(args) => cmd_devour(args).await,
        Commands::Blackhole { bind_addr } => cmd_blackhole(bind_addr.as_deref()).await,
        Commands::GenerateJson {
            num_lines,
            output_file,
        } => cmd_generate_json(num_lines, &output_file),
        Commands::EffectiveConfig { config } => cmd_effective_config(config.as_deref()),
        Commands::Init => cmd_init(),
        Commands::Wizard => cmd_wizard(),
        Commands::Completions { shell } => {
            cmd_completions(shell);
            Ok(())
        }
    }
}

fn parse_cli_from<I, T>(args: I, stdin_is_terminal: bool) -> Result<Cli, clap::Error>
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

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn cmd_run(config_path: &str, validate_only: bool, dry_run: bool) -> Result<(), CliError> {
    let config_yaml = std::fs::read_to_string(config_path)
        .map_err(|e| CliError::Config(format!("cannot read {config_path}: {e}")))?;
    let base_path = std::path::Path::new(config_path).parent();
    let config = match logfwd_config::Config::load_str_with_base_path(&config_yaml, base_path) {
        Ok(c) => c,
        Err(e) => {
            return Err(CliError::Config(e.to_string()));
        }
    };

    if validate_only || dry_run {
        // Both `validate` and `dry-run` build pipelines to catch SQL/wiring errors.
        return validate_pipelines(&config, dry_run, base_path);
    }

    run_pipelines(config, base_path, config_path, &config_yaml, None).await
}

async fn cmd_send(args: SendArgs) -> Result<(), CliError> {
    if io::stdin().is_terminal() {
        return Err(CliError::Config(
            "stdin is interactive; pipe data into `ff` or use `ff run --config <file>` for daemon mode"
                .to_owned(),
        ));
    }

    let config_path = resolve_send_config_path(args.config.as_deref())?;
    let config_yaml = std::fs::read_to_string(&config_path)
        .map_err(|e| CliError::Config(format!("cannot read {config_path}: {e}")))?;
    let runnable_yaml = build_stdin_send_config_yaml(
        &config_yaml,
        args.format,
        args.service.as_deref(),
        &args.resource,
    )?;
    let base_path = std::path::Path::new(&config_path).parent();
    let config = logfwd_config::Config::load_str_with_base_path(&runnable_yaml, base_path)
        .map_err(|e| CliError::Config(e.to_string()))?;

    run_pipelines(config, base_path, &config_path, &runnable_yaml, None).await
}

fn build_stdin_send_config_yaml(
    config_yaml: &str,
    format: Option<SendFormat>,
    service: Option<&str>,
    resources: &[String],
) -> Result<String, CliError> {
    let mut value: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(config_yaml).map_err(|e| CliError::Config(e.to_string()))?;
    let mapping = value.as_mapping_mut().ok_or_else(|| {
        CliError::Config("send destination config must be a YAML mapping".to_owned())
    })?;

    for unsupported in ["input", "pipelines"] {
        if mapping.contains_key(yaml_string(unsupported)) {
            return Err(CliError::Config(format!(
                "`ff send` expects a destination-only config; remove top-level `{unsupported}` or use `ff run`"
            )));
        }
    }
    let has_output = mapping.contains_key(yaml_string("output"));
    let has_outputs = mapping.contains_key(yaml_string("outputs"));
    if has_output && has_outputs {
        return Err(CliError::Config(
            "`ff send` destination config must define only one of top-level `output` or `outputs`"
                .to_owned(),
        ));
    }
    if !has_output && !has_outputs {
        return Err(CliError::Config(
            "`ff send` destination config must define top-level `output` or `outputs`".to_owned(),
        ));
    }

    let mut input = serde_yaml_ng::Mapping::new();
    input.insert(yaml_string("type"), yaml_string("stdin"));
    if let Some(format) = format {
        let format = format.as_config_format().to_string();
        input.insert(yaml_string("format"), yaml_string(&format));
    }

    merge_send_resource_attrs(mapping, service, resources)?;

    if has_outputs {
        rewrite_send_config_with_outputs(mapping, input);
    } else {
        mapping.insert(yaml_string("input"), serde_yaml_ng::Value::Mapping(input));
    }

    serde_yaml_ng::to_string(&value).map_err(|e| CliError::Config(e.to_string()))
}

fn rewrite_send_config_with_outputs(
    mapping: &mut serde_yaml_ng::Mapping,
    input: serde_yaml_ng::Mapping,
) {
    let outputs = mapping
        .remove(yaml_string("outputs"))
        .expect("internal invariant violated: `ff send` rewrite requires top-level `outputs`");

    let mut pipeline = serde_yaml_ng::Mapping::new();
    pipeline.insert(
        yaml_string("inputs"),
        serde_yaml_ng::Value::Sequence(vec![serde_yaml_ng::Value::Mapping(input)]),
    );
    pipeline.insert(yaml_string("outputs"), outputs);

    for key in ["transform", "enrichment", "resource_attrs"] {
        if let Some(value) = mapping.remove(yaml_string(key)) {
            pipeline.insert(yaml_string(key), value);
        }
    }

    let mut pipelines = serde_yaml_ng::Mapping::new();
    pipelines.insert(
        yaml_string("default"),
        serde_yaml_ng::Value::Mapping(pipeline),
    );
    mapping.insert(
        yaml_string("pipelines"),
        serde_yaml_ng::Value::Mapping(pipelines),
    );
}

fn merge_send_resource_attrs(
    mapping: &mut serde_yaml_ng::Mapping,
    service: Option<&str>,
    resources: &[String],
) -> Result<(), CliError> {
    if service.is_none() && resources.is_empty() {
        return Ok(());
    }

    let key = yaml_string("resource_attrs");
    if !mapping.contains_key(&key) {
        mapping.insert(
            key.clone(),
            serde_yaml_ng::Value::Mapping(serde_yaml_ng::Mapping::new()),
        );
    }
    let attrs = mapping
        .get_mut(&key)
        .and_then(serde_yaml_ng::Value::as_mapping_mut)
        .ok_or_else(|| {
            CliError::Config(
                "top-level `resource_attrs` must be a mapping when using send overrides".to_owned(),
            )
        })?;

    if let Some(service) = service {
        attrs.insert(yaml_string("service.name"), yaml_string(service));
    }
    for raw in resources {
        let (name, value) = parse_key_value(raw, "--resource")?;
        attrs.insert(yaml_string(name), yaml_string(value));
    }

    Ok(())
}

fn parse_key_value<'a>(raw: &'a str, flag: &str) -> Result<(&'a str, &'a str), CliError> {
    let (name, value) = raw.split_once('=').ok_or_else(|| {
        CliError::Config(format!("{flag} must be in KEY=VALUE form, got `{raw}`"))
    })?;
    let name = name.trim();
    let value = value.trim();
    if name.is_empty() {
        return Err(CliError::Config(format!(
            "{flag} key must not be empty, got `{raw}`"
        )));
    }
    Ok((name, value))
}

fn yaml_string(value: &str) -> serde_yaml_ng::Value {
    serde_yaml_ng::Value::String(value.to_owned())
}

fn resolve_send_config_path(config_path: Option<&str>) -> Result<String, CliError> {
    resolve_config_path(config_path).map_err(|err| match err {
        CliError::Config(_) => CliError::Config(
            "no destination config file found (use `ff send --config <file>` or set LOGFWD_CONFIG)"
                .to_owned(),
        ),
        CliError::Runtime(e) => CliError::Runtime(e),
    })
}

async fn cmd_blast(mut args: BlastArgs) -> Result<(), CliError> {
    maybe_prompt_blast_setup(&mut args)?;

    if args.duration_secs == Some(0) {
        return Err(CliError::Config(
            "--duration-secs must be at least 1 when provided".to_owned(),
        ));
    }
    if args.workers == 0 {
        return Err(CliError::Config("--workers must be at least 1".to_owned()));
    }
    if args.batch_lines == 0 {
        return Err(CliError::Config(
            "--batch-lines must be at least 1".to_owned(),
        ));
    }
    logfwd_config::validate_host_port(&args.diagnostics_addr)
        .map_err(|e| CliError::Config(format!("invalid diagnostics address: {e}")))?;

    let destination = args
        .destination
        .ok_or_else(|| CliError::Config("blast requires --destination".to_owned()))?;
    let generated = logfwd_runtime::generated_cli::build_blast_generated_config(
        destination.as_output_type(),
        args.endpoint.as_deref(),
        args.auth_bearer_token.as_deref(),
        &args.auth_header,
        args.workers,
        args.batch_lines,
        &args.diagnostics_addr,
    )
    .map_err(CliError::Config)?;

    println!("Starting blast (runtime pipeline shortcut)...");
    println!(
        "destination={} workers={} batch_lines={}",
        destination.as_output_type(),
        args.workers,
        args.batch_lines
    );
    match args.duration_secs {
        Some(duration) => println!("duration={}s", duration),
        None => println!("duration=until-stopped (Ctrl-C)"),
    }
    if let Some(endpoint) = &args.endpoint {
        println!("endpoint={endpoint}");
    }
    println!("diagnostics={}", args.diagnostics_addr);

    let auto_shutdown_after = args.duration_secs.map(Duration::from_secs);
    run_pipelines(
        generated.config,
        None,
        "<blast-generated>",
        &generated.yaml,
        auto_shutdown_after,
    )
    .await
}

async fn cmd_devour(args: DevourArgs) -> Result<(), CliError> {
    let listen = args
        .listen
        .clone()
        .unwrap_or_else(|| args.mode.default_listen().to_string());

    logfwd_config::validate_host_port(&listen)
        .map_err(|e| CliError::Config(format!("invalid listen address: {e}")))?;
    logfwd_config::validate_host_port(&args.diagnostics_addr)
        .map_err(|e| CliError::Config(format!("invalid diagnostics address: {e}")))?;

    if args.duration_secs == Some(0) {
        return Err(CliError::Config(
            "--duration-secs must be at least 1 when provided".to_owned(),
        ));
    }

    println!(
        "devour mode={:?} listening={} target={}",
        args.mode,
        listen,
        args.mode.target_hint(&listen)
    );
    println!("dropping all received data (blackhole mode)");
    println!("diagnostics={}", args.diagnostics_addr);
    if let Some(duration) = args.duration_secs {
        println!("duration={}s", duration);
    }

    let generated = logfwd_runtime::generated_cli::build_devour_generated_config(
        args.mode.spec(),
        &listen,
        &args.diagnostics_addr,
    )
    .map_err(CliError::Config)?;

    let auto_shutdown_after = args.duration_secs.map(Duration::from_secs);
    run_pipelines(
        generated.config,
        None,
        "<devour-generated>",
        &generated.yaml,
        auto_shutdown_after,
    )
    .await
}

async fn cmd_blackhole(bind_addr: Option<&str>) -> Result<(), CliError> {
    let args = DevourArgs {
        mode: DevourMode::Otlp,
        listen: bind_addr.map(ToOwned::to_owned),
        duration_secs: None,
        diagnostics_addr: "127.0.0.1:0".to_string(),
    };
    cmd_devour(args).await
}

fn maybe_prompt_blast_setup(args: &mut BlastArgs) -> Result<(), CliError> {
    if args.destination.is_some() {
        return Ok(());
    }

    if !wizard_uses_interactive_terminals(io::stdin().is_terminal(), io::stdout().is_terminal()) {
        return Err(CliError::Config(
            "no destination provided. Pass --destination (wizard requires interactive stdin/stdout)"
                .to_owned(),
        ));
    }

    println!("blast quick setup");
    println!("Press Enter to accept defaults.");
    println!("Leave duration blank to run until Ctrl-C.");

    let destination_raw = prompt_text(
        "Destination [otlp/elasticsearch_otlp/elasticsearch/elasticsearch_bulk/loki/arrow_ipc/udp/tcp/null]",
        "otlp",
    )?;
    let destination = BlastDestination::parse(&destination_raw).ok_or_else(|| {
        CliError::Config(format!(
            "unknown destination '{destination_raw}' (expected otlp/elasticsearch_otlp/elasticsearch/elasticsearch_bulk/loki/arrow_ipc/udp/tcp/null)"
        ))
    })?;
    args.destination = Some(destination);

    if destination.should_require_endpoint() && args.endpoint.is_none() {
        let endpoint = prompt_text("Endpoint", destination.default_endpoint())?;
        args.endpoint = Some(endpoint);
    }

    if args.auth_bearer_token.is_none() {
        println!("Bearer token prompt skipped in wizard; pass --auth-bearer-token if needed.");
    }

    args.workers = prompt_text("Workers", &args.workers.to_string())?
        .parse::<usize>()
        .map_err(|_| CliError::Config("Workers must be a positive integer".to_owned()))?;
    args.batch_lines = prompt_text("Batch lines", &args.batch_lines.to_string())?
        .parse::<usize>()
        .map_err(|_| CliError::Config("Batch lines must be a positive integer".to_owned()))?;
    let duration_default = args
        .duration_secs
        .map(|v| v.to_string())
        .unwrap_or_default();
    let duration_raw = prompt_text(
        "Duration seconds (optional, blank=run until Ctrl-C)",
        &duration_default,
    )?;
    args.duration_secs = if duration_raw.trim().is_empty() {
        None
    } else {
        Some(duration_raw.parse::<u64>().map_err(|_| {
            CliError::Config("Duration seconds must be a positive integer".to_owned())
        })?)
    };

    Ok(())
}

#[cfg(test)]
fn resolve_blast_output_config(
    args: &BlastArgs,
) -> Result<logfwd_config::OutputConfigV2, CliError> {
    let destination = args
        .destination
        .ok_or_else(|| CliError::Config("blast requires --destination".to_owned()))?;
    logfwd_runtime::generated_cli::resolve_blast_output_config(
        destination.as_output_type(),
        args.endpoint.as_deref(),
        args.auth_bearer_token.as_deref(),
        &args.auth_header,
    )
    .map_err(CliError::Config)
}

#[cfg(test)]
fn render_devour_yaml(args: &DevourArgs, listen: &str) -> String {
    logfwd_runtime::generated_cli::render_devour_yaml(
        args.mode.spec(),
        listen,
        &args.diagnostics_addr,
    )
}

#[cfg(test)]
fn yaml_quote(value: &str) -> String {
    logfwd_runtime::generated_cli::yaml_quote(value)
}

fn cmd_generate_json(num_lines: usize, output_file: &str) -> Result<(), CliError> {
    generate_json_log_file(num_lines, output_file).map_err(CliError::Runtime)
}

fn cmd_effective_config(config_path: Option<&str>) -> Result<(), CliError> {
    let config_path = resolve_config_path(config_path)?;

    let config_yaml = std::fs::read_to_string(&config_path)
        .map_err(|e| CliError::Config(format!("cannot read {config_path}: {e}")))?;
    let effective_yaml = logfwd_config::Config::expand_env_yaml_str(&config_yaml)
        .map_err(|e| CliError::Config(e.to_string()))?;

    // Read-only validation for inspection flows: reject configs that would
    // fail format or SQL-plan checks without constructing runtime inputs that
    // bind sockets or touch long-lived resources.
    let base_path = std::path::Path::new(&config_path).parent();
    let config = logfwd_config::Config::load_str_with_base_path(&config_yaml, base_path)
        .map_err(|e| CliError::Config(e.to_string()))?;
    validate_pipelines_read_only(
        &config,
        base_path,
        |_name| {},
        |err| eprintln!("  {}error{}: {err}", red(), reset()),
    )?;

    let redacted_yaml = logfwd_diagnostics::diagnostics::redact_config_yaml(&effective_yaml);
    eprintln!("{}# validated from {config_path}{}", dim(), reset());
    print!("{redacted_yaml}");
    Ok(())
}

fn cmd_init() -> Result<(), CliError> {
    let path = std::path::Path::new("logfwd.yaml");

    let template = r#"# ff configuration
# Docs: https://github.com/strawgate/memagent

# ── Quick start ─────────────────────────────────────────────
# 1. Generate sample data:  ff generate-json 10000 sample.json
# 2. Validate this config:  ff validate --config logfwd.yaml
# 3. Run the pipeline:      ff run --config logfwd.yaml
#
# For production, change the input path and output to your real
# source/destination — see the examples at https://github.com/strawgate/memagent/tree/main/examples/use-cases/
# Or run `ff wizard` for an interactive setup.

# Tail a JSON log file and stream new lines as they appear.
input:
  type: file
  path: ./sample.json
  format: json

# SQL transform (optional) — filter, reshape, or enrich logs.
# Remove this section to forward all logs unmodified.
transform: |
  SELECT * FROM logs
  WHERE level IN ('WARN', 'ERROR')

# Print matching logs to the terminal so you can see results immediately.
output:
  type: stdout

# Optional: expose a diagnostics dashboard on http://127.0.0.1:9191
# server:
#   diagnostics: "127.0.0.1:9191"
"#;

    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .and_then(|mut f| f.write_all(template.as_bytes()))
        .map_err(|e| {
            if e.kind() == io::ErrorKind::AlreadyExists {
                CliError::Config(format!(
                    "{} already exists — refusing to overwrite",
                    path.display()
                ))
            } else {
                CliError::Config(format!("cannot write {}: {e}", path.display()))
            }
        })?;
    eprintln!("{}created{} {}", green(), reset(), path.display());
    eprintln!();
    eprintln!("{}Try it now:{}", bold(), reset());
    eprintln!(
        "  ff generate-json 10000 sample.json   {}# create sample data{}",
        dim(),
        reset()
    );
    eprintln!(
        "  ff run --config logfwd.yaml           {}# run the pipeline{}",
        dim(),
        reset()
    );
    Ok(())
}

fn cmd_wizard() -> Result<(), CliError> {
    use config_templates::{
        INPUT_TEMPLATES, OUTPUT_TEMPLATES, USE_CASE_TEMPLATES, render_config, render_use_case,
    };

    if !wizard_uses_interactive_terminals(io::stdin().is_terminal(), io::stdout().is_terminal()) {
        return Err(CliError::Config(
            "wizard requires an interactive terminal on stdin and stdout".to_owned(),
        ));
    }
    println!("{}ff config wizard{}", bold(), reset());
    println!();

    // Step 1: Choose between a use-case preset or custom input/output.
    let mode = prompt_select(
        "How do you want to get started?",
        &[
            "Start from a use-case preset",
            "Pick input and output manually",
        ],
    )?;

    let cfg = if mode == 0 {
        // Use-case mode.
        let labels: Vec<&str> = USE_CASE_TEMPLATES.iter().map(|t| t.title).collect();
        let descs: Vec<&str> = USE_CASE_TEMPLATES.iter().map(|t| t.description).collect();
        let uc_idx = prompt_select_described("Pick a scenario:", &labels, &descs)?;
        let uc = &USE_CASE_TEMPLATES[uc_idx];
        println!("{}selected{}: {}", green(), reset(), uc.title);
        println!();
        let sql = prompt_text(
            "SQL transform (blank = keep the preset default)",
            uc.transform,
        )?;
        let sql_final = if sql.trim().is_empty() {
            uc.transform
        } else {
            sql.trim()
        };
        render_use_case(uc, sql_final)
    } else {
        // Manual input/output mode.
        let input_labels: Vec<&str> = INPUT_TEMPLATES.iter().map(|t| t.label).collect();
        let input_descs: Vec<&str> = INPUT_TEMPLATES.iter().map(|t| t.description).collect();
        let input_idx =
            prompt_select_described("What do you want to collect?", &input_labels, &input_descs)?;
        let output_labels: Vec<&str> = OUTPUT_TEMPLATES.iter().map(|t| t.label).collect();
        let output_descs: Vec<&str> = OUTPUT_TEMPLATES.iter().map(|t| t.description).collect();
        let output_idx = prompt_select_described(
            "Where do you want to send logs?",
            &output_labels,
            &output_descs,
        )?;

        let sql = prompt_text(
            "Optional SQL transform (blank = SELECT * FROM logs)",
            "SELECT * FROM logs",
        )?;
        let sql = if sql.trim().is_empty() {
            "SELECT * FROM logs"
        } else {
            sql.trim()
        };

        let input = &INPUT_TEMPLATES[input_idx];
        let output = &OUTPUT_TEMPLATES[output_idx];
        render_config(input, output, sql)
    };

    let output_path = prompt_text("Output file path", "logfwd.generated.yaml")?;
    let path = std::path::PathBuf::from(output_path.trim());
    validate_generated_config_read_only(&cfg, &path)?;

    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .and_then(|mut f| f.write_all(cfg.as_bytes()))
        .map_err(|e| {
            if e.kind() == io::ErrorKind::AlreadyExists {
                CliError::Config(format!(
                    "{} already exists — refusing to overwrite",
                    path.display()
                ))
            } else {
                CliError::Config(format!("cannot write {}: {e}", path.display()))
            }
        })?;

    eprintln!("{}created{} {}", green(), reset(), path.as_path().display());
    eprintln!(
        "{}next{}: run {}ff validate --config {}{}",
        dim(),
        reset(),
        bold(),
        path.display(),
        reset()
    );
    Ok(())
}

fn validate_generated_config_read_only(
    config_yaml: &str,
    output_path: &std::path::Path,
) -> Result<(), CliError> {
    let base_path = output_path.parent();
    let config = logfwd_config::Config::load_str_with_base_path(config_yaml, base_path)
        .map_err(|e| CliError::Config(e.to_string()))?;
    let mut validation_errors = Vec::new();
    let result = validate_pipelines_read_only(
        &config,
        base_path,
        |_name| {},
        |err| validation_errors.push(err),
    );
    match result {
        Ok(()) => Ok(()),
        Err(_) if validation_errors.is_empty() => Err(CliError::Config(
            "generated config failed validation".to_owned(),
        )),
        Err(_) => Err(CliError::Config(format!(
            "generated config failed validation:\n{}",
            validation_errors.join("\n")
        ))),
    }
}

fn prompt_select(prompt: &str, options: &[&str]) -> Result<usize, CliError> {
    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut stdin = stdin.lock();
    loop {
        println!("{prompt}");
        for (i, option) in options.iter().enumerate() {
            println!("  {}) {option}", i + 1);
        }
        print!("Enter choice [1-{}]: ", options.len());
        stdout.flush()?;

        let line = read_wizard_line(&mut stdin)?;
        let trimmed = line.trim();
        if let Ok(v) = trimmed.parse::<usize>()
            && (1..=options.len()).contains(&v)
        {
            println!();
            return Ok(v - 1);
        }
        eprintln!(
            "{}invalid choice{}: enter a number from 1 to {}",
            yellow(),
            reset(),
            options.len()
        );
    }
}

/// Like [`prompt_select`] but shows a one-line description below each option.
fn prompt_select_described(
    prompt: &str,
    options: &[&str],
    descriptions: &[&str],
) -> Result<usize, CliError> {
    debug_assert_eq!(
        descriptions.len(),
        options.len(),
        "options/descriptions length mismatch"
    );
    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut stdin = stdin.lock();
    loop {
        println!("{prompt}");
        for (i, option) in options.iter().enumerate() {
            println!("  {}{}) {option}{}", bold(), i + 1, reset());
            if let Some(desc) = descriptions.get(i)
                && !desc.is_empty()
            {
                println!("     {}{desc}{}", dim(), reset());
            }
        }
        print!("Enter choice [1-{}]: ", options.len());
        stdout.flush()?;

        let line = read_wizard_line(&mut stdin)?;
        let trimmed = line.trim();
        if let Ok(v) = trimmed.parse::<usize>()
            && (1..=options.len()).contains(&v)
        {
            println!();
            return Ok(v - 1);
        }
        eprintln!(
            "{}invalid choice{}: enter a number from 1 to {}",
            yellow(),
            reset(),
            options.len()
        );
    }
}

fn prompt_text(prompt: &str, default: &str) -> Result<String, CliError> {
    let mut stdout = io::stdout();
    print!("{prompt} [{default}]: ");
    stdout.flush()?;
    let stdin = io::stdin();
    let mut stdin = stdin.lock();
    let line = read_wizard_line(&mut stdin)?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        Ok(default.to_owned())
    } else {
        Ok(trimmed.to_owned())
    }
}

fn read_wizard_line<R: io::BufRead>(reader: &mut R) -> Result<String, CliError> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(CliError::Config(
            "stdin closed while reading wizard input".to_owned(),
        ));
    }
    Ok(line)
}

fn cmd_completions(shell: CompletionShell) {
    let mut cmd = Cli::command();
    let mut stdout = io::stdout();

    match shell {
        CompletionShell::Bash => {
            clap_complete::generate(clap_complete::Shell::Bash, &mut cmd, "ff", &mut stdout);
        }
        CompletionShell::Elvish => {
            clap_complete::generate(clap_complete::Shell::Elvish, &mut cmd, "ff", &mut stdout);
        }
        CompletionShell::Fish => {
            clap_complete::generate(clap_complete::Shell::Fish, &mut cmd, "ff", &mut stdout);
        }
        CompletionShell::PowerShell => {
            clap_complete::generate(
                clap_complete::Shell::PowerShell,
                &mut cmd,
                "ff",
                &mut stdout,
            );
        }
        CompletionShell::Zsh => {
            clap_complete::generate(clap_complete::Shell::Zsh, &mut cmd, "ff", &mut stdout);
        }
        CompletionShell::Nushell => {
            clap_complete::generate(clap_complete_nushell::Nushell, &mut cmd, "ff", &mut stdout);
        }
    }
}

fn resolve_config_path(config_path: Option<&str>) -> Result<String, CliError> {
    if let Some(path) = config_path {
        return Ok(path.to_owned());
    }
    if let Some(path) = discover_config() {
        return Ok(path.to_string_lossy().to_string());
    }
    Err(CliError::Config(
        "no config file found (use --config or set LOGFWD_CONFIG)".to_owned(),
    ))
}

// ---------------------------------------------------------------------------
// Config auto-discovery
// ---------------------------------------------------------------------------

/// Search well-known locations for a logfwd config file.
///
/// Search order:
///   1. `$LOGFWD_CONFIG`
///   2. `./logfwd.yaml`
///   3. `~/.config/logfwd/config.yaml`
///   4. `/etc/logfwd/config.yaml`
fn discover_config() -> Option<std::path::PathBuf> {
    use std::path::PathBuf;

    // 1. Environment variable
    if let Ok(path) = env::var("LOGFWD_CONFIG") {
        let p = PathBuf::from(&path);
        if p.is_file() {
            return Some(p);
        }
    }

    // 2. Current directory
    let cwd = PathBuf::from("logfwd.yaml");
    if cwd.is_file() {
        return Some(cwd);
    }

    // 3. XDG config dir
    let xdg_base = env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .ok()
        .or_else(|| {
            env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".config"))
        });
    if let Some(xdg) = xdg_base.map(|b| b.join("logfwd/config.yaml"))
        && xdg.is_file()
    {
        return Some(xdg);
    }

    // 4. System config
    let etc = PathBuf::from("/etc/logfwd/config.yaml");
    if etc.is_file() {
        return Some(etc);
    }

    None
}

// ---------------------------------------------------------------------------
// Pipeline runner
// ---------------------------------------------------------------------------

/// Validate config by building all pipelines. Used by `validate` and `dry-run`.
fn validate_pipelines_inner<FReady, FError>(
    config: &logfwd_config::Config,
    base_path: Option<&std::path::Path>,
    mut on_ready: FReady,
    mut on_error: FError,
) -> Result<(), CliError>
where
    FReady: FnMut(&str),
    FError: FnMut(String),
{
    use logfwd::pipeline::Pipeline;

    // Build a no-op meter for validation (no OTel export needed).
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
    let meter = meter_provider.meter("logfwd");

    let mut errors = 0;
    for (name, pipe_cfg) in &config.pipelines {
        if let Err(err) = validate_pipeline_read_only(pipe_cfg, base_path) {
            on_error(format!("pipeline '{name}': {err}"));
            errors += 1;
            continue;
        }

        match Pipeline::from_config(name, pipe_cfg, &meter, base_path) {
            Ok(mut pipeline) => {
                // Execute a probe batch through the SQL plan to catch planning
                // errors (duplicate aliases, bad window specs) that only
                // surface on the first real batch at runtime.
                if let Err(e) = pipeline.validate_sql_plan() {
                    on_error(format!("pipeline '{name}' SQL plan: {e}"));
                    errors += 1;
                    continue;
                }
                on_ready(name);
            }
            Err(e) => {
                on_error(format!("pipeline '{name}': {e}"));
                errors += 1;
            }
        }
    }

    if errors > 0 {
        return Err(CliError::Config(format!(
            "{errors} error(s) during validation"
        )));
    }

    Ok(())
}

fn validate_pipelines_read_only<FReady, FError>(
    config: &logfwd_config::Config,
    base_path: Option<&std::path::Path>,
    mut on_ready: FReady,
    mut on_error: FError,
) -> Result<(), CliError>
where
    FReady: FnMut(&str),
    FError: FnMut(String),
{
    let mut errors = 0;
    for (name, pipe_cfg) in &config.pipelines {
        match validate_pipeline_read_only(pipe_cfg, base_path) {
            Ok(()) => on_ready(name),
            Err(err) => {
                on_error(format!("pipeline '{name}': {err}"));
                errors += 1;
            }
        }
    }

    if errors > 0 {
        return Err(CliError::Config(format!(
            "{errors} error(s) during validation"
        )));
    }

    Ok(())
}

fn validate_transform_probe_read_only(
    transform: &mut logfwd::transform::SqlTransform,
) -> Result<(), String> {
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let scan_config = transform.scan_config();
    let fields: Vec<Field> = if scan_config.extract_all || scan_config.wanted_fields.is_empty() {
        vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("msg", DataType::Utf8, true),
        ]
    } else {
        scan_config
            .wanted_fields
            .iter()
            .map(|field| Field::new(field.name.as_str(), DataType::Utf8, true))
            .collect()
    };

    let schema = Arc::new(Schema::new(fields.clone()));
    let arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|_| Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("failed to build probe batch: {e}"))?;
    transform
        .execute_blocking(batch)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

const GENERATOR_LOGS_SIMPLE_COLUMNS: &[&str] = &[
    "timestamp",
    "level",
    "message",
    "duration_ms",
    "request_id",
    "service",
    "status",
];

/// Internal columns injected by the CRI format decoder (`_timestamp`,
/// `_stream`) plus the plain-text fallback field (`body`).
const CRI_INTERNAL_COLUMNS: &[&str] = &["_timestamp", "_stream", "body"];

/// Describes how strictly column validation should be applied.
enum KnownColumnsMode {
    /// All columns in the input are known — any column not in the list is
    /// rejected. Used for inputs with a fixed schema (e.g. generator/logs/simple).
    Strict(&'static [&'static str]),
    /// Only internal/framework columns (typically `_`-prefixed) are known.
    /// Columns starting with `_` that are not in the list are rejected;
    /// other column names are assumed to be user-defined data fields and
    /// allowed. Used for inputs that parse dynamic JSON content (CRI, JSON).
    InternalOnly(&'static [&'static str]),
}

fn known_input_columns_read_only(
    input_cfg: &logfwd_config::InputConfig,
) -> Option<KnownColumnsMode> {
    use logfwd_config::{
        Format, GeneratorComplexityConfig, GeneratorProfileConfig, InputTypeConfig,
    };

    match &input_cfg.type_config {
        // Generator logs/simple has a stable built-in schema. Other profiles
        // or complexities are broader or user-defined, so skip strict checks.
        InputTypeConfig::Generator(generator_cfg) => {
            let is_logs_profile = generator_cfg
                .generator
                .as_ref()
                .and_then(|cfg| cfg.profile.as_ref())
                .is_none_or(|profile| matches!(profile, GeneratorProfileConfig::Logs));
            let is_simple_complexity = generator_cfg
                .generator
                .as_ref()
                .and_then(|cfg| cfg.complexity.as_ref())
                .is_none_or(|complexity| matches!(complexity, GeneratorComplexityConfig::Simple));
            if !is_logs_profile || !is_simple_complexity {
                None
            } else {
                Some(KnownColumnsMode::Strict(GENERATOR_LOGS_SIMPLE_COLUMNS))
            }
        }
        // File/stdin inputs with an explicit CRI format have known internal
        // columns injected by the format decoder. JSON body keys are dynamic,
        // so we only validate `_`-prefixed names.
        InputTypeConfig::File(_) | InputTypeConfig::Stdin(_) => {
            match input_cfg.format.as_ref() {
                Some(Format::Cri) => Some(KnownColumnsMode::InternalOnly(CRI_INTERNAL_COLUMNS)),
                // JSON and raw formats have fully dynamic schemas — no
                // internal columns to validate.
                _ => None,
            }
        }
        _ => None,
    }
}

fn validate_known_columns_read_only(
    input_name: &str,
    transform: &logfwd::transform::SqlTransform,
    mode: Option<KnownColumnsMode>,
) -> Result<(), String> {
    let Some(mode) = mode else {
        return Ok(());
    };

    // SELECT * expands to whatever the engine provides — skip validation.
    let analyzer = transform.analyzer();
    if analyzer.uses_select_star {
        return Ok(());
    }

    let scan_config = transform.scan_config();
    if scan_config.extract_all {
        return Ok(());
    }

    let (known_columns, internal_only) = match &mode {
        KnownColumnsMode::Strict(cols) => (*cols, false),
        KnownColumnsMode::InternalOnly(cols) => (*cols, true),
    };

    let known: std::collections::HashSet<&str> = known_columns.iter().copied().collect();

    let mut unknown: Vec<String> = scan_config
        .wanted_fields
        .iter()
        .map(|field| field.name.as_str())
        .filter(|column| {
            // Strip table qualifier (e.g. "logs.level" -> "level").
            let bare = match column.rfind('.') {
                Some(pos) => &column[pos + 1..],
                None => column,
            };
            let lower = bare.to_lowercase();

            // In internal-only mode, allow columns that don't start with `_`
            // because they could be user-defined JSON keys.
            if internal_only && !lower.starts_with('_') {
                return false;
            }

            !known.contains(lower.as_str())
        })
        .map(str::to_owned)
        .collect();
    unknown.sort_unstable();
    unknown.dedup();

    if unknown.is_empty() {
        return Ok(());
    }

    let mut supported: Vec<&str> = known_columns.to_vec();
    supported.sort_unstable();

    if internal_only {
        Err(format!(
            "input '{input_name}': SQL references unknown internal column(s) {unknown} \
             for this input format (known internal columns: {supported}; \
             non-underscore-prefixed columns are assumed to be user-defined JSON keys)",
            unknown = unknown.join(", "),
            supported = supported.join(", "),
        ))
    } else {
        Err(format!(
            "input '{input_name}': SQL references unknown column(s) {} for this input schema (known: {})",
            unknown.join(", "),
            supported.join(", ")
        ))
    }
}

fn validate_pipeline_read_only(
    config: &logfwd_config::PipelineConfig,
    base_path: Option<&std::path::Path>,
) -> Result<(), String> {
    use logfwd::transform::SqlTransform;
    #[cfg(feature = "datafusion")]
    use logfwd_config::{EnrichmentConfig, GeoDatabaseFormat};
    use logfwd_config::{Format, InputTypeConfig};
    #[cfg(feature = "datafusion")]
    use std::path::PathBuf;

    if config.workers == Some(0) {
        return Err("workers must be >= 1".to_owned());
    }
    if config.batch_target_bytes == Some(0) {
        return Err("batch_target_bytes must be > 0".to_owned());
    }
    #[cfg(not(feature = "datafusion"))]
    let _ = base_path;

    #[cfg(feature = "datafusion")]
    let (enrichment_tables, geo_database) = {
        let mut enrichment_tables: Vec<Arc<dyn logfwd::transform::enrichment::EnrichmentTable>> =
            Vec::new();
        let mut geo_database: Option<Arc<dyn logfwd::transform::enrichment::GeoDatabase>> = None;

        for enrichment in &config.enrichment {
            match enrichment {
                EnrichmentConfig::GeoDatabase(geo_cfg) => {
                    let mut path = PathBuf::from(&geo_cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let db: Arc<dyn logfwd::transform::enrichment::GeoDatabase> =
                        match geo_cfg.format {
                            GeoDatabaseFormat::Mmdb => {
                                let mmdb =
                                    logfwd::transform::udf::geo_lookup::MmdbDatabase::open(&path)
                                        .map_err(|e| {
                                        format!(
                                            "failed to open geo database '{}': {e}",
                                            path.display()
                                        )
                                    })?;
                                Arc::new(mmdb)
                            }
                            GeoDatabaseFormat::CsvRange => {
                                let csv = logfwd::transform::udf::CsvRangeDatabase::open(&path)
                                    .map_err(|e| {
                                        format!(
                                            "failed to open CSV range geo database '{}': {e}",
                                            path.display()
                                        )
                                    })?;
                                Arc::new(csv)
                            }
                            _ => {
                                return Err(format!(
                                    "unsupported geo database format: {:?}",
                                    geo_cfg.format
                                ));
                            }
                        };
                    geo_database = Some(db);
                }
                EnrichmentConfig::Static(cfg) => {
                    let labels: Vec<(String, String)> = cfg
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    let table = Arc::new(
                        logfwd::transform::enrichment::StaticTable::new(&cfg.table_name, &labels)
                            .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?,
                    );
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::HostInfo(_) => {
                    enrichment_tables
                        .push(Arc::new(logfwd::transform::enrichment::HostInfoTable::new()));
                }
                EnrichmentConfig::K8sPath(cfg) => {
                    enrichment_tables.push(Arc::new(
                        logfwd::transform::enrichment::K8sPathTable::new(&cfg.table_name),
                    ));
                }
                EnrichmentConfig::Csv(cfg) => {
                    let mut path = PathBuf::from(&cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let table = Arc::new(logfwd::transform::enrichment::CsvFileTable::new(
                        &cfg.table_name,
                        &path,
                    ));
                    table
                        .reload()
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::Jsonl(cfg) => {
                    let mut path = PathBuf::from(&cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let table = Arc::new(logfwd::transform::enrichment::JsonLinesFileTable::new(
                        &cfg.table_name,
                        &path,
                    ));
                    table
                        .reload()
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::EnvVars(cfg) => {
                    let table = Arc::new(
                        logfwd::transform::enrichment::EnvTable::from_prefix(
                            &cfg.table_name,
                            &cfg.prefix,
                        )
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?,
                    );
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::ProcessInfo(_) => {
                    enrichment_tables.push(Arc::new(
                        logfwd::transform::enrichment::ProcessInfoTable::new(),
                    ));
                }
                EnrichmentConfig::KvFile(cfg) => {
                    let mut path = PathBuf::from(&cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let table = Arc::new(logfwd::transform::enrichment::KvFileTable::new(
                        &cfg.table_name,
                        &path,
                    ));
                    table
                        .reload()
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::NetworkInfo(_) => {
                    enrichment_tables.push(Arc::new(
                        logfwd::transform::enrichment::NetworkInfoTable::new(),
                    ));
                }
                EnrichmentConfig::ContainerInfo(_) => {
                    enrichment_tables.push(Arc::new(
                        logfwd::transform::enrichment::ContainerInfoTable::new(),
                    ));
                }
                EnrichmentConfig::K8sClusterInfo(_) => {
                    enrichment_tables.push(Arc::new(
                        logfwd::transform::enrichment::K8sClusterInfoTable::new(),
                    ));
                }
            }
        }

        (enrichment_tables, geo_database)
    };

    #[cfg(not(feature = "datafusion"))]
    if !config.enrichment.is_empty() {
        return Err(
            "pipeline enrichment requires DataFusion. Build default/full logfwd \
             (or add `--features datafusion`)"
                .to_owned(),
        );
    }

    let pipeline_sql = config.transform.as_deref().unwrap_or("SELECT * FROM logs");
    for (i, input_cfg) in config.inputs.iter().enumerate() {
        let input_name = input_cfg
            .name
            .clone()
            .unwrap_or_else(|| format!("input_{i}"));
        if matches!(
            input_cfg.type_config,
            InputTypeConfig::LinuxEbpfSensor(_)
                | InputTypeConfig::MacosEsSensor(_)
                | InputTypeConfig::WindowsEbpfSensor(_)
                | InputTypeConfig::HostMetrics(_)
                | InputTypeConfig::ArrowIpc(_)
        ) {
            if input_cfg.format.is_some() {
                return Err(format!(
                    "input '{input_name}': sensor inputs do not support 'format' (Arrow-native input)"
                ));
            }
        } else {
            let format = input_cfg
                .format
                .clone()
                .unwrap_or(match input_cfg.type_config {
                    InputTypeConfig::File(_) | InputTypeConfig::Stdin(_) => Format::Auto,
                    _ => Format::Json,
                });
            validate_input_format_read_only(&input_name, input_cfg.input_type(), &format)?;
        }

        let input_sql = input_cfg.sql.as_deref().unwrap_or(pipeline_sql);
        let mut transform = SqlTransform::new(input_sql).map_err(|e| e.to_string())?;
        let known_columns = if config.enrichment.is_empty() {
            known_input_columns_read_only(input_cfg)
        } else {
            None
        };
        validate_known_columns_read_only(&input_name, &transform, known_columns)?;
        #[cfg(feature = "datafusion")]
        {
            if let Some(ref db) = geo_database {
                transform.set_geo_database(Arc::clone(db));
            }
            for table in &enrichment_tables {
                transform
                    .add_enrichment_table(Arc::clone(table))
                    .map_err(|e| format!("input '{}': enrichment error: {e}", input_name))?;
            }
        }
        validate_transform_probe_read_only(&mut transform)
            .map_err(|e| format!("input '{}': {e}", input_name))?;
    }

    Ok(())
}

fn validate_input_format_read_only(
    name: &str,
    input_type: logfwd_config::InputType,
    format: &logfwd_config::Format,
) -> Result<(), String> {
    use logfwd_config::{Format, InputType};

    let supported = match input_type {
        InputType::File => matches!(
            format,
            Format::Cri | Format::Auto | Format::Json | Format::Raw
        ),
        InputType::Stdin => format.is_stdin_compatible(),
        InputType::Generator | InputType::Otlp => matches!(format, Format::Json),
        InputType::Http => matches!(format, Format::Json | Format::Raw),
        InputType::Udp | InputType::Tcp => matches!(format, Format::Json | Format::Raw),
        InputType::ArrowIpc => false,
        InputType::Journald => matches!(format, Format::Json),
        other => {
            tracing::warn!(
                "validate_input_format_read_only: unhandled input type {other:?} for input {name}"
            );
            return Err(format!(
                "input '{name}': type {:?} is not yet supported in read-only validation",
                other
            ));
        }
    };

    if supported {
        return Ok(());
    }

    Err(format!(
        "input '{name}': format {:?} is not supported for {:?} inputs",
        format, input_type
    ))
}

#[cfg(test)]
fn collect_yaml_files_recursive(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_yaml_files_recursive(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "yaml") {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
fn replace_env_placeholders_for_example_validation(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut cursor = 0usize;

    while let Some(rel_start) = input[cursor..].find("${") {
        let start = cursor + rel_start;
        out.push_str(&input[cursor..start]);
        let value_start = start + 2;
        if let Some(rel_end) = input[value_start..].find('}') {
            let end = value_start + rel_end;
            if end > value_start {
                out.push_str("example-env-value");
            } else {
                out.push_str("${}");
            }
            cursor = end + 1;
        } else {
            out.push_str(&input[start..]);
            cursor = input.len();
            break;
        }
    }

    if cursor < input.len() {
        out.push_str(&input[cursor..]);
    }

    out
}

fn validate_pipelines(
    config: &logfwd_config::Config,
    dry_run: bool,
    base_path: Option<&std::path::Path>,
) -> Result<(), CliError> {
    validate_pipelines_inner(
        config,
        base_path,
        |name| {
            // Success output goes to stdout so scripts can capture it.
            println!("  {}ready{}: {}{name}{}", green(), reset(), bold(), reset());
        },
        |err| {
            eprintln!("  {}error{}: {err}", red(), reset());
        },
    )?;

    let label = if dry_run { "dry run ok" } else { "config ok" };
    // Success summary goes to stdout so scripts can parse it reliably.
    println!(
        "{}{label}{}: {} pipeline(s)",
        green(),
        reset(),
        config.pipelines.len(),
    );
    Ok(())
}

async fn run_pipelines(
    config: logfwd_config::Config,
    base_path: Option<&std::path::Path>,
    config_path: &str,
    config_yaml: &str,
    auto_shutdown_after: Option<Duration>,
) -> Result<(), CliError> {
    logfwd_runtime::bootstrap::run_pipelines(
        config,
        base_path,
        logfwd_runtime::bootstrap::RunOptions {
            config_path,
            config_yaml,
            version: VERSION,
            use_color: use_color(),
            json_logs_for_stderr: use_json_logs_for_stderr(io::stderr().is_terminal()),
            auto_shutdown_after,
        },
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
fn format_duration(d: Duration) -> String {
    logfwd_runtime::bootstrap::format_duration(d)
}

#[cfg(test)]
fn format_count(n: u64) -> String {
    logfwd_runtime::bootstrap::format_count(n)
}

#[cfg(test)]
fn format_bytes(b: u64) -> String {
    logfwd_runtime::bootstrap::format_bytes(b)
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimestampParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u64,
    minute: u64,
    second: u64,
    millisecond: u64,
}

fn timestamp_parts_for_generated_log(offset_ms: u64) -> TimestampParts {
    // Base timestamp for generated logs: 2024-01-15T10:00:00.000Z.
    const BASE_HOUR_MS: u64 = 10 * 60 * 60 * 1000;
    const MILLIS_PER_DAY: u64 = 24 * 60 * 60 * 1000;

    let total_ms = BASE_HOUR_MS + offset_ms;
    let day_offset = (total_ms / MILLIS_PER_DAY) as i64;
    let ms_in_day = total_ms % MILLIS_PER_DAY;
    let hour = ms_in_day / (60 * 60 * 1000);
    let minute = (ms_in_day / (60 * 1000)) % 60;
    let second = (ms_in_day / 1000) % 60;
    let millisecond = ms_in_day % 1000;

    let base_days = days_from_civil(2024, 1, 15);
    let (year, month, day) = civil_from_days(base_days + day_offset);

    TimestampParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        millisecond,
    }
}

// Howard Hinnant civil-date conversion helpers:
// https://howardhinnant.github.io/date_algorithms.html
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let y = year - i32::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i32;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i64::from(era) * 146_097 + i64::from(doe) - 719_468
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i32 + (era as i32) * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = mp + if mp < 10 { 3 } else { -9 }; // [1, 12]
    let year = y + i32::from(month <= 2);
    (year, month as u32, day as u32)
}

fn generate_json_log_file(num_lines: usize, output: &str) -> io::Result<()> {
    use std::io::BufWriter;

    eprintln!(
        "Generating {}{num_lines}{} JSON log lines to {}{output}{}...",
        bold(),
        reset(),
        bold(),
        reset(),
    );

    let file = std::fs::File::create(output)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v2/products",
        "/health",
        "/api/v1/auth",
    ];

    for i in 0..num_lines {
        let seq = i as u64;
        let level = levels[i % 4];
        let path = paths[i % 5];
        let id = 10000 + (i * 7) % 90000;
        let dur = 1 + (i * 13) % 500;
        let rid = format!("{:016x}", seq.wrapping_mul(0x517cc1b727220a95));
        let status = [200, 201, 400, 404, 500, 503][i % 6];
        let ts = timestamp_parts_for_generated_log(seq);

        write!(
            writer,
            r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millisecond:03}Z","level":"{level}","message":"request handled GET {path}/{id}","duration_ms":{dur},"request_id":"{rid}","service":"myapp","status":{status}}}"#,
            year = ts.year,
            month = ts.month,
            day = ts.day,
            hour = ts.hour,
            minute = ts.minute,
            second = ts.second,
            millisecond = ts.millisecond,
            level = level,
            path = path,
            id = id,
            dur = dur,
            rid = rid,
            status = status,
        )?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    let size = std::fs::metadata(output)?.len();
    if num_lines == 0 {
        eprintln!("{}done{}: 0 lines, 0 bytes", green(), reset());
    } else {
        eprintln!(
            "{}done{}: {:.1} MB, avg {:.0} bytes/line",
            green(),
            reset(),
            size as f64 / (1024.0 * 1024.0),
            size as f64 / num_lines as f64,
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod cli_tests {
    use super::*;

    fn blast_args_for_validation() -> BlastArgs {
        BlastArgs {
            destination: Some(BlastDestination::Otlp),
            endpoint: Some("http://127.0.0.1:4318/v1/logs".to_owned()),
            auth_bearer_token: None,
            auth_header: Vec::new(),
            workers: 2,
            batch_lines: 5_000,
            duration_secs: None,
            diagnostics_addr: "127.0.0.1:0".to_owned(),
        }
    }

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
    fn stdin_send_config_injects_input_and_merges_resources() {
        let yaml = r"
resource_attrs:
  env: prod
output:
  type: stdout
";
        let resource = vec![" deployment = blue ".to_owned()];
        let generated =
            build_stdin_send_config_yaml(yaml, Some(SendFormat::Json), Some("checkout"), &resource)
                .expect("send config should render");
        let config =
            logfwd_config::Config::load_str(&generated).expect("generated config should parse");
        let pipeline = &config.pipelines["default"];
        let input = &pipeline.inputs[0];

        assert_eq!(input.input_type(), logfwd_config::InputType::Stdin);
        assert_eq!(input.format, Some(logfwd_config::Format::Json));
        assert_eq!(
            pipeline
                .resource_attrs
                .get("service.name")
                .map(String::as_str),
            Some("checkout")
        );
        assert_eq!(
            pipeline
                .resource_attrs
                .get("deployment")
                .map(String::as_str),
            Some("blue")
        );
        assert_eq!(
            pipeline.resource_attrs.get("env").map(String::as_str),
            Some("prod")
        );
    }

    #[test]
    fn stdin_send_config_accepts_plural_outputs() {
        let yaml = r"
resource_attrs:
  env: prod
transform: |
  SELECT * FROM logs
enrichment:
  - type: static
    table_name: labels
    labels:
      env: dogfood
outputs:
  - type: stdout
    format: json
  - type: file
    path: ./sent.ndjson
";
        let generated =
            build_stdin_send_config_yaml(yaml, Some(SendFormat::Raw), Some("checkout"), &[])
                .expect("send config should render");
        let config =
            logfwd_config::Config::load_str(&generated).expect("generated config should parse");
        let pipeline = &config.pipelines["default"];

        assert_eq!(
            pipeline.inputs[0].input_type(),
            logfwd_config::InputType::Stdin
        );
        assert_eq!(pipeline.inputs[0].format, Some(logfwd_config::Format::Raw));
        assert_eq!(pipeline.outputs.len(), 2);
        assert_eq!(
            pipeline
                .resource_attrs
                .get("service.name")
                .map(String::as_str),
            Some("checkout")
        );
        assert_eq!(
            pipeline.resource_attrs.get("env").map(String::as_str),
            Some("prod")
        );
        assert_eq!(pipeline.transform.as_deref(), Some("SELECT * FROM logs\n"));
        match &pipeline.enrichment[..] {
            [logfwd_config::EnrichmentConfig::Static(static_cfg)] => {
                assert_eq!(static_cfg.table_name, "labels");
                assert_eq!(
                    static_cfg.labels.get("env").map(String::as_str),
                    Some("dogfood")
                );
            }
            other => panic!("expected one static enrichment config, got {other:?}"),
        }
    }

    #[test]
    fn stdin_send_config_rejects_runtime_inputs() {
        for (field, yaml) in [
            (
                "input",
                "input:\n  type: file\n  path: /tmp/app.log\noutput:\n  type: stdout\n",
            ),
            (
                "pipelines",
                "pipelines:\n  default:\n    input:\n      type: file\n      path: /tmp/app.log\n    output:\n      type: stdout\n",
            ),
        ] {
            let err = build_stdin_send_config_yaml(yaml, None, None, &[])
                .expect_err("send config should reject runtime input shape");
            assert!(
                err.to_string().contains(field),
                "error should mention {field}: {err}"
            );
        }
    }

    #[test]
    fn stdin_send_config_requires_destination_output() {
        let err = build_stdin_send_config_yaml("server: {}\n", None, None, &[])
            .expect_err("send config should require output");
        assert!(
            err.to_string()
                .contains("must define top-level `output` or `outputs`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn stdin_send_config_rejects_output_and_outputs_together() {
        let yaml = r"
output:
  type: stdout
outputs:
  - type: stdout
";
        let err = build_stdin_send_config_yaml(yaml, None, None, &[])
            .expect_err("send config should reject ambiguous destination output shape");
        assert!(
            err.to_string()
                .contains("only one of top-level `output` or `outputs`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn stdin_send_config_rejects_malformed_resource_override() {
        let err = build_stdin_send_config_yaml(
            "output:\n  type: stdout\n",
            None,
            None,
            &["deployment".to_owned()],
        )
        .expect_err("send config should reject malformed resource override");
        assert!(
            err.to_string().contains("--resource"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_input_format_read_only_accepts_stdin_auto() {
        validate_input_format_read_only(
            "stdin",
            logfwd_config::InputType::Stdin,
            &logfwd_config::Format::Auto,
        )
        .expect("stdin should accept auto format");
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
    fn format_duration_seconds() {
        assert_eq!(format_duration(Duration::from_secs(42)), "42s");
    }

    #[test]
    fn format_duration_minutes() {
        assert_eq!(format_duration(Duration::from_secs(125)), "2m 5s");
    }

    #[test]
    fn format_duration_hours() {
        assert_eq!(format_duration(Duration::from_secs(7260)), "2h 1m");
    }

    #[test]
    fn format_count_various() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_500), "1.5K");
        assert_eq!(format_count(2_500_000), "2.5M");
        assert_eq!(format_count(1_200_000_000), "1.2B");
    }

    #[test]
    fn format_bytes_various() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(5 * 1024 * 1024), "5.0 MB");
        assert_eq!(format_bytes(2 * 1024 * 1024 * 1024), "2.0 GB");
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
    fn blast_missing_destination_fails_in_non_interactive_mode() {
        let mut args = blast_args_for_validation();
        args.destination = None;
        args.endpoint = None;
        let err = maybe_prompt_blast_setup(&mut args).expect_err("missing destination should fail");
        assert!(
            err.to_string().contains("no destination provided"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn blast_arrow_ipc_default_endpoint_is_http_url() {
        assert_eq!(
            BlastDestination::ArrowIpc.default_endpoint(),
            "http://127.0.0.1:18081/v1/arrow"
        );
    }

    #[test]
    fn devour_elasticsearch_bulk_uses_bulk_path() {
        let args = DevourArgs {
            mode: DevourMode::ElasticsearchBulk,
            listen: None,
            duration_secs: None,
            diagnostics_addr: "127.0.0.1:0".to_owned(),
        };
        let yaml = render_devour_yaml(&args, "127.0.0.1:9200");
        assert!(yaml.contains("path: '/_bulk'"));
        assert!(
            yaml.contains(
                "response_body: \"{\\\"took\\\":0,\\\"errors\\\":false,\\\"items\\\":[]}\""
            )
        );
    }

    #[test]
    fn resolve_blast_output_config_rejects_auth_for_udp_destination() {
        let mut args = blast_args_for_validation();
        args.destination = Some(BlastDestination::Udp);
        args.endpoint = Some("127.0.0.1:15514".to_owned());
        args.auth_bearer_token = Some("token".to_owned());
        let err =
            resolve_blast_output_config(&args).expect_err("udp destination should reject auth");
        assert!(
            err.to_string()
                .contains("only supported for otlp, http, elasticsearch, loki, and arrow_ipc"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_blast_output_config_preserves_tcp_endpoint() {
        let mut args = blast_args_for_validation();
        args.destination = Some(BlastDestination::Tcp);
        args.endpoint = Some("127.0.0.1:15140".to_owned());

        let output_cfg =
            resolve_blast_output_config(&args).expect("tcp destination should preserve endpoint");
        let logfwd_config::OutputConfigV2::Tcp(output_cfg) = output_cfg else {
            panic!("expected tcp output config");
        };
        assert_eq!(output_cfg.endpoint.as_deref(), Some("127.0.0.1:15140"));
    }

    #[test]
    fn cmd_devour_rejects_invalid_diagnostics_addr() {
        let args = DevourArgs {
            mode: DevourMode::Otlp,
            listen: Some("127.0.0.1:4318".to_owned()),
            duration_secs: None,
            diagnostics_addr: "http://127.0.0.1:9191".to_owned(),
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let err = rt
            .block_on(cmd_devour(args))
            .expect_err("invalid diagnostics addr should fail");
        assert!(
            err.to_string().contains("invalid diagnostics address"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cmd_blast_rejects_zero_workers() {
        let mut args = blast_args_for_validation();
        args.workers = 0;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let err = rt
            .block_on(cmd_blast(args))
            .expect_err("zero workers should fail");
        assert!(
            err.to_string().contains("--workers must be at least 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cmd_blast_rejects_zero_batch_lines() {
        let mut args = blast_args_for_validation();
        args.batch_lines = 0;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let err = rt
            .block_on(cmd_blast(args))
            .expect_err("zero batch lines should fail");
        assert!(
            err.to_string().contains("--batch-lines must be at least 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cmd_blast_rejects_zero_duration_when_provided() {
        let mut args = blast_args_for_validation();
        args.duration_secs = Some(0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let err = rt
            .block_on(cmd_blast(args))
            .expect_err("zero duration should fail");
        assert!(
            err.to_string()
                .contains("--duration-secs must be at least 1 when provided"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn yaml_quote_escapes_newlines_and_control_chars() {
        let quoted = yaml_quote("line1\nline2\r\n\0tab\tquote\"slash\\");
        assert_eq!(
            quoted,
            "\"line1\\nline2\\r\\n\\u0000tab\\tquote\\\"slash\\\\\""
        );
        assert!(!quoted.contains('\n'));
        assert!(!quoted.contains('\r'));
    }

    #[test]
    fn replace_env_placeholders_rewrites_all_braced_vars() {
        let input = "a: ${FOO}\nb: ${BAR_BAZ}\nc: ${}\nd: ${UNTERMINATED";
        let actual = replace_env_placeholders_for_example_validation(input);
        assert_eq!(
            actual,
            "a: example-env-value\nb: example-env-value\nc: ${}\nd: ${UNTERMINATED"
        );
    }

    #[test]
    fn wizard_template_renders_with_sql() {
        let input = &config_templates::INPUT_TEMPLATES[0];
        let output = &config_templates::OUTPUT_TEMPLATES[0];
        let cfg = config_templates::render_config(
            input,
            output,
            "SELECT * FROM logs WHERE level='ERROR'",
        );
        assert!(cfg.contains("input:"));
        assert!(cfg.contains("output:"));
        assert!(cfg.contains("transform: |"));
        assert!(cfg.contains("WHERE level='ERROR'"));
    }

    #[test]
    fn validate_pipelines_read_only_rejects_otlp_raw() {
        let yaml = r#"
input:
  type: otlp
  listen: 127.0.0.1:4318
  format: raw
output:
  type: "null"
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let result = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(matches!(result, Err(CliError::Config(_))));
    }

    #[test]
    fn validate_pipelines_read_only_rejects_sensor_format() {
        let yaml = r#"
input:
  type: linux_ebpf_sensor
  format: raw
output:
  type: "null"
"#;
        let err = logfwd_config::Config::load_str(yaml)
            .expect_err("sensor format should fail config validation");
        assert!(
            err.to_string()
                .contains("sensor inputs do not support 'format'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_pipelines_read_only_accepts_arrow_native_sensor() {
        let yaml = r#"
input:
  type: linux_ebpf_sensor
  sensor:
    poll_interval_ms: 1000
output:
  type: "null"
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        validate_pipelines_read_only(&config, None, |_name| {}, |_err| {})
            .expect("sensor input without format should validate");
    }
    #[test]
    fn read_wizard_line_rejects_eof() {
        let mut cursor = io::Cursor::new(Vec::<u8>::new());
        let err = read_wizard_line(&mut cursor).expect_err("EOF should fail");
        assert_eq!(err.to_string(), "stdin closed while reading wizard input");
    }

    #[test]
    fn effective_config_validation_does_not_bind_runtime_ports() {
        use std::io::Write as _;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test port");
        let port = listener.local_addr().expect("local addr").port();

        let mut file = tempfile::NamedTempFile::new().expect("temp config");
        writeln!(
            file,
            r#"input:
  type: otlp
  listen: 127.0.0.1:{port}
output:
  type: "null"
"#
        )
        .expect("write config");

        cmd_effective_config(Some(file.path().to_str().expect("utf-8 temp path")))
            .expect("read-only validation should not bind the configured port");
    }

    #[test]
    fn templates_avoid_unsupported_syslog_and_bare_otlp_http_endpoints() {
        for input in config_templates::INPUT_TEMPLATES {
            assert!(
                !input.snippet.contains("format: syslog"),
                "wizard input template should not emit unsupported syslog format: {}",
                input.id
            );
        }
        for output in config_templates::OUTPUT_TEMPLATES {
            if output.id == "otlp" {
                assert!(
                    output.snippet.contains("/v1/logs"),
                    "wizard OTLP output should include the HTTP logs path"
                );
            }
        }
    }

    #[test]
    fn generated_wizard_config_is_validated_before_write() {
        let output_path = std::path::Path::new("logfwd.generated.yaml");
        let invalid_yaml = r#"
input:
  type: otlp
  listen: 127.0.0.1:4318
output:
  type: "null"
transform: |
  SELECT level AS x, msg AS x FROM logs
"#;
        let err = validate_generated_config_read_only(invalid_yaml, output_path)
            .expect_err("invalid generated config should fail validation");
        assert!(
            err.to_string()
                .contains("generated config failed validation"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn effective_config_validation_matches_runtime_sql_planning_errors() {
        let yaml = r#"
input:
  type: otlp
  listen: 127.0.0.1:4318
output:
  type: "null"
transform: |
  SELECT level AS x, msg AS x FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let runtime = validate_pipelines_inner(&config, None, |_name| {}, |_err| {});
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_err(),
            "runtime validation should reject duplicate aliases"
        );
        assert!(
            read_only.is_err(),
            "effective-config validation should reject duplicate aliases"
        );
    }

    #[test]
    fn issue_1955_dry_run_rejects_unknown_generator_logs_column() {
        let yaml = r#"
input:
  type: generator
output:
  type: "null"
transform: |
  SELECT missing_column FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_err(),
            "dry-run validation should reject unknown columns"
        );
        assert!(
            read_only.is_err(),
            "read-only validation should reject unknown columns"
        );
    }

    #[test]
    fn issue_1955_dry_run_accepts_mixed_case_generator_logs_columns() {
        let yaml = r#"
input:
  type: generator
output:
  type: "null"
transform: |
  SELECT Level FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_ok(),
            "dry-run validation should accept case-insensitive column references"
        );
        assert!(
            read_only.is_ok(),
            "read-only validation should accept case-insensitive column references"
        );
    }

    #[test]
    fn issue_1955_dry_run_rejects_generator_message_source_parts() {
        let yaml = r#"
input:
  type: generator
output:
  type: "null"
transform: |
  SELECT method FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_err(),
            "dry-run validation should reject fields embedded only inside message"
        );
        assert!(
            read_only.is_err(),
            "read-only validation should reject fields embedded only inside message"
        );
    }

    #[test]
    fn issue_1955_dry_run_skips_strict_check_when_enrichment_tables_are_present() {
        let yaml = r#"
input:
  type: generator
output:
  type: "null"
enrichment:
  - type: static
    table_name: labels
    labels:
      environment: production
transform: |
  SELECT labels.environment FROM logs CROSS JOIN labels
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_ok(),
            "dry-run validation should not reject enrichment-only columns"
        );
        assert!(
            read_only.is_ok(),
            "read-only validation should not reject enrichment-only columns"
        );
    }

    #[test]
    fn issue_1955_dry_run_skips_strict_check_for_complex_generator_logs() {
        let yaml = r#"
input:
  type: generator
  generator:
    complexity: complex
output:
  type: "null"
transform: |
  SELECT bytes_in FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "complex generator profile has additional fields and should not use simple schema checks"
        );
    }

    #[test]
    fn issue_1955_dry_run_rejects_unknown_cri_internal_column() {
        let yaml = r#"
input:
  type: file
  path: /var/log/*.log
  format: cri
output:
  type: "null"
transform: |
  SELECT _timestampp FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_err(),
            "CRI format should reject misspelled internal columns like _timestampp"
        );
    }

    #[test]
    fn issue_1955_dry_run_accepts_valid_cri_columns() {
        let yaml = r#"
input:
  type: file
  path: /var/log/*.log
  format: cri
output:
  type: "null"
transform: |
  SELECT _timestamp, _stream, body FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "CRI format should accept known internal columns: {read_only:?}"
        );
    }

    #[test]
    fn issue_1955_dry_run_accepts_cri_user_json_columns() {
        let yaml = r#"
input:
  type: file
  path: /var/log/*.log
  format: cri
output:
  type: "null"
transform: |
  SELECT level, msg, custom_field FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "CRI format should allow non-underscore-prefixed columns (user JSON keys): {read_only:?}"
        );
    }

    #[test]
    fn issue_1955_dry_run_allows_json_format_any_columns() {
        let yaml = r#"
input:
  type: file
  path: /var/log/*.log
  format: json
output:
  type: "null"
transform: |
  SELECT custom_field FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "JSON format has fully dynamic schema — should allow any columns: {read_only:?}"
        );
    }

    #[test]
    fn issue_1955_dry_run_skips_cri_check_when_enrichment_present() {
        let yaml = r#"
input:
  type: file
  path: /var/log/*.log
  format: cri
output:
  type: "null"
enrichment:
  - type: static
    table_name: labels
    labels:
      environment: production
transform: |
  SELECT _unknown_col FROM logs CROSS JOIN labels
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "enrichment tables may add columns — skip strict internal column checks: {read_only:?}"
        );
    }

    #[test]
    fn issue_1955_dry_run_skips_cri_check_for_auto_format() {
        let yaml = r#"
input:
  type: file
  path: /var/log/*.log
output:
  type: "null"
transform: |
  SELECT _unknown_col FROM logs
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "auto format (default for file) can't be validated statically: {read_only:?}"
        );
    }

    #[test]
    fn all_yaml_examples_parse_and_validate_read_only() {
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut yaml_files = Vec::new();

        collect_yaml_files_recursive(&repo_root.join("examples"), &mut yaml_files)
            .expect("should collect yaml files under examples/");
        collect_yaml_files_recursive(&repo_root.join("bench/scenarios"), &mut yaml_files)
            .expect("should collect yaml files under bench/scenarios/");
        yaml_files.sort();

        assert!(
            !yaml_files.is_empty(),
            "expected at least one yaml example in examples/ or bench/scenarios/"
        );

        let mut failures = Vec::new();
        for path in yaml_files {
            let raw = match std::fs::read_to_string(&path) {
                Ok(value) => value,
                Err(err) => {
                    failures.push(format!("{}: failed to read file: {err}", path.display()));
                    continue;
                }
            };

            // Keep example validation hermetic: inline secrets placeholders with dummy values.
            let raw = replace_env_placeholders_for_example_validation(&raw);

            let config = match logfwd_config::Config::load_str(&raw) {
                Ok(cfg) => cfg,
                Err(err) => {
                    failures.push(format!(
                        "{}: failed to parse config yaml: {err}",
                        path.display()
                    ));
                    continue;
                }
            };

            if let Err(err) =
                validate_pipelines_read_only(&config, path.parent(), |_name| {}, |_err| {})
            {
                failures.push(format!(
                    "{}: failed read-only config validation: {err}",
                    path.display()
                ));
            }
        }

        assert!(
            failures.is_empty(),
            "yaml examples must parse and validate read-only:\n{}",
            failures.join("\n")
        );
    }
}

#[cfg(test)]
mod generate_json_tests {
    use super::*;

    #[test]
    fn generated_timestamp_carries_hour_and_day() {
        // 10:59:59.999 -> 11:00:00.000
        let before_hour = timestamp_parts_for_generated_log(3_599_999);
        let after_hour = timestamp_parts_for_generated_log(3_600_000);
        assert_eq!(
            (
                before_hour.year,
                before_hour.month,
                before_hour.day,
                before_hour.hour,
                before_hour.minute,
                before_hour.second,
                before_hour.millisecond
            ),
            (2024, 1, 15, 10, 59, 59, 999)
        );
        assert_eq!(
            (
                after_hour.year,
                after_hour.month,
                after_hour.day,
                after_hour.hour,
                after_hour.minute,
                after_hour.second,
                after_hour.millisecond
            ),
            (2024, 1, 15, 11, 0, 0, 0)
        );

        // 2024-01-15T23:59:59.999 -> 2024-01-16T00:00:00.000
        // Base is 10:00:00, so day rollover happens at +14h.
        let before_day = timestamp_parts_for_generated_log(50_399_999);
        let after_day = timestamp_parts_for_generated_log(50_400_000);
        assert_eq!(
            (
                before_day.year,
                before_day.month,
                before_day.day,
                before_day.hour,
                before_day.minute,
                before_day.second,
                before_day.millisecond
            ),
            (2024, 1, 15, 23, 59, 59, 999)
        );
        assert_eq!(
            (
                after_day.year,
                after_day.month,
                after_day.day,
                after_day.hour,
                after_day.minute,
                after_day.second,
                after_day.millisecond
            ),
            (2024, 1, 16, 0, 0, 0, 0)
        );
    }

    /// generate_json_log_file must produce monotonically increasing timestamps
    /// for files larger than 1000 lines.  Before the fix, line 1000 had the
    /// same millisecond component as line 0 (both .000Z), making the timestamp
    /// go backward between lines 999 and 1000.
    #[test]
    fn generate_json_timestamps_are_monotonic_across_1000_line_boundary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("logs.json");
        generate_json_log_file(1002, path.to_str().unwrap()).expect("generate");

        let content = std::fs::read_to_string(&path).expect("read");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1002);

        let ts_at = |idx: usize| -> String {
            let obj: serde_json::Value = serde_json::from_str(lines[idx]).expect("parse json");
            obj["timestamp"]
                .as_str()
                .expect("timestamp field")
                .to_owned()
        };

        let ts_999 = ts_at(999);
        let ts_1000 = ts_at(1000);

        assert!(
            ts_1000 > ts_999,
            "timestamp at line 1000 ({ts_1000:?}) must be greater than at line 999 ({ts_999:?}); \
             timestamps must not wrap at the 1000-line boundary"
        );
    }
}
