#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(unix, not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::env;
use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use clap::{CommandFactory, Parser, Subcommand, ValueEnum, error::ErrorKind};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use tokio_util::sync::CancellationToken;

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
  logfwd run --config config.yaml
  logfwd validate --config config.yaml
  logfwd dry-run --config config.yaml
  logfwd effective-config --config config.yaml
  logfwd blackhole
  logfwd generate-json 10000 test.json
  logfwd wizard
  logfwd completions bash

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

// ---------------------------------------------------------------------------
// Color support (respects NO_COLOR, checks stderr TTY)
// ---------------------------------------------------------------------------

fn use_color() -> bool {
    // SAFETY: isatty is a simple query on a well-known fd; no invariants to uphold.
    env::var_os("NO_COLOR").is_none() && unsafe { libc::isatty(libc::STDERR_FILENO) != 0 }
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

#[derive(Debug, Parser)]
#[command(
    name = "logfwd",
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
    /// Generate starter config in current directory.
    Init,
    /// Interactive config wizard.
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
    let cli = match Cli::try_parse() {
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
                "{}      run {}logfwd run --config {}{}",
                dim(),
                bold(),
                path.display(),
                reset()
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

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn cmd_run(config_path: &str, validate_only: bool, dry_run: bool) -> Result<(), CliError> {
    let config_yaml = std::fs::read_to_string(config_path)
        .map_err(|e| CliError::Config(format!("cannot read {config_path}: {e}")))?;
    let config = match logfwd_config::Config::load_str(&config_yaml) {
        Ok(c) => c,
        Err(e) => {
            return Err(CliError::Config(e.to_string()));
        }
    };

    let base_path = std::path::Path::new(config_path).parent();

    if validate_only || dry_run {
        // Both `validate` and `dry-run` build pipelines to catch SQL/wiring errors.
        return validate_pipelines(&config, dry_run, base_path);
    }

    run_pipelines(config, base_path, config_path, &config_yaml).await
}

async fn cmd_blackhole(bind_addr: Option<&str>) -> Result<(), CliError> {
    let addr = bind_addr.unwrap_or("127.0.0.1:4318");

    // Validate addr using the same hostname-accepting logic as the config
    // validator (validate_host_port) so that a hostname like `localhost:4318`
    // is accepted here and not only rejected later by the config layer.
    logfwd_config::validate_host_port(addr)
        .map_err(|e| CliError::Config(format!("invalid bind address: {e}")))?;

    // Use port 0 for diagnostics so it never collides with an in-use port.
    // Quote addr as a YAML string so bracketed IPv6 (e.g. [::1]:4318) does not
    // break YAML parsing. Single-quote with '' escaping for any embedded quotes.
    let yaml_addr = addr.replace('\'', "''");
    let yaml = format!(
        "input:\n  type: otlp\n  listen: '{yaml_addr}'\noutput:\n  type: null\nserver:\n  diagnostics: 127.0.0.1:0\n"
    );
    let config = logfwd_config::Config::load_str(&yaml)
        .map_err(|e| CliError::Config(format!("internal config error: {e}")))?;

    eprintln!(
        "{}logfwd blackhole{} starting on {}{addr}{}",
        bold(),
        reset(),
        bold(),
        reset(),
    );

    run_pipelines(config, None, "<blackhole>", &yaml).await
}

fn cmd_generate_json(num_lines: usize, output_file: &str) -> Result<(), CliError> {
    generate_json_log_file(num_lines, output_file).map_err(CliError::Runtime)
}

fn cmd_effective_config(config_path: Option<&str>) -> Result<(), CliError> {
    let config_path = resolve_config_path(config_path)?;

    let config_yaml = std::fs::read_to_string(&config_path)
        .map_err(|e| CliError::Config(format!("cannot read {config_path}: {e}")))?;
    let effective_yaml = logfwd_config::Config::expand_env_str(&config_yaml)
        .map_err(|e| CliError::Config(e.to_string()))?;

    // Read-only validation for inspection flows: reject configs that would
    // fail format or SQL-plan checks without constructing runtime inputs that
    // bind sockets or touch long-lived resources.
    let config = logfwd_config::Config::load_str(&config_yaml)
        .map_err(|e| CliError::Config(e.to_string()))?;
    let base_path = std::path::Path::new(&config_path).parent();
    validate_pipelines_read_only(
        &config,
        base_path,
        |_name| {},
        |err| eprintln!("  {}error{}: {err}", red(), reset()),
    )?;

    eprintln!("{}# validated from {config_path}{}", dim(), reset());
    print!("{effective_yaml}");
    Ok(())
}

fn cmd_init() -> Result<(), CliError> {
    let path = std::path::Path::new("logfwd.yaml");

    let template = r#"# logfwd configuration
# Docs: https://github.com/strawgate/memagent

# Simple pipeline: tail a log file, transform with SQL, send via OTLP.
input:
  type: file
  path: /var/log/*.log
  # format: json           # auto-detected; uncomment to force

# SQL transform (optional) — filter, reshape, or enrich logs.
# Remove this section to forward all logs unmodified.
transform: |
  SELECT * FROM logs
  WHERE level IN ('WARN', 'ERROR')

output:
  type: otlp
  endpoint: http://localhost:4318/v1/logs

# Optional: diagnostics dashboard
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
    eprintln!(
        "{}created{} {} — edit it, then run: {}logfwd validate --config logfwd.yaml{}",
        green(),
        reset(),
        path.display(),
        bold(),
        reset(),
    );
    Ok(())
}

fn cmd_wizard() -> Result<(), CliError> {
    use config_templates::{INPUT_TEMPLATES, OUTPUT_TEMPLATES, render_config};

    if !wizard_uses_interactive_terminals(io::stdin().is_terminal(), io::stdout().is_terminal()) {
        return Err(CliError::Config(
            "wizard requires an interactive terminal on stdin and stdout".to_owned(),
        ));
    }
    println!("{}logfwd config wizard{}", bold(), reset());
    println!("Pick what you want to collect and where to send it.");
    println!();

    let input_idx = prompt_select(
        "What do you want to collect?",
        &INPUT_TEMPLATES.iter().map(|t| t.label).collect::<Vec<_>>(),
    )?;
    let output_idx = prompt_select(
        "Where do you want to send logs?",
        &OUTPUT_TEMPLATES.iter().map(|t| t.label).collect::<Vec<_>>(),
    )?;

    let sql = prompt_text(
        "Optional SQL transform (blank = SELECT * FROM logs)",
        "SELECT * FROM logs",
    )?;

    let output_path = prompt_text("Output file path", "logfwd.generated.yaml")?;
    let path = std::path::PathBuf::from(output_path.trim());

    let input = &INPUT_TEMPLATES[input_idx];
    let output = &OUTPUT_TEMPLATES[output_idx];
    let sql = if sql.trim().is_empty() {
        "SELECT * FROM logs"
    } else {
        sql.trim()
    };
    let cfg = render_config(input, output, sql);
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

    eprintln!("{}created{} {}", green(), reset(), path.as_path().display(),);
    eprintln!(
        "{}next{}: run {}logfwd validate --config {}{}",
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
    let config = logfwd_config::Config::load_str(config_yaml)
        .map_err(|e| CliError::Config(e.to_string()))?;
    let base_path = output_path.parent();
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
            clap_complete::generate(clap_complete::Shell::Bash, &mut cmd, "logfwd", &mut stdout);
        }
        CompletionShell::Elvish => {
            clap_complete::generate(
                clap_complete::Shell::Elvish,
                &mut cmd,
                "logfwd",
                &mut stdout,
            );
        }
        CompletionShell::Fish => {
            clap_complete::generate(clap_complete::Shell::Fish, &mut cmd, "logfwd", &mut stdout);
        }
        CompletionShell::PowerShell => {
            clap_complete::generate(
                clap_complete::Shell::PowerShell,
                &mut cmd,
                "logfwd",
                &mut stdout,
            );
        }
        CompletionShell::Zsh => {
            clap_complete::generate(clap_complete::Shell::Zsh, &mut cmd, "logfwd", &mut stdout);
        }
        CompletionShell::Nushell => {
            clap_complete::generate(
                clap_complete_nushell::Nushell,
                &mut cmd,
                "logfwd",
                &mut stdout,
            );
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
    if let Some(xdg) = xdg_base.map(|b| b.join("logfwd/config.yaml")) {
        if xdg.is_file() {
            return Some(xdg);
        }
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
    transform: &mut logfwd_transform::SqlTransform,
) -> Result<(), String> {
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let fields: Vec<Field> = if transform.analyzer().referenced_columns.is_empty() {
        vec![
            Field::new("_raw", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("msg", DataType::Utf8, true),
        ]
    } else {
        transform
            .analyzer()
            .referenced_columns
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
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

fn validate_pipeline_read_only(
    config: &logfwd_config::PipelineConfig,
    base_path: Option<&std::path::Path>,
) -> Result<(), String> {
    use logfwd_config::{EnrichmentConfig, Format, GeoDatabaseFormat, InputType};
    use logfwd_transform::SqlTransform;
    use std::path::PathBuf;

    if config.workers == Some(0) {
        return Err("workers must be >= 1".to_owned());
    }
    if config.batch_target_bytes == Some(0) {
        return Err("batch_target_bytes must be > 0".to_owned());
    }

    let mut enrichment_tables: Vec<Arc<dyn logfwd_transform::enrichment::EnrichmentTable>> =
        Vec::new();
    let mut geo_database: Option<Arc<dyn logfwd_transform::enrichment::GeoDatabase>> = None;

    for enrichment in &config.enrichment {
        match enrichment {
            EnrichmentConfig::GeoDatabase(geo_cfg) => {
                let mut path = PathBuf::from(&geo_cfg.path);
                if path.is_relative()
                    && let Some(base) = base_path
                {
                    path = base.join(path);
                }
                let db: Arc<dyn logfwd_transform::enrichment::GeoDatabase> = match geo_cfg.format {
                    GeoDatabaseFormat::Mmdb => {
                        let mmdb = logfwd_transform::udf::geo_lookup::MmdbDatabase::open(&path)
                            .map_err(|e| {
                                format!("failed to open geo database '{}': {e}", path.display())
                            })?;
                        Arc::new(mmdb)
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
                    logfwd_transform::enrichment::StaticTable::new(&cfg.table_name, &labels)
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?,
                );
                enrichment_tables.push(table);
            }
            EnrichmentConfig::HostInfo(_) => {
                enrichment_tables
                    .push(Arc::new(logfwd_transform::enrichment::HostInfoTable::new()));
            }
            EnrichmentConfig::K8sPath(cfg) => {
                enrichment_tables.push(Arc::new(logfwd_transform::enrichment::K8sPathTable::new(
                    &cfg.table_name,
                )));
            }
            EnrichmentConfig::Csv(cfg) => {
                let mut path = PathBuf::from(&cfg.path);
                if path.is_relative()
                    && let Some(base) = base_path
                {
                    path = base.join(path);
                }
                let table = Arc::new(logfwd_transform::enrichment::CsvFileTable::new(
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
                let table = Arc::new(logfwd_transform::enrichment::JsonLinesFileTable::new(
                    &cfg.table_name,
                    &path,
                ));
                table
                    .reload()
                    .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                enrichment_tables.push(table);
            }
        }
    }

    let pipeline_sql = config.transform.as_deref().unwrap_or("SELECT * FROM logs");
    for (i, input_cfg) in config.inputs.iter().enumerate() {
        let input_name = input_cfg
            .name
            .clone()
            .unwrap_or_else(|| format!("input_{i}"));
        let format = input_cfg
            .format
            .clone()
            .unwrap_or(match input_cfg.input_type {
                InputType::File => Format::Auto,
                _ => Format::Json,
            });
        validate_input_format_read_only(&input_name, input_cfg.input_type.clone(), &format)?;

        let input_sql = input_cfg.sql.as_deref().unwrap_or(pipeline_sql);
        let mut transform = SqlTransform::new(input_sql).map_err(|e| e.to_string())?;
        if let Some(ref db) = geo_database {
            transform.set_geo_database(Arc::clone(db));
        }
        for table in &enrichment_tables {
            transform
                .add_enrichment_table(Arc::clone(table))
                .map_err(|e| format!("input '{}': enrichment error: {e}", input_name))?;
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
        InputType::Generator | InputType::Otlp => matches!(format, Format::Json),
        InputType::Udp | InputType::Tcp => matches!(format, Format::Json | Format::Raw),
        InputType::ArrowIpc => false,
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

fn input_label(i: &logfwd_config::InputConfig) -> String {
    use logfwd_config::InputType;
    match i.input_type {
        InputType::File => format!("file  {}", i.path.as_deref().unwrap_or("*")),
        InputType::Tcp => format!("tcp   {}", i.listen.as_deref().unwrap_or(":514")),
        InputType::Udp => format!("udp   {}", i.listen.as_deref().unwrap_or(":514")),
        InputType::Otlp => format!("otlp  {}", i.listen.as_deref().unwrap_or(":4318")),
        InputType::Generator => "generator".to_string(),
        _ => "unknown".to_string(),
    }
}

fn output_label(o: &logfwd_config::OutputConfig) -> String {
    use logfwd_config::OutputType;
    match o.output_type {
        OutputType::Otlp => format!("otlp  {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::Http => format!("http  {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::Elasticsearch => {
            format!("elasticsearch  {}", o.endpoint.as_deref().unwrap_or(""))
        }
        OutputType::Loki => format!("loki  {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::Tcp => format!("tcp   {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::Udp => format!("udp   {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::File => format!("file  {}", o.path.as_deref().unwrap_or("")),
        OutputType::Parquet => format!("parquet  {}", o.path.as_deref().unwrap_or("")),
        OutputType::Stdout => "stdout".to_string(),
        OutputType::Null => "null".to_string(),
        _ => "unknown".to_string(),
    }
}

async fn run_pipelines(
    config: logfwd_config::Config,
    base_path: Option<&std::path::Path>,
    config_path: &str,
    config_yaml: &str,
) -> Result<(), CliError> {
    let startup_start = std::time::Instant::now();
    use logfwd::pipeline::Pipeline;
    use logfwd_io::diagnostics::DiagnosticsServer;
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // Acquire exclusive lock only when tailing files — OTLP-only and
    // blackhole pipelines don't need filesystem locking (#737).
    let has_file_inputs = config.pipelines.values().any(|pipe| {
        pipe.inputs
            .iter()
            .any(|input| matches!(input.input_type, logfwd_config::InputType::File))
    });
    let _lock_guard = if has_file_inputs {
        acquire_instance_lock(&config)?
    } else {
        None
    };

    let shutdown = CancellationToken::new();

    #[cfg(unix)]
    let (mut sigterm, mut sighup) = {
        use tokio::signal::unix::{SignalKind, signal};
        let sigterm = signal(SignalKind::terminate()).map_err(|err| {
            CliError::Runtime(io::Error::other(format!(
                "failed to register SIGTERM handler: {err}"
            )))
        })?;
        let sighup = signal(SignalKind::hangup()).map_err(|err| {
            CliError::Runtime(io::Error::other(format!(
                "failed to register SIGHUP handler: {err}"
            )))
        })?;
        (sigterm, sighup)
    };

    // Listen for SIGINT (Ctrl-C) and SIGTERM to trigger graceful shutdown.
    #[cfg(feature = "dhat-heap")]
    let profiler = dhat::Profiler::new_heap();

    #[cfg(feature = "cpu-profiling")]
    let pprof_guard = pprof::ProfilerGuardBuilder::default()
        .frequency(999)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| {
            CliError::Runtime(io::Error::other(format!(
                "failed to initialize pprof profiler: {e}"
            )))
        })?;

    let shutdown_for_signal = shutdown.clone();
    tokio::spawn(async move {
        #[cfg(feature = "dhat-heap")]
        let _profiler_to_drop = profiler;

        #[cfg(feature = "cpu-profiling")]
        let _pprof_to_drop = pprof_guard;

        #[cfg(unix)]
        {
            // Install a SIGHUP handler so logrotate / supervisors don't kill us.
            // Config reload is not yet implemented; we ignore SIGHUP and log a warning.
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = sigterm.recv() => break,
                    _ = sighup.recv() => {
                        eprintln!(
                            "{}logfwd{}: SIGHUP received — config reload not yet implemented, ignoring",
                            yellow(), reset(),
                        );
                        // Continue the loop — SIGHUP does not trigger shutdown.
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }

        #[cfg(feature = "cpu-profiling")]
        {
            if let Ok(report) = _pprof_to_drop.report().build() {
                if let Ok(file) = std::fs::File::create("flamegraph.svg") {
                    let _ = report.flamegraph(file);
                }
            }
        }

        #[cfg(feature = "dhat-heap")]
        drop(_profiler_to_drop);

        shutdown_for_signal.cancel();
    });

    let meter_provider = build_meter_provider(&config)?;
    let meter = meter_provider.meter("logfwd");

    // Set up the tracing subscriber with an OTel layer that routes spans
    // to our in-process ring buffer (and optionally to an OTLP endpoint),
    // plus a stderr fmt layer so tracing events are visible on the console.
    let trace_buf = logfwd_io::span_exporter::SpanBuffer::new();
    let tracer_provider = build_tracer_provider(trace_buf.clone(), &config)?;
    let tracer = opentelemetry::trace::TracerProvider::tracer(&tracer_provider, "logfwd");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let env_filter = tracing_subscriber::EnvFilter::try_from_env("LOGFWD_LOG")
        .or_else(|_| tracing_subscriber::EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = if use_json_logs_for_stderr(io::stderr().is_terminal()) {
        tracing_subscriber::fmt::layer()
            .json()
            .with_writer(io::stderr)
            .with_target(true)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_writer(io::stderr)
            .with_target(true)
            .boxed()
    };
    // Apply env_filter only to the fmt layer so it doesn't suppress OTel spans.
    let _ = tracing_subscriber::registry()
        .with(fmt_layer.with_filter(env_filter))
        .with(otel_layer)
        .try_init(); // ignore error if a subscriber is already installed (e.g. in tests)
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let mut pipelines = Vec::new();
    for (name, pipe_cfg) in &config.pipelines {
        match Pipeline::from_config(name, pipe_cfg, &meter, base_path) {
            Ok(pipeline) => {
                pipelines.push(pipeline);
            }
            Err(e) => {
                return Err(CliError::Config(format!("pipeline '{name}': {e}")));
            }
        }
    }

    let diag_handle = if let Some(ref addr) = config.server.diagnostics {
        let mut server = DiagnosticsServer::new(addr);
        server.set_config(config_path, config_yaml);
        server.set_trace_buffer(trace_buf);
        for p in &pipelines {
            server.add_pipeline(Arc::clone(p.metrics()));
        }
        #[cfg(unix)]
        server.set_memory_stats_fn(jemalloc_stats);
        let (handle, _) = server.start()?;
        Some((handle, addr.clone()))
    } else {
        None
    };

    // Save metrics references for shutdown summary.
    let pipeline_metrics: Vec<_> = pipelines.iter().map(|p| Arc::clone(p.metrics())).collect();

    eprintln!("{}logfwd{} {}v{VERSION}{}", bold(), reset(), dim(), reset());

    // Print startup summary after everything is ready.
    for (name, pipe_cfg) in &config.pipelines {
        eprintln!();
        eprintln!("  {}✓{}  {}{name}{}", green(), reset(), bold(), reset());
        for input in &pipe_cfg.inputs {
            eprintln!("     {}in{}   {}", dim(), reset(), input_label(input));
        }
        if let Some(sql) = pipe_cfg.transform.as_deref() {
            let sql = sql.trim();
            let first_line = sql.lines().next().unwrap_or(sql);
            let truncated = if first_line.chars().count() > 100 {
                format!("{}…", first_line.chars().take(100).collect::<String>())
            } else {
                first_line.to_string()
            };
            eprintln!("     {}sql{}  {truncated}", dim(), reset());
        }
        for output in &pipe_cfg.outputs {
            eprintln!("     {}out{}  {}", dim(), reset(), output_label(output));
        }
    }
    if let Some((_, ref addr)) = diag_handle {
        eprintln!();
        eprintln!("  {}dashboard{}  http://{addr}", bold(), reset());
    }
    eprintln!();
    let n = pipelines.len();
    let startup_ms = startup_start.elapsed().as_millis();
    eprintln!(
        "{}ready{} · {n} pipeline{} {}(started in {startup_ms}ms){}",
        green(),
        reset(),
        if n == 1 { "" } else { "s" },
        dim(),
        reset(),
    );

    let mut handles = Vec::new();
    let main_pipeline = pipelines.pop();

    for mut pipeline in pipelines {
        let sd = shutdown.clone();
        handles.push(tokio::spawn(async move { pipeline.run_async(&sd).await }));
    }

    if let Some(mut main_pipe) = main_pipeline {
        let result = main_pipe.run_async(&shutdown).await;
        // Always cancel + join siblings, even if main pipeline errored.
        shutdown.cancel();
        let mut had_sibling_error = false;
        for h in handles {
            match h.await {
                Ok(Err(e)) => {
                    eprintln!("pipeline error: {e}");
                    had_sibling_error = true;
                }
                Err(e) => {
                    eprintln!("pipeline task panicked: {e}");
                    had_sibling_error = true;
                }
                Ok(Ok(())) => {}
            }
        }
        result?;
        if had_sibling_error {
            return Err(CliError::Runtime(io::Error::other(
                "one or more sibling pipelines failed",
            )));
        }
    } else {
        let mut had_error = false;
        for h in handles {
            match h.await {
                Ok(Err(e)) => {
                    eprintln!("pipeline error: {e}");
                    had_error = true;
                }
                Err(e) => {
                    eprintln!("pipeline task panicked: {e}");
                    had_error = true;
                }
                Ok(Ok(())) => {}
            }
        }
        if had_error {
            return Err(CliError::Runtime(io::Error::other(
                "one or more pipelines failed",
            )));
        }
    }

    // Print shutdown summary.
    print_shutdown_stats(&pipeline_metrics, startup_start.elapsed());

    if let Err(e) = meter_provider.shutdown() {
        eprintln!(
            "{}warning{}: meter provider shutdown: {e}",
            yellow(),
            reset()
        );
    }
    if let Err(e) = tracer_provider.shutdown() {
        eprintln!(
            "{}warning{}: tracer provider shutdown: {e}",
            yellow(),
            reset()
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Shutdown summary
// ---------------------------------------------------------------------------

fn print_shutdown_stats(
    metrics: &[Arc<logfwd_io::diagnostics::PipelineMetrics>],
    uptime: std::time::Duration,
) {
    use std::sync::atomic::Ordering::Relaxed;

    let total_lines_in: u64 = metrics
        .iter()
        .map(|m| m.transform_in.lines_total.load(Relaxed))
        .sum();
    let total_lines_out: u64 = metrics
        .iter()
        .map(|m| m.transform_out.lines_total.load(Relaxed))
        .sum();
    let total_bytes_in: u64 = metrics
        .iter()
        .map(|m| {
            m.inputs
                .iter()
                .map(|(_, _, stats)| stats.bytes_total.load(Relaxed))
                .sum::<u64>()
        })
        .sum();
    let total_batches: u64 = metrics.iter().map(|m| m.batches_total.load(Relaxed)).sum();
    let total_errors: u64 = metrics
        .iter()
        .map(|m| m.transform_errors.load(Relaxed))
        .sum();
    let total_dropped: u64 = metrics
        .iter()
        .map(|m| m.dropped_batches_total.load(Relaxed))
        .sum();

    eprintln!();
    eprintln!(
        "{}stopped{} · uptime {}",
        dim(),
        reset(),
        format_duration(uptime),
    );

    if total_lines_in > 0 || total_batches > 0 {
        eprintln!(
            "  lines  {} in → {} out  ({} batches)",
            format_count(total_lines_in),
            format_count(total_lines_out),
            format_count(total_batches),
        );
        if total_bytes_in > 0 {
            eprintln!("  bytes  {} in", format_bytes(total_bytes_in));
        }
        if total_errors > 0 || total_dropped > 0 {
            eprintln!(
                "  {}errors{} {} transform, {} dropped batches",
                yellow(),
                reset(),
                total_errors,
                total_dropped,
            );
        }
    }
}

fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

fn format_count(n: u64) -> String {
    if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else if n < 1_000_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    }
}

fn format_bytes(b: u64) -> String {
    if b < 1024 {
        format!("{b} B")
    } else if b < 1024 * 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else if b < 1024 * 1024 * 1024 {
        format!("{:.1} MB", b as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", b as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Instance lock (#737)
// ---------------------------------------------------------------------------

/// Acquire an exclusive lock file to prevent multiple logfwd instances from
/// processing the same data directory. Returns a guard that holds the lock
/// for the lifetime of the caller.
///
/// On non-Unix platforms, logs a warning and returns a dummy guard.
fn acquire_instance_lock(
    config: &logfwd_config::Config,
) -> Result<Option<std::fs::File>, CliError> {
    let data_dir = config.storage.data_dir.as_ref().map_or_else(
        logfwd_io::checkpoint::default_data_dir,
        std::path::PathBuf::from,
    );
    std::fs::create_dir_all(&data_dir)?;
    let lock_path = data_dir.join("logfwd.lock");
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;

    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `lock_file` is an open `File`, so `as_raw_fd()` returns a valid
        // file descriptor. `libc::flock` is safe to call on any valid fd — it only
        // manipulates the kernel-level advisory lock, no memory mutation.
        let ret = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if ret != 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock || err.raw_os_error() == Some(libc::EAGAIN) {
                return Err(CliError::Runtime(io::Error::other(format!(
                    "another logfwd instance is already running (lock: {})",
                    lock_path.display()
                ))));
            }
            return Err(CliError::Runtime(err));
        }
        // Note: tracing subscriber not yet initialized at this point.
    }

    #[cfg(not(unix))]
    eprintln!("warn: file-based instance locking not supported on this platform");

    // Return the File so the lock is held until the caller drops it.
    Ok(Some(lock_file))
}

// ---------------------------------------------------------------------------
// OTel metrics
// ---------------------------------------------------------------------------

fn build_meter_provider(
    config: &logfwd_config::Config,
) -> io::Result<opentelemetry_sdk::metrics::SdkMeterProvider> {
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    if let Some(ref endpoint) = config.server.metrics_endpoint {
        let interval_secs = config.server.metrics_interval_secs.unwrap_or(60);

        let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| io::Error::other(format!("OTLP metric exporter: {e}")))?;

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(otlp_exporter)
            .with_interval(std::time::Duration::from_secs(interval_secs))
            .build();

        eprintln!(
            "  {}metrics push{}: {endpoint} (every {interval_secs}s)",
            dim(),
            reset(),
        );

        Ok(SdkMeterProvider::builder().with_reader(reader).build())
    } else {
        Ok(SdkMeterProvider::builder().build())
    }
}

// ---------------------------------------------------------------------------
// OTel tracing + in-process span buffer
// ---------------------------------------------------------------------------

/// Build a `SdkTracerProvider` that writes completed spans into `buf`.
/// If `config.server.traces_endpoint` is set, also pushes via OTLP.
pub fn build_tracer_provider(
    buf: logfwd_io::span_exporter::SpanBuffer,
    config: &logfwd_config::Config,
) -> io::Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    use logfwd_io::span_exporter::RingBufferExporter;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SimpleSpanProcessor};

    let ring_processor = SimpleSpanProcessor::new(RingBufferExporter::new(buf));

    let mut builder = SdkTracerProvider::builder().with_span_processor(ring_processor);

    if let Some(ref endpoint) = config.server.traces_endpoint {
        let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| io::Error::other(format!("OTLP trace exporter: {e}")))?;
        builder = builder.with_span_processor(
            opentelemetry_sdk::trace::BatchSpanProcessor::builder(otlp_exporter).build(),
        );
        eprintln!(
            "  {}traces push{}: {}",
            dim(),
            reset(),
            redact_url(endpoint)
        );
    }

    Ok(builder.build())
}

/// Return a URL with credentials and query parameters stripped, for safe logging.
/// Falls back to the original string if parsing fails.
fn redact_url(url: &str) -> String {
    // Find scheme end ("://")
    let after_scheme = url.find("://").map_or(0, |i| i + 3);
    let rest = &url[after_scheme..];
    // Strip userinfo (anything before '@' in the authority)
    let host_start = rest.find('@').map_or(0, |i| i + 1);
    let authority_and_path = &rest[host_start..];
    // Strip path/query/fragment — keep only host:port
    let host_end = authority_and_path
        .find(['/', '?', '#'])
        .unwrap_or(authority_and_path.len());
    let host = &authority_and_path[..host_end];
    if host.is_empty() {
        return url.to_string();
    }
    format!("{}://{}", &url[..after_scheme.saturating_sub(3)], host)
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

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
        let level = levels[i % 4];
        let path = paths[i % 5];
        let id = 10000 + (i * 7) % 90000;
        let dur = 1 + (i * 13) % 500;
        let rid = format!("{:016x}", (i as u64).wrapping_mul(0x517cc1b727220a95));
        let status = [200, 201, 400, 404, 500, 503][i % 6];

        write!(
            writer,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request handled GET {}/{}","duration_ms":{},"request_id":"{}","service":"myapp","status":{}}}"#,
            i % 1000,
            level,
            path,
            id,
            dur,
            rid,
            status,
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
// Allocator memory stats
// ---------------------------------------------------------------------------

#[cfg(unix)]
/// Read jemalloc memory stats: resident, allocated, and active bytes.
///
/// Returns `None` if the stats are unavailable (e.g. the epoch refresh fails).
/// This function is passed to [`DiagnosticsServer`] so the `/admin/v1/status`
/// endpoint can expose live allocator metrics.
fn jemalloc_stats() -> Option<logfwd_io::diagnostics::MemoryStats> {
    use tikv_jemalloc_ctl::{epoch, stats};

    // Refresh the epoch so subsequent reads reflect current allocator state.
    epoch::mib().ok()?.advance().ok()?;

    let resident = stats::resident::mib().ok()?.read().ok()?;
    let allocated = stats::allocated::mib().ok()?.read().ok()?;
    let active = stats::active::mib().ok()?.read().ok()?;

    Some(logfwd_io::diagnostics::MemoryStats {
        resident,
        allocated,
        active,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn clap_parses_validate_subcommand() {
        let cli = Cli::try_parse_from(["logfwd", "validate", "--config", "foo.yaml"])
            .expect("parser should accept validate subcommand");
        match cli.command.expect("command") {
            Commands::Validate { config } => assert_eq!(config.as_deref(), Some("foo.yaml")),
            other => panic!("expected validate command, got {other:?}"),
        }
    }

    #[test]
    fn clap_rejects_unknown_old_top_level_flags() {
        let err = Cli::try_parse_from(["logfwd", "--config", "foo.yaml"])
            .expect_err("parser should reject old top-level flags");
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn clap_parses_effective_config_with_optional_config_flag() {
        let with_path = Cli::try_parse_from(["logfwd", "effective-config", "--config", "foo.yaml"])
            .expect("parser should accept effective-config with path");
        match with_path.command.expect("command") {
            Commands::EffectiveConfig { config } => assert_eq!(config.as_deref(), Some("foo.yaml")),
            other => panic!("expected effective-config command, got {other:?}"),
        }

        let without_path = Cli::try_parse_from(["logfwd", "effective-config"])
            .expect("parser should accept effective-config without path");
        match without_path.command.expect("command") {
            Commands::EffectiveConfig { config } => assert!(config.is_none()),
            other => panic!("expected effective-config command, got {other:?}"),
        }
    }

    #[test]
    fn clap_rejects_removed_dump_config_alias() {
        let err = Cli::try_parse_from(["logfwd", "dump-config"])
            .expect_err("parser should reject removed dump-config alias");
        assert_eq!(err.kind(), ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn clap_parses_blackhole_default_form() {
        let cli = Cli::try_parse_from(["logfwd", "blackhole"])
            .expect("parser should accept blackhole without addr");
        match cli.command.expect("command") {
            Commands::Blackhole { bind_addr } => assert!(bind_addr.is_none()),
            other => panic!("expected blackhole command, got {other:?}"),
        }
    }

    #[test]
    fn clap_completions_supports_nushell_and_powershell_alias() {
        let nu = Cli::try_parse_from(["logfwd", "completions", "nushell"])
            .expect("nushell should parse");
        match nu.command.expect("command") {
            Commands::Completions { shell } => assert!(matches!(shell, CompletionShell::Nushell)),
            other => panic!("expected completions command, got {other:?}"),
        }

        let pwsh = Cli::try_parse_from(["logfwd", "completions", "pwsh"])
            .expect("pwsh alias should parse");
        match pwsh.command.expect("command") {
            Commands::Completions { shell } => {
                assert!(matches!(shell, CompletionShell::PowerShell))
            }
            other => panic!("expected completions command, got {other:?}"),
        }
    }

    #[test]
    fn clap_parses_generate_json_subcommand() {
        let cli = Cli::try_parse_from(["logfwd", "generate-json", "5", "out.json"])
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
        let err = Cli::try_parse_from(["logfwd", "help"])
            .expect_err("help subcommand should trigger help display");
        assert_eq!(err.kind(), ErrorKind::DisplayHelp);
    }

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration(std::time::Duration::from_secs(42)), "42s");
    }

    #[test]
    fn format_duration_minutes() {
        assert_eq!(
            format_duration(std::time::Duration::from_secs(125)),
            "2m 5s"
        );
    }

    #[test]
    fn format_duration_hours() {
        assert_eq!(
            format_duration(std::time::Duration::from_secs(7260)),
            "2h 1m"
        );
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
  type: null
"#;
        let config = logfwd_config::Config::load_str(yaml).expect("config should parse");
        let result = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(matches!(result, Err(CliError::Config(_))));
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
  type: null
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
  type: null
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
  type: null
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
