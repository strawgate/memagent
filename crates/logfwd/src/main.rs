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
    use logfwd::transform::SqlTransform;
    #[cfg(feature = "datafusion")]
    use logfwd_config::{EnrichmentConfig, GeoDatabaseFormat};
    use logfwd_config::{Format, InputType};
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
        InputType::Generator
        | InputType::Otlp
        | InputType::LinuxSensorBeta
        | InputType::MacosSensorBeta
        | InputType::WindowsSensorBeta => matches!(format, Format::Json),
        InputType::Http => matches!(format, Format::Json | Format::Raw),
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

async fn run_pipelines(
    config: logfwd_config::Config,
    base_path: Option<&std::path::Path>,
    config_path: &str,
    config_yaml: &str,
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
        },
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
fn format_duration(d: std::time::Duration) -> String {
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
