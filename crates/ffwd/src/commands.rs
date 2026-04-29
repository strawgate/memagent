//! Subcommand handler functions, wizard prompts, and config discovery.

use std::io::{self, IsTerminal, Write};
use std::time::Duration;

use clap::CommandFactory;

use crate::VERSION;
use crate::cli::{
    BlastArgs, BlastDestination, Cli, CliError, Commands, CompletionShell, DevourArgs, DevourMode,
    SendArgs, bold, dim, green, red, reset, use_color, use_json_logs_for_stderr,
    wizard_uses_interactive_terminals, yellow,
};
use crate::generate::generate_json_log_file;
use crate::send::{build_stdin_send_config_yaml, resolve_send_config_path};
use crate::validate::{
    validate_generated_config_read_only, validate_pipelines, validate_pipelines_read_only,
};

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

pub(crate) async fn run_command(command: Commands) -> Result<(), CliError> {
    match command {
        Commands::Run {
            config,
            watch_config,
        } => {
            let config_path = resolve_config_path(config.as_deref())?;
            cmd_run(&config_path, false, false, watch_config).await
        }
        Commands::Validate { config } => {
            let config_path = resolve_config_path(config.as_deref())?;
            cmd_run(&config_path, true, false, false).await
        }
        Commands::DryRun { config } => {
            let config_path = resolve_config_path(config.as_deref())?;
            cmd_run(&config_path, false, true, false).await
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
        #[cfg(unix)]
        Commands::Supervised { config } => {
            let config_path = resolve_config_path(config.as_deref())?;
            crate::supervisor::cmd_supervised(&config_path).await
        }
    }
}

// ---------------------------------------------------------------------------
// Individual commands
// ---------------------------------------------------------------------------

async fn cmd_run(
    config_path: &str,
    validate_only: bool,
    dry_run: bool,
    watch_config: bool,
) -> Result<(), CliError> {
    let config_yaml = std::fs::read_to_string(config_path)
        .map_err(|e| CliError::Config(format!("cannot read {config_path}: {e}")))?;
    let base_path = std::path::Path::new(config_path).parent();
    let config = match ffwd_config::Config::load_str_with_base_path(&config_yaml, base_path) {
        Ok(c) => c,
        Err(e) => {
            return Err(CliError::Config(e.to_string()));
        }
    };

    if validate_only || dry_run {
        // Both `validate` and `dry-run` build pipelines to catch SQL/wiring errors.
        return validate_pipelines(&config, dry_run, base_path);
    }

    run_pipelines(
        config,
        base_path,
        config_path,
        &config_yaml,
        None,
        watch_config,
    )
    .await
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
    let config = ffwd_config::Config::load_str_with_base_path(&runnable_yaml, base_path)
        .map_err(|e| CliError::Config(e.to_string()))?;

    run_pipelines(config, base_path, &config_path, &runnable_yaml, None, false).await
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
    ffwd_config::validate_host_port(&args.diagnostics_addr)
        .map_err(|e| CliError::Config(format!("invalid diagnostics address: {e}")))?;

    let destination = args
        .destination
        .ok_or_else(|| CliError::Config("blast requires --destination".to_owned()))?;
    let generated = ffwd_runtime::generated_cli::build_blast_generated_config(
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
        Some(duration) => println!("duration={duration}s"),
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
        false,
    )
    .await
}

async fn cmd_devour(args: DevourArgs) -> Result<(), CliError> {
    let listen = args
        .listen
        .clone()
        .unwrap_or_else(|| args.mode.default_listen().to_string());

    ffwd_config::validate_host_port(&listen)
        .map_err(|e| CliError::Config(format!("invalid listen address: {e}")))?;
    ffwd_config::validate_host_port(&args.diagnostics_addr)
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
        println!("duration={duration}s");
    }

    let generated = ffwd_runtime::generated_cli::build_devour_generated_config(
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
        false,
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

pub(crate) fn maybe_prompt_blast_setup(args: &mut BlastArgs) -> Result<(), CliError> {
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
        .map_err(|_e| CliError::Config("Workers must be a positive integer".to_owned()))?;
    args.batch_lines = prompt_text("Batch lines", &args.batch_lines.to_string())?
        .parse::<usize>()
        .map_err(|_e| CliError::Config("Batch lines must be a positive integer".to_owned()))?;
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
        Some(duration_raw.parse::<u64>().map_err(|_e| {
            CliError::Config("Duration seconds must be a positive integer".to_owned())
        })?)
    };

    Ok(())
}

#[cfg(test)]
pub(crate) fn resolve_blast_output_config(
    args: &BlastArgs,
) -> Result<ffwd_config::OutputConfigV2, CliError> {
    let destination = args
        .destination
        .ok_or_else(|| CliError::Config("blast requires --destination".to_owned()))?;
    ffwd_runtime::generated_cli::resolve_blast_output_config(
        destination.as_output_type(),
        args.endpoint.as_deref(),
        args.auth_bearer_token.as_deref(),
        &args.auth_header,
    )
    .map_err(CliError::Config)
}

#[cfg(test)]
pub(crate) fn render_devour_yaml(args: &DevourArgs, listen: &str) -> String {
    ffwd_runtime::generated_cli::render_devour_yaml(
        args.mode.spec(),
        listen,
        &args.diagnostics_addr,
    )
}

#[cfg(test)]
pub(crate) fn yaml_quote(value: &str) -> String {
    ffwd_runtime::generated_cli::yaml_quote(value)
}

fn cmd_generate_json(num_lines: usize, output_file: &str) -> Result<(), CliError> {
    generate_json_log_file(num_lines, output_file).map_err(CliError::Runtime)
}

fn cmd_effective_config(config_path: Option<&str>) -> Result<(), CliError> {
    let config_path = resolve_config_path(config_path)?;

    let config_yaml = std::fs::read_to_string(&config_path)
        .map_err(|e| CliError::Config(format!("cannot read {config_path}: {e}")))?;
    let effective_yaml = ffwd_config::Config::expand_env_yaml_str(&config_yaml)
        .map_err(|e| CliError::Config(e.to_string()))?;

    // Read-only validation for inspection flows: reject configs that would
    // fail format or SQL-plan checks without constructing runtime inputs that
    // bind sockets or touch long-lived resources.
    let base_path = std::path::Path::new(&config_path).parent();
    let config = ffwd_config::Config::load_str_with_base_path(&config_yaml, base_path)
        .map_err(|e| CliError::Config(e.to_string()))?;
    validate_pipelines_read_only(
        &config,
        base_path,
        |_name| {},
        |err| eprintln!("  {}error{}: {err}", red(), reset()),
    )?;

    let redacted_yaml = ffwd_diagnostics::diagnostics::redact_config_yaml(&effective_yaml);
    eprintln!("{}# validated from {config_path}{}", dim(), reset());
    print!("{redacted_yaml}");
    Ok(())
}

fn cmd_init() -> Result<(), CliError> {
    let path = std::path::Path::new("ffwd.yaml");

    let template = r#"# ff configuration
# Docs: https://github.com/strawgate/fastforward

# ── Quick start ─────────────────────────────────────────────
# 1. Generate test data:   ff generate-json 10000 logs.json
# 2. Validate this config: ff validate --config ffwd.yaml
# 3. Run the pipeline:     ff run --config ffwd.yaml
#
# For production, change the input path and output to your real
# source/destination — see the examples at https://github.com/strawgate/fastforward/tree/main/examples/use-cases/
# Or run `ff wizard` for an interactive setup.

pipelines:
  default:
    # Tail a JSON log file and stream new lines as they appear.
    inputs:
      - type: file
        path: ./logs.json
        format: json

    # SQL transform (optional) — filter, reshape, or enrich logs.
    # Remove this section to forward all logs unmodified.
    transform: |
      SELECT * FROM logs
      WHERE level IN ('WARN', 'ERROR')

    # Print matching logs to the terminal so you can see results immediately.
    outputs:
      - type: stdout

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
        "  ff generate-json 10000 logs.json    {}# create sample data{}",
        dim(),
        reset()
    );
    eprintln!(
        "  ff run --config ffwd.yaml          {}# run the pipeline{}",
        dim(),
        reset()
    );
    Ok(())
}

fn cmd_wizard() -> Result<(), CliError> {
    use crate::config_templates::{
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
        let uc = USE_CASE_TEMPLATES.get(uc_idx).ok_or_else(|| {
            CliError::Config(format!("selected use-case index {uc_idx} is invalid"))
        })?;
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

        let input = INPUT_TEMPLATES.get(input_idx).ok_or_else(|| {
            CliError::Config(format!("selected input index {input_idx} is invalid"))
        })?;
        let output = OUTPUT_TEMPLATES.get(output_idx).ok_or_else(|| {
            CliError::Config(format!("selected output index {output_idx} is invalid"))
        })?;
        render_config(input, output, sql)
    };

    let output_path = prompt_text("Output file path", "ffwd.generated.yaml")?;
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

// ---------------------------------------------------------------------------
// Prompt helpers
// ---------------------------------------------------------------------------

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

pub(crate) fn prompt_text(prompt: &str, default: &str) -> Result<String, CliError> {
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

pub(crate) fn read_wizard_line<R: io::BufRead>(reader: &mut R) -> Result<String, CliError> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(CliError::Config(
            "stdin closed while reading wizard input".to_owned(),
        ));
    }
    Ok(line)
}

// ---------------------------------------------------------------------------
// Config auto-discovery
// ---------------------------------------------------------------------------

pub(crate) fn resolve_config_path(config_path: Option<&str>) -> Result<String, CliError> {
    if let Some(path) = config_path {
        return Ok(path.to_owned());
    }
    if let Some(path) = discover_config() {
        return Ok(path.to_string_lossy().to_string());
    }
    Err(CliError::Config(
        "no config file found (use --config or set FFWD_CONFIG)".to_owned(),
    ))
}

/// Search well-known locations for a ffwd config file.
///
/// Search order:
///   1. `$FFWD_CONFIG` (or deprecated `$LOGFWD_CONFIG`)
///   2. `./ffwd.yaml`
///   3. `~/.config/ffwd/config.yaml`
///   4. `/etc/ffwd/config.yaml`
pub(crate) fn discover_config() -> Option<std::path::PathBuf> {
    use std::env;
    use std::path::PathBuf;

    // 1. Environment variable (new name first, legacy fallback second)
    if let Ok(path) = env::var("FFWD_CONFIG") {
        let p = PathBuf::from(&path);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Ok(path) = env::var("LOGFWD_CONFIG") {
        let p = PathBuf::from(&path);
        if p.is_file() {
            tracing::warn!("LOGFWD_CONFIG is deprecated — use FFWD_CONFIG instead");
            return Some(p);
        }
    }

    // 2. Current directory
    let cwd = PathBuf::from("ffwd.yaml");
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
    if let Some(xdg) = xdg_base.map(|b| b.join("ffwd/config.yaml"))
        && xdg.is_file()
    {
        return Some(xdg);
    }

    // 4. System config
    let etc = PathBuf::from("/etc/ffwd/config.yaml");
    if etc.is_file() {
        return Some(etc);
    }

    None
}

// ---------------------------------------------------------------------------
// Pipeline runner
// ---------------------------------------------------------------------------

pub(crate) async fn run_pipelines(
    config: ffwd_config::Config,
    base_path: Option<&std::path::Path>,
    config_path: &str,
    config_yaml: &str,
    auto_shutdown_after: Option<Duration>,
    watch_config: bool,
) -> Result<(), CliError> {
    ffwd_runtime::bootstrap::run_pipelines(
        config,
        base_path,
        ffwd_runtime::bootstrap::RunOptions {
            config_path,
            config_yaml,
            version: VERSION,
            use_color: use_color(),
            json_logs_for_stderr: use_json_logs_for_stderr(io::stderr().is_terminal()),
            auto_shutdown_after,
            watch_config,
        },
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn format_duration(d: Duration) -> String {
    ffwd_runtime::bootstrap::format_duration(d)
}

#[cfg(test)]
pub(crate) fn format_count(n: u64) -> String {
    ffwd_runtime::bootstrap::format_count(n)
}

#[cfg(test)]
pub(crate) fn format_bytes(b: u64) -> String {
    ffwd_runtime::bootstrap::format_bytes(b)
}

/// Test-only synchronous wrapper around [`cmd_effective_config`].
#[cfg(test)]
pub(crate) fn run_command_sync_effective_config(config_path: Option<&str>) -> Result<(), CliError> {
    cmd_effective_config(config_path)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

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
        let ffwd_config::OutputConfigV2::Tcp(output_cfg) = output_cfg else {
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
            .block_on(run_command(Commands::Devour(args)))
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
            .block_on(run_command(Commands::Blast(args)))
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
            .block_on(run_command(Commands::Blast(args)))
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
            .block_on(run_command(Commands::Blast(args)))
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
            r#"pipelines:
  default:
    inputs:
      - type: otlp
        listen: 127.0.0.1:{port}
    outputs:
      - type: "null"
"#
        )
        .expect("write config");

        run_command_sync_effective_config(Some(file.path().to_str().expect("utf-8 temp path")))
            .expect("read-only validation should not bind the configured port");
    }

    #[test]
    fn templates_avoid_unsupported_syslog_and_bare_otlp_http_endpoints() {
        use crate::config_templates;
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
    fn wizard_template_renders_with_sql() {
        use crate::config_templates;
        let input = &config_templates::INPUT_TEMPLATES[0];
        let output = &config_templates::OUTPUT_TEMPLATES[0];
        let cfg = config_templates::render_config(
            input,
            output,
            "SELECT * FROM logs WHERE level='ERROR'",
        );
        assert!(cfg.contains("pipelines:"));
        assert!(cfg.contains("inputs:"));
        assert!(cfg.contains("outputs:"));
        assert!(cfg.contains("    transform: |"));
        assert!(cfg.contains("WHERE level='ERROR'"));
    }

    #[test]
    fn wizard_use_case_renders_pipelines_only() {
        use crate::config_templates;
        let use_case = &config_templates::USE_CASE_TEMPLATES[0];
        let cfg = config_templates::render_use_case(use_case, use_case.transform);
        assert!(cfg.contains("pipelines:"));
        assert!(cfg.contains("inputs:"));
        assert!(cfg.contains("outputs:"));
        assert!(!cfg.contains("\ninput:\n"));
        assert!(!cfg.contains("\noutput:\n"));
    }

    #[test]
    fn wizard_loki_templates_preserve_nested_yaml() {
        use crate::config_templates;

        let input = &config_templates::INPUT_TEMPLATES[0];
        let output = config_templates::OUTPUT_TEMPLATES
            .iter()
            .find(|template| template.id == "loki")
            .expect("loki output template");
        let cfg = config_templates::render_config(input, output, "SELECT * FROM logs");
        assert!(cfg.contains("        static_labels:\n          service: myapp"));
        assert!(cfg.contains("        label_columns:\n          - level"));
        ffwd_config::Config::load_str(&cfg).expect("rendered loki template should parse");

        let use_case = config_templates::USE_CASE_TEMPLATES
            .iter()
            .find(|template| template.id == "nginx_access_to_loki")
            .expect("loki use-case template");
        let cfg = config_templates::render_use_case(use_case, use_case.transform);
        assert!(cfg.contains("        static_labels:\n          service: myapp"));
        assert!(cfg.contains("        label_columns:\n          - level"));
        ffwd_config::Config::load_str(&cfg).expect("rendered loki use case should parse");
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
}
