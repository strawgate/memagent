#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(unix, not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::env;
use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use tokio_util::sync::CancellationToken;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_HASH: &str = env!("LOGFWD_GIT_HASH");
const GIT_DIRTY: &str = env!("LOGFWD_GIT_DIRTY");
const BUILD_DATE: &str = env!("LOGFWD_BUILD_DATE");
const BUILD_TARGET: &str = env!("LOGFWD_TARGET");
const BUILD_PROFILE: &str = env!("LOGFWD_PROFILE");

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

/// Print a usage example with a dim comment aligned to column 42.
fn print_example(cmd: &str, comment: &str) {
    let pad = 42usize.saturating_sub(cmd.len());
    println!("  {cmd}{:pad$}{}# {comment}{}", "", dim(), reset());
}

/// Print a "did you mean ...?" hint on stderr.
fn print_hint(suggestion: &str) {
    eprintln!(
        "{}hint{}: did you mean {}{}{}?",
        dim(),
        reset(),
        bold(),
        suggestion,
        reset()
    );
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
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        // No args: check for config in well-known locations before showing help.
        if let Some(path) = discover_config() {
            eprintln!(
                "{}hint{}: found config at {}{}{} — running it.",
                dim(),
                reset(),
                bold(),
                path.display(),
                reset(),
            );
            eprintln!(
                "{}      (use --config <path> to specify a different file){}",
                dim(),
                reset()
            );
            eprintln!();
            let synth_args = vec![
                args[0].clone(),
                "--config".to_string(),
                path.to_string_lossy().to_string(),
            ];
            return match cmd_config(&synth_args).await {
                Ok(()) => EXIT_OK,
                Err(e) => {
                    eprintln!("{}error{}: {e}", red(), reset());
                    e.exit_code()
                }
            };
        }
        print_usage();
        return EXIT_OK;
    }

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return EXIT_OK;
    }

    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!(
            "logfwd {VERSION} ({GIT_HASH}{GIT_DIRTY} {BUILD_DATE}, {BUILD_TARGET}, {BUILD_PROFILE})"
        );
        return EXIT_OK;
    }

    // Normalise arg order: allow flags like --validate before --config.
    // Scan for the primary command flag regardless of position.
    let args = normalize_args(args);

    let result = match args[1].as_str() {
        "--config" | "-c" => cmd_config(&args).await,
        "--blackhole" => cmd_blackhole(&args).await,
        "--generate-json" => cmd_generate_json(&args),
        "--dump-config" => cmd_dump_config(&args),
        "--init" => cmd_init(),
        "--completions" => cmd_completions(&args),
        other => {
            eprintln!("{}error{}: unknown command: {other}", red(), reset());
            if let Some(suggestion) = suggest_flag(other) {
                print_hint(suggestion);
            }
            eprintln!("Run {}logfwd --help{} for usage.", bold(), reset());
            return EXIT_CONFIG;
        }
    };

    match result {
        Ok(()) => EXIT_OK,
        Err(e) => {
            eprintln!("{}error{}: {e}", red(), reset());
            e.exit_code()
        }
    }
}

fn print_usage() {
    println!(
        "{}logfwd{} {}v{VERSION}{} -- fast log forwarder with SQL transforms",
        bold(),
        reset(),
        dim(),
        reset(),
    );
    println!();
    println!("{}USAGE:{}", bold(), reset());
    println!("  logfwd --config <config.yaml> [--validate] [--dry-run]");
    println!("  logfwd --blackhole [bind_addr]");
    println!("  logfwd --generate-json <num_lines> <output_file>");
    println!("  logfwd --dump-config [config.yaml]");
    println!("  logfwd --init");
    println!("  logfwd --completions <shell>");
    println!();
    println!("{}OPTIONS:{}", bold(), reset());
    println!("  -c, --config <path>    Run pipeline from YAML config");
    println!("      --validate         Validate config and exit (alias: --check)");
    println!("      --dry-run          Build pipelines without running");
    println!("      --blackhole [addr] OTLP blackhole receiver (default: 127.0.0.1:4318)");
    println!("      --generate-json    Generate synthetic JSON log file");
    println!("      --dump-config      Print resolved config to stdout");
    println!("      --init             Generate starter config in current directory");
    println!("      --completions      Print shell completions (bash, zsh, fish)");
    println!("  -h, --help             Show this help");
    println!("  -V, --version          Show version");
    println!();
    println!("{}EXAMPLES:{}", bold(), reset());
    print_example("logfwd -c config.yaml", "run pipelines");
    print_example("logfwd -c config.yaml --validate", "validate config only");
    print_example("logfwd -c config.yaml --dry-run", "test SQL planning");
    print_example("logfwd --blackhole", "start OTLP test receiver");
    print_example("logfwd --generate-json 10000 test.json", "synthetic data");
    print_example("logfwd --dump-config config.yaml", "show resolved config");
    print_example("logfwd --init", "generate starter config");
    print_example("logfwd --completions bash", "output bash completions");
    println!();
    println!("{}ENVIRONMENT:{}", bold(), reset());
    println!("  LOGFWD_CONFIG          Config file path (auto-discovered if not set)");
    println!("  LOGFWD_LOG             Set log filter (e.g. LOGFWD_LOG=debug)");
    println!("  RUST_LOG               Fallback if LOGFWD_LOG is not set");
    println!();
    println!("{}CONFIG SEARCH ORDER:{}", bold(), reset());
    print_example("1. --config <path>", "explicit flag always wins");
    print_example("2. $LOGFWD_CONFIG", "environment variable");
    print_example("3. ./logfwd.yaml", "current directory");
    println!("  4. ~/.config/logfwd/config.yaml");
    println!("  5. /etc/logfwd/config.yaml");
    println!();
    println!("{}EXIT CODES:{}", bold(), reset());
    println!("  0  Success");
    println!("  1  Configuration error");
    println!("  2  Runtime error");
    println!();
    println!(
        "{}Respects NO_COLOR (https://no-color.org){}",
        dim(),
        reset(),
    );
}

// ---------------------------------------------------------------------------
// Arg normalisation
// ---------------------------------------------------------------------------

/// Reorder args so that --config/-c always appears at position 1.
///
/// Users of tools like `nginx -t -c config` expect to put flags before the
/// config path. This allows `logfwd --validate --config foo.yaml` in addition
/// to the canonical `logfwd --config foo.yaml --validate`.
fn normalize_args(args: Vec<String>) -> Vec<String> {
    // If already in canonical form, nothing to do.
    if args.get(1).is_some_and(|a| a == "--config" || a == "-c") {
        return args;
    }

    // Find --config/-c anywhere in the arg list.
    let config_pos = args
        .iter()
        .skip(1)
        .position(|a| a == "--config" || a == "-c")
        .map(|i| i + 1);

    let Some(pos) = config_pos else {
        return args;
    };

    // config_path is the value after --config/-c.
    if pos + 1 >= args.len() {
        return args;
    }

    // Build normalised list: program, --config, path, then everything else.
    let mut out = Vec::with_capacity(args.len());
    out.push(args[0].clone());
    out.push(args[pos].clone());
    out.push(args[pos + 1].clone());
    for (i, a) in args.iter().enumerate().skip(1) {
        if i == pos || i == pos + 1 {
            continue;
        }
        out.push(a.clone());
    }
    out
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn cmd_config(args: &[String]) -> Result<(), CliError> {
    if args.len() < 3 {
        eprintln!("{}error{}: --config requires a path", red(), reset(),);
        eprintln!("  logfwd --config <config.yaml> [--validate] [--dry-run]");
        return Err(CliError::Config("missing config path".to_owned()));
    }

    let config_path = &args[2];
    let mut validate_only = false;
    let mut dry_run = false;

    // Parse flags after the config path — reject unknown flags with suggestions.
    for arg in &args[3..] {
        match arg.as_str() {
            "--validate" | "--check" => validate_only = true,
            "--dry-run" => dry_run = true,
            other => {
                eprintln!("{}error{}: unknown flag: {other}", red(), reset());
                let config_flags = &["--validate", "--check", "--dry-run"];
                if let Some(closest) = closest_match(other, config_flags) {
                    print_hint(closest);
                }
                eprintln!("  logfwd --config <config.yaml> [--validate] [--dry-run]");
                return Err(CliError::Config(format!("unknown flag: {other}")));
            }
        }
    }

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
        // Both --validate and --dry-run build pipelines to catch SQL/wiring errors.
        return validate_pipelines(&config, dry_run, base_path);
    }

    run_pipelines(config, base_path, config_path, &config_yaml).await
}

async fn cmd_blackhole(args: &[String]) -> Result<(), CliError> {
    let addr = args.get(2).map_or("127.0.0.1:4318", String::as_str);

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

fn cmd_generate_json(args: &[String]) -> Result<(), CliError> {
    if args.len() < 4 {
        eprintln!(
            "{}error{}: --generate-json requires <num_lines> <output_file>",
            red(),
            reset(),
        );
        return Err(CliError::Config("missing arguments".to_owned()));
    }
    let num_lines: usize = match args[2].parse() {
        Ok(n) => n,
        Err(e) => {
            return Err(CliError::Config(format!("invalid num_lines: {e}")));
        }
    };
    generate_json_log_file(num_lines, &args[3]).map_err(CliError::Runtime)
}

fn cmd_dump_config(args: &[String]) -> Result<(), CliError> {
    let config_path = if args.len() >= 3 {
        args[2].clone()
    } else if let Some(path) = discover_config() {
        path.to_string_lossy().to_string()
    } else {
        eprintln!(
            "{}error{}: --dump-config requires a config file",
            red(),
            reset()
        );
        eprintln!("  logfwd --dump-config <config.yaml>");
        return Err(CliError::Config("no config file found".to_owned()));
    };

    let config_yaml = std::fs::read_to_string(&config_path)
        .map_err(|e| CliError::Config(format!("cannot read {config_path}: {e}")))?;

    // Validate that the config parses before dumping.
    logfwd_config::Config::load_str(&config_yaml).map_err(|e| CliError::Config(e.to_string()))?;

    eprintln!("{}# validated from {config_path}{}", dim(), reset());
    print!("{config_yaml}");
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
  endpoint: http://localhost:4318

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
        "{}created{} {} — edit it, then run: {}logfwd -c logfwd.yaml --validate{}",
        green(),
        reset(),
        path.display(),
        bold(),
        reset(),
    );
    Ok(())
}

fn cmd_completions(args: &[String]) -> Result<(), CliError> {
    let shell = args.get(2).map_or("", String::as_str);
    match shell {
        "bash" => print!("{}", completions_bash()),
        "zsh" => print!("{}", completions_zsh()),
        "fish" => print!("{}", completions_fish()),
        other => {
            let msg = if other.is_empty() {
                "--completions requires a shell name".to_string()
            } else {
                format!("unknown shell: {other}")
            };
            eprintln!("{}error{}: {msg}", red(), reset());
            eprintln!("  logfwd --completions <bash|zsh|fish>");
            return Err(CliError::Config(msg));
        }
    }
    Ok(())
}

fn completions_bash() -> &'static str {
    r#"# logfwd bash completion — add to ~/.bashrc or /etc/bash_completion.d/logfwd
_logfwd() {
    local cur prev commands
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    commands="--config -c --blackhole --generate-json --dump-config --init --completions --help -h --version -V"

    case "$prev" in
        --config|-c|--dump-config)
            COMPREPLY=( $(compgen -f -X '!*.yaml' -- "$cur") $(compgen -f -X '!*.yml' -- "$cur") $(compgen -d -- "$cur") )
            return 0
            ;;
        --completions)
            COMPREPLY=( $(compgen -W "bash zsh fish" -- "$cur") )
            return 0
            ;;
    esac

    # After --config <path>, suggest sub-flags
    local i has_config=0
    for (( i=1; i < COMP_CWORD; i++ )); do
        case "${COMP_WORDS[i]}" in
            --config|-c) has_config=1 ;;
        esac
    done
    if (( has_config )); then
        COMPREPLY=( $(compgen -W "--validate --check --dry-run" -- "$cur") )
        return 0
    fi

    COMPREPLY=( $(compgen -W "$commands" -- "$cur") )
}
complete -F _logfwd logfwd
"#
}

fn completions_zsh() -> &'static str {
    r#"#compdef logfwd
# logfwd zsh completion — add to your fpath or source directly

_logfwd() {
    local -a commands subflags
    commands=(
        '--config[Run pipeline from YAML config]:config file:_files -g "*.y(a|)ml"'
        '-c[Run pipeline from YAML config]:config file:_files -g "*.y(a|)ml"'
        '--blackhole[OTLP blackhole receiver]:bind addr:'
        '--generate-json[Generate synthetic JSON log file]:lines: :output file:_files'
        '--dump-config[Print resolved config]:config file:_files -g "*.y(a|)ml"'
        '--init[Generate starter config]'
        '--completions[Print shell completions]:shell:(bash zsh fish)'
        '--help[Show help]'
        '-h[Show help]'
        '--version[Show version]'
        '-V[Show version]'
    )
    subflags=(
        '--validate[Validate config and exit]'
        '--check[Validate config and exit]'
        '--dry-run[Build pipelines without running]'
    )

    # If --config/-c already appears, offer subflags
    if (( ${words[(I)--config]} || ${words[(I)-c]} )); then
        _describe 'flag' subflags
    else
        _describe 'command' commands
    fi
}

_logfwd "$@"
"#
}

fn completions_fish() -> &'static str {
    r"# logfwd fish completion — save to ~/.config/fish/completions/logfwd.fish

# Disable file completions by default
complete -c logfwd -f

# Top-level commands
complete -c logfwd -l config -s c -d 'Run pipeline from YAML config' -rF
complete -c logfwd -l blackhole -d 'OTLP blackhole receiver'
complete -c logfwd -l generate-json -d 'Generate synthetic JSON log file'
complete -c logfwd -l dump-config -d 'Print resolved config' -rF
complete -c logfwd -l init -d 'Generate starter config'
complete -c logfwd -l completions -d 'Print shell completions' -xa 'bash zsh fish'
complete -c logfwd -l help -s h -d 'Show help'
complete -c logfwd -l version -s V -d 'Show version'

# Sub-flags for --config
complete -c logfwd -l validate -d 'Validate config and exit' -n '__fish_seen_argument -l config -s c'
complete -c logfwd -l check -d 'Validate config and exit' -n '__fish_seen_argument -l config -s c'
complete -c logfwd -l dry-run -d 'Build pipelines without running' -n '__fish_seen_argument -l config -s c'
"
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
// Fuzzy flag suggestions (Levenshtein distance)
// ---------------------------------------------------------------------------

/// Suggest the closest known top-level command flag for a mistyped input.
fn suggest_flag(input: &str) -> Option<&'static str> {
    let known = &[
        "--config",
        "-c",
        "--blackhole",
        "--generate-json",
        "--dump-config",
        "--init",
        "--completions",
        "--help",
        "-h",
        "--version",
        "-V",
    ];
    closest_match(input, known)
}

/// Return the closest string from `candidates` if within edit distance 3.
fn closest_match<'a>(input: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let mut best: Option<(&str, usize)> = None;
    for &candidate in candidates {
        let dist = edit_distance(input, candidate);
        if dist <= 3 && best.as_ref().is_none_or(|(_, d)| dist < *d) {
            best = Some((candidate, dist));
        }
    }
    best.map(|(s, _)| s)
}

/// Levenshtein edit distance between two strings.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let m = a.len();
    let n = b.len();

    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

// ---------------------------------------------------------------------------
// Pipeline runner
// ---------------------------------------------------------------------------

/// Validate config by building all pipelines. Used by --validate and --dry-run.
fn validate_pipelines(
    config: &logfwd_config::Config,
    dry_run: bool,
    base_path: Option<&std::path::Path>,
) -> Result<(), CliError> {
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
                    eprintln!(
                        "  {}error{}: pipeline '{name}' SQL plan: {e}",
                        red(),
                        reset()
                    );
                    errors += 1;
                    continue;
                }
                // Success output goes to stdout so scripts can capture it.
                println!("  {}ready{}: {}{name}{}", green(), reset(), bold(), reset());
            }
            Err(e) => {
                eprintln!("  {}error{}: pipeline '{name}': {e}", red(), reset());
                errors += 1;
            }
        }
    }

    if errors > 0 {
        return Err(CliError::Config(format!(
            "{errors} error(s) during validation"
        )));
    }

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

    let mut _diag_server = None;
    let diag_handle = if let Some(ref addr) = config.server.diagnostics {
        let mut server = DiagnosticsServer::new(addr);
        server.set_config(config_path, config_yaml);
        server.set_trace_buffer(trace_buf);
        for p in &pipelines {
            server.add_pipeline(Arc::clone(p.metrics()));
        }
        #[cfg(unix)]
        server.set_memory_stats_fn(jemalloc_stats);
        let _bound_addr = server.start()?;
        _diag_server = Some(server);
        Some(addr.clone())
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
    if let Some(ref addr) = diag_handle {
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
        .map(|m| m.transform_in.bytes_total.load(Relaxed))
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
    fn edit_distance_identical() {
        assert_eq!(edit_distance("--config", "--config"), 0);
    }

    #[test]
    fn edit_distance_one_off() {
        assert_eq!(edit_distance("--confg", "--config"), 1);
    }

    #[test]
    fn edit_distance_swap() {
        assert_eq!(edit_distance("--conifg", "--config"), 2);
    }

    #[test]
    fn suggest_flag_typo() {
        assert_eq!(suggest_flag("--confg"), Some("--config"));
        assert_eq!(suggest_flag("--confi"), Some("--config"));
        assert_eq!(suggest_flag("--blackhol"), Some("--blackhole"));
        assert_eq!(suggest_flag("--versin"), Some("--version"));
    }

    #[test]
    fn suggest_flag_no_match() {
        assert_eq!(suggest_flag("--something-totally-different"), None);
    }

    #[test]
    fn closest_match_prefers_exact() {
        let candidates = &["--check", "--config"];
        assert_eq!(closest_match("--check", candidates), Some("--check"));
    }

    #[test]
    fn closest_match_within_threshold() {
        let flags = &["--validate", "--check", "--dry-run"];
        assert_eq!(closest_match("--validat", flags), Some("--validate"));
        assert_eq!(closest_match("--chck", flags), Some("--check"));
        assert_eq!(closest_match("--dry-ru", flags), Some("--dry-run"));
    }

    #[test]
    fn normalize_args_canonical() {
        let args = vec![
            "logfwd".to_string(),
            "--config".to_string(),
            "foo.yaml".to_string(),
            "--validate".to_string(),
        ];
        let out = normalize_args(args.clone());
        assert_eq!(out, args);
    }

    #[test]
    fn normalize_args_reorders() {
        let args = vec![
            "logfwd".to_string(),
            "--validate".to_string(),
            "--config".to_string(),
            "foo.yaml".to_string(),
        ];
        let out = normalize_args(args);
        assert_eq!(out[1], "--config");
        assert_eq!(out[2], "foo.yaml");
        assert_eq!(out[3], "--validate");
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
    fn suggest_flag_includes_new_commands() {
        assert_eq!(suggest_flag("--ini"), Some("--init"));
        assert_eq!(suggest_flag("--completion"), Some("--completions"));
        assert_eq!(suggest_flag("--dump-confi"), Some("--dump-config"));
    }

    #[test]
    fn json_logs_are_used_only_when_stderr_is_not_a_tty() {
        assert!(!use_json_logs_for_stderr(true));
        assert!(use_json_logs_for_stderr(false));
    }
}
