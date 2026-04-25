#![allow(clippy::print_stdout, clippy::print_stderr)]
// Binary crate: user-facing CLI output is intentional.

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(unix, not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::env;
use std::io::{self, IsTerminal};

use clap::{CommandFactory, error::ErrorKind};

mod cli;
mod commands;
mod config_templates;
mod generate;
mod send;
mod validate;

use cli::{Cli, Commands, SendArgs, bold, dim, red, reset};
use commands::{discover_config, run_command};

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
  3. ./ffwd.yaml
  4. ~/.config/ffwd/config.yaml
  5. /etc/ffwd/config.yaml

Exit Codes:
  0 success
  1 configuration error
  2 runtime error";

// Exit codes.
const EXIT_OK: i32 = 0;
const EXIT_CONFIG: i32 = 1;
const EXIT_RUNTIME: i32 = 2;

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
    let cli = match cli::parse_cli_from(env::args_os(), stdin_is_terminal) {
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
