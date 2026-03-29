use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Agent, SetupState};
use crate::runner::BenchContext;

pub struct Logfwd;

impl Agent for Logfwd {
    fn name(&self) -> &str {
        "logfwd"
    }

    fn binary_name(&self) -> &str {
        "logfwd"
    }

    fn download_url(&self, _os: &str, _arch: &str) -> Option<String> {
        // logfwd is built locally; never downloaded.
        None
    }

    fn write_config(&self, ctx: &BenchContext) -> Result<PathBuf, String> {
        let cfg_path = ctx.bench_dir.join("logfwd.yaml");
        let config = format!(
            r#"server:
  diagnostics: "127.0.0.1:19876"
pipelines:
  bench:
    inputs:
      - type: file
        path: "{data_file}"
        format: json
    transform: "SELECT * FROM logs"
    outputs:
      - type: http
        endpoint: "http://{blackhole}"
        format: json
"#,
            data_file = ctx.data_file.display(),
            blackhole = ctx.blackhole_addr,
        );
        std::fs::write(&cfg_path, config).map_err(|e| e.to_string())?;
        Ok(cfg_path)
    }

    fn command(&self, binary: &Path, config: &Path, _ctx: &BenchContext) -> Command {
        let mut cmd = Command::new(binary);
        cmd.arg("--config").arg(config);
        cmd
    }

    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }
}
