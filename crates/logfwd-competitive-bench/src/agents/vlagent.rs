use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use super::{Agent, Scenario, SetupState};
use crate::download::arch_alt;
use crate::fake_k8s::{FakeK8sApi, FakeK8sConfig};
use crate::runner::{self, BenchContext};

const VERSION: &str = "1.48.0";
const DOCKER_IMAGE: &str = "victoriametrics/vlagent";
const FAKE_K8S_PORT: u16 = 16443;
const VLAGENT_PORT: u16 = 9429;
const CONTAINER_ID: &str = "deadbeef0123456789abcdef";
const POD_NAME: &str = "bench-pod";
const NAMESPACE: &str = "default";
const CONTAINER_NAME: &str = "bench";
const NODE_NAME: &str = "bench-node";
const POD_UID: &str = "bench-uid-00000000-0000-0000-0000-000000000000";

pub struct Vlagent;

impl Agent for Vlagent {
    fn name(&self) -> &'static str {
        "vlagent"
    }

    fn binary_name(&self) -> &'static str {
        "vlagent-prod"
    }

    fn docker_image(&self) -> Option<String> {
        Some(format!("{DOCKER_IMAGE}:v{VERSION}"))
    }

    fn download_url(&self, os: &str, arch: &str) -> Option<String> {
        let alt = arch_alt(arch);
        Some(format!(
            "https://github.com/VictoriaMetrics/VictoriaLogs/releases/download/v{VERSION}/vlutils-{os}-{alt}-v{VERSION}.tar.gz"
        ))
    }

    fn write_config(&self, ctx: &BenchContext, _scenario: Scenario) -> Result<PathBuf, String> {
        // vlagent uses CLI flags, not a config file. Write a kubeconfig instead.
        // Use the blackhole host IP so the Docker config rewriter can replace it
        // with the Docker-accessible address (host.docker.internal on macOS).
        let host = ctx.blackhole_addr.split(':').next().unwrap_or("127.0.0.1");
        let kubeconfig_path = ctx.bench_dir.join("vlagent-kubeconfig");
        let kubeconfig = format!(
            r"apiVersion: v1
kind: Config
clusters:
  - cluster:
      server: http://{host}:{FAKE_K8S_PORT}
      insecure-skip-tls-verify: true
    name: fake
contexts:
  - context:
      cluster: fake
      user: fake
      namespace: {NAMESPACE}
    name: fake
current-context: fake
users:
  - name: fake
    user: {{}}
"
        );
        std::fs::write(&kubeconfig_path, kubeconfig).map_err(|e| e.to_string())?;
        Ok(kubeconfig_path)
    }

    fn command(&self, binary: &Path, _config: &Path, ctx: &BenchContext) -> Command {
        let containers_dir = ctx.bench_dir.join("var/log/containers");
        let data_dir = ctx.bench_dir.join("vlagent-data");

        let mut cmd = Command::new(binary);
        cmd.arg("-kubernetesCollector")
            .arg(format!(
                "-kubernetesCollector.logsPath={}",
                containers_dir.display()
            ))
            .arg(format!(
                "-remoteWrite.url=http://{}/insert/jsonline",
                ctx.blackhole_addr
            ))
            .arg("-remoteWrite.format=jsonline")
            .arg("-remoteWrite.flushInterval=100ms")
            .arg(format!("-httpListenAddr=127.0.0.1:{VLAGENT_PORT}"))
            .arg(format!("-tmpDataPath={}", data_dir.display()));
        cmd
    }

    fn setup(&self, ctx: &BenchContext) -> Result<SetupState, String> {
        // 1. Convert JSON lines to CRI format and lay out directory structure.
        let containers_dir = ctx.bench_dir.join("var/log/containers");
        let pods_dir = ctx.bench_dir.join(format!(
            "var/log/pods/{NAMESPACE}_{POD_NAME}_{POD_UID}/{CONTAINER_NAME}"
        ));
        let vlagent_data = ctx.bench_dir.join("vlagent-data");
        std::fs::create_dir_all(&containers_dir).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(&pods_dir).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(&vlagent_data).map_err(|e| e.to_string())?;

        let cri_file = pods_dir.join("0.log");
        eprintln!("  Converting {} lines to CRI format...", ctx.lines);
        convert_to_cri(&ctx.data_file, &cri_file).map_err(|e| e.to_string())?;

        // Create symlink: /var/log/containers/<pod>_<ns>_<container>-<id>.log
        let symlink_name = format!("{POD_NAME}_{NAMESPACE}_{CONTAINER_NAME}-{CONTAINER_ID}.log");
        let symlink_path = containers_dir.join(symlink_name);
        #[cfg(unix)]
        std::os::unix::fs::symlink(&cri_file, &symlink_path).map_err(|e| e.to_string())?;
        #[cfg(not(unix))]
        std::fs::copy(&cri_file, &symlink_path).map_err(|e| e.to_string())?;

        // 2. Start fake K8s API.
        let fake_api = FakeK8sApi::start(
            &format!("127.0.0.1:{FAKE_K8S_PORT}"),
            FakeK8sConfig {
                pod_name: POD_NAME.to_string(),
                namespace: NAMESPACE.to_string(),
                container_name: CONTAINER_NAME.to_string(),
                container_id: CONTAINER_ID.to_string(),
                node_name: NODE_NAME.to_string(),
            },
        )?;

        // Wait for fake API to be ready.
        runner::wait_for_ready(
            &format!("http://127.0.0.1:{FAKE_K8S_PORT}/healthz"),
            Duration::from_secs(5),
        )
        .map_err(|e| format!("fake K8s API: {e}"))?;

        // 3. Set KUBECONFIG env var.
        let kubeconfig_path = ctx.bench_dir.join("vlagent-kubeconfig");
        let mut env = HashMap::new();
        env.insert(
            "KUBECONFIG".to_string(),
            kubeconfig_path.to_string_lossy().to_string(),
        );

        Ok(SetupState {
            env: Some(env),
            handles: vec![Box::new(fake_api)],
            children: Vec::new(),
        })
    }
}

/// Convert newline-delimited JSON to CRI log format.
fn convert_to_cri(json_file: &Path, cri_file: &Path) -> std::io::Result<()> {
    let reader = BufReader::new(std::fs::File::open(json_file)?);
    let mut writer =
        std::io::BufWriter::with_capacity(1024 * 1024, std::fs::File::create(cri_file)?);

    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        // CRI format: <timestamp> <stream> <partial_flag> <content>
        writeln!(
            writer,
            "2024-01-15T10:30:{:09.6}Z stdout F {}",
            i as f64 / 1_000_000.0,
            line,
        )?;
    }
    writer.flush()?;
    Ok(())
}
