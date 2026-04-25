#![allow(clippy::print_stdout)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use logfwd_config::docspec::{self, INPUT_TYPE_DOCS, OUTPUT_TYPE_DOCS};
use quote::ToTokens;
use regex::Regex;
use syn::visit::Visit;
use syn::{Item, ItemFn};
use walkdir::WalkDir;

const ALLOW_PREFIX: &str = "xtask-verify: allow(";

#[derive(Parser, Debug)]
#[command(name = "xtask")]
#[command(about = "Repository maintenance tasks")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Regenerate config support tables in user documentation.
    GenerateConfigDocs,
    /// Run structural verification checks.
    Verify,
}

#[derive(Debug, Clone)]
struct ModuleFacts {
    path: String,
    text: String,
    pub_fns: Vec<FnFacts>,
    has_tests: bool,
    has_roundtrip_tests: bool,
    exemptions: HashSet<String>,
    kani_proofs: Vec<String>,
}

#[derive(Debug, Clone)]
struct FnFacts {
    name: String,
    line: usize,
    is_pub: bool,
    is_test_only: bool,
    is_kani_only: bool,
    is_non_trivial: bool,
}

#[derive(Debug, Clone)]
struct Finding {
    check: &'static str,
    path: String,
    line: usize,
    message: String,
}

#[derive(Debug, Default)]
struct Collector {
    pub_fns: Vec<FnFacts>,
    has_tests: bool,
    kani_proofs: Vec<String>,
}

impl<'ast> Visit<'ast> for Collector {
    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        let is_pub = matches!(node.vis, syn::Visibility::Public(_));
        let mut is_test_only = false;
        let mut is_kani_only = false;
        let mut is_proof = false;

        for attr in &node.attrs {
            if attr.path().is_ident("test") {
                is_test_only = true;
            }
            if attr.path().is_ident("cfg") {
                let txt = attr.meta.to_token_stream().to_string();
                if txt.contains("test") {
                    is_test_only = true;
                }
                if txt.contains("kani") {
                    is_kani_only = true;
                }
            }
            if attr.path().segments.iter().any(|s| s.ident == "proof") {
                is_proof = true;
            }
        }

        if is_proof {
            self.kani_proofs.push(node.sig.ident.to_string());
        }

        if is_test_only {
            self.has_tests = true;
        }

        let is_non_trivial = is_non_trivial_fn(node);
        self.pub_fns.push(FnFacts {
            name: node.sig.ident.to_string(),
            line: 1,
            is_pub,
            is_test_only,
            is_kani_only,
            is_non_trivial,
        });

        syn::visit::visit_item_fn(self, node);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        let mut cfg_test = false;
        let mut cfg_kani = false;
        for attr in &node.attrs {
            if attr.path().is_ident("cfg") {
                let txt = attr.meta.to_token_stream().to_string();
                if txt.contains("test") {
                    cfg_test = true;
                }
                if txt.contains("kani") {
                    cfg_kani = true;
                }
            }
        }

        if cfg_test {
            self.has_tests = true;
        }
        if cfg_kani {
            for item in node.content.iter().flat_map(|(_, items)| items.iter()) {
                if let Item::Fn(f) = item
                    && f.attrs
                        .iter()
                        .any(|a| a.path().segments.iter().any(|s| s.ident == "proof"))
                {
                    self.kani_proofs.push(f.sig.ident.to_string());
                }
            }
        }

        syn::visit::visit_item_mod(self, node);
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::GenerateConfigDocs => run_generate_config_docs(),
        Commands::Verify => run_verify(),
    }
}

fn run_verify() -> Result<()> {
    let repo_root = repo_root()?;

    let modules = load_modules(&repo_root)?;
    let mut findings = Vec::new();

    check_pub_fn_needs_proof(&modules, &mut findings);
    check_unsafe_needs_safety_comment(&modules, &mut findings)?;
    check_pub_module_needs_tests(&modules, &mut findings);
    check_encode_decode_roundtrip(&modules, &mut findings);
    check_trust_boundary_manifest(&repo_root, &mut findings)?;
    check_proof_count_manifest(&repo_root, &modules, &mut findings)?;

    if findings.is_empty() {
        println!("xtask verify: OK (all checks passed)");
        return Ok(());
    }

    findings.sort_by(|a, b| {
        a.check
            .cmp(b.check)
            .then(a.path.cmp(&b.path))
            .then(a.line.cmp(&b.line))
    });

    println!(
        "xtask verify: {} finding(s) (warnings — not yet enforced as errors)",
        findings.len()
    );
    for finding in &findings {
        println!(
            "  warning: [{}] {}:{} {}",
            finding.check, finding.path, finding.line, finding.message
        );
    }

    // TODO(#2409): promote to hard failure once known gaps are addressed
    Ok(())
}

fn run_generate_config_docs() -> Result<()> {
    let repo_root = repo_root()?;
    let reference_path = repo_root.join("book/src/content/docs/configuration/reference.mdx");
    let mut reference = fs::read_to_string(&reference_path).with_context(|| {
        format!(
            "failed to read config reference {}",
            reference_path.display()
        )
    })?;

    reference = replace_generated_block(
        &reference,
        "input-types",
        &docspec::render_component_type_table(INPUT_TYPE_DOCS),
    )?;
    reference = replace_generated_block(
        &reference,
        "output-types",
        &docspec::render_component_type_table(OUTPUT_TYPE_DOCS),
    )?;

    fs::write(&reference_path, reference).with_context(|| {
        format!(
            "failed to write regenerated config reference {}",
            reference_path.display()
        )
    })?;

    println!(
        "xtask generate-config-docs: updated {}",
        reference_path.display()
    );
    Ok(())
}

fn repo_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("xtask must live under repo root")
        .map(Path::to_path_buf)
}

fn replace_generated_block(document: &str, name: &str, content: &str) -> Result<String> {
    let begin = format!("{{/* BEGIN GENERATED: {name} */}}");
    let end = format!("{{/* END GENERATED: {name} */}}");
    let start = document
        .find(&begin)
        .with_context(|| format!("missing begin marker for generated block '{name}'"))?;
    let block_start = start + begin.len();
    let end_index = document[block_start..]
        .find(&end)
        .map(|offset| block_start + offset)
        .with_context(|| format!("missing end marker for generated block '{name}'"))?;

    let mut updated = String::with_capacity(document.len() + content.len());
    updated.push_str(&document[..block_start]);
    updated.push('\n');
    updated.push_str(content.trim_matches('\n'));
    updated.push('\n');
    updated.push_str(&document[end_index..]);
    Ok(updated)
}

fn load_modules(repo_root: &Path) -> Result<Vec<ModuleFacts>> {
    let mut modules = Vec::new();
    for entry in WalkDir::new(repo_root.join("crates")) {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type().is_file() || path.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        let rel = path
            .strip_prefix(repo_root)
            .context("path must be under repository root")?
            .to_string_lossy()
            .replace('\\', "/");
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read source file {rel}"))?;
        let syntax = syn::parse_file(&text).with_context(|| format!("failed to parse {rel}"))?;
        let mut collector = Collector::default();
        collector.visit_file(&syntax);
        modules.push(ModuleFacts {
            path: rel,
            has_roundtrip_tests: detect_roundtrip_tests(&text),
            exemptions: parse_exemptions(&text),
            text,
            pub_fns: collector.pub_fns,
            has_tests: collector.has_tests,
            kani_proofs: collector.kani_proofs,
        });
    }
    Ok(modules)
}

fn is_production_src(path: &str) -> bool {
    if !path.contains("/src/") {
        return false;
    }
    if path.contains("/tests/") || path.contains("/fuzz/") || path.contains("/generated/") {
        return false;
    }
    if path.ends_with("/main.rs") {
        return false;
    }
    if path.starts_with("crates/logfwd-bench/")
        || path.starts_with("crates/logfwd-proto-build/")
        || path.starts_with("crates/logfwd-ebpf-proto/")
        || path.starts_with("crates/logfwd-config-wasm/")
        || path.starts_with("crates/logfwd-lints/")
        || path.starts_with("crates/logfwd-lint-attrs/")
        || path.starts_with("crates/logfwd-test-utils/")
    {
        return false;
    }
    true
}

fn parse_exemptions(text: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(start) = trimmed.find(ALLOW_PREFIX) {
            let rest = &trimmed[start + ALLOW_PREFIX.len()..];
            if let Some(end) = rest.find(')') {
                out.insert(rest[..end].trim().to_string());
            }
        }
    }
    out
}

fn is_allowed(module: &ModuleFacts, check: &str) -> bool {
    module.exemptions.contains(check)
}

fn detect_roundtrip_tests(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("roundtrip") && (lower.contains("#[test]") || lower.contains("proptest!"))
}

fn is_non_trivial_fn(node: &ItemFn) -> bool {
    if node.block.stmts.len() > 2 {
        return true;
    }
    for stmt in &node.block.stmts {
        let stmt_txt = stmt.to_token_stream().to_string();
        if stmt_txt.contains("match ")
            || stmt_txt.contains("if ")
            || stmt_txt.contains("for ")
            || stmt_txt.contains("while ")
            || stmt_txt.contains("loop ")
            || stmt_txt.contains('?')
        {
            return true;
        }
    }
    false
}

fn check_pub_fn_needs_proof(modules: &[ModuleFacts], findings: &mut Vec<Finding>) {
    let check = "pub_fn_needs_proof";
    for module in modules {
        if !module.path.starts_with("crates/logfwd-core/src/") || is_allowed(module, check) {
            continue;
        }

        let proofs_text = module.text.as_str();
        for f in &module.pub_fns {
            if !f.is_pub || f.is_test_only || f.is_kani_only || !f.is_non_trivial {
                continue;
            }

            let pattern_1 = format!("verify_{}", f.name);
            let pattern_2 = format!("{}(", f.name);
            let has_proof = proofs_text.contains(&pattern_1)
                || module
                    .kani_proofs
                    .iter()
                    .any(|p| p.contains(&f.name) || p.contains(&pattern_1))
                || (proofs_text.contains("#[cfg(kani)]") && proofs_text.contains(&pattern_2));

            if !has_proof {
                findings.push(Finding {
                    check,
                    path: module.path.clone(),
                    line: f.line,
                    message: format!("public non-trivial function `{}` has no Kani proof", f.name),
                });
            }
        }
    }
}

fn check_unsafe_needs_safety_comment(
    modules: &[ModuleFacts],
    findings: &mut Vec<Finding>,
) -> Result<()> {
    let check = "unsafe_needs_safety_comment";
    let unsafe_re = Regex::new(r"\bunsafe\b\s*\{")?;
    for module in modules {
        if is_allowed(module, check) || !is_production_src(&module.path) {
            continue;
        }
        let lines: Vec<&str> = module.text.lines().collect();
        for (idx, line) in lines.iter().enumerate() {
            if !unsafe_re.is_match(line) {
                continue;
            }
            let start = idx.saturating_sub(6);
            let mut ok = false;
            for ctx in &lines[start..=idx] {
                if ctx.contains("SAFETY:") {
                    ok = true;
                    break;
                }
            }
            if !ok {
                findings.push(Finding {
                    check,
                    path: module.path.clone(),
                    line: idx + 1,
                    message: "unsafe block missing preceding // SAFETY: comment".to_string(),
                });
            }
        }
    }
    Ok(())
}

fn check_pub_module_needs_tests(modules: &[ModuleFacts], findings: &mut Vec<Finding>) {
    let check = "pub_module_needs_tests";
    for module in modules {
        if is_allowed(module, check) || !is_production_src(&module.path) {
            continue;
        }
        let has_pub_fn = module
            .pub_fns
            .iter()
            .any(|f| f.is_pub && !f.is_test_only && !f.is_kani_only);
        if has_pub_fn && !module.has_tests {
            findings.push(Finding {
                check,
                path: module.path.clone(),
                line: 1,
                message: "module has public functions but no tests".to_string(),
            });
        }
    }
}

fn check_encode_decode_roundtrip(modules: &[ModuleFacts], findings: &mut Vec<Finding>) {
    let check = "encode_decode_roundtrip";
    for module in modules {
        if is_allowed(module, check) || !is_production_src(&module.path) {
            continue;
        }
        let mut by_name = HashMap::new();
        for f in &module.pub_fns {
            by_name.insert(f.name.clone(), f.line);
        }
        for (name, line) in &by_name {
            if let Some(suffix) = name.strip_prefix("encode_") {
                let decode_name = format!("decode_{suffix}");
                if by_name.contains_key(&decode_name)
                    && !(module.has_roundtrip_tests
                        && module.text.contains(name)
                        && module.text.contains(&decode_name))
                {
                    findings.push(Finding {
                        check,
                        path: module.path.clone(),
                        line: *line,
                        message: format!(
                            "encode/decode pair `{name}` + `{decode_name}` is missing roundtrip test"
                        ),
                    });
                }
            }
        }
    }
}

fn check_trust_boundary_manifest(repo_root: &Path, findings: &mut Vec<Finding>) -> Result<()> {
    let check = "trust_boundary_manifest";
    let manifest_path = repo_root.join("dev-docs/verification/fuzz-manifest.toml");
    if !manifest_path.is_file() {
        findings.push(Finding {
            check,
            path: "dev-docs/verification/fuzz-manifest.toml".to_string(),
            line: 1,
            message: "fuzz manifest is missing".to_string(),
        });
        return Ok(());
    }

    let manifest_text = fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let value: toml::Value =
        toml::from_str(&manifest_text).context("invalid fuzz-manifest.toml")?;

    // Collect all fuzz target source files.
    let mut fuzz_sources = Vec::new();
    for crate_dir in [
        "crates/logfwd-core",
        "crates/logfwd-io",
        "crates/logfwd-output",
    ] {
        let root = repo_root.join(crate_dir);
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(root) {
            let entry = entry?;
            let p = entry.path();
            if entry.file_type().is_file()
                && p.extension().and_then(|s| s.to_str()) == Some("rs")
                && p.to_string_lossy().contains("fuzz")
            {
                fuzz_sources.push(fs::read_to_string(p)?);
            }
        }
    }

    // Support both formats:
    // 1. [[boundaries]] array with `function` and `fuzz_target` keys
    // 2. [trust_boundaries] table with values like "path::function_name"
    let mut functions_to_check: Vec<String> = Vec::new();

    if let Some(entries) = value.get("boundaries").and_then(|v| v.as_array()) {
        for entry in entries {
            if let Some(function) = entry.get("function").and_then(|v| v.as_str()) {
                functions_to_check.push(function.to_string());
            }
        }
    }

    if let Some(table) = value.get("trust_boundaries").and_then(|v| v.as_table()) {
        for (_key, val) in table {
            if let Some(s) = val.as_str() {
                // Format: "path::fn_name" or "path::{fn1,fn2}"
                if let Some(after) = s.split("::").last() {
                    let trimmed = after.trim_matches(|c| c == '{' || c == '}');
                    for func in trimmed.split(',') {
                        functions_to_check.push(func.trim().to_string());
                    }
                }
            }
        }
    }

    for function in &functions_to_check {
        if !fuzz_sources
            .iter()
            .any(|src| src.contains(function.as_str()))
        {
            findings.push(Finding {
                check,
                path: "dev-docs/verification/fuzz-manifest.toml".to_string(),
                line: 1,
                message: format!("manifest function `{function}` has no fuzz target reference"),
            });
        }
    }

    Ok(())
}

fn check_proof_count_manifest(
    repo_root: &Path,
    modules: &[ModuleFacts],
    findings: &mut Vec<Finding>,
) -> Result<()> {
    let check = "proof_count_manifest";
    let manifest = repo_root.join("dev-docs/verification/kani-boundary-contract.toml");
    let txt = fs::read_to_string(&manifest)
        .with_context(|| format!("failed to read {}", manifest.display()))?;
    let value: toml::Value = toml::from_str(&txt).context("invalid kani boundary manifest")?;

    let mut module_map = BTreeMap::new();
    for m in modules {
        module_map.insert(m.path.as_str(), m);
    }

    let Some(seams) = value.get("seams").and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for seam in seams {
        let Some(path) = seam.get("path").and_then(|p| p.as_str()) else {
            continue;
        };
        let status = seam
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("recommended");
        let expected = seam
            .get("proof_count")
            .and_then(toml::Value::as_integer)
            .map(|v| v as usize);

        let Some(module) = module_map.get(path) else {
            findings.push(Finding {
                check,
                path: path.to_string(),
                line: 1,
                message: "manifest path does not exist".to_string(),
            });
            continue;
        };

        let actual = module.kani_proofs.len();
        if let Some(expected) = expected {
            if actual != expected {
                findings.push(Finding {
                    check,
                    path: path.to_string(),
                    line: 1,
                    message: format!("proof count mismatch: expected {expected}, found {actual}"),
                });
            }
            continue;
        }

        if status == "required" && actual == 0 {
            findings.push(Finding {
                check,
                path: path.to_string(),
                line: 1,
                message: "required seam has zero Kani proofs".to_string(),
            });
        }
        if status == "exempt" && actual > 0 {
            findings.push(Finding {
                check,
                path: path.to_string(),
                line: 1,
                message: "exempt seam unexpectedly has Kani proofs".to_string(),
            });
        }
    }

    Ok(())
}
