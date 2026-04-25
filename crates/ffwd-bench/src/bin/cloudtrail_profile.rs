#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use ffwd_bench::generators::cloudtrail::{
    gen_cloudtrail_audit_with_profile, gen_cloudtrail_batch_with_profile,
};
use ffwd_bench::generators::{CloudTrailProfile, CloudTrailRegionMix, CloudTrailServiceMix};
use serde_json::Value;

const DEFAULT_LINES: usize = 50_000;
const DEFAULT_SEED: u64 = 42;
const DEFAULT_ACCOUNT_COUNT: usize = 12;
const DEFAULT_PRINCIPAL_COUNT: usize = 64;
const DEFAULT_ACCOUNT_TENURE: usize = 48;
const DEFAULT_PRINCIPAL_TENURE: usize = 12;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let profile = CloudTrailProfile::benchmark_default()
        .with_account_count(cli.account_count)
        .with_principal_count(cli.principal_count)
        .with_account_tenure(cli.account_tenure)
        .with_principal_tenure(cli.principal_tenure)
        .with_service_mix(cli.service_mix)
        .with_region_mix(cli.region_mix)
        .with_optional_field_density(cli.optional_field_density);

    if cli.mode != Mode::Both {
        run_single_mode(&cli, profile);
        return Ok(());
    }

    let gen_started = Instant::now();
    let data = gen_cloudtrail_audit_with_profile(cli.lines, cli.seed, profile);
    let generation = gen_started.elapsed();

    let batch_started = Instant::now();
    let batch = gen_cloudtrail_batch_with_profile(cli.lines, cli.seed, profile);
    let batch_generation = batch_started.elapsed();

    let parse_started = Instant::now();
    let summary = summarize(&data);
    let parsing = parse_started.elapsed();

    let compress_started = Instant::now();
    let compressed = zstd::bulk::compress(&data, 1)?;
    let compression = compress_started.elapsed();

    println!("# CloudTrail profile");
    println!();
    println!(
        "- lines: {}  seed: {}  accounts: {}  principals: {}  account_tenure: {}  principal_tenure: {}",
        cli.lines,
        cli.seed,
        cli.account_count,
        cli.principal_count,
        cli.account_tenure,
        cli.principal_tenure
    );
    println!(
        "- service_mix: {:?}  region_mix: {:?}  optional_density: {}%",
        cli.service_mix, cli.region_mix, cli.optional_field_density
    );
    println!(
        "- generation: {} ({:.1} rows/s)",
        format_duration(generation),
        cli.lines as f64 / generation.as_secs_f64()
    );
    println!(
        "- direct batch generation: {} ({:.1} rows/s)",
        format_duration(batch_generation),
        cli.lines as f64 / batch_generation.as_secs_f64()
    );
    println!(
        "- parse summary: {} ({:.1} rows/s)",
        format_duration(parsing),
        cli.lines as f64 / parsing.as_secs_f64()
    );
    println!(
        "- raw bytes: {}  zstd-1 bytes: {}  ratio: {:.2}x",
        data.len(),
        compressed.len(),
        data.len() as f64 / compressed.len() as f64
    );
    println!("- compression: {}", format_duration(compression));
    println!(
        "- direct batch columns: {}  batch bytes: {}",
        batch.num_columns(),
        batch.get_array_memory_size()
    );
    println!();
    println!("## Line lengths");
    println!(
        "- avg: {:.1} bytes  min: {}  p50: {}  p95: {}  max: {}",
        summary.avg_len, summary.min_len, summary.p50_len, summary.p95_len, summary.max_len
    );
    println!();
    println!("## Cardinality");
    println!(
        "- unique accounts: {}  principals: {}  services: {}  actions: {}  regions: {}",
        summary.unique_accounts,
        summary.unique_principals,
        summary.unique_services,
        summary.unique_actions,
        summary.unique_regions
    );
    println!();
    println!("## Optional field coverage");
    println!(
        "- userIdentity.sessionContext: {:.1}%",
        pct(summary.session_context, summary.lines)
    );
    println!(
        "- requestParameters: {:.1}%",
        pct(summary.request_parameters, summary.lines)
    );
    println!(
        "- responseElements: {:.1}%",
        pct(summary.response_elements, summary.lines)
    );
    println!("- resources: {:.1}%", pct(summary.resources, summary.lines));
    println!(
        "- additionalEventData: {:.1}%",
        pct(summary.additional_event_data, summary.lines)
    );
    println!(
        "- errorCode/errorMessage: {:.1}%",
        pct(summary.errors, summary.lines)
    );
    println!(
        "- sharedEventID: {:.1}%",
        pct(summary.shared_event_id, summary.lines)
    );
    println!(
        "- vpcEndpointId: {:.1}%",
        pct(summary.vpc_endpoint_id, summary.lines)
    );
    println!(
        "- tlsDetails: {:.1}%",
        pct(summary.tls_details, summary.lines)
    );
    println!();
    println!("## Top values");
    print_top("eventSource", summary.lines, &summary.services);
    print_top("eventName", summary.lines, &summary.actions);
    print_top("awsRegion", summary.lines, &summary.regions);
    print_top("recipientAccountId", summary.lines, &summary.accounts);

    Ok(())
}

fn run_single_mode(cli: &Cli, profile: CloudTrailProfile) {
    let started = Instant::now();
    match cli.mode {
        Mode::Ndjson => {
            for i in 0..cli.iterations {
                let _ = std::hint::black_box(gen_cloudtrail_audit_with_profile(
                    cli.lines,
                    cli.seed.wrapping_add(i as u64),
                    profile,
                ));
            }
        }
        Mode::Batch => {
            for i in 0..cli.iterations {
                let _ = std::hint::black_box(gen_cloudtrail_batch_with_profile(
                    cli.lines,
                    cli.seed.wrapping_add(i as u64),
                    profile,
                ));
            }
        }
        Mode::Both => unreachable!("single-mode runner is only used for isolated modes"),
    }
    let elapsed = started.elapsed();
    let rows = cli.lines.saturating_mul(cli.iterations.max(1));
    println!(
        "mode={:?} iterations={} lines={} elapsed={} rows_per_sec={:.1}",
        cli.mode,
        cli.iterations,
        cli.lines,
        format_duration(elapsed),
        rows as f64 / elapsed.as_secs_f64()
    );
}

struct Cli {
    lines: usize,
    seed: u64,
    iterations: usize,
    mode: Mode,
    account_count: usize,
    principal_count: usize,
    account_tenure: usize,
    principal_tenure: usize,
    service_mix: CloudTrailServiceMix,
    region_mix: CloudTrailRegionMix,
    optional_field_density: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Both,
    Ndjson,
    Batch,
}

impl Cli {
    fn parse() -> Self {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        for arg in &args {
            if matches!(arg.as_str(), "--help" | "-h") {
                print_help();
                std::process::exit(0);
            }
        }

        Self {
            lines: parse_usize(&args, "--lines", DEFAULT_LINES),
            seed: parse_u64(&args, "--seed", DEFAULT_SEED),
            iterations: parse_usize(&args, "--iterations", 1).max(1),
            mode: parse_mode(&args, "--mode").unwrap_or(Mode::Both),
            account_count: parse_usize(&args, "--account-count", DEFAULT_ACCOUNT_COUNT).max(1),
            principal_count: parse_usize(&args, "--principal-count", DEFAULT_PRINCIPAL_COUNT)
                .max(1),
            account_tenure: parse_usize(&args, "--account-tenure", DEFAULT_ACCOUNT_TENURE).max(1),
            principal_tenure: parse_usize(&args, "--principal-tenure", DEFAULT_PRINCIPAL_TENURE)
                .max(1),
            service_mix: parse_service_mix(&args, "--service-mix")
                .unwrap_or(CloudTrailServiceMix::Balanced),
            region_mix: parse_region_mix(&args, "--region-mix")
                .unwrap_or(CloudTrailRegionMix::MultiRegion),
            optional_field_density: parse_usize(&args, "--optional-density", 65).min(100) as u8,
        }
    }
}

fn summarize(data: &[u8]) -> Summary {
    let text = std::str::from_utf8(data).expect("cloudtrail generator must emit utf-8");
    let mut lengths = Vec::new();
    let mut services = BTreeMap::<String, usize>::new();
    let mut actions = BTreeMap::<String, usize>::new();
    let mut regions = BTreeMap::<String, usize>::new();
    let mut accounts = BTreeMap::<String, usize>::new();
    let mut principals = HashSet::<String>::new();
    let mut session_context = 0usize;
    let mut request_parameters = 0usize;
    let mut response_elements = 0usize;
    let mut resources = 0usize;
    let mut additional_event_data = 0usize;
    let mut errors = 0usize;
    let mut shared_event_id = 0usize;
    let mut vpc_endpoint_id = 0usize;
    let mut tls_details = 0usize;

    for line in text.lines() {
        lengths.push(line.len());
        let value: Value = serde_json::from_str(line).expect("valid cloudtrail json");
        let event_source = value
            .get("eventSource")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let event_name = value
            .get("eventName")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let region = value
            .get("awsRegion")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let account = value
            .get("recipientAccountId")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        *services.entry(event_source.to_string()).or_insert(0) += 1;
        *actions.entry(event_name.to_string()).or_insert(0) += 1;
        *regions.entry(region.to_string()).or_insert(0) += 1;
        *accounts.entry(account.to_string()).or_insert(0) += 1;

        if let Some(user_identity) = value.get("userIdentity") {
            if let Some(principal_id) = user_identity.get("principalId").and_then(Value::as_str) {
                principals.insert(principal_id.to_string());
            }
            if user_identity.get("sessionContext").is_some() {
                session_context += 1;
            }
        }
        if value.get("requestParameters").is_some() {
            request_parameters += 1;
        }
        if value.get("responseElements").is_some() {
            response_elements += 1;
        }
        if value.get("resources").is_some() {
            resources += 1;
        }
        if value.get("additionalEventData").is_some() {
            additional_event_data += 1;
        }
        if value.get("errorCode").is_some() {
            errors += 1;
        }
        if value.get("sharedEventID").is_some() {
            shared_event_id += 1;
        }
        if value.get("vpcEndpointId").is_some() {
            vpc_endpoint_id += 1;
        }
        if value.get("tlsDetails").is_some() {
            tls_details += 1;
        }
    }

    lengths.sort_unstable();
    let min_len = *lengths.first().unwrap_or(&0);
    let max_len = *lengths.last().unwrap_or(&0);
    let p50_len = percentile(&lengths, 0.50);
    let p95_len = percentile(&lengths, 0.95);
    let avg_len = if lengths.is_empty() {
        0.0
    } else {
        lengths.iter().sum::<usize>() as f64 / lengths.len() as f64
    };

    Summary {
        lines: lengths.len(),
        avg_len,
        min_len,
        p50_len,
        p95_len,
        max_len,
        unique_accounts: accounts.len(),
        unique_principals: principals.len(),
        unique_services: services.len(),
        unique_actions: actions.len(),
        unique_regions: regions.len(),
        session_context,
        request_parameters,
        response_elements,
        resources,
        additional_event_data,
        errors,
        shared_event_id,
        vpc_endpoint_id,
        tls_details,
        services: top_n(services, 5),
        actions: top_n(actions, 5),
        regions: top_n(regions, 5),
        accounts: top_n(accounts, 5),
    }
}

struct Summary {
    lines: usize,
    avg_len: f64,
    min_len: usize,
    p50_len: usize,
    p95_len: usize,
    max_len: usize,
    unique_accounts: usize,
    unique_principals: usize,
    unique_services: usize,
    unique_actions: usize,
    unique_regions: usize,
    session_context: usize,
    request_parameters: usize,
    response_elements: usize,
    resources: usize,
    additional_event_data: usize,
    errors: usize,
    shared_event_id: usize,
    vpc_endpoint_id: usize,
    tls_details: usize,
    services: Vec<(String, usize)>,
    actions: Vec<(String, usize)>,
    regions: Vec<(String, usize)>,
    accounts: Vec<(String, usize)>,
}

fn top_n(map: BTreeMap<String, usize>, n: usize) -> Vec<(String, usize)> {
    let mut items: Vec<_> = map.into_iter().collect();
    items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    items.truncate(n);
    items
}

fn percentile(values: &[usize], pct: f64) -> usize {
    if values.is_empty() {
        return 0;
    }
    let idx = ((values.len() as f64 - 1.0) * pct).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn format_duration(duration: std::time::Duration) -> String {
    if duration.as_secs() > 0 {
        format!("{:.3} s", duration.as_secs_f64())
    } else if duration.as_millis() > 0 {
        format!("{:.3} ms", duration.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} μs", duration.as_secs_f64() * 1_000_000.0)
    }
}

fn pct(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 * 100.0 / denominator as f64
    }
}

fn print_top(label: &str, total: usize, values: &[(String, usize)]) {
    println!("{label}:");
    for (value, count) in values {
        println!("  - {} ({:.1}%)", value, pct(*count, total));
    }
}

fn parse_usize(args: &[String], flag: &str, default: usize) -> usize {
    args.iter()
        .rposition(|arg| arg == flag)
        .and_then(|idx| args.get(idx + 1))
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_u64(args: &[String], flag: &str, default: u64) -> u64 {
    args.iter()
        .rposition(|arg| arg == flag)
        .and_then(|idx| args.get(idx + 1))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_service_mix(args: &[String], flag: &str) -> Option<CloudTrailServiceMix> {
    args.iter()
        .rposition(|arg| arg == flag)
        .and_then(|idx| args.get(idx + 1))
        .and_then(|value| match value.as_str() {
            "balanced" => Some(CloudTrailServiceMix::Balanced),
            "security" | "security-heavy" => Some(CloudTrailServiceMix::SecurityHeavy),
            "storage" | "storage-heavy" => Some(CloudTrailServiceMix::StorageHeavy),
            "compute" | "compute-heavy" => Some(CloudTrailServiceMix::ComputeHeavy),
            _ => None,
        })
}

fn parse_region_mix(args: &[String], flag: &str) -> Option<CloudTrailRegionMix> {
    args.iter()
        .rposition(|arg| arg == flag)
        .and_then(|idx| args.get(idx + 1))
        .and_then(|value| match value.as_str() {
            "global" | "global-only" => Some(CloudTrailRegionMix::GlobalOnly),
            "regional" => Some(CloudTrailRegionMix::Regional),
            "multi" | "multi-region" => Some(CloudTrailRegionMix::MultiRegion),
            _ => None,
        })
}

fn parse_mode(args: &[String], flag: &str) -> Option<Mode> {
    args.iter()
        .rposition(|arg| arg == flag)
        .and_then(|idx| args.get(idx + 1))
        .and_then(|value| match value.as_str() {
            "both" => Some(Mode::Both),
            "ndjson" => Some(Mode::Ndjson),
            "batch" => Some(Mode::Batch),
            _ => None,
        })
}

fn print_help() {
    println!(
        "Usage: just bench, or: cargo run -p ffwd-bench --release --bin cloudtrail_profile -- [OPTIONS]"
    );
    println!("  --lines <N>              Number of CloudTrail records (default: {DEFAULT_LINES})");
    println!("  --seed <N>               RNG seed (default: {DEFAULT_SEED})");
    println!("  --iterations <N>         Repeat the selected generation path (default: 1)");
    println!("  --mode <both|ndjson|batch>  Which generation path to run (default: both)");
    println!("  --account-count <N>      Number of account IDs in the rotating pool");
    println!("  --principal-count <N>    Number of principals in the rotating pool");
    println!("  --account-tenure <N>    Rows to keep an account hot before rotating");
    println!("  --principal-tenure <N>   Rows to keep a principal hot before rotating");
    println!("  --service-mix <balanced|security-heavy|storage-heavy|compute-heavy>");
    println!("  --region-mix <global-only|regional|multi-region>");
    println!("  --optional-density <0-100>  Probability of optional nested fields");
}
