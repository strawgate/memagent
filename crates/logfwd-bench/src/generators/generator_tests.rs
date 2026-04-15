use super::test_support::{assert_batch_matches_scanned_json, scan_json};
use super::*;
use std::collections::HashSet;

#[test]
fn cri_k8s_deterministic() {
    let a = gen_cri_k8s(100, 42);
    let b = gen_cri_k8s(100, 42);
    assert_eq!(a, b, "same seed must produce identical output");
}

#[test]
fn cri_k8s_different_seeds_differ() {
    let a = gen_cri_k8s(100, 42);
    let b = gen_cri_k8s(100, 99);
    assert_ne!(a, b, "different seeds must produce different output");
}

#[test]
fn cri_k8s_format_correctness() {
    let data = gen_cri_k8s(200, 1);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");
    for line in text.lines() {
        // CRI format: <timestamp> <stream> <P|F> <payload>
        let parts: Vec<&str> = line.splitn(4, ' ').collect();
        assert_eq!(parts.len(), 4, "CRI line must have 4 parts: {line}");
        assert!(
            parts[0].ends_with('Z'),
            "timestamp must end with Z: {}",
            parts[0]
        );
        assert!(
            parts[1] == "stdout" || parts[1] == "stderr",
            "stream must be stdout/stderr: {}",
            parts[1]
        );
        assert!(
            parts[2] == "P" || parts[2] == "F",
            "flag must be P or F: {}",
            parts[2]
        );
        assert!(
            parts[3].starts_with('{'),
            "payload must be JSON: {}",
            parts[3]
        );
    }
}

#[test]
fn cri_k8s_has_partial_lines() {
    let data = gen_cri_k8s(1000, 42);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");
    let partial_count = text.lines().filter(|l| l.contains(" P ")).count();
    assert!(
        partial_count > 0,
        "expected some partial (P) lines in 1000 lines"
    );
    assert!(
        partial_count < 200,
        "expected ~10% partial lines, got {partial_count}"
    );
}

#[test]
fn production_mixed_deterministic() {
    let a = gen_production_mixed(100, 42);
    let b = gen_production_mixed(100, 42);
    assert_eq!(a, b, "same seed must produce identical output");
}

#[test]
fn production_mixed_format_correctness() {
    let data = gen_production_mixed(200, 1);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");
    for line in text.lines() {
        assert!(line.starts_with('{'), "each line must be JSON: {line}");
        assert!(line.ends_with('}'), "each line must end with }}: {line}");
        // Must parse as valid JSON
        let _: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
    }
}

#[test]
fn production_mixed_has_length_variety() {
    let data = gen_production_mixed(1000, 42);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");
    let lengths: Vec<usize> = text.lines().map(str::len).collect();
    let min = *lengths.iter().min().unwrap();
    let max = *lengths.iter().max().unwrap();
    assert!(
        max > min * 3,
        "expected significant length variety: min={min}, max={max}"
    );
}

#[test]
fn narrow_deterministic() {
    let a = gen_narrow(100, 42);
    let b = gen_narrow(100, 42);
    assert_eq!(a, b);
}

#[test]
fn wide_deterministic() {
    let a = gen_wide(100, 42);
    let b = gen_wide(100, 42);
    assert_eq!(a, b);
}

#[test]
fn narrow_valid_json() {
    let data = gen_narrow(50, 1);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");
    for line in text.lines() {
        let _: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
    }
}

#[test]
fn wide_valid_json() {
    let data = gen_wide(50, 1);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");
    for line in text.lines() {
        let _: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
    }
}

#[test]
fn envoy_access_deterministic() {
    let profile = EnvoyAccessProfile::benchmark();
    let a = gen_envoy_access_with_profile(200, 42, profile);
    let b = gen_envoy_access_with_profile(200, 42, profile);
    assert_eq!(a, b, "same seed/profile must produce identical output");
}

#[test]
fn envoy_access_valid_json_and_realistic_skew() {
    let profile = EnvoyAccessProfile::benchmark();
    let data = gen_envoy_access_with_profile(300, 7, profile);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");

    let mut prev_service: Option<String> = None;
    let mut same_service_runs = 0usize;
    let mut success_2xx = 0usize;
    let mut errors_5xx = 0usize;
    let mut routes = HashSet::new();
    let mut services = HashSet::new();

    for line in text.lines() {
        let v: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));

        let service = v["service"].as_str().expect("service string");
        let route_name = v["route_name"].as_str().expect("route_name string");
        let response_code = v["response_code"].as_u64().expect("response_code integer");

        if let Some(prev) = &prev_service {
            if prev == service {
                same_service_runs += 1;
            }
        }
        prev_service = Some(service.to_string());
        services.insert(service.to_string());
        routes.insert(route_name.to_string());

        if (200..300).contains(&response_code) {
            success_2xx += 1;
        }
        if response_code >= 500 {
            errors_5xx += 1;
        }
    }

    assert!(
        same_service_runs > 120,
        "expected bursty locality, got only {same_service_runs} same-service adjacencies"
    );
    assert!(
        success_2xx > 180,
        "expected strong 2xx skew, got {success_2xx} successes"
    );
    assert!(errors_5xx > 0, "expected some 5xx traffic");
    assert!(
        routes.len() > 8,
        "expected route cardinality beyond a trivial set, got {}",
        routes.len()
    );
    assert!(
        services.len() > 3,
        "expected service diversity, got {}",
        services.len()
    );
}

#[test]
fn narrow_batch_matches_scanned_json() {
    let scanned = scan_json(gen_narrow(256, 42));
    let direct = gen_narrow_batch(256, 42);
    assert_batch_matches_scanned_json(&scanned, &direct);
}

#[test]
fn wide_batch_matches_scanned_json() {
    let scanned = scan_json(gen_wide(256, 42));
    let direct = gen_wide_batch(256, 42);
    assert_batch_matches_scanned_json(&scanned, &direct);
}

#[test]
fn production_mixed_batch_matches_scanned_json() {
    let scanned = scan_json(gen_production_mixed(256, 42));
    let direct = gen_production_mixed_batch(256, 42);
    assert_batch_matches_scanned_json(&scanned, &direct);
}

#[test]
fn envoy_access_batch_matches_scanned_json() {
    let profile = EnvoyAccessProfile::benchmark();
    let scanned = scan_json(gen_envoy_access_with_profile(256, 42, profile));
    let direct = gen_envoy_access_batch_with_profile(256, 42, profile);
    assert_batch_matches_scanned_json(&scanned, &direct);
}

#[test]
fn envoy_access_scale_controls_cardinality() {
    let narrow = gen_envoy_access_with_profile(300, 5, EnvoyAccessProfile::for_scale(1));
    let wide = gen_envoy_access_with_profile(300, 5, EnvoyAccessProfile::for_scale(4));

    let narrow_routes: HashSet<String> = std::str::from_utf8(&narrow)
        .expect("valid UTF-8")
        .lines()
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            v["route_name"]
                .as_str()
                .expect("route_name string")
                .to_string()
        })
        .collect();
    let wide_routes: HashSet<String> = std::str::from_utf8(&wide)
        .expect("valid UTF-8")
        .lines()
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            v["route_name"]
                .as_str()
                .expect("route_name string")
                .to_string()
        })
        .collect();

    assert!(
        wide_routes.len() > narrow_routes.len(),
        "scale should increase route cardinality: narrow={} wide={}",
        narrow_routes.len(),
        wide_routes.len()
    );
}
