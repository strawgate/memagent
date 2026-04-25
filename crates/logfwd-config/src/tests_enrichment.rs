//! Enrichment configuration tests: geo_database, CSV, JSONL, static, host_info,
//! k8s_path, path validation, and simple-form wiring.

#[cfg(test)]
mod tests {
    use crate::test_yaml::single_pipeline_yaml_with_extra;
    use crate::*;
    use std::fs;

    #[test]
    fn enrichment_missing_path_rejected() {
        let cases = [
            (
                "geo_database",
                "format: mmdb\n        path: /nonexistent/path/to/GeoLite2-City.mmdb",
                &["not found", "geo database"][..],
            ),
            (
                "csv",
                "table_name: assets\n        path: /nonexistent/path/to/assets.csv",
                &["not found", "csv"][..],
            ),
            (
                "jsonl",
                "table_name: ips\n        path: /nonexistent/path/to/data.jsonl",
                &["not found", "jsonl"][..],
            ),
        ];
        for (etype, extra, expected) in cases {
            let yaml = format!(
                "pipelines:\n  app:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: {etype}\n        {extra}\n"
            );
            let err = Config::load_str(&yaml).unwrap_err();
            let msg = err.to_string();
            for substr in expected {
                assert!(
                    msg.contains(substr),
                    "expected {substr:?} in error for {etype}: {msg}"
                );
            }
        }
    }

    #[test]
    fn enrichment_relative_path_accepted_at_validation_time() {
        // Relative paths are resolved against base_path in Pipeline::from_config,
        // so Config::validate() must not reject them.
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: csv
        table_name: assets
        path: data/assets.csv
      - type: jsonl
        table_name: ips
        path: data/ips.jsonl
      - type: geo_database
        format: mmdb
        path: data/GeoLite2-City.mmdb
";
        Config::load_str(yaml).expect("relative enrichment paths should pass validation");
    }

    #[test]
    fn enrichment_static_empty_labels_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: static
        table_name: env
        labels: {}
";
        assert_config_err!(yaml, "at least one label");
    }

    #[test]
    fn enrichment_static_config_accepted() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: static
        table_name: env
        labels:
          dc: us-east-1
          team: platform
";
        let cfg = Config::load_str(yaml).expect("static enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Static(c) => {
                assert_eq!(c.table_name, "env");
                assert_eq!(c.labels.get("dc").map(String::as_str), Some("us-east-1"));
                assert_eq!(c.labels.get("team").map(String::as_str), Some("platform"));
            }
            other => panic!("expected Static, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_host_info_config_accepted() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: host_info
";
        let cfg = Config::load_str(yaml).expect("host_info enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        assert!(matches!(&pipe.enrichment[0], EnrichmentConfig::HostInfo(_)));
    }

    #[test]
    fn enrichment_k8s_path_config_accepted() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: k8s_path
        table_name: pods
";
        let cfg = Config::load_str(yaml).expect("k8s_path enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::K8sPath(c) => assert_eq!(c.table_name, "pods"),
            other => panic!("expected K8sPath, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_k8s_path_default_table_name() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: k8s_path
";
        let cfg = Config::load_str(yaml).expect("k8s_path with default table_name should parse");
        let pipe = &cfg.pipelines["app"];
        match &pipe.enrichment[0] {
            EnrichmentConfig::K8sPath(c) => assert_eq!(c.table_name, "k8s_pods"),
            other => panic!("expected K8sPath, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_csv_config_accepted() {
        // Use a path that exists to pass validation.
        let tmp = std::env::temp_dir().join("logfwd_test_enrichment.csv");
        fs::write(&tmp, "host,owner\nweb1,alice\n").expect("create temp csv");
        let yaml = format!(
            "pipelines:\n  app:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: csv\n        table_name: assets\n        path: {}\n",
            tmp.display()
        );
        let cfg = Config::load_str(yaml).expect("csv enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Csv(c) => {
                assert_eq!(c.table_name, "assets");
                assert_eq!(c.path, tmp.to_str().unwrap());
            }
            other => panic!("expected Csv, got {other:?}"),
        }
        let _ = fs::remove_file(&tmp);
    }

    #[test]
    fn enrichment_jsonl_config_accepted() {
        // Use a path that exists to pass validation.
        let tmp = std::env::temp_dir().join("logfwd_test_enrichment.jsonl");
        fs::write(&tmp, "{\"ip\":\"1.2.3.4\",\"owner\":\"alice\"}\n").expect("create temp jsonl");
        let yaml = format!(
            "pipelines:\n  app:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: jsonl\n        table_name: ip_owners\n        path: {}\n",
            tmp.display()
        );
        let cfg = Config::load_str(yaml).expect("jsonl enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Jsonl(c) => {
                assert_eq!(c.table_name, "ip_owners");
                assert_eq!(c.path, tmp.to_str().unwrap());
            }
            other => panic!("expected Jsonl, got {other:?}"),
        }
        let _ = fs::remove_file(&tmp);
    }

    #[test]
    fn enrichment_simple_form_preserved() {
        // Enrichment in simple form should be wired into the default pipeline,
        // not silently dropped (#540).
        let yaml = single_pipeline_yaml_with_extra(
            "type: file\npath: /tmp/x.log",
            "type: stdout",
            "enrichment:\n  - type: host_info\n  - type: k8s_path",
        );
        let cfg = Config::load_str(yaml).expect("simple form with enrichment should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.enrichment.len(),
            2,
            "enrichment should not be dropped in simple form"
        );
        assert!(matches!(&pipe.enrichment[0], EnrichmentConfig::HostInfo(_)));
        assert!(matches!(&pipe.enrichment[1], EnrichmentConfig::K8sPath(_)));
    }

    // -----------------------------------------------------------------------
    // Empty/whitespace path rejection (issue #1667)
    // -----------------------------------------------------------------------

    #[test]
    fn enrichment_empty_or_whitespace_path_rejected() {
        let cases = [
            ("geo_database", "format: mmdb\n        path: \"\""),
            ("csv", "table_name: assets\n        path: \"\""),
            ("jsonl", "table_name: owners\n        path: \"   \""),
            ("geo_database", "format: mmdb\n        path: \"   \""),
            ("csv", "table_name: assets\n        path: \"   \""),
        ];
        for (etype, extra) in cases {
            let yaml = format!(
                "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: {etype}\n        {extra}\n"
            );
            let err = Config::load_str(&yaml).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("path") && msg.contains("empty"),
                "expected empty-path rejection for {etype}: {msg}"
            );
        }
    }
}
