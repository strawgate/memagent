//! Configuration diffing for live reload.
//!
//! Compares two `Config` instances and produces a [`ConfigDiff`] describing
//! which pipelines were added, removed, changed, or left unchanged.

use crate::Config;

/// Result of comparing two configurations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDiff {
    /// Pipeline names present in the new config but not the old.
    pub added: Vec<String>,
    /// Pipeline names present in the old config but not the new.
    pub removed: Vec<String>,
    /// Pipeline names present in both but with different configuration.
    pub changed: Vec<String>,
    /// Pipeline names present in both with identical configuration.
    pub unchanged: Vec<String>,
    /// Whether the server config block changed.
    pub server_changed: bool,
    /// Whether the storage config block changed.
    pub storage_changed: bool,
    /// Whether the OpAMP config block changed.
    pub opamp_changed: bool,
}

impl ConfigDiff {
    /// Compute the diff between an old and new configuration.
    pub fn between(old: &Config, new: &Config) -> Self {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut changed = Vec::new();
        let mut unchanged = Vec::new();

        for (name, new_pipe) in &new.pipelines {
            match old.pipelines.get(name) {
                None => added.push(name.clone()),
                Some(old_pipe) if old_pipe != new_pipe => changed.push(name.clone()),
                Some(_) => unchanged.push(name.clone()),
            }
        }
        for name in old.pipelines.keys() {
            if !new.pipelines.contains_key(name) {
                removed.push(name.clone());
            }
        }

        // Sort for deterministic output in tests and logs.
        added.sort();
        removed.sort();
        changed.sort();
        unchanged.sort();

        Self {
            added,
            removed,
            changed,
            unchanged,
            server_changed: old.server != new.server,
            storage_changed: old.storage != new.storage,
            opamp_changed: old.opamp != new.opamp,
        }
    }

    /// Returns `true` if the diff has no meaningful changes (all pipelines
    /// unchanged and server/storage/opamp identical).
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.changed.is_empty()
            && !self.server_changed
            && !self.storage_changed
            && !self.opamp_changed
    }

    /// Returns `true` if all changes can be applied via graceful reload
    /// (currently: server bind address changes require a full restart).
    pub fn is_reloadable(&self) -> bool {
        !self.server_changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(yaml: &str) -> Config {
        Config::load_str(yaml).unwrap()
    }

    #[test]
    fn identical_configs_produce_empty_diff() {
        let yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let a = load(yaml);
        let b = load(yaml);
        let diff = ConfigDiff::between(&a, &b);
        assert!(diff.is_empty());
        assert_eq!(diff.unchanged, vec!["default"]);
    }

    #[test]
    fn different_configs_are_not_equal() {
        let yaml_a = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/a.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let yaml_b = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/b.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let a = load(yaml_a);
        let b = load(yaml_b);
        assert_ne!(a, b);
    }

    #[test]
    fn added_pipeline_detected() {
        let old_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let new_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
  extra:
    inputs:
      - type: file
        path: /tmp/extra.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let old = load(old_yaml);
        let new = load(new_yaml);
        let diff = ConfigDiff::between(&old, &new);
        assert_eq!(diff.added, vec!["extra"]);
        assert_eq!(diff.unchanged, vec!["default"]);
        assert!(diff.removed.is_empty());
        assert!(diff.changed.is_empty());
        assert!(diff.is_reloadable());
    }

    #[test]
    fn removed_pipeline_detected() {
        let old_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
  extra:
    inputs:
      - type: file
        path: /tmp/extra.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let new_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let old = load(old_yaml);
        let new = load(new_yaml);
        let diff = ConfigDiff::between(&old, &new);
        assert_eq!(diff.removed, vec!["extra"]);
        assert_eq!(diff.unchanged, vec!["default"]);
        assert!(diff.added.is_empty());
        assert!(diff.changed.is_empty());
    }

    #[test]
    fn changed_pipeline_detected() {
        let old_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/old.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let new_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/new.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let old = load(old_yaml);
        let new = load(new_yaml);
        let diff = ConfigDiff::between(&old, &new);
        assert_eq!(diff.changed, vec!["default"]);
        assert!(diff.unchanged.is_empty());
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn server_change_makes_diff_non_reloadable() {
        let old_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
server:
  diagnostics: 127.0.0.1:8686";
        let new_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
server:
  diagnostics: 127.0.0.1:9999";
        let old = load(old_yaml);
        let new = load(new_yaml);
        let diff = ConfigDiff::between(&old, &new);
        assert!(diff.server_changed);
        assert!(!diff.is_reloadable());
    }

    #[test]
    fn storage_change_detected() {
        let old_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
storage:
  data_dir: /tmp/ffwd-old";
        let new_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
storage:
  data_dir: /tmp/ffwd-new";
        let old = load(old_yaml);
        let new = load(new_yaml);
        let diff = ConfigDiff::between(&old, &new);
        assert!(diff.storage_changed);
        assert!(diff.is_reloadable());
        assert_eq!(diff.unchanged, vec!["default"]);
    }

    #[test]
    fn identical_configs_are_equal() {
        let yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let a = load(yaml);
        let b = load(yaml);
        assert_eq!(a, b);
    }

    #[test]
    fn opamp_change_detected() {
        let old_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
opamp:
  endpoint: http://localhost:4320/v1/opamp";
        let new_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
opamp:
  endpoint: http://other-server:4320/v1/opamp";
        let old = load(old_yaml);
        let new = load(new_yaml);
        let diff = ConfigDiff::between(&old, &new);
        assert!(diff.opamp_changed);
        assert!(diff.is_reloadable());
        assert_eq!(diff.unchanged, vec!["default"]);
    }

    #[test]
    fn opamp_added_detected() {
        let old_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json";
        let new_yaml = "\
pipelines:
  default:
    inputs:
      - type: file
        path: /tmp/test.log
        format: json
    outputs:
      - type: stdout
        format: json
opamp:
  endpoint: http://localhost:4320/v1/opamp";
        let old = load(old_yaml);
        let new = load(new_yaml);
        let diff = ConfigDiff::between(&old, &new);
        assert!(diff.opamp_changed);
        assert!(!diff.is_empty());
    }
}

/// Property-based tests for ConfigDiff invariants.
///
/// These verify that ConfigDiff maintains set-theoretic properties regardless
/// of which pipelines are added/removed/changed between configurations.
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    use crate::types::PipelineConfig;

    /// Generate a pipeline name (short, alphanumeric).
    fn pipeline_name() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,7}".prop_filter("non-empty", |s| !s.is_empty())
    }

    /// Generate a minimal PipelineConfig from a YAML template with a unique path.
    fn pipeline_config(path_seed: &str) -> PipelineConfig {
        let yaml = format!(
            r#"
inputs:
  - type: file
    path: /tmp/{path_seed}.log
    format: json
outputs:
  - type: stdout
    format: json
"#
        );
        serde_yaml_ng::from_str(&yaml).expect("valid pipeline yaml")
    }

    /// Generate a Config with a given set of pipeline names.
    fn config_with_pipelines(names: &[String], path_prefix: &str) -> Config {
        let pipelines: HashMap<String, PipelineConfig> = names
            .iter()
            .map(|n| (n.clone(), pipeline_config(&format!("{path_prefix}_{n}"))))
            .collect();
        Config {
            pipelines,
            server: Default::default(),
            storage: Default::default(),
            opamp: None,
        }
    }

    proptest! {
        /// Diffing identical configs always produces an empty diff.
        #[test]
        fn self_diff_is_empty(
            names in proptest::collection::vec(pipeline_name(), 1..8)
        ) {
            let config = config_with_pipelines(&names, "same");
            let diff = ConfigDiff::between(&config, &config);
            prop_assert!(diff.is_empty(), "self-diff must be empty: {diff:?}");
            prop_assert!(diff.added.is_empty());
            prop_assert!(diff.removed.is_empty());
            prop_assert!(diff.changed.is_empty());
        }

        /// The total count of categorized pipelines equals the union of old and new.
        #[test]
        fn partition_covers_union(
            old_names in proptest::collection::vec(pipeline_name(), 1..6),
            new_names in proptest::collection::vec(pipeline_name(), 1..6),
        ) {
            let old = config_with_pipelines(&old_names, "old");
            let new = config_with_pipelines(&new_names, "new");
            let diff = ConfigDiff::between(&old, &new);

            // Union of all categories equals the set of all pipeline names.
            let mut all_in_diff: Vec<String> = diff.added.iter()
                .chain(diff.removed.iter())
                .chain(diff.changed.iter())
                .chain(diff.unchanged.iter())
                .cloned()
                .collect();
            all_in_diff.sort();
            all_in_diff.dedup();

            let mut union: Vec<String> = old.pipelines.keys()
                .chain(new.pipelines.keys())
                .cloned()
                .collect();
            union.sort();
            union.dedup();

            prop_assert_eq!(all_in_diff, union,
                "diff categories must partition the union of old+new pipeline names");
        }

        /// Categories are mutually exclusive (no pipeline appears in more than one).
        #[test]
        fn categories_are_disjoint(
            old_names in proptest::collection::vec(pipeline_name(), 1..6),
            new_names in proptest::collection::vec(pipeline_name(), 1..6),
        ) {
            let old = config_with_pipelines(&old_names, "old");
            let new = config_with_pipelines(&new_names, "new");
            let diff = ConfigDiff::between(&old, &new);

            let mut all: Vec<&String> = diff.added.iter()
                .chain(diff.removed.iter())
                .chain(diff.changed.iter())
                .chain(diff.unchanged.iter())
                .collect();
            let total = all.len();
            all.sort();
            all.dedup();
            prop_assert_eq!(all.len(), total,
                "no pipeline should appear in multiple categories");
        }

        /// Added pipelines are exactly those in new but not old.
        #[test]
        fn added_is_new_minus_old(
            old_names in proptest::collection::vec(pipeline_name(), 1..6),
            new_names in proptest::collection::vec(pipeline_name(), 1..6),
        ) {
            let old = config_with_pipelines(&old_names, "old");
            let new = config_with_pipelines(&new_names, "new");
            let diff = ConfigDiff::between(&old, &new);

            for name in &diff.added {
                prop_assert!(!old.pipelines.contains_key(name),
                    "added pipeline {name} must not exist in old config");
                prop_assert!(new.pipelines.contains_key(name),
                    "added pipeline {name} must exist in new config");
            }
        }

        /// Removed pipelines are exactly those in old but not new.
        #[test]
        fn removed_is_old_minus_new(
            old_names in proptest::collection::vec(pipeline_name(), 1..6),
            new_names in proptest::collection::vec(pipeline_name(), 1..6),
        ) {
            let old = config_with_pipelines(&old_names, "old");
            let new = config_with_pipelines(&new_names, "new");
            let diff = ConfigDiff::between(&old, &new);

            for name in &diff.removed {
                prop_assert!(old.pipelines.contains_key(name),
                    "removed pipeline {name} must exist in old config");
                prop_assert!(!new.pipelines.contains_key(name),
                    "removed pipeline {name} must not exist in new config");
            }
        }

        /// is_empty implies no structural changes anywhere.
        #[test]
        fn is_empty_iff_no_changes(
            names in proptest::collection::vec(pipeline_name(), 1..6),
        ) {
            let config = config_with_pipelines(&names, "same");
            let diff = ConfigDiff::between(&config, &config);
            prop_assert!(diff.is_empty());
            prop_assert!(!diff.server_changed);
            prop_assert!(!diff.storage_changed);
            prop_assert!(!diff.opamp_changed);
        }
    }
}
