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
