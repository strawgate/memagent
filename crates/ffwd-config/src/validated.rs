//! Parse-don't-validate wrapper for [`Config`].
//!
//! [`ValidatedConfig`] witnesses that a YAML string has passed validation.
//! Once constructed, the inner [`Config`] can be accessed cheaply without
//! re-parsing. This eliminates redundant validation across the config
//! pipeline (OpAMP → supervisor → child reload loop).

use std::path::Path;

use crate::{Config, ConfigError};

/// A configuration that has been parsed and validated.
///
/// This type can only be constructed via [`ValidatedConfig::from_yaml`] or
/// [`ValidatedConfig::from_file`], both of which enforce validation. Code
/// receiving a `ValidatedConfig` can trust the inner [`Config`] is well-formed
/// without re-validating.
#[derive(Debug, Clone)]
pub struct ValidatedConfig {
    config: Config,
    /// The original YAML source text that was parsed and validated.
    source_yaml: String,
}

impl ValidatedConfig {
    /// Parse and validate a YAML string.
    ///
    /// `base_path` is used to resolve relative file paths in the config
    /// (e.g., enrichment table paths). Pass `None` for string-only validation.
    pub fn from_yaml(yaml: &str, base_path: Option<&Path>) -> Result<Self, ConfigError> {
        let config = Config::load_str_with_base_path(yaml, base_path)?;
        Ok(Self {
            config,
            source_yaml: yaml.to_owned(),
        })
    }

    /// Read a file and validate its contents.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let yaml = std::fs::read_to_string(path)?;
        let config = Config::load_str_with_base_path(&yaml, path.parent())?;
        Ok(Self {
            config,
            source_yaml: yaml,
        })
    }

    /// Access the validated configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The raw YAML source text that was validated.
    ///
    /// Note: this is the input as-provided, not a normalized/canonical form.
    pub fn effective_yaml(&self) -> &str {
        &self.source_yaml
    }

    /// Consume this wrapper and return the inner config.
    pub fn into_config(self) -> Config {
        self.config
    }

    /// Consume this wrapper and return both the config and source YAML.
    pub fn into_parts(self) -> (Config, String) {
        (self.config, self.source_yaml)
    }
}

impl AsRef<Config> for ValidatedConfig {
    fn as_ref(&self) -> &Config {
        &self.config
    }
}

impl std::ops::Deref for ValidatedConfig {
    type Target = Config;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_YAML: &str = "\
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
";

    #[test]
    fn valid_yaml_produces_validated_config() {
        let vc = ValidatedConfig::from_yaml(VALID_YAML, None).unwrap();
        assert!(vc.config().pipelines.contains_key("test"));
        assert_eq!(vc.effective_yaml(), VALID_YAML);
    }

    #[test]
    fn invalid_yaml_returns_error() {
        let yaml = "not: valid: yaml: [[[";
        assert!(ValidatedConfig::from_yaml(yaml, None).is_err());
    }

    #[test]
    fn missing_pipelines_returns_error() {
        let yaml = "server:\n  diagnostics: 127.0.0.1:8080\n";
        assert!(ValidatedConfig::from_yaml(yaml, None).is_err());
    }

    #[test]
    fn into_parts_returns_both() {
        let vc = ValidatedConfig::from_yaml(VALID_YAML, None).unwrap();
        let (config, effective) = vc.into_parts();
        assert!(config.pipelines.contains_key("test"));
        assert_eq!(effective, VALID_YAML);
    }

    #[test]
    fn deref_provides_config_access() {
        let vc = ValidatedConfig::from_yaml(VALID_YAML, None).unwrap();
        // Deref lets us call Config methods directly
        assert!(!vc.pipelines.is_empty());
    }
}
