use crate::types::ConfigError;
use std::collections::HashMap;
use std::env;

/// Expands braced `${VAR}` placeholders using the process environment.
///
/// Only braced placeholders are expanded. Unbraced `$VAR` text stays literal,
/// invalid closed placeholders return validation errors, and an unterminated
/// `${VAR` tail is preserved after expanding any complete placeholders before it.
pub(crate) fn expand_env_vars(text: &str) -> Result<String, ConfigError> {
    if !text.contains("${") {
        return Ok(text.to_owned());
    }

    expand_env_vars_with(text, lookup_env_var)
}

fn expand_env_vars_with<F>(text: &str, mut lookup: F) -> Result<String, ConfigError>
where
    F: FnMut(&str) -> Result<Option<String>, ConfigError>,
{
    match first_unclosed_placeholder_start(text)? {
        Some(start) => {
            let expanded_prefix = expand_closed_env_vars(&text[..start], &mut lookup)?;
            Ok(format!("{expanded_prefix}{}", &text[start..]))
        }
        None => expand_closed_env_vars(text, &mut lookup),
    }
}

fn lookup_env_var(name: &str) -> Result<Option<String>, ConfigError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::Validation(format!(
            "environment variable '{name}' is not valid Unicode"
        ))),
    }
}

fn expand_closed_env_vars<F>(text: &str, lookup: &mut F) -> Result<String, ConfigError>
where
    F: FnMut(&str) -> Result<Option<String>, ConfigError>,
{
    validate_substitution_syntax(text)?;

    let mut variables: HashMap<&str, String> = HashMap::new();
    for name in braced_placeholder_names(text) {
        if let Some(value) = lookup(name)? {
            variables.entry(name).or_insert(value);
        }
    }

    let expanded = substitute_validated(text, &variables)?;
    let present_vars: HashMap<&str, &str> = variables.keys().map(|key| (*key, "")).collect();
    let masked = substitute_validated(text, &present_vars)?;
    if let Some(var_name) = first_braced_placeholder(&masked) {
        return Err(ConfigError::Validation(format!(
            "environment variable '{var_name}' is not set"
        )));
    }

    Ok(expanded)
}

fn validate_substitution_syntax(text: &str) -> Result<(), ConfigError> {
    let variables: HashMap<&str, &str> = HashMap::new();
    substitute_validated(text, &variables).map(|_| ())
}

fn substitute_validated<K, V>(text: &str, variables: &HashMap<K, V>) -> Result<String, ConfigError>
where
    K: AsRef<str> + std::hash::Hash + Eq,
    V: AsRef<str>,
{
    varsubst::substitute(text, variables).map_err(|err| match err {
        varsubst::SubstError::UnclosedBrace { position } => ConfigError::Validation(format!(
            "unclosed environment variable substitution at character {position}"
        )),
        varsubst::SubstError::InvalidVarName { .. } => {
            ConfigError::Validation(format!("invalid environment variable substitution: {err}"))
        }
    })
}

fn first_unclosed_placeholder_start(text: &str) -> Result<Option<usize>, ConfigError> {
    let variables: HashMap<&str, &str> = HashMap::new();
    match varsubst::substitute(text, &variables) {
        Ok(_) => Ok(None),
        Err(varsubst::SubstError::UnclosedBrace { position }) => {
            Ok(Some(char_position_to_byte_index(text, position)))
        }
        Err(err @ varsubst::SubstError::InvalidVarName { .. }) => Err(ConfigError::Validation(
            format!("invalid environment variable substitution: {err}"),
        )),
    }
}

fn char_position_to_byte_index(text: &str, position: usize) -> usize {
    text.char_indices()
        .nth(position)
        .map_or(text.len(), |(index, _)| index)
}

fn braced_placeholder_names(text: &str) -> impl Iterator<Item = &str> {
    let mut offset = 0;

    std::iter::from_fn(move || {
        let start_rel = text[offset..].find("${")?;
        let name_start = offset + start_rel + 2;
        if let Some(end_rel) = text[name_start..].find('}') {
            let name_end = name_start + end_rel;
            offset = name_end + 1;
            Some(&text[name_start..name_end])
        } else {
            offset = text.len();
            None
        }
    })
}

fn first_braced_placeholder(text: &str) -> Option<&str> {
    braced_placeholder_names(text).next()
}

#[cfg(test)]
mod tests {
    use super::expand_env_vars;
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            // SAFETY: tests that mutate process environment hold ENV_LOCK.
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            // SAFETY: tests that mutate process environment hold ENV_LOCK.
            unsafe { std::env::remove_var(key) };
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(val) => {
                    // SAFETY: tests that mutate process environment hold ENV_LOCK.
                    unsafe { std::env::set_var(self.key, val) }
                }
                None => {
                    // SAFETY: tests that mutate process environment hold ENV_LOCK.
                    unsafe { std::env::remove_var(self.key) }
                }
            }
        }
    }

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().expect("env lock should not be poisoned")
    }

    #[test]
    fn braced_env_var_expands() {
        let _guard = env_lock();
        let _var = EnvVarGuard::set("FFWD_ENV_TEST_BRACED", "expanded");

        let expanded = expand_env_vars("value=${FFWD_ENV_TEST_BRACED}").expect("env should expand");

        assert_eq!(expanded, "value=expanded");
    }

    #[test]
    fn missing_env_var_is_rejected() {
        let _guard = env_lock();
        let _var = EnvVarGuard::unset("FFWD_ENV_TEST_MISSING");

        let err = expand_env_vars("${FFWD_ENV_TEST_MISSING}").expect_err("missing env should fail");

        assert!(
            err.to_string().contains("FFWD_ENV_TEST_MISSING"),
            "error should name missing variable: {err}"
        );
    }

    #[test]
    fn unterminated_env_var_is_preserved() {
        let _guard = env_lock();
        let expanded = expand_env_vars("value=${FFWD_ENV_TEST_UNTERMINATED")
            .expect("unterminated variable should be preserved");

        assert_eq!(expanded, "value=${FFWD_ENV_TEST_UNTERMINATED");
    }

    #[test]
    fn closed_vars_before_unterminated_tail_are_expanded() {
        let _guard = env_lock();
        let _var = EnvVarGuard::set("FFWD_ENV_TEST_BRACED", "expanded");

        let expanded = expand_env_vars("${FFWD_ENV_TEST_BRACED}${FFWD_ENV_TEST_UNTERMINATED")
            .expect("closed variables before an unterminated tail should expand");

        assert_eq!(expanded, "expanded${FFWD_ENV_TEST_UNTERMINATED",);
    }

    #[test]
    fn dollar_name_syntax_is_literal() {
        let _guard = env_lock();
        let _var = EnvVarGuard::set("FFWD_ENV_TEST_SHORT", "expanded");

        let expanded =
            expand_env_vars("value=$FFWD_ENV_TEST_SHORT").expect("env should not expand");

        assert_eq!(expanded, "value=$FFWD_ENV_TEST_SHORT");
    }

    #[test]
    fn default_value_syntax_is_rejected() {
        let _guard = env_lock();
        let err = expand_env_vars("value=${FFWD_ENV_TEST_DEFAULT:fallback}")
            .expect_err("default syntax should be rejected");

        assert!(
            err.to_string()
                .contains("invalid environment variable substitution"),
            "error should describe invalid substitution: {err}"
        );
    }

    #[test]
    fn regex_backslashes_and_end_anchor_without_env_are_literal() {
        let _guard = env_lock();
        let regex = r"/([^/]+)/[^/]+\\.log$";

        let expanded = expand_env_vars(regex).expect("regex should not be treated as env syntax");

        assert_eq!(expanded, regex);
    }

    #[test]
    fn backslashes_with_env_placeholder_are_literal() {
        let _guard = env_lock();
        let _var = EnvVarGuard::set("FFWD_ENV_TEST_FILE", "app.log");

        let expanded = expand_env_vars(r"C:\logs\${FFWD_ENV_TEST_FILE}")
            .expect("backslashes should remain literal");

        assert_eq!(expanded, r"C:\logs\app.log");
    }

    #[test]
    fn env_value_containing_placeholder_text_is_not_recursively_expanded() {
        let _guard = env_lock();
        let _var = EnvVarGuard::set("FFWD_ENV_TEST_LITERAL", "${NOT_RECURSIVE}");

        let expanded = expand_env_vars("${FFWD_ENV_TEST_LITERAL}")
            .expect("env value should be inserted literally");

        assert_eq!(expanded, "${NOT_RECURSIVE}");
    }
}
