use crate::types::ConfigError;

pub(crate) fn expand_env_vars(text: &str) -> Result<String, ConfigError> {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next();
            let mut var_name = String::new();
            let mut found_close = false;
            for c in chars.by_ref() {
                if c == '}' {
                    found_close = true;
                    break;
                }
                var_name.push(c);
            }
            if !found_close {
                result.push_str("${");
                result.push_str(&var_name);
                continue;
            }
            match std::env::var(&var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => {
                    return Err(ConfigError::Validation(format!(
                        "environment variable '{var_name}' is not set"
                    )));
                }
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}
