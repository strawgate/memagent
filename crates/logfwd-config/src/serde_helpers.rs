use serde::Deserialize;

pub(crate) fn deserialize_one_or_many<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        Many(Vec<T>),
        One(T),
    }

    match OneOrMany::deserialize(deserializer)? {
        OneOrMany::Many(v) => Ok(v),
        OneOrMany::One(v) => Ok(vec![v]),
    }
}

pub(crate) fn deserialize_duration_str(s: &str) -> Result<std::time::Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("duration string is empty".into());
    }

    let chars: Vec<char> = s.chars().collect();
    let split_idx = chars
        .iter()
        .position(|c| !c.is_ascii_digit())
        .unwrap_or(chars.len());

    if split_idx == 0 {
        return Err(format!("duration string '{s}' missing numeric value"));
    }

    let num_str: String = chars[..split_idx].iter().collect();
    let unit_str: String = chars[split_idx..].iter().collect();

    let val: u64 = num_str
        .parse()
        .map_err(|e| format!("invalid number in duration '{s}': {e}"))?;

    match unit_str.trim() {
        "s" | "sec" | "secs" | "second" | "seconds" => Ok(std::time::Duration::from_secs(val)),
        "m" | "min" | "mins" | "minute" | "minutes" => Ok(std::time::Duration::from_secs(val * 60)),
        "h" | "hr" | "hrs" | "hour" | "hours" => Ok(std::time::Duration::from_secs(val * 3600)),
        "ms" | "millis" | "milliseconds" => Ok(std::time::Duration::from_millis(val)),
        "d" | "day" | "days" => Ok(std::time::Duration::from_secs(val * 86400)),
        _ => Err(format!(
            "unknown or missing unit in duration '{s}' (expected s, m, h, ms, d)"
        )),
    }
}
