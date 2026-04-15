use serde::{Deserialize, Deserializer, de::Error as DeError};

pub(crate) fn deserialize_one_or_many<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
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

pub(crate) fn deserialize_duration_ms<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DurationValue {
        String(String),
        Integer(u64),
    }

    let val = match Option::<DurationValue>::deserialize(deserializer)? {
        Some(v) => v,
        None => return Ok(None),
    };

    match val {
        DurationValue::Integer(v) => Ok(Some(v)),
        DurationValue::String(s) => {
            let s = s.trim();
            let (num_part, unit_part) =
                s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
            let num: u64 = num_part.parse().map_err(DeError::custom)?;
            let ms = match unit_part.trim() {
                "ms" | "" => num,
                "s" | "sec" | "secs" => num.saturating_mul(1000),
                "m" | "min" | "mins" => num.saturating_mul(60_000),
                "h" | "hr" | "hrs" => num.saturating_mul(3_600_000),
                other => {
                    return Err(DeError::custom(format!(
                        "unknown duration unit '{}'",
                        other
                    )));
                }
            };
            Ok(Some(ms))
        }
    }
}
