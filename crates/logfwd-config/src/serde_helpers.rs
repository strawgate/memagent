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

pub(crate) fn deserialize_duration<'de, D>(
    deserializer: D,
) -> Result<Option<std::time::Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct DurationVisitor;

    impl<'de> serde::de::Visitor<'de> for DurationVisitor {
        type Value = Option<std::time::Duration>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a duration string like '5s' or '100ms'")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if let Some(stripped) = value.strip_suffix("ms") {
                let ms: u64 = stripped.parse().map_err(serde::de::Error::custom)?;
                Ok(Some(std::time::Duration::from_millis(ms)))
            } else if let Some(stripped) = value.strip_suffix('s') {
                let s: u64 = stripped.parse().map_err(serde::de::Error::custom)?;
                Ok(Some(std::time::Duration::from_secs(s)))
            } else if let Some(stripped) = value.strip_suffix('m') {
                let m: u64 = stripped.parse().map_err(serde::de::Error::custom)?;
                let secs = m
                    .checked_mul(60)
                    .ok_or_else(|| serde::de::Error::custom("duration minutes overflow"))?;
                Ok(Some(std::time::Duration::from_secs(secs)))
            } else if let Some(stripped) = value.strip_suffix('h') {
                let h: u64 = stripped.parse().map_err(serde::de::Error::custom)?;
                let secs = h
                    .checked_mul(3600)
                    .ok_or_else(|| serde::de::Error::custom("duration hours overflow"))?;
                Ok(Some(std::time::Duration::from_secs(secs)))
            } else {
                Err(serde::de::Error::custom(format!(
                    "invalid duration string: {}",
                    value
                )))
            }
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_str(self)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }
    }

    deserializer.deserialize_option(DurationVisitor)
}
