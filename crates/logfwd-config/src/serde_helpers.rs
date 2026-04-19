use serde::Deserialize;
use std::fmt;
use std::str::FromStr;

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

pub(crate) fn deserialize_option_from_string_or_value<'de, T, D>(
    deserializer: D,
) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de> + FromStr,
    T::Err: fmt::Display,
    D: serde::Deserializer<'de>,
{
    Option::<StringOrValue<T>>::deserialize(deserializer)?
        .map(StringOrValue::into_value)
        .transpose()
}

pub(crate) fn deserialize_from_string_or_value<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr,
    T::Err: fmt::Display,
    D: serde::Deserializer<'de>,
{
    StringOrValue::<T>::deserialize(deserializer)?.into_value()
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrValue<T> {
    Value(T),
    String(String),
}

impl<T> StringOrValue<T>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    fn into_value<E>(self) -> Result<T, E>
    where
        E: serde::de::Error,
    {
        match self {
            Self::Value(value) => Ok(value),
            Self::String(value) => value.parse().map_err(E::custom),
        }
    }
}
