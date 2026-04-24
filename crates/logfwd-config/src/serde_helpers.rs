use serde::Deserialize;
use serde::de::{
    Error as DeError, IntoDeserializer, MapAccess, SeqAccess, Unexpected,
    value::{MapAccessDeserializer, UnitDeserializer},
};
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::{collections::HashMap, hash::Hash};

pub(crate) fn deserialize_one_or_many<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    struct OneOrManyVisitor<T> {
        marker: PhantomData<T>,
    }

    impl<'de, T> serde::de::Visitor<'de> for OneOrManyVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a value or a sequence of values")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(item) = seq.next_element()? {
                items.push(item);
            }
            Ok(items)
        }

        fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            T::deserialize(value.into_deserializer()).map(|item| vec![item])
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            T::deserialize(value.into_deserializer()).map(|item| vec![item])
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            T::deserialize(value.into_deserializer()).map(|item| vec![item])
        }

        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            T::deserialize(value.into_deserializer()).map(|item| vec![item])
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            T::deserialize(value.into_deserializer()).map(|item| vec![item])
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            T::deserialize(value.into_deserializer()).map(|item| vec![item])
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            T::deserialize(UnitDeserializer::<E>::new()).map(|item| vec![item])
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: DeError,
        {
            self.visit_unit()
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            T::deserialize(deserializer).map(|item| vec![item])
        }

        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            T::deserialize(MapAccessDeserializer::new(map)).map(|item| vec![item])
        }
    }

    deserializer.deserialize_any(OneOrManyVisitor {
        marker: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct StringValues {
        #[serde(deserialize_with = "deserialize_one_or_many")]
        values: Vec<String>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct NumberValues {
        #[serde(deserialize_with = "deserialize_one_or_many")]
        values: Vec<u64>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct BoolValues {
        #[serde(deserialize_with = "deserialize_one_or_many")]
        values: Vec<bool>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct OptionalValues {
        #[serde(deserialize_with = "deserialize_one_or_many")]
        values: Vec<Option<String>>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct NamedValue {
        name: String,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct MapValues {
        #[serde(deserialize_with = "deserialize_one_or_many")]
        values: Vec<NamedValue>,
    }

    #[test]
    fn deserialize_one_or_many_accepts_scalar_string() {
        let parsed: StringValues = serde_yaml_ng::from_str("values: alpha\n").unwrap();

        assert_eq!(parsed.values, vec!["alpha"]);
    }

    #[test]
    fn deserialize_one_or_many_accepts_scalar_number() {
        let parsed: NumberValues = serde_yaml_ng::from_str("values: 42\n").unwrap();

        assert_eq!(parsed.values, vec![42]);
    }

    #[test]
    fn deserialize_one_or_many_accepts_scalar_bool() {
        let parsed: BoolValues = serde_yaml_ng::from_str("values: true\n").unwrap();

        assert_eq!(parsed.values, vec![true]);
    }

    #[test]
    fn deserialize_one_or_many_accepts_scalar_null_when_item_type_accepts_it() {
        let parsed: OptionalValues = serde_yaml_ng::from_str("values: null\n").unwrap();

        assert_eq!(parsed.values, vec![None]);
    }

    #[test]
    fn deserialize_one_or_many_accepts_sequence() {
        let parsed: StringValues =
            serde_yaml_ng::from_str("values:\n  - alpha\n  - beta\n").unwrap();

        assert_eq!(parsed.values, vec!["alpha", "beta"]);
    }

    #[test]
    fn deserialize_one_or_many_accepts_single_map() {
        let parsed: MapValues = serde_yaml_ng::from_str("values:\n  name: alpha\n").unwrap();

        assert_eq!(
            parsed.values,
            vec![NamedValue {
                name: "alpha".to_owned()
            }]
        );
    }
}

/// Deserializes an optional numeric or boolean field from a strict YAML scalar.
///
/// Native YAML scalars must have the target kind, while string values are parsed
/// through `T` so env-expanded values like `${PORT}` can populate typed fields.
pub fn deserialize_option_from_string_or_value<'de, T, D>(
    deserializer: D,
) -> Result<Option<T>, D::Error>
where
    T: StrictScalar,
    D: serde::Deserializer<'de>,
{
    Ok(Option::<StrictScalarValue<T>>::deserialize(deserializer)?
        .map(StrictScalarValue::into_inner))
}

/// Deserializes a numeric or boolean field from a strict YAML scalar.
///
/// Native YAML scalars must have the target kind, while string values are parsed
/// through `T` so env-expanded values like `${ENABLED}` can populate typed fields.
pub(crate) fn deserialize_from_string_or_value<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: StrictScalar,
    D: serde::Deserializer<'de>,
{
    Ok(StrictScalarValue::<T>::deserialize(deserializer)?.into_inner())
}

/// Deserializes a string field without accepting non-string YAML scalars.
pub(crate) fn deserialize_strict_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    StrictString::deserialize(deserializer).map(StrictString::into_inner)
}

/// Deserializes an optional string field without accepting non-string YAML scalars.
pub fn deserialize_option_strict_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<StrictString>::deserialize(deserializer)?.map(StrictString::into_inner))
}

/// Deserializes a string list without accepting non-string YAML scalars.
pub(crate) fn deserialize_vec_strict_string<'de, D>(
    deserializer: D,
) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Vec::<StrictString>::deserialize(deserializer).map(strict_strings_into_inner)
}

/// Deserializes an optional string list without accepting non-string YAML scalars.
pub(crate) fn deserialize_option_vec_strict_string<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Vec<StrictString>>::deserialize(deserializer)?.map(strict_strings_into_inner))
}

/// Deserializes a string map whose values must be YAML strings.
pub(crate) fn deserialize_string_map_strict_values<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_map_strict_values(deserializer)
}

/// Deserializes an optional string map whose values must be YAML strings.
pub(crate) fn deserialize_option_string_map_strict_values<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(
        Option::<HashMap<String, StrictString>>::deserialize(deserializer)?
            .map(strict_string_map_into_inner),
    )
}

fn deserialize_map_strict_values<'de, K, D>(deserializer: D) -> Result<HashMap<K, String>, D::Error>
where
    K: Deserialize<'de> + Eq + Hash,
    D: serde::Deserializer<'de>,
{
    HashMap::<K, StrictString>::deserialize(deserializer).map(strict_string_map_into_inner)
}

fn strict_strings_into_inner(values: Vec<StrictString>) -> Vec<String> {
    values.into_iter().map(StrictString::into_inner).collect()
}

fn strict_string_map_into_inner<K>(values: HashMap<K, StrictString>) -> HashMap<K, String>
where
    K: Eq + Hash,
{
    values
        .into_iter()
        .map(|(key, value)| (key, value.into_inner()))
        .collect()
}

struct StrictScalarValue<T>(T);

impl<T> StrictScalarValue<T> {
    fn into_inner(self) -> T {
        self.0
    }
}

impl<'de, T> Deserialize<'de> for StrictScalarValue<T>
where
    T: StrictScalar,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StrictScalarVisitor<T> {
            marker: PhantomData<T>,
        }

        impl<T> serde::de::Visitor<'_> for StrictScalarVisitor<T>
        where
            T: StrictScalar,
        {
            type Value = StrictScalarValue<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                T::expecting(formatter)
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                T::from_bool(value).map(StrictScalarValue)
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                T::from_i64(value).map(StrictScalarValue)
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                T::from_u64(value).map(StrictScalarValue)
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                T::from_f64(value).map(StrictScalarValue)
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                T::from_string(value).map(StrictScalarValue)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(StrictScalarVisitor {
            marker: PhantomData,
        })
    }
}

/// Parses scalar config fields while preserving raw YAML type strictness.
///
/// The `config` crate intentionally coerces between scalar kinds. This trait
/// lets visitors keep raw YAML strict while still allowing env-expanded strings
/// to parse as typed numeric and boolean fields.
pub(crate) trait StrictScalar: Sized {
    fn expecting(formatter: &mut fmt::Formatter<'_>) -> fmt::Result;

    fn from_string<E>(value: &str) -> Result<Self, E>
    where
        E: DeError;

    fn from_bool<E>(value: bool) -> Result<Self, E>
    where
        E: DeError,
    {
        Err(E::invalid_type(
            Unexpected::Bool(value),
            &StrictScalarExpected::<Self>::new(),
        ))
    }

    fn from_i64<E>(value: i64) -> Result<Self, E>
    where
        E: DeError,
    {
        Err(E::invalid_type(
            Unexpected::Signed(value),
            &StrictScalarExpected::<Self>::new(),
        ))
    }

    fn from_u64<E>(value: u64) -> Result<Self, E>
    where
        E: DeError,
    {
        Err(E::invalid_type(
            Unexpected::Unsigned(value),
            &StrictScalarExpected::<Self>::new(),
        ))
    }

    fn from_f64<E>(value: f64) -> Result<Self, E>
    where
        E: DeError,
    {
        Err(E::invalid_type(
            Unexpected::Float(value),
            &StrictScalarExpected::<Self>::new(),
        ))
    }
}

struct StrictScalarExpected<T> {
    marker: PhantomData<T>,
}

impl<T> StrictScalarExpected<T> {
    fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T> serde::de::Expected for StrictScalarExpected<T>
where
    T: StrictScalar,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        T::expecting(formatter)
    }
}

impl StrictScalar for bool {
    fn expecting(formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a boolean")
    }

    fn from_string<E>(value: &str) -> Result<Self, E>
    where
        E: DeError,
    {
        match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "on" | "yes" => Ok(true),
            "0" | "false" | "off" | "no" => Ok(false),
            _ => value.parse().map_err(E::custom),
        }
    }

    fn from_bool<E>(value: bool) -> Result<Self, E>
    where
        E: DeError,
    {
        Ok(value)
    }
}

macro_rules! impl_strict_unsigned {
    ($($ty:ty),* $(,)?) => {
        $(
            impl StrictScalar for $ty {
                fn expecting(formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("an unsigned integer")
                }

                fn from_string<E>(value: &str) -> Result<Self, E>
                where
                    E: DeError,
                {
                    value.parse().map_err(E::custom)
                }

                fn from_i64<E>(value: i64) -> Result<Self, E>
                where
                    E: DeError,
                {
                    value.try_into().map_err(|_e| {
                        E::invalid_value(Unexpected::Signed(value), &StrictScalarExpected::<Self>::new())
                    })
                }

                fn from_u64<E>(value: u64) -> Result<Self, E>
                where
                    E: DeError,
                {
                    value.try_into().map_err(|_e| {
                        E::invalid_value(Unexpected::Unsigned(value), &StrictScalarExpected::<Self>::new())
                    })
                }
            }
        )*
    };
}

macro_rules! impl_strict_signed {
    ($($ty:ty),* $(,)?) => {
        $(
            impl StrictScalar for $ty {
                fn expecting(formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a signed integer")
                }

                fn from_string<E>(value: &str) -> Result<Self, E>
                where
                    E: DeError,
                {
                    value.parse().map_err(E::custom)
                }

                fn from_i64<E>(value: i64) -> Result<Self, E>
                where
                    E: DeError,
                {
                    value.try_into().map_err(|_e| {
                        E::invalid_value(Unexpected::Signed(value), &StrictScalarExpected::<Self>::new())
                    })
                }

                fn from_u64<E>(value: u64) -> Result<Self, E>
                where
                    E: DeError,
                {
                    value.try_into().map_err(|_e| {
                        E::invalid_value(Unexpected::Unsigned(value), &StrictScalarExpected::<Self>::new())
                    })
                }
            }
        )*
    };
}

impl_strict_unsigned!(u8, u16, u32, u64, usize);
impl_strict_signed!(i32, i64);

struct StrictString(String);

impl StrictString {
    fn into_inner(self) -> String {
        self.0
    }
}

impl<'de> Deserialize<'de> for StrictString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StrictStringVisitor;

        impl serde::de::Visitor<'_> for StrictStringVisitor {
            type Value = StrictString;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictString(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictString(value))
            }
        }

        deserializer.deserialize_any(StrictStringVisitor)
    }
}

// ── Positive-duration newtypes ────────────────────────────────────────

/// A millisecond duration that must be > 0.
/// Wraps `NonZeroU64` so zero values are rejected at deserialization time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PositiveMillis(NonZeroU64);

impl PositiveMillis {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl From<PositiveMillis> for u64 {
    fn from(v: PositiveMillis) -> u64 {
        v.get()
    }
}

impl From<PositiveMillis> for std::time::Duration {
    fn from(v: PositiveMillis) -> std::time::Duration {
        std::time::Duration::from_millis(v.get())
    }
}

impl serde::Serialize for PositiveMillis {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.get().serialize(serializer)
    }
}

impl StrictScalar for PositiveMillis {
    fn expecting(formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a positive integer (> 0)")
    }

    fn from_string<E>(value: &str) -> Result<Self, E>
    where
        E: DeError,
    {
        let n: u64 = value.parse().map_err(E::custom)?;
        NonZeroU64::new(n)
            .map(Self)
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(0), &"a positive integer (> 0)"))
    }

    fn from_i64<E>(value: i64) -> Result<Self, E>
    where
        E: DeError,
    {
        let n: u64 = value.try_into().map_err(|_e| {
            E::invalid_value(Unexpected::Signed(value), &"a positive integer (> 0)")
        })?;
        NonZeroU64::new(n)
            .map(Self)
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(0), &"a positive integer (> 0)"))
    }

    fn from_u64<E>(value: u64) -> Result<Self, E>
    where
        E: DeError,
    {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(0), &"a positive integer (> 0)"))
    }
}

/// A seconds duration that must be > 0.
/// Wraps `NonZeroU64` so zero values are rejected at deserialization time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PositiveSecs(NonZeroU64);

impl PositiveSecs {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl From<PositiveSecs> for u64 {
    fn from(v: PositiveSecs) -> u64 {
        v.get()
    }
}

impl From<PositiveSecs> for std::time::Duration {
    fn from(v: PositiveSecs) -> std::time::Duration {
        std::time::Duration::from_secs(v.get())
    }
}

impl serde::Serialize for PositiveSecs {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.get().serialize(serializer)
    }
}

impl StrictScalar for PositiveSecs {
    fn expecting(formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a positive integer (> 0)")
    }

    fn from_string<E>(value: &str) -> Result<Self, E>
    where
        E: DeError,
    {
        let n: u64 = value.parse().map_err(E::custom)?;
        NonZeroU64::new(n)
            .map(Self)
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(0), &"a positive integer (> 0)"))
    }

    fn from_i64<E>(value: i64) -> Result<Self, E>
    where
        E: DeError,
    {
        let n: u64 = value.try_into().map_err(|_e| {
            E::invalid_value(Unexpected::Signed(value), &"a positive integer (> 0)")
        })?;
        NonZeroU64::new(n)
            .map(Self)
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(0), &"a positive integer (> 0)"))
    }

    fn from_u64<E>(value: u64) -> Result<Self, E>
    where
        E: DeError,
    {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(0), &"a positive integer (> 0)"))
    }
}
