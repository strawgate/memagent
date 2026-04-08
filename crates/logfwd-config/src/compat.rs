use crate::types::OutputType;
use std::sync::OnceLock;

const OUTPUT_TYPE_MAPPING: &[(&str, OutputType)] = &[
    ("otlp", OutputType::Otlp),
    ("http", OutputType::Http),
    ("elasticsearch", OutputType::Elasticsearch),
    ("loki", OutputType::Loki),
    ("stdout", OutputType::Stdout),
    ("file", OutputType::File),
    ("parquet", OutputType::Parquet),
    ("null", OutputType::Null),
    ("tcp", OutputType::Tcp),
    ("udp", OutputType::Udp),
    ("arrow_ipc", OutputType::ArrowIpc),
];

pub(crate) fn parse_output_type_name(value: &str) -> Option<OutputType> {
    OUTPUT_TYPE_MAPPING
        .iter()
        .find(|(name, _)| *name == value)
        .map(|(_, ty)| ty.clone())
}

pub(crate) fn supported_output_type_names_for_errors() -> &'static [&'static str] {
    static NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();
    NAMES
        .get_or_init(|| OUTPUT_TYPE_MAPPING.iter().map(|(name, _)| *name).collect())
        .as_slice()
}
