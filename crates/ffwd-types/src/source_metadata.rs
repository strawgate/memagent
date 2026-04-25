//! Source metadata attachment policy shared by runtime and transform crates.

use crate::field_names;

/// Public source path column style.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SourcePathColumn {
    /// Do not attach a source path column.
    #[default]
    None,
    /// Attach the source path as ECS/Beats `file.path`.
    Ecs,
    /// Attach the source path as OpenTelemetry `log.file.path`.
    Otel,
    /// Attach the source path as Vector legacy `file`.
    Vector,
}

impl SourcePathColumn {
    /// Return the Arrow column name for this source path style.
    pub fn to_column_name(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Ecs => Some(field_names::ECS_FILE_PATH),
            Self::Otel => Some(field_names::OTEL_LOG_FILE_PATH),
            Self::Vector => Some(field_names::VECTOR_FILE),
        }
    }
}

/// SQL-visible source metadata columns that should be attached after scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceMetadataPlan {
    /// Attach stable row-level source identity as `__source_id`.
    pub has_source_id: bool,
    /// Attach source file path as a public column.
    pub source_path: SourcePathColumn,
}

impl SourceMetadataPlan {
    /// Return true when any source metadata column is required.
    pub fn has_any(self) -> bool {
        self.has_source_id || self.has_source_path()
    }

    /// Return true when any source path column is required.
    pub fn has_source_path(self) -> bool {
        self.source_path.to_column_name().is_some()
    }
}
