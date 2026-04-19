//! Source metadata attachment policy shared by runtime and transform crates.

/// SQL-visible source metadata columns that should be attached after scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceMetadataPlan {
    /// Attach stable row-level source identity as `_source_id`.
    pub source_id: bool,
    /// Attach configured input name as `_input`.
    pub input: bool,
    /// Attach file path compatibility column as `_source_path`.
    pub source_path: bool,
}

impl SourceMetadataPlan {
    /// Return true when any source metadata column is required.
    pub fn any(self) -> bool {
        self.source_id || self.input || self.source_path
    }
}
