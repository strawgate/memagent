//! Source metadata attachment policy shared by runtime and transform crates.

/// SQL-visible source metadata columns that should be attached after scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceMetadataPlan {
    /// Attach stable row-level source identity as `_source_id`.
    pub has_source_id: bool,
    /// Attach configured input name as `_input`.
    pub has_input: bool,
    /// Attach file path compatibility column as `_source_path`.
    pub has_source_path: bool,
}

impl SourceMetadataPlan {
    /// Return true when any source metadata column is required.
    pub fn has_any(self) -> bool {
        self.has_source_id || self.has_input || self.has_source_path
    }
}
