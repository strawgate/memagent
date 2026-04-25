// plan.rs — BatchPlan, FieldHandle, and FieldKind.
//
// Provides the planning layer for columnar batch construction.  Producers
// declare fields up front (planned) or discover them dynamically; the plan
// assigns stable `FieldHandle`s that later map to column indices in the
// shared builder.
//
// See dev-docs/research/columnar-batch-builder.md for the design intent.
//
// These types are wired into the shared ColumnarBatchBuilder path used by
// structured producers such as OTLP projection. StreamingBuilder remains the
// scanner-facing adapter.

use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// FieldKind — protocol-agnostic column types
// ---------------------------------------------------------------------------

/// Protocol-agnostic column type for the shared columnar engine.
///
/// These are Arrow-level capabilities, not source-specific semantics.
/// OTLP field numbers, JSON mixed-type conflict logic, and CSV column
/// indices do not belong here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldKind {
    /// 64-bit signed integer (`Int64`).
    Int64,
    /// 64-bit IEEE 754 float (`Float64`).
    Float64,
    /// Boolean.
    Bool,
    /// Variable-length UTF-8 string. When `utf8_trusted` is true (default),
    /// materialized as `StringViewArray` (zero-copy). When false, materialized
    /// as `StringArray` with full UTF-8 validation.
    Utf8View,
    /// Variable-length binary backed by Arrow `BinaryViewArray`.
    BinaryView,
    /// Fixed-size binary (e.g., 16 bytes for UUIDs, trace IDs).
    FixedBinary(usize),
}

// ---------------------------------------------------------------------------
// FieldHandle — stable column reference
// ---------------------------------------------------------------------------

/// Opaque handle to a column in a `BatchPlan`.
///
/// Handles are stable within a plan: the same field always maps to the same
/// handle regardless of when it was declared or resolved.  Producers store
/// handles for repeated use across rows without re-resolving by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FieldHandle(u32);

impl FieldHandle {
    /// The underlying column index.
    #[inline(always)]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

// ---------------------------------------------------------------------------
// FieldSchemaMode — planned vs dynamic
// ---------------------------------------------------------------------------

/// How a field's type was established.
///
/// Schema-fixed fields have a declared kind that must not change.
/// Dynamic fields accumulate observed kinds and may develop type conflicts
/// (like JSON's mixed int/string columns).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldSchemaMode {
    /// Producer declared the field with a fixed kind up front.
    /// Attempts to write a different kind are rejected (not silently
    /// promoted to a conflict column).
    Planned(FieldKind),
    /// Field was discovered dynamically (e.g., JSON key resolution).
    /// Multiple observed kinds are tracked; the builder decides whether
    /// to emit a conflict struct column or a single-type column at
    /// finalization.
    Dynamic {
        /// Set of kinds observed so far.
        observed: Vec<FieldKind>,
    },
}

impl FieldSchemaMode {
    fn new_planned(kind: FieldKind) -> Self {
        FieldSchemaMode::Planned(kind)
    }

    fn new_dynamic(kind: FieldKind) -> Self {
        FieldSchemaMode::Dynamic {
            observed: vec![kind],
        }
    }

    /// Record an additional observed kind for a dynamic field.
    /// Returns `true` if this was a new kind (conflict may be needed).
    fn observe(&mut self, kind: FieldKind) -> bool {
        match self {
            FieldSchemaMode::Dynamic { observed } => {
                if observed.contains(&kind) {
                    false
                } else {
                    observed.push(kind);
                    true
                }
            }
            FieldSchemaMode::Planned(_) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// FieldEntry — per-field metadata in the plan
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct FieldEntry {
    name: Arc<str>,
    handle: FieldHandle,
    mode: FieldSchemaMode,
}

// ---------------------------------------------------------------------------
// BatchPlan — field registry and handle allocator
// ---------------------------------------------------------------------------

/// Registry of fields for a columnar batch.
///
/// A `BatchPlan` collects field declarations (planned or dynamic) and
/// assigns stable [`FieldHandle`]s.  It does **not** own column storage
/// or Arrow materialization — that remains the builder's job.
///
/// # Planned vs dynamic fields
///
/// - **Planned fields** are declared with a fixed [`FieldKind`] before any
///   rows are written.  The kind is immutable; attempting to write a
///   different kind through `declare_planned` again returns the existing
///   handle only if the kinds match, otherwise returns an error.
///
/// - **Dynamic fields** are resolved by name + observed kind at write time
///   (like `StreamingBuilder::resolve_field` for JSON).  Multiple observed
///   kinds are accumulated and may produce conflict columns.
pub struct BatchPlan {
    /// Ordered field entries (index = handle value).
    fields: Vec<FieldEntry>,
    /// Name → handle for O(1) lookup.  Keys share the same `Arc<str>`
    /// as the corresponding `FieldEntry::name` to avoid double-allocation.
    index: HashMap<Arc<str>, FieldHandle>,
    /// Number of planned (schema-fixed) fields. These occupy the first
    /// `num_planned` slots and are never removed by `reset_dynamic`.
    num_planned: usize,
}

impl Default for BatchPlan {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchPlan {
    /// Create an empty plan.
    pub fn new() -> Self {
        BatchPlan {
            fields: Vec::new(),
            index: HashMap::new(),
            num_planned: 0,
        }
    }

    /// Create a plan with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        BatchPlan {
            fields: Vec::with_capacity(cap),
            index: HashMap::with_capacity(cap),
            num_planned: 0,
        }
    }

    /// Number of fields in the plan.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Whether the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Number of planned (schema-fixed) fields.
    pub fn num_planned(&self) -> usize {
        self.num_planned
    }

    /// Remove all dynamic fields, keeping planned fields intact.
    ///
    /// Called by `ColumnarBatchBuilder::begin_batch` so that dynamic field
    /// names are re-resolved from scratch each batch — matching
    /// `StreamingBuilder`'s `field_index.clear()` behavior. Without this,
    /// dynamic fields accumulate unboundedly across batches.
    pub fn reset_dynamic(&mut self) {
        // Remove dynamic entries from the name index.
        for entry in &self.fields[self.num_planned..] {
            self.index.remove(&entry.name);
        }
        // Truncate the field list to planned-only.
        self.fields.truncate(self.num_planned);
    }

    /// Declare a schema-fixed field.
    ///
    /// If the field already exists as planned with the same kind, returns
    /// the existing handle (idempotent).  If it exists with a different
    /// kind or as a dynamic field, returns `Err`.
    ///
    /// Planned fields must be declared before any dynamic fields are
    /// resolved. They form a stable prefix of the field list that
    /// `reset_dynamic` preserves.
    pub fn declare_planned(
        &mut self,
        name: &str,
        kind: FieldKind,
    ) -> Result<FieldHandle, PlanError> {
        if let Some(&handle) = self.index.get(name) {
            let entry = &self.fields[handle.index()];
            match &entry.mode {
                FieldSchemaMode::Planned(existing_kind) if *existing_kind == kind => Ok(handle),
                FieldSchemaMode::Planned(existing_kind) => Err(PlanError::KindMismatch {
                    field: name.to_string(),
                    declared: *existing_kind,
                    requested: kind,
                }),
                FieldSchemaMode::Dynamic { .. } => Err(PlanError::ModeMismatch {
                    field: name.to_string(),
                    reason: "field already exists as dynamic",
                }),
            }
        } else {
            if self.fields.len() != self.num_planned {
                return Err(PlanError::ModeMismatch {
                    field: name.to_string(),
                    reason: "planned fields must be declared before any dynamic resolution",
                });
            }
            let handle = self.alloc_handle()?;
            let shared_name: Arc<str> = Arc::from(name);
            self.fields.push(FieldEntry {
                name: Arc::clone(&shared_name),
                handle,
                mode: FieldSchemaMode::new_planned(kind),
            });
            self.index.insert(shared_name, handle);
            self.num_planned += 1;
            Ok(handle)
        }
    }

    /// Resolve a dynamic field by name and observed kind.
    ///
    /// If the field already exists (planned or dynamic), returns its handle.
    /// For dynamic fields, the observed kind is accumulated (for conflict
    /// detection). For planned fields, the handle is returned as-is — the
    /// Dynamic accumulator in the builder handles type mixing uniformly.
    pub fn resolve_dynamic(
        &mut self,
        name: &str,
        kind: FieldKind,
    ) -> Result<FieldHandle, PlanError> {
        if let Some(&handle) = self.index.get(name) {
            let entry = &mut self.fields[handle.index()];
            match &mut entry.mode {
                FieldSchemaMode::Dynamic { .. } => {
                    entry.mode.observe(kind);
                }
                FieldSchemaMode::Planned(_) => {
                    // Pre-registered fields accept dynamic resolution: the
                    // underlying Dynamic accumulator handles all types, and
                    // dedup ensures first-write-wins when canonical and
                    // attribute names collide.
                }
            }
            Ok(handle)
        } else {
            let handle = self.alloc_handle()?;
            let shared_name: Arc<str> = Arc::from(name);
            self.fields.push(FieldEntry {
                name: Arc::clone(&shared_name),
                handle,
                mode: FieldSchemaMode::new_dynamic(kind),
            });
            self.index.insert(shared_name, handle);
            Ok(handle)
        }
    }

    /// Look up a field by name without creating it.
    pub fn lookup(&self, name: &str) -> Option<FieldHandle> {
        self.index.get(name).copied()
    }

    /// Get the schema mode for a field handle.
    pub fn field_mode(&self, handle: FieldHandle) -> Option<&FieldSchemaMode> {
        self.fields.get(handle.index()).map(|e| &e.mode)
    }

    /// Get the field name for a handle.
    pub fn field_name(&self, handle: FieldHandle) -> Option<&str> {
        self.fields.get(handle.index()).map(|e| &*e.name)
    }

    /// Iterate over all fields in declaration order.
    pub fn fields(&self) -> impl Iterator<Item = (FieldHandle, &str, &FieldSchemaMode)> {
        self.fields.iter().map(|e| (e.handle, &*e.name, &e.mode))
    }

    fn alloc_handle(&self) -> Result<FieldHandle, PlanError> {
        let idx = u32::try_from(self.fields.len()).map_err(|_e| PlanError::TooManyFields {
            count: self.fields.len(),
        })?;
        Ok(FieldHandle(idx))
    }
}

// ---------------------------------------------------------------------------
// PlanError
// ---------------------------------------------------------------------------

/// Error from `BatchPlan` field declaration or resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// A planned field was re-declared with a different kind.
    KindMismatch {
        field: String,
        declared: FieldKind,
        requested: FieldKind,
    },
    /// A field mode conflict (e.g., planned field resolved as dynamic).
    ModeMismatch { field: String, reason: &'static str },
    /// The plan has more fields than can be addressed by `FieldHandle` (u32).
    TooManyFields { count: usize },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::KindMismatch {
                field,
                declared,
                requested,
            } => write!(
                f,
                "field {field:?} declared as {declared:?}, requested as {requested:?}"
            ),
            PlanError::ModeMismatch { field, reason } => {
                write!(f, "field {field:?}: {reason}")
            }
            PlanError::TooManyFields { count } => {
                write!(f, "too many fields: {count} exceeds u32::MAX")
            }
        }
    }
}

impl std::error::Error for PlanError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planned_field_returns_stable_handle() {
        let mut plan = BatchPlan::new();
        let h1 = plan.declare_planned("timestamp", FieldKind::Int64).unwrap();
        let h2 = plan.declare_planned("timestamp", FieldKind::Int64).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.index(), 0);
    }

    #[test]
    fn planned_field_kind_mismatch_errors() {
        let mut plan = BatchPlan::new();
        plan.declare_planned("severity", FieldKind::Int64).unwrap();
        let err = plan
            .declare_planned("severity", FieldKind::Utf8View)
            .unwrap_err();
        assert!(matches!(err, PlanError::KindMismatch { .. }));
    }

    #[test]
    fn dynamic_field_returns_stable_handle() {
        let mut plan = BatchPlan::new();
        let h1 = plan.resolve_dynamic("level", FieldKind::Utf8View).unwrap();
        let h2 = plan.resolve_dynamic("level", FieldKind::Utf8View).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn dynamic_field_accumulates_observed_kinds() {
        let mut plan = BatchPlan::new();
        let h = plan.resolve_dynamic("value", FieldKind::Int64).unwrap();
        plan.resolve_dynamic("value", FieldKind::Utf8View).unwrap();

        let mode = plan.field_mode(h).unwrap();
        match mode {
            FieldSchemaMode::Dynamic { observed } => {
                assert_eq!(observed.len(), 2);
                assert!(observed.contains(&FieldKind::Int64));
                assert!(observed.contains(&FieldKind::Utf8View));
            }
            _ => panic!("expected Dynamic mode"),
        }
    }

    #[test]
    fn dynamic_field_deduplicates_same_kind() {
        let mut plan = BatchPlan::new();
        plan.resolve_dynamic("count", FieldKind::Int64).unwrap();
        plan.resolve_dynamic("count", FieldKind::Int64).unwrap();
        plan.resolve_dynamic("count", FieldKind::Int64).unwrap();

        let h = plan.lookup("count").unwrap();
        match plan.field_mode(h).unwrap() {
            FieldSchemaMode::Dynamic { observed } => assert_eq!(observed.len(), 1),
            _ => panic!("expected Dynamic mode"),
        }
    }

    #[test]
    fn planned_field_accepts_dynamic_resolution() {
        let mut plan = BatchPlan::new();
        let h1 = plan.declare_planned("ts", FieldKind::Int64).unwrap();
        let h2 = plan.resolve_dynamic("ts", FieldKind::Int64).unwrap();
        assert_eq!(h1, h2, "dynamic resolution returns same handle as planned");
    }

    #[test]
    fn dynamic_field_cannot_become_planned() {
        let mut plan = BatchPlan::new();
        plan.resolve_dynamic("msg", FieldKind::Utf8View).unwrap();
        let err = plan
            .declare_planned("msg", FieldKind::Utf8View)
            .unwrap_err();
        assert!(matches!(err, PlanError::ModeMismatch { .. }));
    }

    #[test]
    fn handles_are_sequential() {
        let mut plan = BatchPlan::new();
        let h0 = plan.declare_planned("a", FieldKind::Int64).unwrap();
        let h1 = plan.declare_planned("b", FieldKind::Float64).unwrap();
        let h2 = plan.resolve_dynamic("c", FieldKind::Utf8View).unwrap();
        assert_eq!(h0.index(), 0);
        assert_eq!(h1.index(), 1);
        assert_eq!(h2.index(), 2);
        assert_eq!(plan.len(), 3);
    }

    #[test]
    fn lookup_returns_none_for_unknown() {
        let plan = BatchPlan::new();
        assert!(plan.lookup("nonexistent").is_none());
    }

    #[test]
    fn field_name_round_trips() {
        let mut plan = BatchPlan::new();
        let h = plan.declare_planned("body", FieldKind::Utf8View).unwrap();
        assert_eq!(plan.field_name(h), Some("body"));
    }

    #[test]
    fn fields_iterator_in_declaration_order() {
        let mut plan = BatchPlan::new();
        plan.declare_planned("z", FieldKind::Bool).unwrap();
        plan.declare_planned("m", FieldKind::Float64).unwrap();
        plan.resolve_dynamic("a", FieldKind::Int64).unwrap();

        let names: Vec<&str> = plan.fields().map(|(_, name, _)| name).collect();
        assert_eq!(names, vec!["z", "m", "a"]);
    }

    #[test]
    fn with_capacity_works() {
        let plan = BatchPlan::with_capacity(64);
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
    }

    #[test]
    fn fixed_binary_kind() {
        let mut plan = BatchPlan::new();
        let h = plan
            .declare_planned("trace_id", FieldKind::FixedBinary(16))
            .unwrap();
        match plan.field_mode(h).unwrap() {
            FieldSchemaMode::Planned(FieldKind::FixedBinary(16)) => {}
            other => panic!("expected FixedBinary(16), got {other:?}"),
        }
    }

    #[test]
    fn fixed_binary_size_mismatch_is_kind_mismatch() {
        let mut plan = BatchPlan::new();
        plan.declare_planned("id", FieldKind::FixedBinary(16))
            .unwrap();
        let err = plan
            .declare_planned("id", FieldKind::FixedBinary(32))
            .unwrap_err();
        assert!(matches!(err, PlanError::KindMismatch { .. }));
    }

    #[test]
    fn plan_error_display() {
        let err = PlanError::KindMismatch {
            field: "ts".to_string(),
            declared: FieldKind::Int64,
            requested: FieldKind::Float64,
        };
        let msg = err.to_string();
        assert!(msg.contains("ts"));
        assert!(msg.contains("Int64"));
        assert!(msg.contains("Float64"));

        let err2 = PlanError::ModeMismatch {
            field: "x".to_string(),
            reason: "already dynamic",
        };
        assert!(err2.to_string().contains("already dynamic"));
    }

    #[test]
    fn reset_dynamic_removes_dynamic_keeps_planned() {
        let mut plan = BatchPlan::new();
        plan.declare_planned("ts", FieldKind::Int64).unwrap();
        plan.declare_planned("msg", FieldKind::Utf8View).unwrap();
        plan.resolve_dynamic("level", FieldKind::Utf8View).unwrap();
        plan.resolve_dynamic("code", FieldKind::Int64).unwrap();

        assert_eq!(plan.len(), 4);
        assert_eq!(plan.num_planned(), 2);

        plan.reset_dynamic();

        assert_eq!(plan.len(), 2);
        assert_eq!(plan.num_planned(), 2);
        // Planned fields survive.
        assert!(plan.lookup("ts").is_some());
        assert!(plan.lookup("msg").is_some());
        // Dynamic fields are gone.
        assert!(plan.lookup("level").is_none());
        assert!(plan.lookup("code").is_none());

        // Re-resolving dynamic fields works after reset.
        let h = plan.resolve_dynamic("level", FieldKind::Int64).unwrap();
        assert_eq!(h.index(), 2);
        assert_eq!(plan.len(), 3);
    }

    #[test]
    fn declare_planned_after_dynamic_is_error() {
        let mut plan = BatchPlan::new();
        plan.declare_planned("ts", FieldKind::Int64).unwrap();
        plan.resolve_dynamic("level", FieldKind::Utf8View).unwrap();
        let err = plan
            .declare_planned("msg", FieldKind::Utf8View)
            .unwrap_err();
        assert!(
            matches!(err, PlanError::ModeMismatch { .. }),
            "expected ModeMismatch, got {err:?}"
        );
    }
}
