//! Row-level predicate IR for scanner-level filtering.
//!
//! The scanner evaluates these predicates during JSON extraction to skip
//! rows that won't pass the SQL WHERE clause, avoiding Arrow builder work
//! for filtered rows. This is an advisory optimization — the SQL transform
//! still applies all predicates for correctness.

use alloc::{boxed::Box, string::String, vec::Vec};

/// A scalar value that a predicate compares against.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// UTF-8 string.
    Str(String),
    /// Signed 64-bit integer.
    Int(i64),
    /// 64-bit floating point.
    Float(f64),
    /// Boolean.
    Bool(bool),
    /// SQL NULL.
    Null,
}

/// Comparison operators for leaf predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal.
    Le,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Ge,
}

/// A row-level predicate the scanner can evaluate during extraction.
///
/// Only simple, pushable predicates are represented here. Complex
/// predicates (LIKE, functions, subqueries) remain in DataFusion as
/// residual filters. This IR is intentionally restrictive to keep
/// evaluation fast and the `no_std` implementation small.
#[derive(Debug, Clone)]
pub enum ScanPredicate {
    /// `field CmpOp value` — compare a field against a scalar literal.
    Compare {
        /// Field name (case-insensitive matching during evaluation).
        field: String,
        /// Comparison operator.
        op: CmpOp,
        /// Literal value to compare against.
        value: ScalarValue,
    },
    /// `field IN (v1, v2, ...)` / `field NOT IN (v1, v2, ...)`.
    InList {
        /// Field name.
        field: String,
        /// Values to check membership against.
        values: Vec<ScalarValue>,
        /// When true, checks NOT IN instead of IN.
        negated: bool,
    },
    /// `field IS NULL` / `field IS NOT NULL`.
    IsNull {
        /// Field name.
        field: String,
        /// When true, checks IS NOT NULL instead of IS NULL.
        negated: bool,
    },
    /// `field LIKE 'prefix%'` — string prefix match.
    StartsWith {
        /// Field name.
        field: String,
        /// Prefix to match (without the trailing `%`).
        prefix: String,
    },
    /// `field LIKE '%substring%'` — string contains match.
    Contains {
        /// Field name.
        field: String,
        /// Substring to search for.
        substring: String,
    },
    /// Conjunction: all sub-predicates must be true.
    And(Vec<ScanPredicate>),
    /// Disjunction: at least one sub-predicate must be true.
    Or(Vec<ScanPredicate>),
    /// Logical negation.
    Not(Box<ScanPredicate>),
}

/// A field value extracted from a JSON row for predicate evaluation.
///
/// Borrows from the scanner's input buffer or scratch space to avoid
/// per-row allocation.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtractedValue<'a> {
    /// UTF-8 string bytes (raw or decoded).
    Str(&'a [u8]),
    /// Parsed integer.
    Int(i64),
    /// Parsed float.
    Float(f64),
    /// Boolean.
    Bool(bool),
    /// JSON null.
    Null,
    /// Field was not present in this row.
    Missing,
}

impl ScanPredicate {
    /// Remove any conjuncts that reference the given field name.
    ///
    /// Returns `None` if the entire predicate references the field.
    /// For AND chains, strips only the matching conjuncts and returns
    /// the remainder (or None if all stripped).
    #[allow(clippy::expect_used)]
    pub fn strip_field(self, field_name: &str) -> Option<Self> {
        if !self.references_field(field_name.as_bytes()) {
            return Some(self);
        }
        match self {
            ScanPredicate::And(preds) => {
                let remaining: Vec<_> = preds
                    .into_iter()
                    .filter_map(|p| p.strip_field(field_name))
                    .collect();
                match remaining.len() {
                    0 => None,
                    1 => Some(remaining.into_iter().next().expect("len checked")),
                    _ => Some(ScanPredicate::And(remaining)),
                }
            }
            _ => None,
        }
    }

    /// Returns true if this predicate references the given field name
    /// (case-insensitive ASCII comparison).
    pub fn references_field(&self, key: &[u8]) -> bool {
        match self {
            ScanPredicate::Compare { field, .. }
            | ScanPredicate::InList { field, .. }
            | ScanPredicate::IsNull { field, .. }
            | ScanPredicate::StartsWith { field, .. }
            | ScanPredicate::Contains { field, .. } => key.eq_ignore_ascii_case(field.as_bytes()),
            ScanPredicate::And(preds) | ScanPredicate::Or(preds) => {
                preds.iter().any(|p| p.references_field(key))
            }
            ScanPredicate::Not(inner) => inner.references_field(key),
        }
    }

    /// Evaluate this predicate against extracted field values.
    ///
    /// The `lookup` function maps a field name to its extracted value for
    /// the current row. Returns `true` if the row should be kept.
    pub fn evaluate<'a, F>(&self, lookup: &F) -> bool
    where
        F: Fn(&str) -> ExtractedValue<'a>,
    {
        match self {
            ScanPredicate::Compare { field, op, value } => {
                let extracted = lookup(field.as_str());
                compare_values(&extracted, *op, value)
            }
            ScanPredicate::InList {
                field,
                values,
                negated,
            } => {
                let extracted = lookup(field.as_str());
                if matches!(extracted, ExtractedValue::Null | ExtractedValue::Missing) {
                    return false; // NULL IN (...) is false per SQL semantics.
                }
                // If any list value has an incomparable type, bail out and keep
                // the row — let DataFusion handle it with full coercion rules.
                // Without this, type-mismatch comparisons return `true` from
                // `compare_values` (conservative for scalar Compare), but IN/NOT IN
                // aggregates equality results: `any(true)` makes IN return true
                // and NOT IN return false, both potentially incorrect.
                if values.iter().any(|v| !types_comparable(&extracted, v)) {
                    return true;
                }
                let found = values
                    .iter()
                    .any(|v| compare_values(&extracted, CmpOp::Eq, v));
                if *negated { !found } else { found }
            }
            ScanPredicate::IsNull { field, negated } => {
                let extracted = lookup(field.as_str());
                let is_null = matches!(extracted, ExtractedValue::Null | ExtractedValue::Missing);
                if *negated { !is_null } else { is_null }
            }
            ScanPredicate::StartsWith { field, prefix } => {
                let extracted = lookup(field.as_str());
                match extracted {
                    ExtractedValue::Str(bytes) => bytes
                        .get(..prefix.len())
                        .is_some_and(|slice| slice == prefix.as_bytes()),
                    // Non-string values can't be evaluated here — keep the row
                    // and let DataFusion handle coercion. Returning false would
                    // silently drop rows (e.g., integer 500 with LIKE '5%').
                    _ => true,
                }
            }
            ScanPredicate::Contains { field, substring } => {
                let extracted = lookup(field.as_str());
                match extracted {
                    ExtractedValue::Str(bytes) => {
                        memchr::memmem::find(bytes, substring.as_bytes()).is_some()
                    }
                    // Non-string values can't be evaluated here — keep the row
                    // and let DataFusion handle coercion. Returning false would
                    // silently drop rows (e.g., integer 500 with LIKE '%00%').
                    _ => true,
                }
            }
            ScanPredicate::And(preds) => preds.iter().all(|p| p.evaluate(lookup)),
            ScanPredicate::Or(preds) => preds.iter().any(|p| p.evaluate(lookup)),
            ScanPredicate::Not(inner) => !inner.evaluate(lookup),
        }
    }
}

/// Returns `true` if the extracted value and scalar literal have comparable
/// types (same type, or numeric cross-type like Int/Float). String-to-number
/// coercion is considered comparable since `compare_values` attempts parsing.
///
/// Used by `InList` evaluation to bail out early when types are incompatible,
/// rather than letting `compare_values`'s conservative `true` return corrupt
/// the IN/NOT IN aggregation logic.
fn types_comparable(extracted: &ExtractedValue<'_>, literal: &ScalarValue) -> bool {
    match (extracted, literal) {
        (ExtractedValue::Str(_), ScalarValue::Str(_))
        | (ExtractedValue::Int(_), ScalarValue::Int(_))
        | (ExtractedValue::Float(_), ScalarValue::Float(_))
        | (ExtractedValue::Bool(_), ScalarValue::Bool(_))
        // Cross-type numeric: Int vs Float in either direction.
        | (ExtractedValue::Int(_), ScalarValue::Float(_))
        | (ExtractedValue::Float(_), ScalarValue::Int(_))
        // String-to-number coercion: compare_values attempts parsing.
        | (ExtractedValue::Str(_), ScalarValue::Int(_))
        | (ExtractedValue::Str(_), ScalarValue::Float(_))
        | (ExtractedValue::Int(_), ScalarValue::Str(_))
        | (ExtractedValue::Float(_), ScalarValue::Str(_))
        // NULL is handled before this is called, but include for completeness.
        | (ExtractedValue::Null, ScalarValue::Null) => true,
        _ => false,
    }
}

/// Compare an extracted value against a scalar literal using the given operator.
///
/// Type coercion rules:
/// - String vs String: case-sensitive byte comparison
/// - Int vs Int: numeric comparison
/// - Float vs Float: numeric comparison (NaN comparisons return false)
/// - Int vs Float or Float vs Int: promote to f64
/// - Null/Missing vs anything: comparison returns false (SQL NULL semantics)
/// - Bool vs Bool: true > false
/// - Mismatched types (e.g., String vs Int): return false
fn compare_values(extracted: &ExtractedValue<'_>, op: CmpOp, literal: &ScalarValue) -> bool {
    // NULL/Missing comparisons always return false (SQL three-valued logic).
    if matches!(extracted, ExtractedValue::Null | ExtractedValue::Missing) {
        return false;
    }
    if matches!(literal, ScalarValue::Null) {
        return false;
    }

    match (extracted, literal) {
        (ExtractedValue::Str(bytes), ScalarValue::Str(lit)) => {
            let ord = (*bytes).cmp(lit.as_bytes());
            apply_cmp_op(ord, op)
        }
        (ExtractedValue::Int(a), ScalarValue::Int(b)) => {
            let ord = a.cmp(b);
            apply_cmp_op(ord, op)
        }
        (ExtractedValue::Float(a), ScalarValue::Float(b)) => compare_floats(*a, *b, op),
        // Cross-type numeric promotion: int vs float.
        (ExtractedValue::Int(a), ScalarValue::Float(b)) => compare_floats(*a as f64, *b, op),
        (ExtractedValue::Float(a), ScalarValue::Int(b)) => compare_floats(*a, *b as f64, op),
        (ExtractedValue::Bool(a), ScalarValue::Bool(b)) => {
            let ord = a.cmp(b);
            apply_cmp_op(ord, op)
        }
        // Cross-type: string vs number — attempt coercion.
        // If coercion fails, return true (keep the row) and let DataFusion
        // handle the comparison with its full coercion rules.
        (ExtractedValue::Str(bytes), ScalarValue::Int(b)) => {
            if let Ok(s) = core::str::from_utf8(bytes)
                && let Ok(a) = s.parse::<i64>()
            {
                let ord = a.cmp(b);
                apply_cmp_op(ord, op)
            } else {
                true
            }
        }
        (ExtractedValue::Int(a), ScalarValue::Str(s)) => {
            if let Ok(b) = s.parse::<i64>() {
                let ord = a.cmp(&b);
                apply_cmp_op(ord, op)
            } else {
                true
            }
        }
        (ExtractedValue::Str(bytes), ScalarValue::Float(b)) => {
            if let Ok(s) = core::str::from_utf8(bytes)
                && let Ok(a) = s.parse::<f64>()
            {
                compare_floats(a, *b, op)
            } else {
                true
            }
        }
        (ExtractedValue::Float(a), ScalarValue::Str(s)) => {
            if let Ok(b) = s.parse::<f64>() {
                compare_floats(*a, b, op)
            } else {
                true
            }
        }
        // Mismatched types: return true (keep the row) — let DataFusion handle
        // coercion. Returning false here would silently drop rows.
        _ => true,
    }
}

/// Apply a comparison operator to an `Ordering`.
#[inline]
fn apply_cmp_op(ord: core::cmp::Ordering, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => ord.is_eq(),
        CmpOp::Ne => !ord.is_eq(),
        CmpOp::Lt => ord.is_lt(),
        CmpOp::Le => ord.is_le(),
        CmpOp::Gt => ord.is_gt(),
        CmpOp::Ge => ord.is_ge(),
    }
}

/// Compare two f64 values with correct NaN handling.
///
/// For `Eq`/`Ne`, uses IEEE 754 semantics directly: `NaN == x` is false,
/// `NaN != x` is true. For ordering ops (`Lt`, `Le`, `Gt`, `Ge`), uses
/// `partial_cmp` so NaN comparisons return false.
#[inline]
#[allow(clippy::float_cmp)]
fn compare_floats(a: f64, b: f64, op: CmpOp) -> bool {
    match op {
        // IEEE 754: NaN == anything is false.
        CmpOp::Eq => a == b,
        // IEEE 754: NaN != anything is true.
        CmpOp::Ne => a != b,
        // Ordering: NaN comparisons return false via partial_cmp.
        _ => match a.partial_cmp(&b) {
            Some(ord) => apply_cmp_op(ord, op),
            None => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn evaluate_eq_string() {
        let pred = ScanPredicate::Compare {
            field: "level".to_string(),
            op: CmpOp::Eq,
            value: ScalarValue::Str("error".to_string()),
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "level" {
                ExtractedValue::Str(b"error")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_eq_string_mismatch() {
        let pred = ScanPredicate::Compare {
            field: "level".to_string(),
            op: CmpOp::Eq,
            value: ScalarValue::Str("error".to_string()),
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "level" {
                ExtractedValue::Str(b"info")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(!pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_gt_int() {
        let pred = ScanPredicate::Compare {
            field: "severity_number".to_string(),
            op: CmpOp::Ge,
            value: ScalarValue::Int(17),
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "severity_number" {
                ExtractedValue::Int(21)
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_is_null_missing_field() {
        let pred = ScanPredicate::IsNull {
            field: "trace_id".to_string(),
            negated: false,
        };
        let lookup = |_name: &str| -> ExtractedValue<'_> { ExtractedValue::Missing };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_is_not_null() {
        let pred = ScanPredicate::IsNull {
            field: "msg".to_string(),
            negated: true,
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "msg" {
                ExtractedValue::Str(b"hello")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_and_chain() {
        let pred = ScanPredicate::And(alloc::vec![
            ScanPredicate::Compare {
                field: "level".to_string(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".to_string()),
            },
            ScanPredicate::Compare {
                field: "status".to_string(),
                op: CmpOp::Ge,
                value: ScalarValue::Int(500),
            },
        ]);
        let lookup = |name: &str| -> ExtractedValue<'_> {
            match name {
                "level" => ExtractedValue::Str(b"error"),
                "status" => ExtractedValue::Int(503),
                _ => ExtractedValue::Missing,
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_and_short_circuits() {
        let pred = ScanPredicate::And(alloc::vec![
            ScanPredicate::Compare {
                field: "level".to_string(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".to_string()),
            },
            ScanPredicate::Compare {
                field: "status".to_string(),
                op: CmpOp::Ge,
                value: ScalarValue::Int(500),
            },
        ]);
        let lookup = |name: &str| -> ExtractedValue<'_> {
            match name {
                "level" => ExtractedValue::Str(b"info"), // fails first
                "status" => ExtractedValue::Int(503),
                _ => ExtractedValue::Missing,
            }
        };
        assert!(!pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_null_comparison_is_false() {
        let pred = ScanPredicate::Compare {
            field: "x".to_string(),
            op: CmpOp::Eq,
            value: ScalarValue::Int(42),
        };
        let lookup = |_name: &str| -> ExtractedValue<'_> { ExtractedValue::Null };
        assert!(!pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_ne_nan_is_true() {
        let pred = ScanPredicate::Compare {
            field: "x".to_string(),
            op: CmpOp::Ne,
            value: ScalarValue::Float(f64::NAN),
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "x" {
                ExtractedValue::Float(42.0)
            } else {
                ExtractedValue::Missing
            }
        };
        // NaN != 42.0 should be true (IEEE 754 semantics).
        assert!(pred.evaluate(&lookup));

        // Also: 42.0 != NaN should be true.
        let pred2 = ScanPredicate::Compare {
            field: "x".to_string(),
            op: CmpOp::Ne,
            value: ScalarValue::Float(42.0),
        };
        let nan_lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "x" {
                ExtractedValue::Float(f64::NAN)
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred2.evaluate(&nan_lookup));

        // NaN == anything should be false.
        let eq_pred = ScanPredicate::Compare {
            field: "x".to_string(),
            op: CmpOp::Eq,
            value: ScalarValue::Float(f64::NAN),
        };
        assert!(!eq_pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_cross_type_int_float() {
        let pred = ScanPredicate::Compare {
            field: "latency".to_string(),
            op: CmpOp::Lt,
            value: ScalarValue::Float(100.5),
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "latency" {
                ExtractedValue::Int(50)
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn references_field_case_insensitive() {
        let pred = ScanPredicate::Compare {
            field: "level".to_string(),
            op: CmpOp::Eq,
            value: ScalarValue::Str("error".to_string()),
        };
        assert!(pred.references_field(b"level"));
        assert!(pred.references_field(b"Level"));
        assert!(pred.references_field(b"LEVEL"));
        assert!(!pred.references_field(b"msg"));
    }

    #[test]
    fn references_field_through_and() {
        let pred = ScanPredicate::And(alloc::vec![
            ScanPredicate::Compare {
                field: "level".to_string(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".to_string()),
            },
            ScanPredicate::IsNull {
                field: "trace_id".to_string(),
                negated: true,
            },
        ]);
        assert!(pred.references_field(b"level"));
        assert!(pred.references_field(b"trace_id"));
        assert!(!pred.references_field(b"msg"));
    }

    #[test]
    fn evaluate_not() {
        let pred = ScanPredicate::Not(Box::new(ScanPredicate::Compare {
            field: "level".to_string(),
            op: CmpOp::Eq,
            value: ScalarValue::Str("debug".to_string()),
        }));
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "level" {
                ExtractedValue::Str(b"error")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_in_list() {
        let pred = ScanPredicate::InList {
            field: "level".to_string(),
            values: alloc::vec![
                ScalarValue::Str("error".to_string()),
                ScalarValue::Str("fatal".to_string()),
            ],
            negated: false,
        };
        let hit = |name: &str| -> ExtractedValue<'_> {
            if name == "level" {
                ExtractedValue::Str(b"fatal")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&hit));

        let miss = |name: &str| -> ExtractedValue<'_> {
            if name == "level" {
                ExtractedValue::Str(b"info")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(!pred.evaluate(&miss));
    }

    #[test]
    fn evaluate_not_in_list() {
        let pred = ScanPredicate::InList {
            field: "level".to_string(),
            values: alloc::vec![
                ScalarValue::Str("debug".to_string()),
                ScalarValue::Str("trace".to_string()),
            ],
            negated: true,
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "level" {
                ExtractedValue::Str(b"error")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_in_list_with_ints() {
        let pred = ScanPredicate::InList {
            field: "status".to_string(),
            values: alloc::vec![
                ScalarValue::Int(500),
                ScalarValue::Int(502),
                ScalarValue::Int(503),
            ],
            negated: false,
        };
        let hit = |name: &str| -> ExtractedValue<'_> {
            if name == "status" {
                ExtractedValue::Int(503)
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&hit));

        let miss = |name: &str| -> ExtractedValue<'_> {
            if name == "status" {
                ExtractedValue::Int(200)
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(!pred.evaluate(&miss));
    }

    #[test]
    fn evaluate_starts_with() {
        let pred = ScanPredicate::StartsWith {
            field: "msg".to_string(),
            prefix: "ERROR".to_string(),
        };
        let hit = |name: &str| -> ExtractedValue<'_> {
            if name == "msg" {
                ExtractedValue::Str(b"ERROR: connection refused")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&hit));

        let miss = |name: &str| -> ExtractedValue<'_> {
            if name == "msg" {
                ExtractedValue::Str(b"INFO: request completed")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(!pred.evaluate(&miss));
    }

    #[test]
    fn evaluate_contains() {
        let pred = ScanPredicate::Contains {
            field: "msg".to_string(),
            substring: "timeout".to_string(),
        };
        let hit = |name: &str| -> ExtractedValue<'_> {
            if name == "msg" {
                ExtractedValue::Str(b"connection timeout after 30s")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&hit));

        let miss = |name: &str| -> ExtractedValue<'_> {
            if name == "msg" {
                ExtractedValue::Str(b"request completed successfully")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(!pred.evaluate(&miss));
    }

    #[test]
    fn evaluate_contains_on_non_string_keeps_row() {
        // Non-string values should return true (keep row) so DataFusion
        // can handle coercion. Returning false would silently drop rows.
        let pred = ScanPredicate::Contains {
            field: "status".to_string(),
            substring: "500".to_string(),
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "status" {
                ExtractedValue::Int(500)
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_starts_with_on_non_string_keeps_row() {
        // Non-string values should return true (keep row) so DataFusion
        // can handle coercion. Returning false would silently drop rows.
        let pred = ScanPredicate::StartsWith {
            field: "status".to_string(),
            prefix: "5".to_string(),
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "status" {
                ExtractedValue::Int(500)
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_or() {
        let pred = ScanPredicate::Or(alloc::vec![
            ScanPredicate::Compare {
                field: "level".to_string(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".to_string()),
            },
            ScanPredicate::Compare {
                field: "level".to_string(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("fatal".to_string()),
            },
        ]);
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "level" {
                ExtractedValue::Str(b"fatal")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_in_list_type_mismatch_keeps_row() {
        // IN with type mismatch (Int value vs Bool list) should keep the row.
        let pred = ScanPredicate::InList {
            field: "x".to_string(),
            values: alloc::vec![ScalarValue::Bool(true), ScalarValue::Bool(false)],
            negated: false,
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "x" {
                ExtractedValue::Int(1)
            } else {
                ExtractedValue::Missing
            }
        };
        // Must return true (keep row) so DataFusion can decide.
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_not_in_list_type_mismatch_keeps_row() {
        // NOT IN with type mismatch (Int value vs Bool list) should keep the row.
        let pred = ScanPredicate::InList {
            field: "x".to_string(),
            values: alloc::vec![ScalarValue::Bool(true), ScalarValue::Bool(false)],
            negated: true,
        };
        let lookup = |name: &str| -> ExtractedValue<'_> {
            if name == "x" {
                ExtractedValue::Int(1)
            } else {
                ExtractedValue::Missing
            }
        };
        // Must return true (keep row) so DataFusion can decide.
        assert!(pred.evaluate(&lookup));
    }

    #[test]
    fn evaluate_in_list_string_vs_int_is_comparable() {
        // String value vs Int list should still work (coercion is attempted).
        let pred = ScanPredicate::InList {
            field: "x".to_string(),
            values: alloc::vec![ScalarValue::Int(42), ScalarValue::Int(99)],
            negated: false,
        };
        let hit = |name: &str| -> ExtractedValue<'_> {
            if name == "x" {
                ExtractedValue::Str(b"42")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(pred.evaluate(&hit));

        let miss = |name: &str| -> ExtractedValue<'_> {
            if name == "x" {
                ExtractedValue::Str(b"100")
            } else {
                ExtractedValue::Missing
            }
        };
        assert!(!pred.evaluate(&miss));
    }
}

// ---------------------------------------------------------------------------
// Kani formal verification proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;
    use alloc::string::ToString;

    /// Prove that NULL comparisons always return false (SQL three-valued logic).
    #[kani::proof]
    #[kani::solver(kissat)]
    fn verify_null_comparison_always_false() {
        let op: u8 = kani::any();
        kani::assume(op < 6);
        let cmp_op = match op {
            0 => CmpOp::Eq,
            1 => CmpOp::Ne,
            2 => CmpOp::Lt,
            3 => CmpOp::Le,
            4 => CmpOp::Gt,
            _ => CmpOp::Ge,
        };

        // NULL extracted value vs any literal
        let result = compare_values(&ExtractedValue::Null, cmp_op, &ScalarValue::Int(42));
        assert!(!result, "NULL compared to non-NULL must be false");

        // Any extracted value vs NULL literal
        let result2 = compare_values(&ExtractedValue::Int(42), cmp_op, &ScalarValue::Null);
        assert!(!result2, "non-NULL compared to NULL must be false");
    }

    /// Prove that Eq and Ne are complements for non-NULL integer values.
    #[kani::proof]
    #[kani::solver(kissat)]
    fn verify_eq_ne_complement_int() {
        let a: i64 = kani::any();
        let b: i64 = kani::any();
        let eq = compare_values(&ExtractedValue::Int(a), CmpOp::Eq, &ScalarValue::Int(b));
        let ne = compare_values(&ExtractedValue::Int(a), CmpOp::Ne, &ScalarValue::Int(b));
        assert!(eq != ne, "Eq and Ne must be complements for non-NULL ints");
    }

    /// Prove that Lt, Eq, Gt partition the integer space.
    #[kani::proof]
    #[kani::solver(kissat)]
    fn verify_trichotomy_int() {
        let a: i64 = kani::any();
        let b: i64 = kani::any();
        let lt = compare_values(&ExtractedValue::Int(a), CmpOp::Lt, &ScalarValue::Int(b));
        let eq = compare_values(&ExtractedValue::Int(a), CmpOp::Eq, &ScalarValue::Int(b));
        let gt = compare_values(&ExtractedValue::Int(a), CmpOp::Gt, &ScalarValue::Int(b));
        // Exactly one must be true.
        assert!(
            (lt as u8) + (eq as u8) + (gt as u8) == 1,
            "exactly one of Lt, Eq, Gt must hold for ints"
        );
    }

    /// Prove And([]) is true (vacuous truth) and Or([]) is false.
    #[kani::proof]
    fn verify_empty_connectives() {
        let lookup = |_: &str| -> ExtractedValue<'_> { ExtractedValue::Missing };
        let and_empty = ScanPredicate::And(alloc::vec![]);
        let or_empty = ScanPredicate::Or(alloc::vec![]);
        assert!(and_empty.evaluate(&lookup), "empty AND is vacuously true");
        assert!(!or_empty.evaluate(&lookup), "empty OR is vacuously false");
    }
}
