/// Filter hints extracted from user SQL for predicate pushdown.
///
/// These hints flow from the transform layer (which parses the SQL) to input
/// sources and scanners, allowing them to skip work early. Every hint is
/// advisory — the SQL transform still applies all predicates, so correctness
/// doesn't depend on pushdown. It only affects performance.
///
/// See docs/PREDICATE_PUSHDOWN.md for the full design.

/// Hints that input sources and parsers can use to filter early.
#[derive(Debug, Clone, Default)]
pub struct FilterHints {
    /// Syslog: only forward messages with severity <= this value.
    /// Extracted from `WHERE severity <= N` or `WHERE severity < N`.
    pub max_severity: Option<u8>,

    /// Syslog: only forward messages with these facilities.
    /// Extracted from `WHERE facility = N` or `WHERE facility IN (...)`.
    pub facilities: Option<Vec<u8>>,

    /// Fields referenced in the query. Parsers/scanners can skip extracting
    /// unreferenced fields. None means "extract all" (SELECT *).
    pub wanted_fields: Option<Vec<String>>,
}

impl FilterHints {
    /// True if any syslog-level predicate was extracted.
    pub fn has_syslog_predicates(&self) -> bool {
        self.max_severity.is_some() || self.facilities.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_hints_push_nothing() {
        let h = FilterHints::default();
        assert!(!h.has_syslog_predicates());
        assert!(h.max_severity.is_none());
        assert!(h.facilities.is_none());
        assert!(h.wanted_fields.is_none());
    }
}
