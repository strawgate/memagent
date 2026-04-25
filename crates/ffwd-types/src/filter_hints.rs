/// Hints that input sources and parsers can use to filter early.
///
/// These flow from the transform layer (which parses the SQL) to input
/// sources and scanners, allowing them to skip work early. Every hint is
/// advisory — the SQL transform still applies all predicates, so correctness
/// doesn't depend on pushdown. It only affects performance.
///
/// See `dev-docs/research/predicate-pushdown-design.md` for the full design.
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

    #[test]
    fn has_syslog_predicates_severity() {
        let h = FilterHints {
            max_severity: Some(4),
            ..Default::default()
        };
        assert!(h.has_syslog_predicates());
    }

    #[test]
    fn has_syslog_predicates_facilities() {
        let h = FilterHints {
            facilities: Some(vec![1, 16]),
            ..Default::default()
        };
        assert!(h.has_syslog_predicates());
    }

    #[test]
    fn has_syslog_predicates_both() {
        let h = FilterHints {
            max_severity: Some(3),
            facilities: Some(vec![16]),
            ..Default::default()
        };
        assert!(h.has_syslog_predicates());
    }

    #[test]
    fn empty_facilities_vec_still_counts() {
        // Some(vec![]) is "explicitly set to empty" — has_syslog_predicates is true
        let h = FilterHints {
            facilities: Some(vec![]),
            ..Default::default()
        };
        assert!(h.has_syslog_predicates());
    }
}
