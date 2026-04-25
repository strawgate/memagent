//! Pure platform-sensor event-type filtering helpers.

/// Return whether `event_kind` should pass the configured include/exclude lists.
///
/// Include lists narrow the set first. Exclude lists are then applied and take
/// precedence when a name appears in both lists.
pub(crate) fn is_event_type_enabled(
    event_kind: &str,
    include_event_types: Option<&[String]>,
    exclude_event_types: Option<&[String]>,
) -> bool {
    let included = include_event_types
        .filter(|types| !types.is_empty())
        .is_none_or(|types| contains_event_type(types, event_kind));
    included
        && !exclude_event_types
            .filter(|types| !types.is_empty())
            .is_some_and(|types| contains_event_type(types, event_kind))
}

fn contains_event_type(types: &[String], event_kind: &str) -> bool {
    types.iter().any(|name| name == event_kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Event type names emitted in the `event_kind` Arrow column.
    const SUPPORTED_EVENT_TYPES: &[&str] = &[
        "exec",
        "exit",
        "tcp_connect",
        "tcp_accept",
        "file_open",
        "file_delete",
        "file_rename",
        "setuid",
        "setgid",
        "module_load",
        "ptrace",
        "memfd_create",
        "dns_query",
    ];

    fn make_string_vec(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn supported_event_types_match_public_event_kind_names() {
        assert_eq!(
            SUPPORTED_EVENT_TYPES,
            &[
                "exec",
                "exit",
                "tcp_connect",
                "tcp_accept",
                "file_open",
                "file_delete",
                "file_rename",
                "setuid",
                "setgid",
                "module_load",
                "ptrace",
                "memfd_create",
                "dns_query",
            ]
        );
    }

    #[test]
    fn no_filters_accepts_all_event_types() {
        assert!(is_event_type_enabled("exec", None, None));
        assert!(is_event_type_enabled("tcp_connect", None, None));
    }

    #[test]
    fn include_list_allows_only_named_event_types() {
        let include = make_string_vec(&["exec", "exit"]);
        assert!(is_event_type_enabled("exec", Some(&include), None));
        assert!(!is_event_type_enabled("tcp_connect", Some(&include), None));
    }

    #[test]
    fn exclude_list_suppresses_named_event_types() {
        let exclude = make_string_vec(&["tcp_connect"]);
        assert!(!is_event_type_enabled("tcp_connect", None, Some(&exclude)));
        assert!(is_event_type_enabled("exec", None, Some(&exclude)));
    }

    #[test]
    fn exclude_list_takes_precedence_over_include_list() {
        let include = make_string_vec(&["exec", "tcp_connect"]);
        let exclude = make_string_vec(&["tcp_connect"]);
        assert!(!is_event_type_enabled(
            "tcp_connect",
            Some(&include),
            Some(&exclude)
        ));
        assert!(is_event_type_enabled(
            "exec",
            Some(&include),
            Some(&exclude)
        ));
    }
}
