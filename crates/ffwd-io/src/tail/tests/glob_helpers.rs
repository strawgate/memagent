//! Unit tests for glob_root and glob_max_depth helper functions.

use std::path::PathBuf;

use super::super::glob::{glob_max_depth, glob_root};

// ---- glob_root / glob_max_depth unit tests ----

#[test]
fn glob_root_absolute_star() {
    assert_eq!(glob_root("/var/log/*.log"), PathBuf::from("/var/log"));
}

#[test]
fn glob_root_absolute_double_star() {
    assert_eq!(glob_root("/var/log/**/*.log"), PathBuf::from("/var/log"));
}

#[test]
fn glob_root_relative_star() {
    assert_eq!(glob_root("*.log"), PathBuf::from("."));
}

#[test]
fn glob_root_mid_filename_wildcard() {
    // Wildcard in the middle of a filename component.
    assert_eq!(glob_root("/var/log/app*.log"), PathBuf::from("/var/log"));
}

#[test]
fn glob_root_no_wildcard() {
    // A literal path has no wildcard, so parent dir is the walk root.
    assert_eq!(glob_root("/var/log/app.log"), PathBuf::from("/var/log"));
}

#[test]
fn glob_root_relative_no_wildcard() {
    // Bare filename with no path separator or wildcard -- parent is current dir.
    assert_eq!(glob_root("test.log"), PathBuf::from("."));
    assert_eq!(glob_root("app.log"), PathBuf::from("."));
}

#[test]
fn glob_root_relative_mid_filename_wildcard() {
    // `app*.log` -- wildcard mid-filename, root is current dir (parent of "app" is "").
    assert_eq!(glob_root("app*.log"), PathBuf::from("."));
}

#[test]
fn glob_max_depth_single_level() {
    // /var/log/*.log -> root=/var/log, pattern has 3 components, root has 2 -> depth 1
    assert_eq!(glob_max_depth("/var/log/*.log"), Some(1));
}

#[test]
fn glob_max_depth_double_star_unbounded() {
    assert_eq!(glob_max_depth("/var/log/**/*.log"), None);
}

#[test]
fn glob_max_depth_relative_is_at_least_1() {
    // *.log -> root=., 1 component each, but max(0,1) = 1
    assert_eq!(glob_max_depth("*.log"), Some(1));
}

#[test]
fn glob_max_depth_relative_subdir() {
    // */*.log -> root=. (0 effective components), pattern has 2 -> depth 2.
    // WalkDir must search at depth 2 to find files in subdirectories.
    assert_eq!(glob_max_depth("*/*.log"), Some(2));
}

#[test]
fn glob_max_depth_relative_dot_prefix() {
    // `./` is syntactic sugar for the current directory and should not
    // change traversal depth compared with the equivalent relative pattern.
    assert_eq!(glob_max_depth("./*.log"), glob_max_depth("*.log"));
    assert_eq!(glob_max_depth("./foo/*.log"), glob_max_depth("foo/*.log"));
}
