# Config Library POC

> **Status:** Active
> **Date:** 2026-04-19
> **Context:** Config-source layering and environment-substitution crate evaluation.

## Question

Can we replace ffwd's hand-written YAML-aware environment expansion and
config loading with a published Rust config library?

## Current Semantics To Preserve

- `${VAR}` expands inside YAML string values.
- YAML-significant characters in env values remain data, not syntax.
- Missing environment variables fail fast and name the missing variable.
- Environment substitution always produces string data.
- The typed Rust config schema parses env-backed strings into field types such as
  numbers, booleans, and enums.
- Mapping keys can contain `${VAR}`.
- Duplicate mapping keys created by env expansion are rejected.
- Deserialization diagnostics include useful field paths.

## POC Results

The implementation-facing behavior is now covered by focused tests in
`crates/ffwd-config/src/env.rs`.

### `subst`

`subst` with the `yaml` feature is the closest match for YAML-aware
interpolation. It parses YAML, substitutes string values, and then
deserializes. That avoids raw-text templating bugs where env values containing
characters such as `#`, `:`, `{`, or `[` are reinterpreted as YAML syntax.

It is not a drop-in replacement:

- it substitutes string values, not mapping keys;
- it cannot reject duplicate mapping keys created by key expansion, because key
  expansion does not happen;
- exact numeric placeholders remain strings, so typed coercion must happen in a
  later config/schema layer;
- it currently depends on `serde_yaml 0.9.34+deprecated`, while ffwd already
  uses `serde_yaml_ng`.

Conclusion: useful reference behavior for safe value interpolation, but not
enough to replace `Config::expand_env_yaml_str` or `Config::load_str`.

### `figment`

Figment supports layered configuration and environment overlays. The POC
confirms `Env::prefixed("LOGFWD_").split("__")` can override nested typed
fields such as `LOGFWD_PIPELINES__APP__WORKERS=9`.

It does not solve `${VAR}` interpolation inside YAML values. Its YAML provider
also depends on `serde_yaml`.

Conclusion: viable shape for typed env overlays, but not a replacement for
YAML-aware placeholder expansion.

### `config`

`config` supports layered configuration and environment overlays. The POC
confirms `Environment::with_prefix("LOGFWD").prefix_separator("_").separator("__")`
can override nested typed fields such as `LOGFWD_PIPELINES__APP__WORKERS=9`.

It does not solve `${VAR}` interpolation inside YAML values. Unlike Figment, its
YAML feature uses `yaml-rust2` rather than deprecated `serde_yaml`.

Conclusion: best candidate if we want a general config library for layering and
typed env overrides.

### `varsubst`

`varsubst` is a narrow string-substitution crate. With default features disabled,
it supports brace-delimited `${VAR}` placeholders without `$VAR` short syntax
and without backslash escape handling. That makes it a good fit for ffwd's
documented placeholder language because regex anchors like `$` and Windows paths
like `C:\logs\${FILE}` remain ordinary config data.

It leaves missing variables unchanged instead of reporting them. The production
wrapper handles that by running a second pass with all present environment
variables masked out, then rejecting any remaining `${VAR}` placeholder. This
keeps missing-variable failure policy in ffwd while delegating placeholder
parsing and substitution to the crate.

Conclusion: best candidate for the `${VAR}` string grammar, provided ffwd owns
the missing-variable policy wrapper.

## Recommendation

Use `config` for schema-directed deserialization/coercion, and later for
config-source layering and typed env overrides. Keep the YAML AST interpolation
pass, but use `varsubst` for the `${VAR}` string grammar inside that pass
instead of hand-parsing placeholders.

The production design should be:

1. Read YAML from file or string.
2. Run ffwd's YAML-aware `${VAR}` expansion over the parsed YAML AST, delegating
   per-string placeholder substitution to `varsubst`; env values remain strings.
3. Convert the expanded YAML AST into a `config` source without reparsing YAML
   text, so YAML-specific values like `.nan` still reach semantic validation.
4. Deserialize to `RawConfig` through `config`, letting the typed Rust schema
   parse env-backed strings into numeric, boolean, and enum fields.
5. Add an optional `LOGFWD_` environment overlay source using `__` as the nested
   separator.
6. Run existing semantic validation.

Do not replace the current interpolation with `subst` unless we intentionally
drop key expansion, duplicate-key detection, and the current braces-only syntax.
