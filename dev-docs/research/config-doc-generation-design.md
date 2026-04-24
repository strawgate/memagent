# Config Doc Generation Design

> **Status:** Active
> **Date:** 2026-04-22
> **Context:** Unifying config models, the config builder, and generated input/output reference docs.

## Goal

Generate the repetitive parts of input/output documentation from a single
structured source without turning the public docs into raw schema dumps.

The target outcome is:

- one canonical source for input/output component inventory;
- one canonical source for curated per-field docs metadata;
- generated support tables, field tables, and starter snippets;
- no drift between:
  - `crates/logfwd-config/src/types.rs`
  - `crates/logfwd-config-wasm/src/lib.rs`
  - `book/src/content/docs/configuration/reference.mdx`
  - `book/src/content/docs/configuration/inputs.md`
  - `book/src/content/docs/configuration/outputs.mdx`

## Non-Goals

- Do not fully generate the public config guide from raw Rust types.
- Do not generate conceptual pages such as quick start, deployment, or SQL docs.
- Do not require JSON Schema to become the user-facing authoring format.
- Do not duplicate the entire typed schema in a second manual DSL.

## Current Problem

FastForward currently has three overlapping sources of truth:

1. Typed config models and validation:
   - `crates/logfwd-config/src/types.rs`
   - `crates/logfwd-config/src/validate.rs`
2. Curated builder/template metadata:
   - `crates/logfwd-config-wasm/src/lib.rs`
3. Hand-authored public docs:
   - `book/src/content/docs/configuration/reference.mdx`
   - `book/src/content/docs/configuration/inputs.md`
   - `book/src/content/docs/configuration/outputs.mdx`

The experiments in `tmp/config-doc-experiments/` showed:

- model-driven extraction has good coverage but weak user quality;
- template-driven extraction has good user tone but weak coverage;
- neither source is sufficient on its own.

## Design Principles

### 1. Keep types as the semantic base

Rust config types remain the source of truth for:

- field names;
- enum tags and aliases;
- required vs optional shape;
- nested object structure;
- deserialization behavior.

### 2. Add docs metadata, not a parallel schema

The missing layer is user-facing metadata:

- short labels;
- summaries;
- defaults worth showing;
- examples;
- support status;
- field-level descriptions;
- which fields belong in starter templates.

This metadata should be small and curated. It should not try to re-describe the
full Rust type system.

### 3. Generate references, not narratives

Generated artifacts should be limited to repetitive reference material:

- component inventory tables;
- per-input/per-output field tables;
- starter snippets;
- builder template payloads.

Narrative pages stay hand-authored.

### 4. Prefer CI-enforced coverage over clever parsing

The first production version should be boring:

- explicit metadata registry;
- explicit coverage checks;
- generated outputs committed to the repo;
- CI fails when generated files or registry coverage drift.

Do not start with a fragile parser that tries to infer all docs from arbitrary
Rust syntax.

## Proposed Source Layout

Add a docs metadata module under `logfwd-config`, for example:

```text
crates/logfwd-config/src/docspec/
├── mod.rs
├── inputs.rs
├── outputs.rs
└── model.rs
```

This module should be usable from:

- a docs generator (`xtask` or `scripts/docs/...`);
- `logfwd-config-wasm`;
- tests that enforce coverage against config enums.

## Proposed Data Model

### ComponentDoc

One entry per input or output type.

```rust
pub struct ComponentDoc {
    pub kind: ComponentKind,        // input | output
    pub type_tag: &'static str,     // "file", "otlp", "stdout"
    pub aliases: &'static [&'static str],
    pub label: &'static str,        // "File", "OTLP collector"
    pub summary: &'static str,
    pub support: SupportLevel,      // stable | experimental | beta | hidden
    pub docs_slug: &'static str,    // for generated anchors / linking
    pub starter_snippet: &'static str,
    pub builder_fields: &'static [BuilderFieldDoc],
    pub field_docs: &'static [FieldDoc],
}
```

### FieldDoc

Curated docs metadata for a config field path within a component.

```rust
pub struct FieldDoc {
    pub path: &'static str,         // "path", "format", "http.path", "s3.bucket"
    pub label: &'static str,        // optional user-facing label
    pub description: &'static str,
    pub default: Option<&'static str>,
    pub examples: &'static [&'static str],
    pub allowed_values: &'static [&'static str],
    pub required_override: Option<bool>,
    pub visibility: FieldVisibility, // reference | builder | advanced | hidden
}
```

### BuilderFieldDoc

Only the small subset of fields that the builder UI exposes directly.

```rust
pub struct BuilderFieldDoc {
    pub path: &'static str,
    pub label: &'static str,
    pub default: &'static str,
    pub placeholder: &'static str,
    pub options: &'static [&'static str],
}
```

## Why This Shape

This keeps the split clean:

- typed model says what is valid;
- docs metadata says how to explain it;
- builder metadata says what to expose in the simple UI.

The builder should reuse the docs metadata entries for labels/defaults/options,
not define them independently.

## Generation Flow

### Inputs

1. Read typed component inventory from `InputTypeConfig`.
2. Read docs metadata registry from `docspec::inputs`.
3. Validate that every public input variant has a matching `ComponentDoc`.
4. Render:
   - support/inventory table for `reference.mdx`;
   - per-input field tables for `inputs.md`;
   - builder template JSON/JS payload.

### Outputs

1. Read typed component inventory from `OutputConfigV2`.
2. Read docs metadata registry from `docspec::outputs`.
3. Validate coverage.
4. Render:
   - support/inventory table for `reference.mdx`;
   - per-output field tables for `outputs.mdx`;
   - builder template JSON/JS payload.

### Public Docs

Generated content should land in partials or generated include files rather than
fully replacing hand-authored pages.

Examples:

```text
book/src/content/docs/configuration/generated/
├── input-support-table.mdx
├── output-support-table.mdx
├── input-field-reference.mdx
└── output-field-reference.mdx
```

Then the hand-authored pages include or import those generated sections.

## Where The Typed Inventory Comes From

The coverage validator needs a robust inventory of known component tags.

There are two reasonable approaches:

### Option A: Explicit inventory functions in Rust

Add small helper functions in `logfwd-config` that return:

- input type tags + aliases
- output type tags

This avoids AST parsing entirely.

Example:

```rust
pub fn input_type_inventory() -> &'static [ComponentInventory] { ... }
pub fn output_type_inventory() -> &'static [ComponentInventory] { ... }
```

This is the simplest and most robust first version.

### Option B: Source parsing in generator code

Parse `types.rs` with `syn` or a source parser to infer the inventory.

This reduces one tiny bit of duplication, but it makes the generator more
complex and more brittle.

Recommendation: start with Option A.

## What To Generate First

### Phase 1: Component inventory and builder unification

Ship first:

- `ComponentDoc` for all inputs and outputs
- support level
- label
- summary
- starter snippet
- builder field subset

Generate:

- support tables for `reference.mdx`
- builder templates for `logfwd-config-wasm`

Do not generate field tables yet.

Why first:

- immediately removes one of the biggest drift points;
- low implementation risk;
- gives us a clean source for component labels/snippets/support status.

### Phase 2: Per-field reference tables

Add curated `FieldDoc` entries for the fields we actually document publicly.

Generate:

- input field tables
- output field tables

This should initially focus on top-level and high-signal nested fields:

- `path`
- `listen`
- `format`
- `endpoint`
- `compression`
- `auth`
- `tls`
- `http.*`
- `generator.*`
- `s3.*`

Do not block on documenting every nested helper type on day one.

### Phase 3: Validation and defaults enrichment

Add optional enrichment from typed/validation logic:

- required/optional detection
- enum values
- aliases
- defaults that are stable and user-visible

This may still be partly manual because many meaningful defaults live in
runtime/validation rather than in the Rust field declaration itself.

## What Should Stay Hand-Written

Keep hand-authored:

- `inputs.md` usage notes and operational caveats
- `outputs.mdx` narrative guidance and warnings
- `reference.mdx` introductory framing
- all Learn / Experience content

Generated docs should answer:

- what exists
- what fields are available
- what the defaults/options are
- what a valid starter snippet looks like

Hand-authored docs should answer:

- when to choose this
- what can go wrong
- what operational tradeoffs matter

## CI / Drift Checks

Add checks that fail if:

- any public input/output type lacks a `ComponentDoc`;
- any builder template is not derived from the registry;
- any generated doc partial is stale;
- any alias/support-status mismatch exists between typed inventory and docs metadata.

This should live in `xtask` or `scripts/docs`, not in ad hoc shell glue.

## Risks

### Risk: metadata becomes a second schema

Mitigation:

- keep metadata intentionally small;
- never re-describe low-level typing in the registry if it can be inferred;
- only store curated user-facing facts there.

### Risk: nested fields become too manual

Mitigation:

- phase the rollout;
- generate top-level coverage first;
- only add curated field docs for fields we actually expose in public docs.

### Risk: defaults drift from runtime behavior

Mitigation:

- mark defaults as curated user-visible defaults, not full semantic defaults;
- where possible, derive them from code helpers later;
- avoid documenting unstable/internal defaults in generated pages.

## Recommendation

Build a hybrid system:

1. add a small Rust docs metadata registry in `logfwd-config`;
2. use explicit Rust inventory helpers for coverage, not source parsing;
3. generate builder templates and support tables first;
4. generate field reference tables second;
5. keep narrative docs hand-authored.

This matches the successful pattern used by projects like Vector, Benthos, and
Terraform providers:

- code/schema provides structural truth;
- curated metadata provides user-facing descriptions;
- generated references remove repetitive drift-prone docs.
