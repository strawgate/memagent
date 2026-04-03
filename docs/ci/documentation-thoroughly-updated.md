# Documentation Thoroughly Updated — Full Review Guidance

You are reviewing whether documentation has been kept in sync with code
changes in logfwd. This project maintains a comprehensive set of developer
and user documentation that agents (both human and AI) depend on to work
effectively. Stale documentation causes the AI-assisted development workflow
(Copilot agents, CodeRabbit) to make incorrect decisions. Flag all missing
documentation updates. This is a warning-level check, but repeated warnings
on the same document indicate a systemic neglect that maintainers should address.


## ARCHITECTURE.md (`dev-docs/ARCHITECTURE.md`)

Update required when: a new crate is added or removed, a new module is added
to `logfwd-core` that participates in the data flow, the data flow diagram changes
(new pipeline stage, new connection between layers), the crate boundaries section
changes (new trait added in core that another crate implements), or the buffer
lifecycle section changes (new copy introduced or eliminated). The document
describes the full pipeline from disk to wire:
disk → FileTailer → FormatParser → ChunkIndex → scanner → ScanBuilder →
RecordBatch → SqlTransform → OutputSink.
Any new participant in this chain must appear in `ARCHITECTURE.md`. The document
also tracks the "Target" state (zero-copy Bytes pipeline, StructuralIndex);
if a PR implements part of a target, the target description must be updated to
reflect the new current state.


## DESIGN.md — Architecture Decision Records (`dev-docs/DESIGN.md`)

An ADR is required when: a PR introduces a new architectural pattern not
previously in the codebase, chooses between two viable approaches after
deliberation, deliberately departs from an existing convention, or makes a
decision that future contributors will likely question. Read the "Architecture
decisions" section of `DESIGN.md` to understand the existing decision format
(named heading, context paragraph, decision made, consequences/trade-offs) and
to verify whether the PR's design is already covered by an existing decision.
Examples of ADR-worthy decisions: choosing deferred builder over incremental
null-padding (already recorded), choosing opaque checkpoints over typed offsets
(already recorded), choosing suffix-only-on-type-conflict (already recorded).
If this PR makes a design choice that required deliberation, an ADR belongs
in `DESIGN.md` under the "Architecture decisions" section.


## VERIFICATION.md (`dev-docs/VERIFICATION.md`)

Update required when: any module's proof count changes (add, remove, refactor
proofs), a module moves to a different verification tier, a new module is added
to `logfwd-core` and needs to be added to the per-module status table, proptest
oracle coverage is added to a module (update the Verification column), TLA+
properties are updated (update the TLA+ section's property table). The per-module
table and the tier-classification section at the bottom must both be accurate.
This document is the primary reference for understanding what is proved and what
is not — inaccuracy here is actively harmful.


## DEVELOPING.md (hard-won lessons section)

The "Things that will bite you" section documents non-obvious implementation
traps that cost significant debugging time. If this PR fixes or discovers a
non-obvious bug pattern — something that would not be apparent from reading the
code — add a lesson. Format: a bold heading naming the trap, then an explanation
of what happens, why it happens, and what the correct approach is. Examples of
lesson-worthy discoveries: the incremental null-padding bug (already documented),
the HashMap field lookup performance problem (already documented), the `prefix_xor`
carry-across-block correctness requirement (already documented). If this PR touches
SIMD boundary handling, buffer lifetime, column alignment, escape detection, or
any other area where the correct behavior is subtle, consider whether a lesson
is warranted.


## CONFIG_REFERENCE.md (`docs/CONFIG_REFERENCE.md`)

Update required when: a new configuration field is added to any config struct in
`logfwd-config`, an existing field's type or semantics change, a new SQL UDF is
added (`int()`, `float()`, `regexp_extract()`, `grok()`, `geo_lookup()`), or the YAML
schema changes in any way. Each new field requires: field name matching the
YAML key exactly, type (string, boolean, integer, duration, etc.), default value
or "required", a description of what it controls, and a minimal YAML example
showing the field in context. Omitting any of these makes the config reference
incomplete for operators.


## COLUMN_NAMING.md (`docs/COLUMN_NAMING.md`)

Update required when: the type conflict suffix format changes (currently double-
underscore: `status__int`, `status__str`), the conflict detection logic changes,
the definition of "type conflict" changes, the first-write-wins behavior for
duplicate keys changes, or the bare-name-by-default behavior changes. This
document is what operators use to write SQL queries against log columns — if
it is wrong, their queries break silently. Include before/after examples showing
input JSON and resulting column names for all affected cases.


## CRATE_RULES.md (`dev-docs/CRATE_RULES.md`)

Update required when: a new crate is added to the workspace (add a section for
it listing purpose, allowed dependencies, and enforcement rules), an existing
crate's allowed dependency set changes, a new CI-enforced structural rule is
added for a crate, or a rule changes from "convention" enforcement to "compiler"
enforcement (or vice versa). This file is the contract that agents use when
deciding what to add to which crate — inaccuracy causes agents to add the wrong
dependency to the wrong crate.


## PHASES.md (`dev-docs/PHASES.md`)

Update required when: a phase task is completed (mark with `✅ DONE` and add the
PR number), a phase is fully completed (add `✅ DONE` to the phase heading), a new
sub-task is identified and added to a phase, or a task is moved between phases.
The roadmap is the shared understanding of project direction between human
maintainers and AI agents. Out-of-date phase status causes agents to implement
work that is already done or skip work that is next in sequence. If this PR
completes a phase task, that task must be marked done in `PHASES.md`. Read
`PHASES.md` to determine current phase status rather than relying on any
cached snapshot.


## SCANNER_CONTRACT.md (`dev-docs/SCANNER_CONTRACT.md`)

Update required when: any parsing guarantee changes — what the scanner does with
UTF-8 violations, how duplicate keys are resolved, how escape sequences in string
values are handled, what "first-writer-wins" means precisely, what happens to
oversized lines, how null values are represented. This is the contract that
downstream code (the output sinks, SQL transforms) relies on. Any deviation from
a documented guarantee is a breaking change even if the code "works" — update the
contract to match the new behavior and justify the change.


## ZERO_COPY_PIPELINE.md (`dev-docs/ZERO_COPY_PIPELINE.md`)

Update required when: the buffer lifecycle changes (when `Bytes` is frozen, when
it is dropped, when `StringViewArray` views are created), a new copy is introduced
into what was a zero-copy path, a planned copy elimination is completed, or the
relationship between `StreamingBuilder` and `StorageBuilder` is clarified. This
document tracks the two unnecessary copies in the current implementation (read
into `Vec`, accumulate in `json_buf`) and the target state (`BytesMut` per file,
freeze to `Bytes`, views all the way to `RecordBatch`).


## Public API Doc Comments

Every new public function, struct, enum, or trait in any crate requires a `///`
doc comment. The comment must describe behavior, not just restate the name.
For `logfwd-core`, especially document: what invariants the caller must satisfy
(pre-conditions), what the function guarantees about its output (post-conditions),
and any performance contracts (e.g., "no allocation after the first call per
batch"). For config structs (serde-deserialized types in `logfwd-config`), every
public field must have a `///` doc comment that matches what is documented in
`CONFIG_REFERENCE.md` — these serve as in-code documentation for operators
reading the source.


## References (`dev-docs/references/`)

Update required when: a major dependency version is bumped (arrow, datafusion,
tokio, opentelemetry-otlp). The references directory contains API pattern guides
for the key dependencies: DataFusion (`SessionContext`, `MemTable`, UDF registration),
Arrow (`RecordBatch`, `StringViewArray`, `IpcWriteOptions`), Tokio (bounded channels,
`CancellationToken`, `block_in_place`, `select!` safety), OpenTelemetry OTLP (protobuf
nesting, HTTP vs gRPC, resource attributes), and Kani (proof API, solver selection,
function contracts). Only Arrow and DataFusion reference files are versioned by major
version number (`arrow-v54.md`, `datafusion-v45.md`); other reference files
(`tokio-async-patterns.md`, `opentelemetry-otlp.md`, `kani-verification.md`, etc.)
are unversioned and updated in-place. When a major version bump changes API patterns
for Arrow or DataFusion, rename the relevant reference doc to the new version and
update it. For unversioned reference files, update in-place.
