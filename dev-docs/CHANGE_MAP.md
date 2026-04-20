# Change Map

Use this map before editing code.
It tells you which files usually need to change together so behavior, docs, and verification stay aligned.

## Configuration changes

When you add, rename, or remove config fields:

- Config schema and validation in `logfwd-config` (and bootstrap wiring in `logfwd` when needed).
- User-facing config reference in `book/src/content/docs/configuration/reference.mdx`.
- Related task pages that show examples (`book/src/content/docs/getting-started/`, `book/src/content/docs/deployment/`).
- Validation and negative tests for invalid configs.

### Output schema is V2-only

Output YAML deserializes straight into `OutputConfigV2` — the typed
`#[serde(tag = "type", rename_all = "snake_case")]` enum in
`crates/logfwd-config/src/types.rs`. The flat `OutputConfig` struct and
its `From<V2>` / `From<&OutputConfig>` bridges were removed; nothing
else in the workspace carries a second shape. If your PR introduces a
new output type, add it as one more variant:

- Define the variant struct (`FooOutputConfig`) alongside the existing
  typed configs with `#[serde(deny_unknown_fields)]`.
- Add the variant to `OutputConfigV2` and extend the `name()`,
  `endpoint()`, and `output_type()` match arms.
- Add the sink construction arm in `logfwd-output::factory::build_sink_factory`.
- Do **not** reintroduce a shared flat struct — a knob that only applies
  to one output belongs on that variant. `deny_unknown_fields` then
  rejects the knob on every other variant for free.
- Cross-output validation messages ("X is only supported for Y outputs")
  from the old `validate.rs` pass are unreachable from YAML now —
  `deny_unknown_fields` catches them at parse time with
  `unknown field ...`. Don't add new cross-output validators unless a
  non-YAML caller can construct the invalid shape.

## Pipeline behavior changes

When scan, transform, batching, or output semantics change:

- Architecture expectations in `dev-docs/ARCHITECTURE.md`.
- Design rationale if tradeoffs changed in `dev-docs/DESIGN.md`.
- Troubleshooting guidance if observable symptoms changed in `book/src/content/docs/troubleshooting.md`.
- Performance docs if throughput/latency profile changed in `book/src/content/docs/architecture/performance.md`.

## Verification-impacting changes

When invariants, safety properties, or core algorithms change:

- Proof requirements and status in `dev-docs/VERIFICATION.md`.
- Non-core seam classification in `dev-docs/verification/kani-boundary-contract.toml`.
- Boundary validation script if rules changed: `scripts/verify_kani_boundary_contract.py`.
- Any Kani harnesses/proptest cases covering changed behavior.
- TLA+ specs if protocol-level behavior changed.

## Crate boundary changes

When logic moves between crates or new dependencies are introduced:

- Update rules in `dev-docs/CRATE_RULES.md`.
- Update crate-level `AGENTS.md` files where scope applies.
- Ensure dependency boundaries still match architecture intent.

## Documentation-only changes

Even docs-only PRs should update all canonical locations for that fact.
Do not duplicate the same reference data in multiple pages.
Link to canonical docs instead.

## Quick pre-PR checklist

Before opening a PR, confirm:

1. Code changes and docs changes are in the same PR when behavior changed.
2. User-visible changes update the user book (`book/src/content/docs/`).
3. Contributor-visible changes update relevant `dev-docs/` pages.
4. Verification expectations are updated if invariants changed.
5. You can point reviewers to exactly where each changed behavior is documented.
