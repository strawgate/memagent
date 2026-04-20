# Developer Docs

Contributor and agent entrypoint for repo-critical engineering context.

## Start Here (5 minutes)

1. [Code Style → Mindset](CODE_STYLE.md#mindset) for the ethos: parse
   don't validate, invalid states unrepresentable, measure don't guess,
   compile time as a quality attribute.
2. [Architecture](ARCHITECTURE.md) for crate boundaries and data flow.
3. [Crate Rules](CRATE_RULES.md) for non-negotiable constraints.
4. [Verification](VERIFICATION.md) for proof/test requirements.
5. [Change Map](CHANGE_MAP.md) for required co-changes.

## Task Routing

- Changing config schema: [Change Map](CHANGE_MAP.md) + [Config Reference](../book/src/content/docs/configuration/reference.mdx).
- Changing scanner/core behavior: [Scanner Contract](SCANNER_CONTRACT.md) + [Verification](VERIFICATION.md).
- Changing IO/output boundaries: [Adapter Contract](ADAPTER_CONTRACT.md).
- Changing architecture or crate placement: [Architecture](ARCHITECTURE.md) + [Design](DESIGN.md).
- Opening/reviewing PRs: [PR Process](PR_PROCESS.md) + [review guides](review-guides/).

## Living Docs (Canonical)

- [ARCHITECTURE.md](ARCHITECTURE.md): data flow and layer boundaries.
- [DESIGN.md](DESIGN.md): architecture decisions and tradeoffs.
- [CRATE_RULES.md](CRATE_RULES.md): crate-level constraints.
- [VERIFICATION.md](VERIFICATION.md): Kani/TLA+/proptest requirements.
- [SCANNER_CONTRACT.md](SCANNER_CONTRACT.md): scanner IO contract.
- [ADAPTER_CONTRACT.md](ADAPTER_CONTRACT.md): boundary correctness contract.
- [CODE_STYLE.md](CODE_STYLE.md): style rules.
- [CHANGE_MAP.md](CHANGE_MAP.md): co-change matrix.
- [DOCS_STANDARDS.md](DOCS_STANDARDS.md): docs governance.
- [PR_PROCESS.md](PR_PROCESS.md): issue-to-merge mechanics.

## References (Concise)

Use [dev-docs/references](references/) for repo-scoped library notes only.
If a reference is generic Rust/library education, remove it or replace with an upstream link.

## Research Archive

Use [dev-docs/research/README.md](research/README.md) to find active investigations.
Research docs are point-in-time artifacts and are never canonical for runtime behavior.
