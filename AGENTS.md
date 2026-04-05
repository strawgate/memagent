# Agent Guide

Use this file as a routing table. Read the linked docs before changing code.

## Start here

- `README.md` — user-facing overview, install, run, and docs entry points
- `DEVELOPING.md` — build/test/lint/bench commands and workspace layout
- `dev-docs/ARCHITECTURE.md` — pipeline data flow, scanner stages, crate map
- `dev-docs/DESIGN.md` — vision, target architecture, major design decisions
- `dev-docs/VERIFICATION.md` — TLA+, Kani, proptest expectations and status

## Required references

- `dev-docs/CRATE_RULES.md` — per-crate rules enforced by CI
- `dev-docs/CODE_STYLE.md` — naming, error handling, hot-path rules
- `dev-docs/DOCS_STANDARDS.md` — doc taxonomy, lifecycle rules, anti-drift
- `dev-docs/PR_PROCESS.md` — Copilot assignment, PR triage, review criteria
- `dev-docs/SCANNER_CONTRACT.md` — scanner input requirements and guarantees

## Useful references

- `dev-docs/references/arrow.md`
- `dev-docs/references/datafusion.md`
- `dev-docs/references/opentelemetry-otlp.md`
- `dev-docs/references/tokio-async-patterns.md`
- `dev-docs/references/kani-verification.md`
- `tla/README.md`
- Roadmap: GitHub issue `#889`

## Repo rules

- User docs live in `book/src/`; use `book/src/SUMMARY.md` as the table of contents.
- Keep changes repo-local and avoid unrelated refactors.
- Run `just ci` before final handoff when practical.
- Follow `dev-docs/CODE_STYLE.md` and `dev-docs/VERIFICATION.md` for code and proof requirements.
