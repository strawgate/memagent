# ffwd-runtime Agent Guide

Read `../../dev-docs/ARCHITECTURE.md`, `../../dev-docs/DESIGN.md`, and
`../../dev-docs/VERIFICATION.md` before changing runtime behavior here.

Key rules:
- Own async orchestration only: pipeline loop, worker pool, processor chain, runtime wiring
- Prefer extracting pure reducers and seam modules instead of growing async shells
- Preserve feature forwarding behavior for `datafusion` and `turmoil`
- When invariants change, update Kani/proptest/TLA artifacts in the same PR
- Keep the `ffwd` crate as a thin CLI/facade layer; avoid reintroducing large runtime logic there
