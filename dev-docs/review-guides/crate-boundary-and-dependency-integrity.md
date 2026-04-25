# Crate Boundary and Dependency Integrity

Use this checklist to reject architectural boundary regressions.

## Hard Fails

Reject PRs that do any of the following:

- Add `std` usage to `ffwd-core`.
- Introduce `unsafe` in `ffwd-core`.
- Add non-allowlisted dependencies to `ffwd-core`.
- Introduce upward or cyclic crate dependencies.
- Move domain logic into `ffwd` binary crate.

## Core Contract (`ffwd-core`)

- Must remain `no_std` + `forbid(unsafe_code)`.
- Must not include IO, async runtime, thread management, or transport logic.
- Public API changes require verification impact review.

## Direction Rules

Dependency flow must remain downward by layer:

1. `ffwd-core`
2. `ffwd-arrow` / `ffwd-types`
3. IO/transform/output/runtime crates
4. `ffwd` binary shell

If a lower crate depends on a higher one, fail review.

## Per-PR Checks

- `Cargo.toml` changes align with `dev-docs/CRATE_RULES.md`.
- Crate purpose is unchanged or intentionally updated in docs.
- New crate (if any) has rules and docs updates.

## Required Companion Updates

If crate boundaries changed, confirm updates to:

- `dev-docs/CRATE_RULES.md`
- `dev-docs/ARCHITECTURE.md`
- `dev-docs/CHANGE_MAP.md` (if process changed)

## Evidence Commands

```bash
cargo check --workspace
cargo tree --workspace
```

Use command output plus manifest diff to support findings.
