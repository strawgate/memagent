# File Discovery Notes (Repo-Scoped)

How file discovery works in `ffwd-io` tailing paths.

## Reliability Model

- File event streams (`notify`) are hints, not source of truth.
- Periodic glob/poll reconciliation is required for correctness.
- Rotation and deletion handling must be idempotent and safe under duplicated or missing events.

## Platform Caveats

- Event delivery can be coalesced or dropped under load.
- Backends differ by OS (inotify/kqueue/etc.), so behavior must not depend on exact event sequences.
- Startup/restart must rebuild state from filesystem reality, not cached watcher assumptions.

## Operational Rules

- Keep poll+watch dual-path design; do not replace polling with watch-only logic.
- Treat watcher disconnect/restart as recoverable and continue with polling.
- Keep LRU/open-file eviction decisions independent from event ordering assumptions.

## Canonical Docs

- `dev-docs/ARCHITECTURE.md` (tailing architecture and boundaries)
- `dev-docs/ADAPTER_CONTRACT.md` (checkpoint and delivery guarantees)

