# ffwd-diagnostics Agent Guide

Read `../../dev-docs/ARCHITECTURE.md`, `../../dev-docs/DESIGN.md`,
`../../dev-docs/CRATE_RULES.md`, and `../../dev-docs/VERIFICATION.md`
before changing diagnostics behavior here.

Key rules:
- Own diagnostics HTTP/control-plane behavior, telemetry shaping, and local buffers
- Own generated diagnostics dashboard asset at `src/dashboard.html`
- Keep runtime orchestration out of this crate; consume snapshots, do not schedule pipelines
- Preserve OTLP JSON compatibility for dashboard/websocket payloads
- When policy/reducer logic changes, update Kani/proptest coverage in the same PR
