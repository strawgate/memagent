# Research: Windows Event Log Feasibility (Issue #1476)

## Context

logfwd has no Windows-native input. The Windows Event Log (via EvtSubscribe API) is the primary structured logging mechanism on Windows. This research evaluates feasibility.

## What to investigate

1. Windows Event Log APIs:
   - `EvtSubscribe` — push-based subscription
   - `EvtNext` — pull-based enumeration
   - `EvtRender` — event rendering/formatting
   - XML vs structured output formats

2. Rust accessibility:
   - `windows-rs` crate — does it expose Evt* APIs?
   - `winlog` crate — any existing Rust Event Log readers?
   - `evtx` crate — for offline .evtx parsing
   - FFI complexity assessment

3. Data model:
   - Event fields: EventID, Level, TimeCreated, Provider, Channel, Computer, UserData
   - How do Windows event levels map to OTLP severity?
   - Structured data extraction from EventData XML

4. Subscription model:
   - Channel selection (Application, System, Security, custom)
   - XPath query filtering
   - Bookmark/checkpoint persistence
   - How does this map to logfwd's SourceId/ByteOffset checkpoint model?

5. Cross-compilation:
   - Can logfwd cross-compile for Windows from Linux CI?
   - Feature-gating strategy for Windows-only code
   - Testing without a Windows host

## Deliverable

Write a research report to `dev-docs/research/fanout-2026-04-21/ws06-windows-eventlog-report.md` containing:

1. API accessibility from Rust
2. Data model mapping to logfwd
3. Subscription and checkpoint design
4. Cross-compilation strategy
5. Effort estimate
6. Go/no-go recommendation

Do NOT implement code changes. Research and design only.
