# Research: macOS OSLog/Unified Logging Feasibility (Issue #1478)

## Context

logfwd has Linux file tailing, journald, and eBPF inputs but no macOS-native log source. Apple's Unified Logging system (OSLog) is the primary logging mechanism on macOS. This research evaluates feasibility.

## What to investigate

1. Apple Unified Logging APIs:
   - OSLog framework — what data is accessible?
   - `log stream` / `log show` CLI tools — can we shell out?
   - OSLogStore API — programmatic access, limitations
   - Endpoint Security framework — relevant for security events?

2. Rust accessibility:
   - Are there existing Rust crates for OSLog? (oslog, apple-sys, etc.)
   - Can we use `objc2` or `core-foundation` to call Apple APIs?
   - FFI complexity assessment

3. Data model:
   - What fields are available? (timestamp, process, subsystem, category, message, level)
   - How do OSLog levels map to OTLP severity?
   - Structured vs unstructured messages

4. Permissions and entitlements:
   - What permissions are needed to read system logs?
   - SIP restrictions?
   - Can a daemon read logs from other processes?

5. Performance considerations:
   - Polling vs streaming?
   - Volume of log data on a typical macOS system
   - Memory/CPU overhead

## Deliverable

Write a research report to `dev-docs/research/fanout-2026-04-21/ws05-macos-oslog-report.md` containing:

1. API accessibility assessment (what works from Rust)
2. Data model mapping to logfwd's InputEvent
3. Permissions/entitlements requirements
4. Recommended approach (API vs CLI vs framework)
5. Effort estimate (days/weeks)
6. Go/no-go recommendation with reasoning

Do NOT implement code changes. Research and design only.
