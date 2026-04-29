---
title: Live Config Reloading
description: Reload ffwd configuration without restarting the process
---

ffwd supports live configuration reloading — you can change the YAML config file
and apply changes without any process downtime. Checkpoints are preserved across
reloads, so no log data is lost or duplicated.

## Reload triggers

There are three ways to trigger a config reload:

### 1. SIGHUP (Unix)

Send a HUP signal to the ffwd process:

```bash
kill -HUP $(pidof ff)
# or
systemctl reload ffwd
```

### 2. HTTP endpoint

POST to the diagnostics reload endpoint:

```bash
curl -X POST http://localhost:8686/api/v1/reload
```

Returns:
- `202 Accepted` — reload triggered
- `503 Service Unavailable` — reload not configured (no diagnostics server)
- `429 Too Many Requests` — a reload is already in progress

> **Security note:** The reload endpoint is served on the diagnostics listener
> (configured via `server.diagnostics`). Bind it to `127.0.0.1` or a private
> interface to prevent unauthorized external reload triggers.

### 3. File watching (`--watch-config`)

Start ffwd with the `-w` / `--watch-config` flag to automatically reload
when the config file changes on disk:

```bash
ff run --config ffwd.yaml --watch-config
```

Changes are debounced (500ms) to avoid reloading during partial writes.

## What happens during a reload

1. The current pipelines are gracefully drained (all in-flight data is flushed)
2. The config file is re-read and validated
3. New pipelines are built from the new config
4. Checkpoints ensure the new pipelines resume from where the old ones left off

Typical reload latency is **< 250ms** for a standard configuration.

## What can be reloaded

| Change | Reloadable? | Notes |
|--------|-------------|-------|
| Pipeline inputs/outputs | ✅ Yes | Pipelines are fully rebuilt |
| Pipeline transforms (SQL) | ✅ Yes | |
| Resource attributes | ✅ Yes | |
| Add/remove pipelines | ✅ Yes | |
| Storage config | ✅ Yes | |
| OpAMP config | ✅ Yes | |
| Server diagnostics address | ❌ No | Requires full restart |

If the new config is invalid (bad YAML, validation errors), the reload is rejected
and ffwd continues running with the previous configuration. An error is logged.

## Telemetry

ffwd reports reload metrics via OpenTelemetry:

| Metric | Type | Description |
|--------|------|-------------|
| `ffwd.config.reload.total` | Counter | Total reload attempts |
| `ffwd.config.reload.success` | Counter | Successful reloads |
| `ffwd.config.reload.error` | Counter | Failed reload attempts |

## Example: systemd reload

```ini
[Service]
ExecStart=/usr/local/bin/ff run --config /etc/ffwd/ffwd.yaml
ExecReload=/bin/kill -HUP $MAINPID
```

Then use `systemctl reload ffwd` to apply config changes.
