# Predicate Pushdown Design

> **Status:** Active
> **Date:** 2026-04-21
> **Context:** capture the transform-to-input/scanner pushdown design space and the current advisory `FilterHints` model.

## Overview

The transform layer (DataFusion SQL) knows what the user wants. Inputs and
scanners can use that information to skip work at every layer — from kernel
(XDP) through parsing to Arrow column construction.

This is the same pattern as database predicate pushdown: push filters as close
to the data source as possible.

## Architecture

```text
User SQL: SELECT * FROM logs WHERE severity <= 4 AND facility = 16
                    │
                    ▼
         ┌─── QueryAnalyzer ───┐
         │  Parse SQL AST      │
         │  Extract predicates │
         │  Extract fields     │
         └────────┬────────────┘
                  │
                  ▼
           FilterHints
           ├── predicates: [SeverityAtMost(4), FacilityIn([16])]
           └── wanted_fields: ["severity", "facility", ...]
                  │
        ┌─────────┼──────────┐
        ▼         ▼          ▼
    InputSource  Scanner   Transform
    (optional)   (field    (SQL runs on
     XDP/kernel   pushdown) remaining
     filtering)             predicates)
```

## FilterHints

The core type that flows from transform → input/scanner:

```rust
/// Hints extracted from the user's SQL that downstream layers can use
/// to skip work. Every hint is optional — layers use what they can and
/// ignore the rest. The SQL transform still applies all predicates, so
/// correctness doesn't depend on pushdown; it only affects performance.
pub struct FilterHints {
    /// Syslog-specific: only forward messages with severity <= this value.
    /// Extracted from WHERE clauses like `severity <= 4` or `severity < 5`.
    pub max_severity: Option<u8>,

    /// Syslog-specific: only forward messages with these facilities.
    /// Extracted from `facility = 16` or `facility IN (1, 4, 16)`.
    pub facilities: Option<Vec<u8>>,

    /// Fields referenced in the query. Scanners can skip extracting
    /// unreferenced fields (field pushdown, already in ScanConfig).
    pub wanted_fields: Option<Vec<String>>,

}
```

Key design decision: **FilterHints are advisory, not mandatory.** The SQL
transform still runs the full query. This means:

- An input that ignores hints is still correct (just slower).
- An input that applies hints is faster but doesn't need to be perfect.
- The transform layer is the source of truth for correctness.

## Layers of Pushdown

### Layer 1: Kernel (XDP)
**What it can push down:** severity threshold, facility bitmap, source IP set

The XDP program reads predicates from BPF maps. When hints change, userspace
updates the maps — no BPF program reload needed.

```text
BPF map "config"[0] = max_severity (u64)
BPF map "config"[1] = syslog_port (u64)
BPF map "facility_filter" = hash set of allowed facilities
```

Packets that don't match → XDP_PASS (still delivered to other consumers)
or XDP_DROP (if we're the sole consumer). Matching packets → XDP_REDIRECT
to AF_XDP socket.

**Cost:** ~50-100ns per packet (BPF interpreter) or ~20ns (JIT)
**Savings:** Entire packet never enters our pipeline (~2μs saved per drop)

### Layer 2: Parser (Syslog/JSON)
**What it can push down:** severity filter, field selection

The syslog batch parser already takes a `severity_threshold` parameter.
With FilterHints, it also gets `wanted_fields` to skip extracting fields
the query doesn't reference.

```rust
// Before: parse all 7 fields
parse_syslog_line(buf) → SyslogLine { timestamp, hostname, app, pid, message, ... }

// After pushdown: only parse what's needed
parse_syslog_line(buf, &hints) → SyslogLine { severity, hostname, message }
// timestamp, app, pid skipped (never extracted from buffer)
```

**Cost:** ~5-10ns saved per skipped field
**Savings:** Modest for syslog (fields are cheap), significant for JSON

### Layer 3: Scanner (JSON → Arrow)
**What it can push down:** field selection (already implemented as ScanConfig)

The existing `ScanConfig.wanted_fields` is exactly this. The scanner skips
JSON keys that aren't in the wanted set.

### Layer 4: Arrow Column Builder
**What it can push down:** type selection

If the query only references `severity_int`, don't build `severity_str`
or `severity_float` columns. Already partially implemented via the
typed column model.

### Layer 5: DataFusion Transform
**What runs here:** Everything that wasn't pushed down.

Complex predicates (`message LIKE '%error%'`), aggregations, joins,
UDFs — these stay in DataFusion. The pushdown layers only handle
simple, common predicates.

## Extracting Predicates from SQL

The `QueryAnalyzer` already parses the SQL AST and walks WHERE clauses.
We extend it to recognize pushable patterns:

```rust
impl QueryAnalyzer {
    /// Extract filter hints from the parsed SQL for pushdown.
    pub fn filter_hints(&self) -> FilterHints {
        let mut hints = FilterHints::default();

        // Walk the WHERE clause AST looking for pushable predicates
        if let Some(where_expr) = &self.where_clause {
            extract_severity_predicate(where_expr, &mut hints);
            extract_facility_predicate(where_expr, &mut hints);
        }

        hints.wanted_fields = if self.uses_select_star {
            None  // need everything
        } else {
            Some(self.referenced_columns.iter().cloned().collect())
        };
        hints
    }
}
```

Recognizable patterns:

| SQL pattern | Extracted hint |
|-------------|---------------|
| `WHERE severity <= 4` | `max_severity: Some(4)` |
| `WHERE severity < 5` | `max_severity: Some(4)` |
| `WHERE severity = 3` | `max_severity: Some(3)` |
| `WHERE facility = 16` | `facilities: Some(vec![16])` |
| `WHERE facility IN (1, 4, 16)` | `facilities: Some(vec![1, 4, 16])` |
| `WHERE severity <= 4 AND facility = 16` | Both hints set |
| `WHERE severity <= 4 OR message LIKE '%x%'` | **Cannot push** (OR with non-pushable) |

The OR case is important: we can only push a predicate if it appears in
a top-level AND chain. If it's OR'd with something we can't push, we must
not push it (would miss matching rows).

## InputSource Integration

```rust
pub trait InputSource: Send {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>>;
    fn name(&self) -> &str;

    /// Apply filter hints for predicate pushdown. Inputs that support
    /// pushdown use these hints to skip data early. The default
    /// implementation ignores hints (correct but slower).
    fn apply_hints(&mut self, _hints: &FilterHints) {}
}
```

For UdpInput with XDP:

```rust
impl InputSource for UdpInput {
    fn apply_hints(&mut self, hints: &FilterHints) {
        // Push severity to parser
        if let Some(max_sev) = hints.max_severity {
            self.severity_threshold = max_sev;
        }

        // Push severity + facility to XDP BPF maps
        if let Some(ref xdp) = self.xdp {
            if let Some(max_sev) = hints.max_severity {
                xdp.set_severity_threshold(max_sev);
            }
            if let Some(ref facs) = hints.facilities {
                xdp.set_facility_filter(facs);
            }
        }
    }
}
```

## Pipeline Wiring

```rust
fn build_pipeline(config: &PipelineConfig) -> Pipeline {
    // 1. Parse SQL, extract hints
    let analyzer = QueryAnalyzer::new(&config.transform_sql)?;
    let scan_config = analyzer.scan_config();      // field pushdown
    let filter_hints = analyzer.filter_hints();    // predicate pushdown

    // 2. Build input with hints applied
    let mut input = UdpInput::new(&config.input)?;
    input.apply_hints(&filter_hints);

    // 3. Build scanner with field pushdown
    let scanner = Scanner::new(scan_config, 1024);

    // 4. Build transform (still runs full SQL for correctness)
    let transform = SqlTransform::new(&config.transform_sql)?;

    Pipeline { input, scanner, transform, ... }
}
```

## What This Means for Performance

For a typical query like `SELECT * FROM logs WHERE severity <= 4`:

| Layer | What's skipped | Savings per packet |
|-------|---------------|-------------------|
| XDP (kernel) | DEBUG/INFO/NOTICE packets never enter pipeline | ~2μs |
| Parser (userspace) | Redundant severity check skipped | ~5ns |
| Scanner | Nothing (SELECT *) | 0 |
| DataFusion | WHERE is redundant but cheap | ~85ns |

For a selective query like `SELECT hostname, message FROM logs WHERE severity <= 2 AND facility = 1`:

| Layer | What's skipped | Savings per packet |
|-------|---------------|-------------------|
| XDP | ~85% of packets dropped in kernel | ~2μs × 0.85 |
| Parser | timestamp, app_name, pid not extracted | ~15ns |
| Scanner | Only hostname + message columns built | ~50ns |
| DataFusion | WHERE is redundant | ~85ns |
