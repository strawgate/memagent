---
title: "Choose the Right Guide"
description: "Find the shortest path to your goal with logfwd"
sidebar:
  order: 0
---

Not sure where to start? Match your goal below.

## I want to see logfwd working quickly

[Quick Start](/getting-started/quickstart/) — working pipeline in about 10 minutes with local sample data. No external dependencies.

## I want to install logfwd for my platform

[Installation](/getting-started/installation/) — binary download, Docker, build from source, or Kubernetes DaemonSet.

## I want a production-ready pipeline

[Your First Pipeline](/getting-started/first-pipeline/) — CRI format, monitoring, multi-pipeline setup, and enrichment tables.

## I want to understand how logfwd works

[How logfwd Works](/architecture/) — interactive pipeline diagram with drill-down into every component.

## I need to configure specific fields or behaviors

[Configuration Reference](/configuration/reference/) — canonical source of truth for all YAML fields, input/output types, and options.

## I need SQL transform examples

[SQL Transforms](/configuration/sql-transforms/) — DataFusion SQL patterns, custom UDFs (`grok`, `regexp_extract`, `json`, `geo_lookup`), and enrichment table JOINs.

## I need deployment help

- [Kubernetes DaemonSet](/deployment/kubernetes/) — manifest, resource sizing, OTLP collector integration
- [Docker](/deployment/docker/) — container images, volume config, health checks
- [Monitoring & Diagnostics](/deployment/monitoring/) — diagnostics API, key metrics, transport observability

## I am debugging an issue

[Troubleshooting](/troubleshooting/) — symptom-first triage table, diagnostic commands, recovery fallback.

## I want to contribute code

[Contributing](/development/contributing/) — workspace layout, build commands, then [Building & Testing](/development/building/) for prerequisites. For internals, see [Pipeline Design](/architecture/pipeline/) and [Scanner](/architecture/scanner/).
