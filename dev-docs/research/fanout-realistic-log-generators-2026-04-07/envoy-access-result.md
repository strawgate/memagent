# Envoy Access Generator Result

> **Status:** Active
> **Date:** 2026-04-08
> **Context:** Evaluate whether an Envoy-shaped realistic generator should ship to improve benchmark fidelity.

## Recommendation

Build a profile-driven Envoy access-log generator that emits six traffic
classes (`browse`, `api_read`, `api_write`, `ingest`, `health`,
`failure_pocket`) with correlated fields, then layer route popularity, client
tenure, size distributions, and retry pockets on top. This is a better fit for
benchmarking than the current mostly-iid synthetic JSON because it preserves
the regularities real proxies see without collapsing into perfectly repetitive
rows.

The design should use Envoy's own access-log semantics as the field anchor:
command operators from [Envoy access logging usage](https://www.envoyproxy.io/docs/envoy/latest/configuration/observability/access_log/usage.html)
and typed JSON access logs from Envoy's access-log docs. The generator should
model the common fields people actually log: start time, method, path,
authority, response code, response flags, bytes received/sent, duration,
upstream service time, user agent, request ID, trace ID, upstream cluster, and
downstream IP metadata.

## Why this profile is realistic

The main realism win is correlation.

Real Envoy traffic is not independent per line. Route choice, client identity,
HTTP method, user agent, request size, response size, latency, and error mode
all tend to move together. A realistic generator should preserve that structure
instead of picking every field independently.

The most important correlations to bake in are:

- route popularity skew: a few routes dominate volume, with a long tail of rare paths
- client IP tenure: most requests come from a sticky client population, but some addresses churn quickly
- method/path/user-agent coupling: browsers, mobile clients, and service-to-service calls should not all hit the same paths equally
- size coupling: POST/PUT/PATCH should have larger request bodies than GET, and successful responses should often be larger than failures
- failure pockets: retries and 5xx/429 bursts should happen in windows or route clusters, not as independent random flips

That combination gives compression and OTLP benchmarks useful structure without
making the data unrealistically uniform.

## Proposed Generator Shape

Use a small profile model with four layers:

1. `TrafficClass`
   - `browse`
   - `api_read`
   - `api_write`
   - `ingest`
   - `health`
   - `failure_pocket`

2. `ClientPopulation`
   - sticky clients with long tenure
   - rotating NAT pools
   - short-lived churners

3. `RouteModel`
   - route popularity weights
   - method/path pairings
   - route-specific latency and response-size distributions

4. `PocketModel`
   - burst windows that raise retry rates and 5xx/429 responses on a subset of routes
   - client sessions in the pocket should keep retrying the same route or upstream cluster for a while

## Sample Profile Sketch

```yaml
envoy_access_v1:
  timestamp:
    clock: monotonic
    jitter_ms: [0, 15]

  routes:
    - name: home
      method: GET
      path: /
      weight: 22
      status_mix: {200: 0.95, 304: 0.04, 500: 0.01}
      bytes_sent_kib: {median: 3, p95: 12}
      duration_ms: {median: 4, p95: 20}
      user_agents: [browser, crawler]

    - name: browse_api
      method: GET
      path_prefixes: [/api/v1/users, /api/v1/orders, /api/v2/search]
      weight: 38
      status_mix: {200: 0.93, 404: 0.03, 429: 0.02, 503: 0.02}
      bytes_sent_kib: {median: 8, p95: 48}
      duration_ms: {median: 12, p95: 65}
      user_agents: [browser, mobile, service]

    - name: write_api
      methods: [POST, PUT, PATCH]
      path_prefixes: [/api/v1/orders, /api/v2/ingest, /api/v2/events]
      weight: 18
      status_mix: {200: 0.84, 201: 0.07, 409: 0.04, 429: 0.03, 500: 0.02}
      bytes_received_kib: {median: 2, p95: 64}
      bytes_sent_kib: {median: 4, p95: 20}
      duration_ms: {median: 28, p95: 180}
      user_agents: [service, mobile]

    - name: health
      method: GET
      path: /health
      weight: 17
      status_mix: {200: 0.995, 503: 0.005}
      bytes_sent_kib: {median: 0.4, p95: 1}
      duration_ms: {median: 1, p95: 4}
      user_agents: [probe, envoy, k8s]

    - name: failure_pocket
      weight: 5
      route_targets: [browse_api, write_api]
      window_seconds: [45, 120]
      status_mix: {429: 0.30, 503: 0.55, 504: 0.15}
      retry_multiplier: 2.5
      latency_multiplier: 3.0

  clients:
    sticky_share: 0.60
    rotating_nat_share: 0.27
    churn_share: 0.13
    tenure_requests:
      sticky: {median: 1500, p95: 12000}
      rotating_nat: {median: 120, p95: 800}
      churn: {median: 12, p95: 80}

  correlations:
    browser_routes: [home, browse_api]
    mobile_routes: [browse_api, write_api]
    service_routes: [write_api, health]
    retry_on_same_route_probability: 0.92
    retry_on_same_client_probability: 0.88

  emitted_fields:
    - timestamp
    - method
    - authority
    - path
    - protocol
    - response_code
    - response_flags
    - bytes_received
    - bytes_sent
    - duration_ms
    - upstream_service_time_ms
    - user_agent
    - request_id
    - trace_id
    - upstream_cluster
    - downstream_remote_address
```

## What Makes This Benchmark-Friendly

- Route skew creates hot strings and repeatable column patterns, which is good
  for measuring compression and scan behavior, but the long tail keeps the data
  from being trivially repetitive.
- Client tenure creates realistic repeated IPs and request IDs across a session,
  which exercises grouping and stateful behavior.
- Failure pockets create local bursts of similar status codes and paths, which
  is much closer to real proxy behavior than independent random 5xx noise.
- Method/path/user-agent coupling means the same path family is not paired with
  every user agent uniformly, so the generated data has meaningful entropy.
- Size distributions are route-specific and log-normal-ish rather than flat,
  which better matches HTTP traffic and keeps compression from being overly
  optimistic.

## What Would Be Unrealistic

- Purely random route and status selection.
- Uniform per-line byte sizes.
- Independent failures with no route or time locality.
- One client IP per row or one route path per method with no skew.
- Identical user-agent mix across all paths.

## Repo Fit

This profile fits the existing benchmark generators well:

- it can be implemented as a reusable helper in `crates/logfwd-bench/src/generators.rs`
- it can also feed the competitive bench data generator if we want on-disk files
- it should expose knobs for route skew, tenure, churn, and failure-pocket density

I did not prototype code in this pass, but the profile is concrete enough to
implement directly without inventing a new framework.
