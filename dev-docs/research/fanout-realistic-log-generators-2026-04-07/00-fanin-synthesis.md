# Realistic Log Generators Fan-In

Date: 2026-04-07

## Summary

This fan-in produced one clear cross-cutting winner and one clear first profile to ship:

- **Winner:** shared cardinality/helper layer
- **Best first profile to land:** CloudTrail-like audit logs
- **Best next profile after that:** Envoy access logs
- **Best design memo to implement next:** Kubernetes audit logs

The broad convergence across attempts is strong:

- CloudTrail: both attempts converged on `SHIP`
- Envoy access: both cloud attempts converged on `SHIP` even though one earlier local memo had `ITERATE`
- Kubernetes audit: both attempts converged on `SHIP`
- Shared helpers: one `SHIP`, one `ITERATE`, but the disagreement was about calibration/rollout timing, not architecture

## Recommendation

### 1. Land the shared helper layer

This is the most leverage-per-line result from the fanout.

Why:

- it directly addresses the current realism gap: sticky identities, heavy-tail hot keys, tenure/churn, burst locality, and sparse optional fields
- it is reusable across multiple generator families instead of baking ad hoc RNG tricks into each profile
- it gives us benchmark knobs that change entropy/cardinality without changing schema shape

Repo-local evidence:

- implementation exists in:
  - `crates/logfwd-bench/src/cardinality.rs`
  - `crates/logfwd-bench/src/generators.rs`
  - `crates/logfwd-bench/src/lib.rs`
  - `crates/logfwd-bench/README.md`
- local memo: `helpers-cardinality-result.md`
- cloud attempts:
  - one said `SHIP`
  - one said `ITERATE`

My read:

- treat this as **SHIP with calibration follow-up**, not a true disagreement
- the second attempt mostly argued for one more realism-validation pass, not for a different helper architecture

### 2. Land CloudTrail as the first new realistic profile

CloudTrail is the best “ready now” profile from this wave.

Why:

- it already has repo-local implementation, tests, and a profile binary
- it adds nested optional structure, repeated boilerplate, and high-cardinality IDs/ARNs in exactly the way our current generators are missing
- it is a good compression and parsing realism step up without requiring a new lifecycle engine first

Repo-local evidence:

- implementation exists in:
  - `crates/logfwd-bench/src/generators/cloudtrail.rs`
  - `crates/logfwd-bench/src/bin/cloudtrail_profile.rs`
  - supporting profile/types in `crates/logfwd-bench/src/generators.rs`
- local memo: `cloudtrail-result.md`
- cloud attempts both concluded `SHIP`

Most decision-grade data point from the landed prototype:

- 20k-line sample:
  - avg line length about `1108 bytes`
  - zstd-1 compression about `9.78x`
  - meaningful nested optionality and mid/high-cardinality principal/account/service mix

### 3. Implement Envoy next on top of the helper layer

Envoy is the best follow-up profile after CloudTrail.

Why:

- it complements CloudTrail well: edge/access traffic instead of control-plane audit
- the strongest ideas are correlation-heavy and should benefit directly from the shared helper layer
- it is likely to be especially useful for throughput/selectivity benchmarking because of route/service/status/UA/IP coupling

Convergence:

- cloud attempts both converged on `SHIP`
- earlier local memo was `ITERATE`

My read:

- the design is good enough to proceed
- but compared with CloudTrail, it is one step less “ready to land” because the strongest evidence is still design-level rather than a dedicated repo-local module/bin with measured profile output

### 4. Use the Kubernetes audit design memo as the next implementation brief

The Kubernetes audit lane produced the strongest pure design memo.

Why:

- it proposes the right unit of generation: request lifecycle first, then staged audit events
- it captures audit-level gating, stage sparsity, actor classes, and `requestURI/objectRef/verb/namespace/sourceIP` correlation
- it is exactly the sort of nested JSON profile that should improve realism for scan/encode/compress paths

But:

- this is design-first, not implementation-first
- I would not land it before CloudTrail unless the immediate benchmark goal is specifically Kubernetes audit behavior

## What The Attempts Agree On

### Shared helpers

Most attempts agree that the repo needs:

- heavy-tail selection
- lease/tenure windows
- burst locality
- sparse optional-field control
- reusable deterministic knobs instead of profile-specific RNG hacks

The disagreement was about confidence to ship immediately, not about the helper direction.

### Generator direction

The workstreams consistently prefer:

- infra-shaped profiles grounded in official schemas/docs
- correlated fields over independent randomness
- stable schema shape with value-level entropy knobs
- deterministic outputs for benchmark repeatability

## What Is Repo-Local Versus Design-Only

### Repo-local and strongest

- shared helper/cardinality layer
- CloudTrail generator and profile binary

### Strong design, weaker implementation evidence

- Envoy access profile
- Kubernetes audit profile

That means the best next engineering order is:

1. helper layer
2. CloudTrail profile
3. Envoy profile
4. Kubernetes audit profile

## Residual Risks

- The helper lane should still get one calibration pass against real sampled logs before we over-trust its presets.
- CloudTrail compression numbers are useful, but still synthetic; use them as directional realism evidence, not production truth.
- Envoy and Kubernetes still need actual repo-local generator implementations before we can call them benchmark-ready.

## Final Call

Recommendation:

- **SHIP** the shared helper/cardinality layer
- **SHIP** the CloudTrail profile as the first new realistic generator
- **IMPLEMENT NEXT** the Envoy profile on top of that helper layer
- **USE AS BRIEF** the Kubernetes audit memo for the next design-to-code pass
