# Workstream 5 — API Boundaries and Module Architecture for Future Crate Splits

Date: 2026-04-09
Scope: Workspace-level guidance for `logfwd-*` crate/module refactors with low semver/maintenance risk.

---

## 1) Bounded orientation summary (repo-grounded)

This guide is based on a bounded orientation pass over:

- `README.md`
- `dev-docs/ARCHITECTURE.md`
- `dev-docs/DESIGN.md`
- `dev-docs/CRATE_RULES.md`
- `dev-docs/CODE_STYLE.md`
- `dev-docs/CHANGE_MAP.md`
- Representative crate surfaces in `logfwd-core`, `logfwd-runtime`, `logfwd-io`, `logfwd-output`, `logfwd-transform`

### Observations that matter for boundaries

1. **Architecture intent is already layered and directionally strict.**
   `core -> arrow -> io/transform/output -> runtime -> binary` intent exists and is clearly documented, but module/public surfaces are not yet uniformly minimized.

2. **Trait seams are the primary stable joints** (good):
   - Scanner seam (`ScanBuilder`) for `core` ↔ `arrow`.
   - Input seam (`InputSource`) for runtime ↔ input adapters.
   - Output seam (`Sink`/`SinkFactory`) for runtime ↔ sinks.

3. **Current public surfaces are wider than needed** in a few places:
   - `logfwd-runtime` exports whole subsystem modules (`pipeline`, `processor`, `worker_pool`, `bootstrap`, `transform`) publicly.
   - `logfwd-io` exposes many implementation-oriented modules as `pub mod`.
   - `logfwd-output` re-exports many concrete sink internals and helper functions.

4. **Compatibility/facade re-exports currently exist in the binary crate** (`logfwd/src/lib.rs` re-exporting runtime modules), which increases churn risk during future splits.

5. **Code-style guidance includes “no pub use re-exports for backwards compatibility,”** but some crate surfaces currently rely heavily on re-exports. For upcoming splits, this should be normalized into a deliberate policy rather than ad hoc exceptions.

6. **Change-map and crate-rules already provide a governance backbone**; boundary hardening can be done incrementally with CI-enforced visibility checks and targeted façade modules.

---

## 2) API boundary principles for this repository

The following principles are opinionated for **this** workspace’s design goals (proof-oriented core, high-throughput pipeline, future crate decomposition):

### P1. Treat trait seams as stable; treat concrete implementations as replaceable

Stable (allowed to be public long-term):
- `logfwd-core` semantic traits/types that define crate joints.
- `logfwd-io::input::InputSource` and wire-level event contracts.
- `logfwd-output::sink::{Sink, SinkFactory}` plus minimal metadata contracts.

Unstable (should default to internal):
- concrete worker-pool internals, pipeline bookkeeping structs, helper modules.
- sink-specific helper functions and row-shaping internals.

### P2. Each crate should expose one “front door” module

For split-friendly evolution, each crate should converge toward:
- `pub mod api;` (or top-level `lib.rs` exports equivalent)
- internal modules private unless explicitly needed cross-crate.

This prevents callers from importing deep paths that later block file/module moves.

### P3. Public surface should model *capabilities*, not *layout*

Export nouns/traits users compose (`PipelineBuilder`, `InputSource`, `SinkFactory`, `SqlTransform`), not submodule names reflecting current filesystem layout (`worker_pool::dispatch`, `pipeline::submit`, etc.).

### P4. Verification boundaries and API boundaries should align

Any public API in proof-sensitive crates (`core`, pure seams in other crates) should map to explicit invariants and verification ownership. If no invariant ownership is defined, keep it non-public.

### P5. Minimize semver blast radius by preferring additive façade evolution

Future splits should:
1. Add new façade path.
2. Move implementation behind façade.
3. Deprecate old path (if needed).
4. Remove old path in major bump only.

This is safer than direct path-breaking moves.

---

## 3) Recommended visibility policy (`pub`, `pub(crate)`, internal conventions)

### 3.1 Workspace-wide policy

- **Default rule:** `mod` private.
- Promote to **`pub(crate)`** only when shared across files/modules in same crate.
- Promote to **`pub`** only for cross-crate consumers or intentional external API.
- For `pub` items:
  - add rustdoc explaining behavior and stability intent;
  - add (or link) invariant ownership/tests/proofs where applicable.

### 3.2 Crate-specific policy targets

#### `logfwd-core`

- Keep semantic modules public (scanner/framer/structural/otlp/etc.) where they represent stable pure logic.
- Introduce internal submodules for helper routines and keep only top-level semantic entrypoints public.
- Avoid exposing verification-only helper functions unless required by another crate.

#### `logfwd-runtime`

- Move toward **minimal public API**:
  - Public: runtime bootstrap/build/run entrypoints and essential config-facing types.
  - `pub(crate)`: `pipeline` internals, dispatch internals, checkpoint plumbing, worker implementation details.
- Keep `worker_pool::types` public only through intentionally curated type aliases/structs in a facade if needed.

#### `logfwd-io`

- Keep `input` contract module public.
- Keep concrete adapters public only if constructible by other crates directly; otherwise route construction through factory/builders and reduce adapter internals to private.
- Keep health and HTTP receiver internals `pub(crate)` unless externally consumed.

#### `logfwd-output`

- Keep `sink` traits and error types public.
- Treat concrete sink types as “semi-public” only if needed for explicit wiring/tests.
- Default helper utilities (`conflict_columns`, `row_json` helpers, auth-header builders) to `pub(crate)`.

#### `logfwd-transform`

- Keep `SqlTransform`, analyzer output surface, and explicitly supported UDF registration API public.
- Keep AST-walking internals and predicate extraction helpers non-public.

### 3.3 Internal module conventions

- Prefer `crate::internal::*` or `mod internal;` namespace for unstable helpers.
- Any module intended to be temporarily public for migration must carry:
  - `#[doc = "Temporary migration surface; may become private"]`
  - issue reference for removal timeline.

---

## 4) Re-export strategy (`lib.rs` surface vs internal paths)

### 4.1 Recommended strategy

1. **Export only from crate root (`lib.rs`) or a single `api` module.**
2. **Do not require downstream callers to import deep filesystem paths.**
3. **Re-export traits and stable types, not low-level utility functions.**
4. **Use re-exports to hide reorganizations, not to mirror every internal module.**

### 4.2 Practical policy for this repo

- `logfwd` binary crate should not be a broad compatibility mirror of runtime internals.
  - Keep only intentionally supported facade APIs.
- `logfwd-output` should reduce broad `pub use` lists and expose capability-focused constructors/factories.
- `logfwd-io` should expose high-level input constructors/contracts rather than every transport/helper module.

### 4.3 Split-safe deprecation pattern

- Phase A: Add `crate::api::*` exports.
- Phase B: Update internal/repo call sites to `crate::api::*`.
- Phase C: Deprecate old paths with clear replacement.
- Phase D: remove in major-version line.

---

## 5) Module size/split heuristics (agent-friendly maintenance)

These heuristics are designed to reduce conflict/churn in fanout workstreams and future crate splits.

### H1. “One concept per file” enforced with quantitative triggers

Trigger split if **any** applies:
- file > 800 lines and mixes 2+ responsibilities;
- contains both public API declarations and 400+ lines of algorithmic internals;
- test/proof sections exceed implementation size by >1.5× and obscure API seam.

### H2. Separate contract from implementation

For major subsystems, maintain:
- `contract.rs` / `types.rs` (public-facing data model)
- `impl_*.rs` (private behavior)
- `mod.rs` or `lib.rs` as façade only.

### H3. Keep hot-path logic and orchestration in different modules

- Parser/scanner tight loops: isolated for performance-focused review.
- Async orchestration and retries: isolated for correctness/liveness review.

### H4. Keep proof harnesses adjacent but non-exported

- `#[cfg(kani)] mod verification` and `#[cfg(test)] mod tests` in same file or sibling module.
- Never expose proof-only helper APIs publicly unless cross-crate proof reuse is required.

### H5. Prefer “vertical slices” for crate splits

When splitting crates, migrate complete vertical capabilities (types + trait + implementation + tests) in one step rather than moving utility fragments across crates repeatedly.

---

## 6) Cross-Crate Impact Map

This section is intended as a practical dependency/risk map for boundary changes.

### 6.1 Core contract seams

- **`logfwd-core`**
  - Owns parsing semantics, structural/framing/reassembly logic, checkpoint-tracker state machine.
  - Boundary risk: high verification coupling; any API churn triggers proof maintenance.

- **`logfwd-arrow`**
  - Implements `core` scanner/building seams and Arrow batch production.
  - Boundary risk: schema/materialization coupling to `transform` and `output`.

### 6.2 Runtime integration seams

- **`logfwd-io` -> `logfwd-runtime`**
  - `InputSource`, `InputEvent`, checkpoint/source identity data.
  - Risk of breakage when changing event shapes or health APIs.

- **`logfwd-transform` -> `logfwd-runtime`**
  - `SqlTransform`, analyzer-derived scan config and filter hints.
  - Risk when SQL analyzer output types change.

- **`logfwd-output` -> `logfwd-runtime`**
  - `Sink`/`SinkFactory`, `BatchMetadata`, delivery/ack semantics.
  - Risk when metadata or delivery result contracts shift.

### 6.3 Facade/binary seam

- **`logfwd` binary**
  - Should remain CLI/bootstrap facade; currently re-exports runtime modules.
  - Highest semver-churn risk if deep runtime paths remain externally visible.

### 6.4 Change coupling checklist for boundary edits

For any public API move/split, evaluate:
1. Does it cross a proof boundary (`core`/pure seam)?
2. Does it change runtime-input or runtime-output trait contracts?
3. Does it alter `logfwd` facade exports?
4. Does it require crate rules/agent docs updates?
5. Does it require change-map/doc updates for architectural intent?

---

## 7) Anti-patterns to avoid (with concrete repo examples)

### A1. Exporting whole subsystems instead of curated capability APIs

**Example:** `logfwd-runtime/src/lib.rs` currently exports full modules (`pub mod pipeline; pub mod worker_pool; ...`).

**Why this is risky:** internal path imports become de facto public API, blocking internal reorgs.

**Safer pattern:** expose a small runtime façade module with builders/entrypoints; keep internals `pub(crate)`.

### A2. Binary crate re-exporting runtime internals as compatibility surface

**Example:** `crates/logfwd/src/lib.rs` re-exports runtime modules directly.

**Why risky:** ties CLI/facade crate to runtime internal module layout and amplifies semver break risk during crate splits.

**Safer pattern:** re-export only stable user-intent APIs (e.g., `run_pipeline`, config model types), not module trees.

### A3. “Utility leak” via broad root `pub use` in integration crates

**Example:** `logfwd-output/src/lib.rs` re-exports many concrete sinks and helper functions.

**Why risky:** encourages downstream coupling to helper internals that should stay movable.

**Safer pattern:** keep low-level helpers `pub(crate)`; export sink registration/factory and essential sink contracts.

### A4. Public module proliferation in adapter crates

**Example:** `logfwd-io/src/lib.rs` exposes many modules directly.

**Why risky:** crate split becomes hard because external callers may rely on internal transport/health modules.

**Safer pattern:** define explicit adapter APIs and treat transport internals as implementation details.

### A5. Public API that reflects storage/layout mechanics instead of semantic contracts

**Example pattern to avoid:** exposing structures whose only purpose is internal worker dispatch ordering or checkpoint flush timing.

**Why risky:** semver locks in implementation details and blocks performance tuning.

---

## 8) Incremental rollout plan (safe evolution over rewrite)

### Phase 0 (now): policy + audit

- Add explicit “public API classification” sections to crate docs:
  - Stable public
  - Internal public (temporary)
  - Private
- Start a small API inventory table in each affected crate.

### Phase 1: façade-first introductions

- Add `api` modules (or curated root exports) in `runtime`, `io`, `output`.
- Keep existing exports for now; internally migrate call sites to new façade paths.

### Phase 2: constrain internals

- downgrade nonessential `pub` to `pub(crate)` behind façade coverage.
- mark temporary exports as deprecated with issue links.

### Phase 3: crate-split execution

- perform splits only after façade adoption is complete to avoid cascading path churn.
- keep semver-safe wrappers during transition.

---

## 9) Suggested enforcement additions (low-friction)

1. **Public API diff check in CI** (e.g., rustdoc JSON / cargo public-api) for key crates.
2. **“No deep import from sibling crate internals” check** via review guideline + lightweight script.
3. **Doc requirement:** any new `pub` item must include intended stability class.
4. **PR template checkbox:** “Does this change widen public API? If yes, why?”
5. **Quarterly boundary audit** tied to roadmap milestones.

---

## 10) Practical recommendations for near-term refactors

### Immediate visibility policy decision

Adopt:
- **Default private + `pub(crate)`-first** across `runtime`, `io`, `output`, `transform`.
- **Public by explicit contract only** (trait seams, key constructors, top-level orchestration types).

### Immediate re-export decision

Adopt:
- **Root façade exports only**.
- Do not add new deep-path module exports unless temporary and documented with removal issue.

### Immediate module split heuristic decision

Adopt:
- Split when a file crosses 800 lines with mixed concerns.
- Separate contracts/types from orchestration implementation.
- Keep tests/proofs adjacent but non-exported.

---

Recommendation: Adopt “Facade-First, Trait-Seam-Only Public API”

Top 5 architecture guardrails
1. Public APIs must be capability-based traits/types, not module layout mirrors.
2. Every nontrivial `pub` item needs an explicit stability class and owning invariant/tests.
3. Crate root (or `api`) is the only supported import surface; deep paths are internal.
4. `runtime`, `io`, and `output` default to `pub(crate)` internals; façade APIs carry cross-crate contracts.
5. Crate splits happen only after façade paths are in place and internal call sites are migrated.

Proposed next 3 refactor candidates and why
1. **`logfwd-runtime` API façade extraction** — highest semver risk reduction; stops module-tree coupling before any runtime split work.
2. **`logfwd` binary crate re-export reduction** — restores CLI/facade boundary and prevents compatibility lock-in to runtime layout.
3. **`logfwd-output` public surface minimization** — reduces helper leakage and allows sink internals to evolve/split independently.
