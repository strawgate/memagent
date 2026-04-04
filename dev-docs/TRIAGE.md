## PR Triage Report — strawgate/memagent

*Triaged on 2026-04-04. 12 open PRs reviewed (skipped #932 — this PR, [WIP]).*

---

### Legend
- ✅ **Safe to merge** — CI green, no blocking issues
- 🟡 **Needs minor fixes** — specific actionable items before merge
- 🔴 **Blocked** — CI failure, merge conflicts, or architectural issues

---

### Summary Table

| PR | Title | CI | Review | Labels | Verdict |
|----|----|----|----|----|----|
| [#921](https://github.com/strawgate/memagent/pull/921) | fix: checkpoint safety | ✅ | none | `bug` `production` `P0: critical` `rust` | ✅ Merge |
| [#919](https://github.com/strawgate/memagent/pull/919) | perf(logfwd-io): reduce allocations | ✅ | none | `performance` `P2: medium` `rust` `copilot` | ✅ Merge |
| [#906](https://github.com/strawgate/memagent/pull/906) | feat: structured logging | ✅ | ✅ approved | `enhancement` `P2: medium` `rust` `copilot` | ✅ Merge |
| [#914](https://github.com/strawgate/memagent/pull/914) | docs: README redesign | ✅ | none | `documentation` `P3: low` | ✅ Merge |
| [#930](https://github.com/strawgate/memagent/pull/930) | fix: file tailer TOCTOU/Truncated | ✅ | ❌ changes requested | `bug` `P1: high` `rust` `jules` | 🟡 Fix stub files |
| [#907](https://github.com/strawgate/memagent/pull/907) | feat: OTLP verification coverage | ⚠️ Kani cancelled | ❌ changes requested | `enhancement` `P2: medium` `rust` `copilot` | 🟡 Minor fixes |
| [#903](https://github.com/strawgate/memagent/pull/903) | feat: config validation hardening | ✅ | ❌ changes requested | `enhancement` `production` `P2: medium` `rust` `copilot` | 🟡 Fix path validation layer |
| [#920](https://github.com/strawgate/memagent/pull/920) | feat: issue lifecycle automation | ✅ | ❌ changes requested | `enhancement` `agentic-workflows` `github_actions` `P3: low` | 🟡 Fix 3 workflow issues |
| [#931](https://github.com/strawgate/memagent/pull/931) | perf(kani): kissat solver | ❌ Kani FAIL | ✅ approved | `performance` `P2: medium` `rust` | 🔴 Kani still failing |
| [#917](https://github.com/strawgate/memagent/pull/917) | perf(elasticsearch): heap allocs | ❌ Tests FAIL | none | `performance` `P2: medium` `rust` `copilot` | 🔴 Fix test failures |
| [#911](https://github.com/strawgate/memagent/pull/911) | feat: Phase 7 TLA+/force_stop | ❌ TLA+ FAIL + Kani cancelled | ❌ changes requested | `enhancement` `research` `P2: medium` `rust` | 🔴 Fix TLA+ CI |
| [#924](https://github.com/strawgate/memagent/pull/924) | feat: Arrow-native transport | CONFLICTING | ✅ approved | `enhancement` `P1: high` `rust` | 🔴 Resolve conflicts |

---

### Verdicts

#### ✅ Immediate Merge Candidates

**[#921](https://github.com/strawgate/memagent/pull/921) — `fix: checkpoint safety`**
- Addresses three data-loss bugs: directory fsync (#386), evicted offset persistence (#697), stale fingerprint (#817)
- CI fully green (Kani skipped — not touching Kani-verified code). All test plan items checked.
- Labels: `bug` `production` `P0: critical` `rust`
- **Action:** `gh pr merge 921 --squash`

**[#919](https://github.com/strawgate/memagent/pull/919) — `perf(logfwd-io): reduce allocations`**
- Removes dead `TailedFile.path` field, pre-allocates read buffer, embeds `source_id` in `TailEvent` to eliminate per-poll HashMap construction.
- CI fully green. Clean, focused change.
- Labels: `performance` `P2: medium` `rust` `copilot`
- ⚠️ Note: modifies `tail.rs` — check for conflicts with #930 before merging.
- **Action:** `gh pr merge 919 --squash`

**[#906](https://github.com/strawgate/memagent/pull/906) — `feat: structured logging`**
- Adds tracing subscriber with env-filter + fmt (TTY-aware) + OTel layers; converts all `eprintln!` to `tracing::*` macros.
- CI green, CodeRabbit approved.
- Labels: `enhancement` `P2: medium` `rust` `copilot`
- **Action:** `gh pr merge 906 --squash`

**[#914](https://github.com/strawgate/memagent/pull/914) — `docs: README redesign`**
- Documentation only: restructures quickstart, fixes Docker image name, corrects artifact names.
- CI green, low risk.
- Labels: `documentation` `P3: low`
- **Action:** `gh pr merge 914 --squash`

---

#### 🟡 Merge After Minor Fixes

**[#930](https://github.com/strawgate/memagent/pull/930) — `fix: file tailer TOCTOU/Truncated`**
- Core logic changes in `tail.rs` are correct (fd-based metadata for TOCTOU, truncation drain fix). CI green.
- Issue: 3 stub files accidentally committed to repo root: `test_816.rs`, `test_symlink.rs`, `test_symlink` (binary).
- Labels: `bug` `P1: high` `rust` `jules`
- **Action:** Delete `test_816.rs`, `test_symlink.rs`, `test_symlink` from repo root, then merge.

**[#907](https://github.com/strawgate/memagent/pull/907) — `feat: OTLP verification coverage`**
- 14 roundtrip oracle tests and OTLP field constants. CI green (Kani cancelled — infrastructure issue, not this PR's fault).
- CodeRabbit items: (1) use `bytes_field_size()` helper in `scope_logs_inner_size` loop; (2) update `VERIFICATION.md` to say "mixed exhaustive + bounded" coverage.
- Labels: `enhancement` `P2: medium` `rust` `copilot`
- **Action:** Apply two minor fixes; merges once #931 unblocks Kani.

**[#903](https://github.com/strawgate/memagent/pull/903) — `feat: config validation hardening`**
- 5 new validation points, 16 new tests. CI green.
- Issue: Path existence checks run in `Config::validate()` before `base_path` resolution in `Pipeline::from_config()` → relative paths incorrectly rejected. Also missing: top-level `enrichment` rejection for advanced-form configs, and table-name dedup check.
- Labels: `enhancement` `production` `P2: medium` `rust` `copilot`
- **Action:** Move path checks to `Pipeline::from_config()`, add 2 missing validation rules.

**[#920](https://github.com/strawgate/memagent/pull/920) — `feat: issue lifecycle automation`**
- PR template + `auto-update-work-units.yml` workflow. CI green.
- CodeRabbit flagged: (1) no `work-unit` label guard — workflow can mutate any issue; (2) missing pagination (only first 20 results); (3) race condition — no retry for concurrent checkbox updates.
- Labels: `enhancement` `agentic-workflows` `github_actions` `P3: low`
- **Action:** Fix 3 workflow issues, then merge.

---

#### 🔴 Blocked — Needs Investigation

**[#931](https://github.com/strawgate/memagent/pull/931) — `perf(kani): kissat solver`**
- Kani proofs still failing at 32m32s timeout despite the kissat solver annotation. CodeRabbit approved.
- Root cause: `verify_write_json_line_no_prefix` proof still exceeds the 45-minute timeout even with kissat.
- Labels: `performance` `P2: medium` `rust`
- **Action:** Check Kani job logs at [run 23983262979](https://github.com/strawgate/memagent/actions/runs/23983262979/job/69950981147); consider `#[kani::proof_for_contract]` with `--unwind` limits or splitting the proof.

**[#917](https://github.com/strawgate/memagent/pull/917) — `perf(elasticsearch): heap allocs`**
- Test (Linux) and Test (macOS) both failing. Build and lint pass.
- Labels: `performance` `P2: medium` `rust` `copilot`
- **Action:** Check test failure logs at [run 23983606004](https://github.com/strawgate/memagent/actions/runs/23983606004); likely integration test failure for new `elasticsearch_streaming` config path.

**[#911](https://github.com/strawgate/memagent/pull/911) — `feat: Phase 7 TLA+/force_stop`**
- TLA+ model checking failing at 16s (spec parse error or TLC assertion). Kani cancelled by 30min timeout.
- CodeRabbit: `MCShutdownProtocol.tla` constants dead; `CheckpointNeverAheadOfFlushed` invariant compares wrong watermark; `ForceStop` fires too eagerly; `with_checkpoint_store()` doesn't restore saved offsets.
- Labels: `enhancement` `research` `P2: medium` `rust`
- **Action:** Check TLA+ CI logs at [run 23973938384](https://github.com/strawgate/memagent/actions/runs/23973938384/job/69927959181); fix spec correctness issues before merging.

**[#924](https://github.com/strawgate/memagent/pull/924) — `feat: Arrow-native transport (Phases A-D)`**
- CONFLICTING with master. Large PR (~4,700 lines). Only CodeRabbit CI ran.
- Labels: `enhancement` `P1: high` `rust`
- **Action:** Resolve merge conflicts with master, re-run CI, then full architectural review before merging.

---

### Recommended Merge Order

```
1. gh pr merge 921 --squash  # P0 data-loss fixes — merge first
2. gh pr merge 906 --squash  # structured logging — no conflicts
3. gh pr merge 914 --squash  # docs only — no conflicts
4. # Fix stub files on #930 branch, then:
   gh pr merge 930 --squash
5. gh pr merge 919 --squash  # after #930 merged (shared tail.rs)
6. # Fix issues on #903, #907, #920, then merge in any order
```
