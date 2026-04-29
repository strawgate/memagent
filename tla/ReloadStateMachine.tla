---------------------------- MODULE ReloadStateMachine ----------------------------
(* 
 * TLA+ specification of the ffwd config reload state machine.
 *
 * Models the lifecycle of a pipeline reload triggered by SIGHUP, file watch,
 * HTTP endpoint, or OpAMP remote config push. Verifies:
 *   - No data loss: pipelines drain before rebuild
 *   - No double-drain: only one reload can be in progress at a time
 *   - Convergence: every reload trigger eventually results in running pipelines
 *   - Config validation: invalid configs do not replace running pipelines
 *
 * Note: We use integer sentinels to avoid TLC type errors from mixing strings
 * and integers in the same variable domain.
 *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxReloads,       \* Bound on total reload events for model checking
    MaxPipelines      \* Bound on pipeline count

NONE == -1       \* Sentinel: no pending config
INVALID == -2    \* Sentinel: pending config failed validation

VARIABLES
    state,            \* Main state: "running" | "draining" | "building" | "waiting"
    config,           \* Current active config (a natural number representing version)
    pending_config,   \* Config read from disk after reload trigger (version, INVALID, or NONE)
    reload_count,     \* Number of completed reloads
    reload_pending,   \* Whether a reload signal is waiting
    pipelines_running \* Number of pipelines currently executing

vars == <<state, config, pending_config, reload_count, reload_pending, pipelines_running>>

TypeOK ==
    /\ state \in {"running", "draining", "building"}
    /\ config \in 0..MaxReloads
    /\ pending_config \in {NONE, INVALID} \cup 0..MaxReloads
    /\ reload_count \in 0..MaxReloads
    /\ reload_pending \in BOOLEAN
    /\ pipelines_running \in 0..MaxPipelines

Init ==
    /\ state = "running"
    /\ config = 0
    /\ pending_config = NONE
    /\ reload_count = 0
    /\ reload_pending = FALSE
    /\ pipelines_running = 1

(* --- Reload trigger arrives (SIGHUP / file watch / HTTP / OpAMP) --- *)
TriggerReload ==
    /\ reload_count < MaxReloads
    /\ ~reload_pending
    /\ state = "running"
    /\ reload_pending' = TRUE
    /\ UNCHANGED <<state, config, pending_config, reload_count, pipelines_running>>

(* --- Begin drain: pipelines receive shutdown signal --- *)
BeginDrain ==
    /\ state = "running"
    /\ reload_pending
    /\ state' = "draining"
    /\ reload_pending' = FALSE
    /\ UNCHANGED <<config, pending_config, reload_count, pipelines_running>>

(* --- Drain completes: all pipelines have flushed --- *)
DrainComplete ==
    /\ state = "draining"
    /\ pipelines_running' = 0
    /\ state' = "building"
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending>>

(* --- Read and validate new config --- *)
ReadConfig ==
    /\ state = "building"
    /\ pending_config = NONE
    \* Non-deterministically choose valid or invalid new config.
    \* Valid config versions are strictly greater than current (monotonic).
    /\ \E v \in {config + 1, INVALID} :
        /\ (v # INVALID) => (v \in 1..MaxReloads)
        /\ pending_config' = v
    /\ UNCHANGED <<state, config, reload_count, reload_pending, pipelines_running>>

(* --- Config is valid: apply it and rebuild pipelines --- *)
ApplyValidConfig ==
    /\ state = "building"
    /\ pending_config \in 1..MaxReloads
    /\ config' = pending_config
    /\ pending_config' = NONE
    /\ pipelines_running' = 1  \* Simplified: at least 1 pipeline rebuilt
    /\ state' = "running"
    /\ reload_count' = reload_count + 1
    /\ UNCHANGED <<reload_pending>>

(* --- Config is invalid: keep old config version, rebuild with it --- *)
RejectInvalidConfig ==
    /\ state = "building"
    /\ pending_config = INVALID
    /\ pending_config' = NONE
    /\ pipelines_running' = 1  \* Rebuild with previous config
    /\ state' = "running"      \* Return to running with old config
    /\ UNCHANGED <<config, reload_count, reload_pending>>

Next ==
    \/ TriggerReload
    \/ BeginDrain
    \/ DrainComplete
    \/ ReadConfig
    \/ ApplyValidConfig
    \/ RejectInvalidConfig

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(* ═══════════ SAFETY PROPERTIES ═══════════ *)

(* Pipelines are always running OR in a transient reload state *)
AlwaysProgress == 
    state = "running" => pipelines_running > 0

(* No pipeline runs during drain/build phases *)  
NoPipelinesDuringBuild ==
    state = "building" => pipelines_running = 0

(* Config version never decreases (monotonic) *)
ConfigMonotonic == [][config' >= config]_vars

(* Only valid configs are applied *)
OnlyValidConfigsApplied ==
    [][config' # config => pending_config \in 1..MaxReloads]_vars

(* ═══════════ LIVENESS PROPERTIES ═══════════ *)

(* Every reload eventually results in running pipelines *)
ReloadEventuallyCompletes ==
    state = "draining" ~> state = "running"

(* The system always returns to a running state *)
AlwaysReturnsToRunning ==
    [](state # "running" => <>(state = "running"))

================================================================================
