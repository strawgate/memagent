---------------------------- MODULE ReloadStateMachine ----------------------------
(* 
 * TLA+ specification of the ffwd config reload state machine.
 *
 * Models the full lifecycle of the ReloadCoordinator as implemented in
 * crates/ffwd-runtime/src/reload.rs. Verifies:
 *   - No data loss: pipelines drain before rebuild
 *   - No double-drain: only one reload can be in progress at a time
 *   - Convergence: every reload trigger eventually results in running pipelines
 *     (unless bounded retry exhausted or shutdown requested)
 *   - Config validation: invalid configs do not replace running pipelines
 *   - ShuttingDown is absorbing (terminal)
 *   - Bounded retry: build failures escalate to fatal after MAX_REBUILD_ATTEMPTS
 *   - ShutdownRequested is handled in all non-terminal states
 *
 * States: Starting, Running, Draining, Validating, Building, ShuttingDown
 * (matches Rust impl exactly)
 *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxReloads,           \* Bound on total reload events for model checking
    MaxPipelines,         \* Bound on pipeline count
    MaxRebuildAttempts    \* Max consecutive build failures before fatal (default: 3)

NONE == -1       \* Sentinel: no pending config
INVALID == -2    \* Sentinel: pending config failed validation
NOT_RELOADABLE == -3  \* Sentinel: config has non-reloadable changes

VARIABLES
    state,              \* "starting" | "running" | "draining" | "validating" | "building" | "shutting_down"
    config,             \* Current active config (a natural number representing version)
    pending_config,     \* Config read from disk after reload (version, INVALID, NOT_RELOADABLE, or NONE)
    reload_count,       \* Number of completed reloads
    reload_pending,     \* Whether a reload signal is waiting
    pipelines_running,  \* Number of pipelines currently executing
    first_run,          \* Whether this is the initial startup
    has_pending,        \* Whether a validated config awaits commit
    rebuild_attempts,   \* Consecutive build failure counter
    shutdown_error      \* Error reason if shutdown was due to failure (NONE = clean)

vars == <<state, config, pending_config, reload_count, reload_pending,
          pipelines_running, first_run, has_pending, rebuild_attempts, shutdown_error>>

TypeOK ==
    /\ state \in {"starting", "running", "draining", "validating", "building", "shutting_down"}
    /\ config \in 0..MaxReloads
    /\ pending_config \in {NONE, INVALID, NOT_RELOADABLE} \cup 0..MaxReloads
    /\ reload_count \in 0..MaxReloads
    /\ reload_pending \in BOOLEAN
    /\ pipelines_running \in 0..MaxPipelines
    /\ first_run \in BOOLEAN
    /\ has_pending \in BOOLEAN
    /\ rebuild_attempts \in 0..MaxRebuildAttempts
    /\ shutdown_error \in {NONE} \cup 1..1  \* Simplified: NONE=clean, 1=error

Init ==
    /\ state = "starting"
    /\ config = 0
    /\ pending_config = NONE
    /\ reload_count = 0
    /\ reload_pending = FALSE
    /\ pipelines_running = 0
    /\ first_run = TRUE
    /\ has_pending = FALSE
    /\ rebuild_attempts = 0
    /\ shutdown_error = NONE

(* ═══════════ STARTING STATE ═══════════ *)

(* Initial pipeline build succeeds → Running *)
StartupBuildSuccess ==
    /\ state = "starting"
    /\ state' = "running"
    /\ first_run' = FALSE
    /\ pipelines_running' = 1
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   has_pending, rebuild_attempts, shutdown_error>>

(* Initial pipeline build fails → Fatal shutdown *)
StartupBuildFailure ==
    /\ state = "starting"
    /\ state' = "shutting_down"
    /\ shutdown_error' = 1
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   pipelines_running, first_run, has_pending, rebuild_attempts>>

(* ═══════════ RUNNING STATE ═══════════ *)

(* Reload trigger arrives (SIGHUP / file watch / HTTP / OpAMP) *)
TriggerReload ==
    /\ state = "running"
    /\ reload_count < MaxReloads
    /\ ~reload_pending
    /\ reload_pending' = TRUE
    /\ UNCHANGED <<state, config, pending_config, reload_count,
                   pipelines_running, first_run, has_pending, rebuild_attempts, shutdown_error>>

(* Begin drain: transition to Draining *)
BeginDrain ==
    /\ state = "running"
    /\ reload_pending
    /\ state' = "draining"
    /\ reload_pending' = FALSE
    /\ UNCHANGED <<config, pending_config, reload_count,
                   pipelines_running, first_run, has_pending, rebuild_attempts, shutdown_error>>

(* Pipeline completes (error or normal) → Shutdown *)
PipelineCompleted ==
    /\ state = "running"
    /\ state' = "shutting_down"
    /\ pipelines_running' = 0
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   first_run, has_pending, rebuild_attempts, shutdown_error>>

(* ═══════════ DRAINING STATE ═══════════ *)

(* Drain completes: all pipelines flushed → Validating *)
DrainComplete ==
    /\ state = "draining"
    /\ pipelines_running' = 0
    /\ state' = "validating"
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   first_run, has_pending, rebuild_attempts, shutdown_error>>

(* Drain times out → Fatal shutdown *)
DrainTimedOut ==
    /\ state = "draining"
    /\ state' = "shutting_down"
    /\ shutdown_error' = 1
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   pipelines_running, first_run, has_pending, rebuild_attempts>>

(* ═══════════ VALIDATING STATE ═══════════ *)

(* Read and validate new config — valid *)
ConfigValid ==
    /\ state = "validating"
    /\ pending_config = NONE
    \* Non-deterministically choose a valid config version (monotonic).
    /\ \E v \in {config + 1} :
        /\ v \in 1..MaxReloads
        /\ pending_config' = v
    /\ state' = "building"
    /\ has_pending' = TRUE
    /\ rebuild_attempts' = 0
    /\ UNCHANGED <<config, reload_count, reload_pending,
                   pipelines_running, first_run, shutdown_error>>

(* Read and validate new config — invalid *)
ConfigInvalid ==
    /\ state = "validating"
    /\ pending_config = NONE
    /\ pending_config' = INVALID
    /\ state' = "building"
    /\ has_pending' = FALSE
    /\ rebuild_attempts' = 0
    /\ UNCHANGED <<config, reload_count, reload_pending,
                   pipelines_running, first_run, shutdown_error>>

(* Config has non-reloadable changes *)
ConfigNotReloadable ==
    /\ state = "validating"
    /\ pending_config = NONE
    /\ pending_config' = NOT_RELOADABLE
    /\ state' = "building"
    /\ has_pending' = FALSE
    /\ rebuild_attempts' = 0
    /\ UNCHANGED <<config, reload_count, reload_pending,
                   pipelines_running, first_run, shutdown_error>>

(* Config unchanged — still need to rebuild since pipelines were drained *)
ConfigUnchanged ==
    /\ state = "validating"
    /\ pending_config = NONE
    /\ state' = "building"
    /\ has_pending' = FALSE
    /\ rebuild_attempts' = 0
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   pipelines_running, first_run, shutdown_error>>

(* ═══════════ BUILDING STATE ═══════════ *)

(* Build succeeds with pending → Apply and run *)
ApplyValidConfig ==
    /\ state = "building"
    /\ has_pending
    /\ pending_config \in 1..MaxReloads
    /\ config' = pending_config
    /\ pending_config' = NONE
    /\ has_pending' = FALSE
    /\ rebuild_attempts' = 0
    /\ pipelines_running' = 1
    /\ state' = "running"
    /\ reload_count' = reload_count + 1
    /\ UNCHANGED <<reload_pending, first_run, shutdown_error>>

(* Build succeeds without pending (invalid/unchanged/not-reloadable fallback) *)
RebuildOldConfig ==
    /\ state = "building"
    /\ ~has_pending
    /\ pending_config' = NONE
    /\ pipelines_running' = 1
    /\ state' = "running"
    /\ rebuild_attempts' = 0
    /\ UNCHANGED <<config, reload_count, reload_pending, first_run, has_pending, shutdown_error>>

(* Build fails — retry if under limit *)
BuildFailRetry ==
    /\ state = "building"
    /\ rebuild_attempts < MaxRebuildAttempts - 1
    /\ rebuild_attempts' = rebuild_attempts + 1
    /\ has_pending' = FALSE
    /\ pending_config' = NONE
    \* Stay in building, re-attempt
    /\ UNCHANGED <<state, config, reload_count, reload_pending,
                   pipelines_running, first_run, shutdown_error>>

(* Build fails — max attempts reached → Fatal *)
BuildFailFatal ==
    /\ state = "building"
    /\ rebuild_attempts >= MaxRebuildAttempts - 1
    /\ state' = "shutting_down"
    /\ shutdown_error' = 1
    /\ has_pending' = FALSE
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   pipelines_running, first_run, rebuild_attempts>>

(* ═══════════ SHUTDOWN (UNIVERSAL) ═══════════ *)

(* ShutdownRequested from Running — drain first *)
ShutdownFromRunning ==
    /\ state = "running"
    /\ state' = "shutting_down"
    /\ shutdown_error' = NONE
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   pipelines_running, first_run, has_pending, rebuild_attempts>>

(* ShutdownRequested from non-Running, non-terminal states — no drain needed *)
ShutdownFromOther ==
    /\ state \in {"starting", "draining", "validating", "building"}
    /\ state' = "shutting_down"
    /\ shutdown_error' = NONE
    /\ UNCHANGED <<config, pending_config, reload_count, reload_pending,
                   pipelines_running, first_run, has_pending, rebuild_attempts>>

(* ═══════════ TERMINAL STATE ═══════════ *)

(* ShuttingDown stuttering — prevents TLC deadlock *)
Terminated ==
    /\ state = "shutting_down"
    /\ UNCHANGED vars

(* ═══════════ NEXT-STATE RELATION ═══════════ *)

Next ==
    \/ StartupBuildSuccess
    \/ StartupBuildFailure
    \/ TriggerReload
    \/ BeginDrain
    \/ PipelineCompleted
    \/ DrainComplete
    \/ DrainTimedOut
    \/ ConfigValid
    \/ ConfigInvalid
    \/ ConfigNotReloadable
    \/ ConfigUnchanged
    \/ ApplyValidConfig
    \/ RebuildOldConfig
    \/ BuildFailRetry
    \/ BuildFailFatal
    \/ ShutdownFromRunning
    \/ ShutdownFromOther
    \/ Terminated

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(* ═══════════ SAFETY PROPERTIES ═══════════ *)

(* ShuttingDown is absorbing — once entered, no state change *)
ShuttingDownIsTerminal ==
    [][state = "shutting_down" => state' = "shutting_down"]_vars

(* Pipelines are running when in "running" state *)
AlwaysProgress == 
    state = "running" => pipelines_running > 0

(* No pipelines during validating/building *)
NoPipelinesDuringBuild ==
    state \in {"validating", "building"} => pipelines_running = 0

(* Config version never decreases (monotonic) *)
ConfigMonotonic == [][config' >= config]_vars

(* Only valid configs are applied *)
OnlyValidConfigsApplied ==
    [][config' # config => pending_config \in 1..MaxReloads]_vars

(* rebuild_attempts bounded *)
RebuildAttemptsBounded ==
    rebuild_attempts <= MaxRebuildAttempts

(* has_pending is true only in Building or ShuttingDown *)
HasPendingInvariant ==
    has_pending => state \in {"building", "shutting_down"}

(* First-run build failure is always fatal *)
FirstRunBuildFailureIsFatal ==
    [](first_run /\ state = "starting" /\ state' = "shutting_down" => shutdown_error' = 1)

(* ═══════════ LIVENESS PROPERTIES ═══════════ *)

(* Every reload eventually results in running pipelines or shutdown *)
ReloadEventuallyCompletes ==
    state = "draining" ~> (state = "running" \/ state = "shutting_down")

(* The system eventually reaches running or shutting_down from any state *)
AlwaysReachesStableState ==
    [](state \notin {"running", "shutting_down"} =>
       <>(state \in {"running", "shutting_down"}))

(* Shutdown request is always honored *)
ShutdownAlwaysHonored ==
    [](state \in {"starting", "draining", "validating", "building"} =>
       <>(state \in {"running", "shutting_down"}))

================================================================================
