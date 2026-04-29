---------------------------- MODULE SupervisorProtocol ----------------------------
(*
 * TLA+ specification of the ffwd supervisor mode protocol.
 *
 * Models config flow through three actors:
 *   1. OpAMP client — receives remote config, writes to intermediate file
 *   2. Supervisor — reads intermediate, validates, writes to main, signals child
 *   3. Child process — handles SIGHUP to reload, may crash at any time
 *
 * Design: config is tracked as a version number flowing through the pipeline.
 * No counters — convergence is expressed as "child eventually has latest main".
 *
 * Integer sentinels avoid TLC mixed-type errors (strings vs ints).
 *)
EXTENDS Integers, TLC

CONSTANTS
    MaxVersion,   \* Bound on config versions (1..MaxVersion)
    MaxCrashes    \* Bound on child crashes

NONE == -1  \* Sentinel for empty/unset

VARIABLES
    intermediate,     \* Config version in intermediate file (NONE or 1..MaxVersion)
    main,             \* Config version in main file (0 = initial, 1..MaxVersion)
    supervisor_state, \* "idle" | "reading" | "writing" | "signaling"
    child_alive,      \* Is the child process running?
    child_pid_gen,    \* PID generation (increments on each respawn)
    signal_gen,       \* PID gen captured when supervisor starts a read cycle
    child_config,     \* Config version the child is running (0..MaxVersion)
    latest_pushed,    \* Highest version pushed by OpAMP (0..MaxVersion)
    shutdown          \* Graceful shutdown requested?

vars == <<intermediate, main, supervisor_state, child_alive,
          child_pid_gen, signal_gen, child_config, latest_pushed, shutdown>>

(* ═══════════ TYPE INVARIANT ═══════════ *)

TypeOK ==
    /\ intermediate \in {NONE} \cup 1..MaxVersion
    /\ main \in 0..MaxVersion
    /\ supervisor_state \in {"idle", "reading", "writing", "signaling"}
    /\ child_alive \in BOOLEAN
    /\ child_pid_gen \in 0..MaxCrashes
    /\ signal_gen \in 0..MaxCrashes
    /\ child_config \in 0..MaxVersion
    /\ latest_pushed \in 0..MaxVersion
    /\ shutdown \in BOOLEAN

(* ═══════════ INITIAL STATE ═══════════ *)

Init ==
    /\ intermediate = NONE
    /\ main = 0
    /\ supervisor_state = "idle"
    /\ child_alive = TRUE
    /\ child_pid_gen = 0
    /\ signal_gen = 0
    /\ child_config = 0
    /\ latest_pushed = 0
    /\ shutdown = FALSE

(* ═══════════ OPAMP ACTIONS ═══════════ *)

(* OpAMP receives a new config version and writes to intermediate.
   Newer configs overwrite older ones — this is intentional (last-writer-wins). *)
OpampPush ==
    /\ latest_pushed < MaxVersion
    /\ ~shutdown
    /\ latest_pushed' = latest_pushed + 1
    /\ intermediate' = latest_pushed + 1
    /\ UNCHANGED <<main, supervisor_state, child_alive,
                   child_pid_gen, signal_gen, child_config, shutdown>>

(* ═══════════ SUPERVISOR ACTIONS ═══════════ *)

(* Supervisor sees a new intermediate config and begins processing *)
BeginRead ==
    /\ supervisor_state = "idle"
    /\ intermediate # NONE
    /\ ~shutdown
    /\ supervisor_state' = "reading"
    /\ signal_gen' = child_pid_gen   \* Capture current child generation
    /\ UNCHANGED <<intermediate, main, child_alive,
                   child_pid_gen, child_config, latest_pushed, shutdown>>

(* Supervisor validates and writes config to main *)
WriteMain ==
    /\ supervisor_state = "reading"
    /\ intermediate # NONE
    /\ main' = intermediate
    /\ intermediate' = NONE
    /\ supervisor_state' = "writing"
    /\ UNCHANGED <<child_alive, child_pid_gen, signal_gen,
                   child_config, latest_pushed, shutdown>>

(* Supervisor sends SIGHUP — child is alive and same generation *)
Signal ==
    /\ supervisor_state = "writing"
    /\ child_alive
    /\ signal_gen = child_pid_gen
    /\ supervisor_state' = "signaling"
    /\ UNCHANGED <<intermediate, main, child_alive,
                   child_pid_gen, signal_gen, child_config, latest_pushed, shutdown>>

(* Signal skipped — child died or was replaced since we started *)
SignalSkip ==
    /\ supervisor_state = "writing"
    /\ (~child_alive \/ signal_gen # child_pid_gen)
    /\ supervisor_state' = "idle"
    /\ UNCHANGED <<intermediate, main, child_alive,
                   child_pid_gen, signal_gen, child_config, latest_pushed, shutdown>>

(* Child receives SIGHUP and reloads (same generation as target) *)
ChildReload ==
    /\ supervisor_state = "signaling"
    /\ child_alive
    /\ signal_gen = child_pid_gen
    /\ child_config' = main
    /\ supervisor_state' = "idle"
    /\ UNCHANGED <<intermediate, main, child_alive,
                   child_pid_gen, signal_gen, latest_pushed, shutdown>>

(* Signal delivery fails — child died while in signaling state *)
SignalFail ==
    /\ supervisor_state = "signaling"
    /\ (~child_alive \/ signal_gen # child_pid_gen)
    /\ supervisor_state' = "idle"
    /\ UNCHANGED <<intermediate, main, child_alive,
                   child_pid_gen, signal_gen, child_config, latest_pushed, shutdown>>

(* ═══════════ CHILD LIFECYCLE ═══════════ *)

(* Child crashes *)
Crash ==
    /\ child_alive
    /\ child_pid_gen < MaxCrashes
    /\ ~shutdown
    /\ child_alive' = FALSE
    /\ UNCHANGED <<intermediate, main, supervisor_state,
                   child_pid_gen, signal_gen, child_config, latest_pushed, shutdown>>

(* Child respawns — reads current main config on startup *)
Respawn ==
    /\ ~child_alive
    /\ ~shutdown
    /\ child_alive' = TRUE
    /\ child_pid_gen' = child_pid_gen + 1
    /\ child_config' = main   \* New process starts with current main config
    /\ UNCHANGED <<intermediate, main, supervisor_state,
                   signal_gen, latest_pushed, shutdown>>

(* ═══════════ SHUTDOWN ═══════════ *)

ShutdownReq ==
    /\ ~shutdown
    /\ shutdown' = TRUE
    /\ UNCHANGED <<intermediate, main, supervisor_state, child_alive,
                   child_pid_gen, signal_gen, child_config, latest_pushed>>

(* Terminal stuttering — prevents TLC deadlock report *)
Terminated ==
    /\ shutdown
    /\ UNCHANGED vars

(* ═══════════ NEXT-STATE RELATION ═══════════ *)

Next ==
    \/ OpampPush
    \/ BeginRead
    \/ WriteMain
    \/ Signal
    \/ SignalSkip
    \/ ChildReload
    \/ SignalFail
    \/ Crash
    \/ Respawn
    \/ ShutdownReq
    \/ Terminated

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(* ═══════════ SAFETY PROPERTIES ═══════════ *)

(* Main config only advances — never goes backward *)
ConfigMonotonic ==
    [][main' >= main]_vars

(* Main config only changes via WriteMain (supervisor_state = "reading") *)
OnlyValidatedConfigsReachMain ==
    [][main' # main => supervisor_state = "reading"]_vars

(* Child config never exceeds main — child reads main, never intermediate *)
ChildNeverAheadOfMain ==
    child_config <= main

(* ═══════════ LIVENESS PROPERTIES ═══════════ *)

(* If a config reaches main, the child eventually gets it (or shutdown) *)
ChildEventuallyConverges ==
    [](child_alive /\ child_config # main /\ ~shutdown =>
       <>(child_config = main \/ shutdown))

(* After a crash, child is eventually respawned (or shutdown) *)
CrashRecovery ==
    [](~child_alive /\ ~shutdown => <>(child_alive \/ shutdown))

(* Supervisor always returns to idle (or shutdown) *)
SupervisorProgress ==
    [](supervisor_state # "idle" /\ ~shutdown =>
       <>(supervisor_state = "idle" \/ shutdown))

================================================================================
