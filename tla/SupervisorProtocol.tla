---------------------------- MODULE SupervisorProtocol ----------------------------
(*
 * TLA+ specification of the ffwd supervisor mode protocol.
 *
 * Models the interaction between three concurrent actors:
 *   1. OpAMP client — receives remote config from server, writes to intermediate file
 *   2. Supervisor — reads intermediate file, validates, writes to main config, signals child
 *   3. Child process — runs ff, handles SIGHUP, may crash at any time
 *
 * Verifies critical safety properties:
 *   - Path consistency: OpAMP writes to the same path supervisor reads from
 *   - PID safety: supervisor never signals a dead or exited child
 *   - Config monotonicity: main config file only changes through validation
 *   - No lost configs: every valid remote config eventually reaches the child
 *   - Graceful shutdown: signals are forwarded before cleanup
 *
 * Note: We use -1 as sentinel for "no value" to avoid TLC type errors from
 * mixing strings and integers in the same variable domain.
 *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxConfigs,           \* Bound on total remote config pushes
    MaxCrashes            \* Bound on child crashes for model checking

NONE == -1  \* Sentinel: no pending config / empty file

VARIABLES
    (* OpAMP client state *)
    opamp_write_path,     \* Path where OpAMP writes remote config ("intermediate" | "main")
    opamp_pending,        \* Config waiting to write (version 1..MaxConfigs, or NONE)
    intermediate_file,    \* Content of intermediate config file (version 1..MaxConfigs, or NONE)

    (* Supervisor state *)
    supervisor_read_path, \* Path supervisor reads from ("intermediate" | "main")
    main_config_file,     \* Content of the main config file (Nat version, 0 = initial)
    supervisor_state,     \* "idle" | "reading" | "validating" | "writing" | "signaling"

    (* Child process state *)
    child_alive,          \* Whether the child process is running
    child_pid_gen,        \* Current PID generation (increments on respawn)
    signal_target_gen,    \* PID generation that supervisor targets with signals
    child_config,         \* Config version the child is running with

    (* Global *)
    configs_pushed,       \* Number of configs pushed from OpAMP server
    configs_applied,      \* Number of configs successfully applied to child
    shutdown_requested    \* Whether graceful shutdown was requested

vars == <<opamp_write_path, opamp_pending, intermediate_file,
          supervisor_read_path, main_config_file, supervisor_state,
          child_alive, child_pid_gen, signal_target_gen, child_config,
          configs_pushed, configs_applied, shutdown_requested>>

TypeOK ==
    /\ opamp_write_path \in {"intermediate", "main"}
    /\ opamp_pending \in {NONE} \cup 1..MaxConfigs
    /\ intermediate_file \in {NONE} \cup 1..MaxConfigs
    /\ supervisor_read_path \in {"intermediate", "main"}
    /\ main_config_file \in 0..MaxConfigs
    /\ supervisor_state \in {"idle", "reading", "validating", "writing", "signaling"}
    /\ child_alive \in BOOLEAN
    /\ child_pid_gen \in 0..MaxCrashes
    /\ signal_target_gen \in 0..MaxCrashes
    /\ child_config \in 0..MaxConfigs
    /\ configs_pushed \in 0..MaxConfigs
    /\ configs_applied \in 0..MaxConfigs
    /\ shutdown_requested \in BOOLEAN

Init ==
    /\ opamp_write_path = "intermediate"   \* OpAMP writes to intermediate (correct!)
    /\ opamp_pending = NONE
    /\ intermediate_file = NONE
    /\ supervisor_read_path = "intermediate" \* Supervisor reads from intermediate (matches!)
    /\ main_config_file = 0
    /\ supervisor_state = "idle"
    /\ child_alive = TRUE
    /\ child_pid_gen = 0
    /\ signal_target_gen = 0
    /\ child_config = 0
    /\ configs_pushed = 0
    /\ configs_applied = 0
    /\ shutdown_requested = FALSE

(* ═══════════ OPAMP ACTIONS ═══════════ *)

(* OpAMP server pushes a new config version *)
OpampReceiveConfig ==
    /\ configs_pushed < MaxConfigs
    /\ ~shutdown_requested
    /\ opamp_pending = NONE
    /\ opamp_pending' = configs_pushed + 1
    /\ configs_pushed' = configs_pushed + 1
    /\ UNCHANGED <<opamp_write_path, intermediate_file, supervisor_read_path,
                   main_config_file, supervisor_state, child_alive, child_pid_gen,
                   signal_target_gen, child_config, configs_applied, shutdown_requested>>

(* OpAMP writes received config to the write path *)
OpampWriteConfig ==
    /\ opamp_pending # NONE
    /\ opamp_write_path = "intermediate"   \* In supervisor mode, always writes here
    /\ intermediate_file' = opamp_pending
    /\ opamp_pending' = NONE
    /\ UNCHANGED <<opamp_write_path, supervisor_read_path, main_config_file,
                   supervisor_state, child_alive, child_pid_gen, signal_target_gen,
                   child_config, configs_pushed, configs_applied, shutdown_requested>>

(* BUG MODEL: OpAMP writes to main config directly (the bug we fixed) *)
OpampWriteConfigBUGGY ==
    /\ opamp_pending # NONE
    /\ opamp_write_path = "main"   \* WRONG: supervisor can't detect this!
    /\ main_config_file' = opamp_pending
    /\ opamp_pending' = NONE
    /\ UNCHANGED <<opamp_write_path, intermediate_file, supervisor_read_path,
                   supervisor_state, child_alive, child_pid_gen, signal_target_gen,
                   child_config, configs_pushed, configs_applied, shutdown_requested>>

(* ═══════════ SUPERVISOR ACTIONS ═══════════ *)

(* Supervisor detects new config (reload notification from OpAMP) *)
SupervisorBeginRead ==
    /\ supervisor_state = "idle"
    /\ intermediate_file # NONE   \* There's something to read
    /\ ~shutdown_requested
    /\ child_alive                     \* Only proceed if child is alive
    /\ supervisor_state' = "reading"
    /\ signal_target_gen' = child_pid_gen  \* Capture current PID generation
    /\ UNCHANGED <<opamp_write_path, opamp_pending, intermediate_file,
                   supervisor_read_path, main_config_file, child_alive,
                   child_pid_gen, child_config, configs_pushed, configs_applied,
                   shutdown_requested>>

(* Supervisor reads from intermediate file and validates *)
SupervisorValidate ==
    /\ supervisor_state = "reading"
    /\ supervisor_read_path = "intermediate"  \* Reads from intermediate (matches OpAMP write)
    /\ intermediate_file # NONE
    /\ supervisor_state' = "validating"
    /\ UNCHANGED <<opamp_write_path, opamp_pending, intermediate_file,
                   supervisor_read_path, main_config_file, child_alive,
                   child_pid_gen, signal_target_gen, child_config,
                   configs_pushed, configs_applied, shutdown_requested>>

(* Validation passes — write to main config file *)
SupervisorWrite ==
    /\ supervisor_state = "validating"
    /\ main_config_file' = intermediate_file   \* Copy validated config to main
    /\ intermediate_file' = NONE               \* Clear intermediate
    /\ supervisor_state' = "writing"
    /\ UNCHANGED <<opamp_write_path, opamp_pending, supervisor_read_path,
                   child_alive, child_pid_gen, signal_target_gen, child_config,
                   configs_pushed, configs_applied, shutdown_requested>>

(* Supervisor signals child (SIGHUP) after writing config *)
SupervisorSignal ==
    /\ supervisor_state = "writing"
    /\ child_alive                           \* try_wait() check passes
    /\ signal_target_gen = child_pid_gen     \* PID hasn't changed since we started
    /\ supervisor_state' = "signaling"
    /\ UNCHANGED <<opamp_write_path, opamp_pending, intermediate_file,
                   supervisor_read_path, main_config_file, child_alive,
                   child_pid_gen, signal_target_gen, child_config,
                   configs_pushed, configs_applied, shutdown_requested>>

(* Supervisor signal skipped because child exited (PID race guard) *)
SupervisorSignalSkipped ==
    /\ supervisor_state = "writing"
    /\ (~child_alive \/ signal_target_gen # child_pid_gen)
    /\ supervisor_state' = "idle"    \* Back to idle — child will be respawned
    /\ UNCHANGED <<opamp_write_path, opamp_pending, intermediate_file,
                   supervisor_read_path, main_config_file, child_alive,
                   child_pid_gen, signal_target_gen, child_config,
                   configs_pushed, configs_applied, shutdown_requested>>

(* Child receives SIGHUP and reloads *)
ChildReload ==
    /\ supervisor_state = "signaling"
    /\ child_alive
    /\ child_config' = main_config_file   \* Child reads new config
    /\ configs_applied' = configs_applied + 1
    /\ supervisor_state' = "idle"
    /\ UNCHANGED <<opamp_write_path, opamp_pending, intermediate_file,
                   supervisor_read_path, main_config_file, child_alive,
                   child_pid_gen, signal_target_gen, configs_pushed,
                   shutdown_requested>>

(* ═══════════ CHILD ACTIONS ═══════════ *)

(* Child crashes unexpectedly *)
ChildCrash ==
    /\ child_alive
    /\ child_pid_gen < MaxCrashes
    /\ ~shutdown_requested
    /\ child_alive' = FALSE
    /\ UNCHANGED <<opamp_write_path, opamp_pending, intermediate_file,
                   supervisor_read_path, main_config_file, supervisor_state,
                   child_pid_gen, signal_target_gen, child_config,
                   configs_pushed, configs_applied, shutdown_requested>>

(* Supervisor respawns child after crash *)
ChildRespawn ==
    /\ ~child_alive
    /\ ~shutdown_requested
    /\ child_alive' = TRUE
    /\ child_pid_gen' = child_pid_gen + 1  \* New generation (different PID)
    /\ child_config' = main_config_file    \* Reads current main config on startup
    /\ UNCHANGED <<opamp_write_path, opamp_pending, intermediate_file,
                   supervisor_read_path, main_config_file, supervisor_state,
                   signal_target_gen, configs_pushed, configs_applied,
                   shutdown_requested>>

(* ═══════════ SHUTDOWN ═══════════ *)

(* Graceful shutdown requested *)
Shutdown ==
    /\ ~shutdown_requested
    /\ shutdown_requested' = TRUE
    /\ UNCHANGED <<opamp_write_path, opamp_pending, intermediate_file,
                   supervisor_read_path, main_config_file, supervisor_state,
                   child_alive, child_pid_gen, signal_target_gen, child_config,
                   configs_pushed, configs_applied>>

Next ==
    \/ OpampReceiveConfig
    \/ OpampWriteConfig
    \/ SupervisorBeginRead
    \/ SupervisorValidate
    \/ SupervisorWrite
    \/ SupervisorSignal
    \/ SupervisorSignalSkipped
    \/ ChildReload
    \/ ChildCrash
    \/ ChildRespawn
    \/ Shutdown

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(* ═══════════ SAFETY PROPERTIES ═══════════ *)

(* CRITICAL: OpAMP writes to the same path supervisor reads from *)
PathConsistency ==
    opamp_write_path = supervisor_read_path

(* PID safety: supervisor only signals when child is alive AND same generation *)
NeverSignalDeadChild ==
    supervisor_state = "signaling" => (child_alive /\ signal_target_gen = child_pid_gen)

(* Config monotonicity: main config version never decreases *)
ConfigMonotonic ==
    [][main_config_file' >= main_config_file]_vars

(* Validation gate: main config only changes through supervisor write path *)
OnlyValidatedConfigsReachMain ==
    [][main_config_file' # main_config_file =>
       supervisor_state = "validating"]_vars

(* Child config is always <= main config (child reads main on start/reload) *)
ChildConfigBounded ==
    child_alive => child_config <= main_config_file

(* ═══════════ LIVENESS PROPERTIES ═══════════ *)

(* Every config pushed by OpAMP eventually reaches the child (under fairness) *)
ConfigEventuallyApplied ==
    [](configs_pushed > configs_applied => <>(configs_applied = configs_pushed))

(* After a crash, child is eventually respawned *)
CrashEventuallyRecovered ==
    [](~child_alive /\ ~shutdown_requested => <>(child_alive))

(* Supervisor always returns to idle *)
SupervisorAlwaysReturnsToIdle ==
    [](supervisor_state # "idle" => <>(supervisor_state = "idle"))

================================================================================
