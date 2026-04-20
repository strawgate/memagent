----------------------- MODULE MCDeliveryRetry ------------------------
(*
 * TLC model configuration for DeliveryRetry.tla
 *
 * This file follows the two-file pattern used by the other specs:
 *   DeliveryRetry.tla      — clean spec, no TLC overrides
 *   MCDeliveryRetry.tla    — this file, TLC-specific constants
 *
 * THREE MODELS:
 *
 * 1. SAFETY (DeliveryRetry.cfg):
 *    Checks TypeOK, BackoffCapRespected, BackoffMonotonic, structural
 *    invariants, and temporal action properties.
 *
 * 2. LIVENESS (DeliveryRetry.liveness.cfg):
 *    Checks TerminalReachable, HealthySinkDelivers, CancelTerminates,
 *    BackoffEventuallyResolves under Fairness.
 *
 * 3. COVERAGE (DeliveryRetry.coverage.cfg):
 *    Reachability witnesses for all interesting states.
 *)

EXTENDS DeliveryRetry

\* --- Safety model constants ---
MCInitialBackoffMs == 100
MCMaxBackoffMs     == 1600
MCMaxRetries       == 5

======================================================================
