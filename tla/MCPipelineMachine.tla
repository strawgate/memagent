------------------------ MODULE MCPipelineMachine ------------------------
(*
 * TLC model-checker configuration for PipelineMachine.tla
 *
 * This file follows the industry-standard two-file pattern:
 *   PipelineMachine.tla  — clean spec, no TLC overrides
 *   MCPipelineMachine.tla — this file, TLC-specific bounds + symmetry
 *
 * See: etcd-io/raft (MCetcdraft.tla), PingCAP/tla-plus, Jack Vanlightly's
 * Kafka verification work, Azure Cosmos TLA+.
 *
 * THREE MODELS:
 *
 * 1. SAFETY (normal path, no ForceStop):
 *    Comment out ForceStop in PipelineMachine.tla Next relation.
 *    Check: TypeOK, NoDoubleComplete, DrainCompleteness,
 *           CheckpointOrderingInvariant, CommittedNeverAheadOfCreated
 *    Use: MCFast constants (2 sources, 3 batches) — < 30s
 *
 * 2. LIVENESS (normal path, no ForceStop):
 *    SMALLER constants to keep TLC liveness check tractable.
 *    Check: EventualDrain, NoBatchLeftBehind, StoppedIsStable
 *    Use: MCLiveness constants (2 sources, 2 batches) — < 5 min
 *    WARNING: do NOT use CONSTRAINT to bound state space — it silently
 *    breaks liveness checking. Use model constants instead (this file).
 *
 * 3. FORCE-STOP (with ForceStop action enabled):
 *    Include ForceStop in Next. Omit DrainCompleteness from INVARIANTS.
 *    Verify: ForceStop reaches Stopped; other invariants still hold.
 *    Use: MCFast constants.
 *)

EXTENDS PipelineMachine

\* --- Fast configuration (development / CI) ---
\* Sources is a symmetry set: sources are interchangeable, so TLC can
\* treat any permutation of {s1, s2} as identical. This typically reduces
\* state space by N! (where N = |Sources|). For N=2 that is 2x; for N=3, 6x.
\* Symmetry is valid here because:
\*   - All sources have the same batch ID range (1..MaxBatchesPerSource)
\*   - The committed, in_flight, acked variables are functions over Sources
\*   - The properties are universally quantified over Sources
\* Symmetry would be INVALID if sources had different initial states or
\* if some property singled out a specific source by identity.

MCSourcesFast == {"s1", "s2"}
MCMaxBatchesFast == 3

\* Override CONSTANTS for the Fast model:
\* In the Toolbox: set Sources <- MCSourcesFast, MaxBatchesPerSource <- 3
\* Declare MCSourcesFast as a SYMMETRY set.

\* --- Liveness configuration (smaller for temporal checking) ---
MCSourcesLiveness == {"s1", "s2"}
MCMaxBatchesLiveness == 2

\* --- Thorough configuration (pre-release) ---
MCSourcesThorough == {"s1", "s2", "s3"}
MCMaxBatchesThorough == 4

=============================================================================
