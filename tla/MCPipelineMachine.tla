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
 * 1. SAFETY (normal + ForceStop):
 *    Check the invariants named in PipelineMachine.cfg:
 *      TypeOK, NoDoubleComplete, DrainCompleteness,
 *      NoHeldWorkAfterStop, QuiescenceHasNoSilentStrandedWork,
 *      CheckpointOrderingInvariant, UnresolvedWorkNotCommittedPast,
 *      CheckpointNeverAheadOfTerminalizedPrefix,
 *      CommittedNeverAheadOfCreated, SentImpliesCreated,
 *      InFlightImpliesCreated, InFlightImpliesSent,
 *      HeldImpliesInFlight, RetriedImpliesSent, PanickedImpliesSent,
 *      HoldCountOnlyForSent,
 *      AckedImpliesCreated, AckedImpliesSent,
 *      RejectedImpliesCreated, RejectedImpliesSent,
 *      AbandonedImpliesCreated, AbandonedImpliesSent
 *    Temporal properties in the safety model:
 *      NoCreateAfterDrain, CommittedMonotonic, HeldTransitionsDoNotCommit,
 *      ForceStopAbandonsAllInFlight, DrainMeansNoNewSending
 *    Use: MCFast constants (2 sources, 3 batches, 1 hold) — < 10 min locally
 *
 * 2. LIVENESS (normal path must converge without ForceStop fairness):
 *    SMALLER constants to keep TLC liveness check tractable.
 *    Check: EventualDrain, NoBatchLeftBehind, StoppedIsStable
 *    plus held/retry/panic terminalization liveness.
 *    Use: MCLiveness constants (2 sources, 2 batches, 1 hold) — < 5 min
 *    WARNING: do NOT use CONSTRAINT to bound state space — it silently
 *    breaks liveness checking. Use model constants instead (this file).
 *
 * 3. COVERAGE (reachability witnesses):
 *    Use PipelineMachine.coverage.cfg. Verify create/ack/reject/force-stop/
 *    hold/retry/panic/abandon paths are all reachable.
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
MCMaxNonTerminalHoldsFast == 1

\* Override CONSTANTS for the Fast model:
\* In the Toolbox: set Sources <- MCSourcesFast, MaxBatchesPerSource <- 3,
\* MaxNonTerminalHolds <- 1
\* Declare MCSourcesFast as a SYMMETRY set.

\* --- Liveness configuration (smaller for temporal checking) ---
MCSourcesLiveness == {"s1", "s2"}
MCMaxBatchesLiveness == 2
MCMaxNonTerminalHoldsLiveness == 1

\* --- Thorough configuration (pre-release) ---
MCSourcesThorough == {"s1", "s2", "s3"}
MCMaxBatchesThorough == 4
MCMaxNonTerminalHoldsThorough == 2

=============================================================================
