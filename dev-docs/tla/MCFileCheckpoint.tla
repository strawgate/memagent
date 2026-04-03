------------------------ MODULE MCFileCheckpoint ------------------------
(*
 * TLC model-checker configuration for FileCheckpoint.tla
 *
 * Follows the two-file pattern from PipelineMachine.tla / MCPipelineMachine.tla.
 *
 * THREE MODELS:
 *
 * 1. SAFETY (FileCheckpoint.cfg):
 *    Check: TypeOK, NoCorruption, CheckpointAtBoundary, ReadOffsetBounded,
 *           RotatedReadBounded, InFlightInvariant, PipelineConsistency,
 *           NoEmitBeforeWrite, CheckpointMonotonicity
 *    Constants: MaxLines=4, MaxBatches=2, MaxCrashes=1, MaxRotations=1, BatchSize=2
 *    Expected: ~200K states, < 2 min
 *
 * 2. LIVENESS (FileCheckpoint.liveness.cfg):
 *    Check: Progress, EventualEmission
 *    Constants: MaxLines=3, MaxBatches=2, MaxCrashes=1, MaxRotations=0, BatchSize=1
 *    Expected: ~50K states, < 5 min
 *    WARNING: No SYMMETRY. No CONSTRAINT. Small constants only.
 *
 * 3. COVERAGE (FileCheckpoint.coverage.cfg):
 *    Check reachability of all key states.
 *    Constants: Same as safety.
 *)

EXTENDS FileCheckpoint

\* --- Safety configuration ---
MCMaxLinesSafety      == 4
MCMaxBatchesSafety    == 2
MCMaxCrashesSafety    == 1
MCMaxRotationsSafety  == 1
MCBatchSizeSafety     == 2

\* --- Liveness configuration (smaller) ---
MCMaxLinesLiveness      == 3
MCMaxBatchesLiveness    == 2
MCMaxCrashesLiveness    == 1
MCMaxRotationsLiveness  == 0
MCBatchSizeLiveness     == 1

\* --- Thorough configuration (CI / pre-release) ---
MCMaxLinesThorough      == 6
MCMaxBatchesThorough    == 3
MCMaxCrashesThorough    == 2
MCMaxRotationsThorough  == 1
MCBatchSizeThorough     == 2

=============================================================================
