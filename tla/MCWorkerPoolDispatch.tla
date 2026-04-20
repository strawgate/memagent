------------------- MODULE MCWorkerPoolDispatch --------------------
(*
 * TLC model configuration for WorkerPoolDispatch.
 *
 * Constants are set directly in the .cfg files (WorkerPoolDispatch.cfg,
 * WorkerPoolDispatch.liveness.cfg, WorkerPoolDispatch.coverage.cfg).
 *
 * Model parameters are intentionally small for TLC feasibility.
 * Production uses max_workers up to 64 and unbounded item streams.
 *)

EXTENDS WorkerPoolDispatch

=====================================================================
