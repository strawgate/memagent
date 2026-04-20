--------------------- MODULE MCShutdownProtocol ----------------------
(*
 * TLC model configuration for ShutdownProtocol.
 *
 * Constants are set directly in the .cfg files (ShutdownProtocol.cfg,
 * ShutdownProtocol.liveness.cfg, ShutdownProtocol.coverage.cfg).
 *
 * Capacities are intentionally small for TLC feasibility.
 * Production uses IoChannelCapacity=4, PipelineChannelCapacity=16.
 *)

EXTENDS ShutdownProtocol

======================================================================
