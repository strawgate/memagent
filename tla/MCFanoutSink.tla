--------------------- MODULE MCFanoutSink ---------------------
(*
 * TLC model configuration for FanoutSink.
 *
 * Constants are set directly in the .cfg files (FanoutSink.cfg,
 * FanoutSink.liveness.cfg, FanoutSink.coverage.cfg).
 *
 * Model parameters are intentionally small for TLC feasibility.
 * Production uses unbounded retry budgets and arbitrary child counts.
 *)

EXTENDS FanoutSink

=====================================================================
