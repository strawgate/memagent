----------------------- MODULE MCTailLifecycle -----------------------
(*
 * TLC model configuration constants for TailLifecycle.
 *)

EXTENDS TailLifecycle

MCThreshold == 2
MCMaxIdle == 4
MCMaxOffset == 4
MCMaxErrors == 8
MCInitialBackoff == 100
MCMaxBackoff == 5000

======================================================================
