--------------------- MODULE MCShutdownProtocol ----------------------
(*
 * TLC model configuration for ShutdownProtocol.
 * Defines concrete constant values for model checking.
 *)

EXTENDS ShutdownProtocol

\* Safety model: 2 inputs, channel capacity 2, max 3 items each
MCNumInputsFast == 2
MCChannelCapacityFast == 2
MCMaxItemsFast == 3

\* Liveness model: smaller constants (O(states^2) for liveness)
MCNumInputsLiveness == 2
MCChannelCapacityLiveness == 2
MCMaxItemsLiveness == 2

\* Thorough model: 3 inputs
MCNumInputsThorough == 3
MCChannelCapacityThorough == 3
MCMaxItemsThorough == 3

======================================================================
