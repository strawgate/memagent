--------------------- MODULE MCShutdownProtocol ----------------------
(*
 * TLC model configuration for ShutdownProtocol.
 * Defines concrete constant values for model checking.
 *)

EXTENDS ShutdownProtocol

\* Safety model: 2 inputs, io channel capacity 4, pipeline channel capacity 3, max 3 items each
MCNumInputsFast == 2
MCIoChannelCapacityFast == 4
MCPipelineChannelCapacityFast == 3
MCMaxItemsFast == 3

\* Liveness model: smaller constants (O(states^2) for liveness)
MCNumInputsLiveness == 2
MCIoChannelCapacityLiveness == 4
MCPipelineChannelCapacityLiveness == 3
MCMaxItemsLiveness == 2

\* Thorough model: 3 inputs
MCNumInputsThorough == 3
MCIoChannelCapacityThorough == 4
MCPipelineChannelCapacityThorough == 3
MCMaxItemsThorough == 3

======================================================================
