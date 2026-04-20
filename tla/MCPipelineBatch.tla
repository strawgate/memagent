---------------------- MODULE MCPipelineBatch -----------------------
(*
 * TLC model configuration for PipelineBatch.
 *)

EXTENDS PipelineBatch

\* Safety model: 2 sources, 3 items each, batch threshold 2
MCSourcesFast == {"s1", "s2"}
MCMaxItemsFast == 3
MCBatchThresholdFast == 2

\* Liveness model: smaller constants
MCSourcesLiveness == {"s1", "s2"}
MCMaxItemsLiveness == 2
MCBatchThresholdLiveness == 2

\* Thorough model: 3 sources
MCSourcesThorough == {"s1", "s2", "s3"}
MCMaxItemsThorough == 3
MCBatchThresholdThorough == 2

======================================================================
