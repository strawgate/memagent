---- MODULE MCFileCheckpoint_TTrace_1775196523 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, MCFileCheckpoint

_expression ==
    LET MCFileCheckpoint_TEExpression == INSTANCE MCFileCheckpoint_TEExpression
    IN MCFileCheckpoint_TEExpression!expression
----

_trace ==
    LET MCFileCheckpoint_TETrace == INSTANCE MCFileCheckpoint_TETrace
    IN MCFileCheckpoint_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        read_offset = (0)
        /\
        file_content = (<<>>)
        /\
        next_line_id = (2)
        /\
        alive = (TRUE)
        /\
        checkpoint_gen = (0)
        /\
        rotated_content = (<<1>>)
        /\
        rotation_count = (1)
        /\
        rotated_active = (TRUE)
        /\
        in_flight_offset = (0)
        /\
        checkpoint_offset = (1)
        /\
        crash_count = (0)
        /\
        emitted = (<<1>>)
        /\
        pipeline_batch = (<<>>)
        /\
        rotated_offset = (1)
        /\
        in_flight_batch = (<<>>)
        /\
        framer_buf = (<<>>)
    )
----

_init ==
    /\ rotated_content = _TETrace[1].rotated_content
    /\ framer_buf = _TETrace[1].framer_buf
    /\ rotation_count = _TETrace[1].rotation_count
    /\ emitted = _TETrace[1].emitted
    /\ alive = _TETrace[1].alive
    /\ file_content = _TETrace[1].file_content
    /\ rotated_active = _TETrace[1].rotated_active
    /\ crash_count = _TETrace[1].crash_count
    /\ pipeline_batch = _TETrace[1].pipeline_batch
    /\ rotated_offset = _TETrace[1].rotated_offset
    /\ in_flight_batch = _TETrace[1].in_flight_batch
    /\ in_flight_offset = _TETrace[1].in_flight_offset
    /\ checkpoint_offset = _TETrace[1].checkpoint_offset
    /\ checkpoint_gen = _TETrace[1].checkpoint_gen
    /\ read_offset = _TETrace[1].read_offset
    /\ next_line_id = _TETrace[1].next_line_id
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ rotated_content  = _TETrace[i].rotated_content
        /\ rotated_content' = _TETrace[j].rotated_content
        /\ framer_buf  = _TETrace[i].framer_buf
        /\ framer_buf' = _TETrace[j].framer_buf
        /\ rotation_count  = _TETrace[i].rotation_count
        /\ rotation_count' = _TETrace[j].rotation_count
        /\ emitted  = _TETrace[i].emitted
        /\ emitted' = _TETrace[j].emitted
        /\ alive  = _TETrace[i].alive
        /\ alive' = _TETrace[j].alive
        /\ file_content  = _TETrace[i].file_content
        /\ file_content' = _TETrace[j].file_content
        /\ rotated_active  = _TETrace[i].rotated_active
        /\ rotated_active' = _TETrace[j].rotated_active
        /\ crash_count  = _TETrace[i].crash_count
        /\ crash_count' = _TETrace[j].crash_count
        /\ pipeline_batch  = _TETrace[i].pipeline_batch
        /\ pipeline_batch' = _TETrace[j].pipeline_batch
        /\ rotated_offset  = _TETrace[i].rotated_offset
        /\ rotated_offset' = _TETrace[j].rotated_offset
        /\ in_flight_batch  = _TETrace[i].in_flight_batch
        /\ in_flight_batch' = _TETrace[j].in_flight_batch
        /\ in_flight_offset  = _TETrace[i].in_flight_offset
        /\ in_flight_offset' = _TETrace[j].in_flight_offset
        /\ checkpoint_offset  = _TETrace[i].checkpoint_offset
        /\ checkpoint_offset' = _TETrace[j].checkpoint_offset
        /\ checkpoint_gen  = _TETrace[i].checkpoint_gen
        /\ checkpoint_gen' = _TETrace[j].checkpoint_gen
        /\ read_offset  = _TETrace[i].read_offset
        /\ read_offset' = _TETrace[j].read_offset
        /\ next_line_id  = _TETrace[i].next_line_id
        /\ next_line_id' = _TETrace[j].next_line_id

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("MCFileCheckpoint_TTrace_1775196523.json", _TETrace)

=============================================================================

 Note that you can extract this module `MCFileCheckpoint_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `MCFileCheckpoint_TEExpression.tla` file takes precedence 
  over the module `MCFileCheckpoint_TEExpression` below).

---- MODULE MCFileCheckpoint_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, MCFileCheckpoint

expression == 
    [
        \* To hide variables of the `MCFileCheckpoint` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        rotated_content |-> rotated_content
        ,framer_buf |-> framer_buf
        ,rotation_count |-> rotation_count
        ,emitted |-> emitted
        ,alive |-> alive
        ,file_content |-> file_content
        ,rotated_active |-> rotated_active
        ,crash_count |-> crash_count
        ,pipeline_batch |-> pipeline_batch
        ,rotated_offset |-> rotated_offset
        ,in_flight_batch |-> in_flight_batch
        ,in_flight_offset |-> in_flight_offset
        ,checkpoint_offset |-> checkpoint_offset
        ,checkpoint_gen |-> checkpoint_gen
        ,read_offset |-> read_offset
        ,next_line_id |-> next_line_id
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_rotated_contentUnchanged |-> rotated_content = rotated_content'
        
        \* Format the `rotated_content` variable as Json value.
        \* ,_rotated_contentJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(rotated_content)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_rotated_contentModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].rotated_content # _TETrace[s-1].rotated_content
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE MCFileCheckpoint_TETrace ----
\*EXTENDS IOUtils, TLC, MCFileCheckpoint
\*
\*trace == IODeserialize("MCFileCheckpoint_TTrace_1775196523.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE MCFileCheckpoint_TETrace ----
EXTENDS TLC, MCFileCheckpoint

trace == 
    <<
    ([read_offset |-> 0,file_content |-> <<>>,next_line_id |-> 1,alive |-> TRUE,checkpoint_gen |-> 0,rotated_content |-> <<>>,rotation_count |-> 0,rotated_active |-> FALSE,in_flight_offset |-> 0,checkpoint_offset |-> 0,crash_count |-> 0,emitted |-> <<>>,pipeline_batch |-> <<>>,rotated_offset |-> 0,in_flight_batch |-> <<>>,framer_buf |-> <<>>]),
    ([read_offset |-> 0,file_content |-> <<1>>,next_line_id |-> 2,alive |-> TRUE,checkpoint_gen |-> 0,rotated_content |-> <<>>,rotation_count |-> 0,rotated_active |-> FALSE,in_flight_offset |-> 0,checkpoint_offset |-> 0,crash_count |-> 0,emitted |-> <<>>,pipeline_batch |-> <<>>,rotated_offset |-> 0,in_flight_batch |-> <<>>,framer_buf |-> <<>>]),
    ([read_offset |-> 1,file_content |-> <<1>>,next_line_id |-> 2,alive |-> TRUE,checkpoint_gen |-> 0,rotated_content |-> <<>>,rotation_count |-> 0,rotated_active |-> FALSE,in_flight_offset |-> 0,checkpoint_offset |-> 0,crash_count |-> 0,emitted |-> <<>>,pipeline_batch |-> <<>>,rotated_offset |-> 0,in_flight_batch |-> <<>>,framer_buf |-> <<1>>]),
    ([read_offset |-> 1,file_content |-> <<1>>,next_line_id |-> 2,alive |-> TRUE,checkpoint_gen |-> 0,rotated_content |-> <<>>,rotation_count |-> 0,rotated_active |-> FALSE,in_flight_offset |-> 0,checkpoint_offset |-> 0,crash_count |-> 0,emitted |-> <<>>,pipeline_batch |-> <<1>>,rotated_offset |-> 0,in_flight_batch |-> <<>>,framer_buf |-> <<>>]),
    ([read_offset |-> 1,file_content |-> <<1>>,next_line_id |-> 2,alive |-> TRUE,checkpoint_gen |-> 0,rotated_content |-> <<>>,rotation_count |-> 0,rotated_active |-> FALSE,in_flight_offset |-> 1,checkpoint_offset |-> 0,crash_count |-> 0,emitted |-> <<>>,pipeline_batch |-> <<>>,rotated_offset |-> 0,in_flight_batch |-> <<1>>,framer_buf |-> <<>>]),
    ([read_offset |-> 0,file_content |-> <<>>,next_line_id |-> 2,alive |-> TRUE,checkpoint_gen |-> 0,rotated_content |-> <<1>>,rotation_count |-> 1,rotated_active |-> TRUE,in_flight_offset |-> 1,checkpoint_offset |-> 0,crash_count |-> 0,emitted |-> <<>>,pipeline_batch |-> <<>>,rotated_offset |-> 1,in_flight_batch |-> <<1>>,framer_buf |-> <<>>]),
    ([read_offset |-> 0,file_content |-> <<>>,next_line_id |-> 2,alive |-> TRUE,checkpoint_gen |-> 0,rotated_content |-> <<1>>,rotation_count |-> 1,rotated_active |-> TRUE,in_flight_offset |-> 0,checkpoint_offset |-> 1,crash_count |-> 0,emitted |-> <<1>>,pipeline_batch |-> <<>>,rotated_offset |-> 1,in_flight_batch |-> <<>>,framer_buf |-> <<>>])
    >>
----


=============================================================================

---- CONFIG MCFileCheckpoint_TTrace_1775196523 ----
CONSTANTS
    MaxLines = 4
    MaxBatches = 2
    MaxCrashes = 1
    MaxRotations = 1
    BatchSize = 2

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Fri Apr 03 06:08:44 UTC 2026