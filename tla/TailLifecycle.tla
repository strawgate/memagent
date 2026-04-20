------------------------- MODULE TailLifecycle -------------------------
EXTENDS Naturals

\* Minimal control-plane model for the tail EOF + error-backoff reducers.
\* This mirrors the pure reducers in `crates/logfwd-io/src/tail/state.rs`.

CONSTANTS
    Threshold,       \* EOF idle-poll threshold (expected: 2)
    MaxIdle,         \* finite bound for model checking
    MaxErrors,       \* finite bound for model checking
    InitialBackoff,  \* expected: 100
    MaxBackoff       \* expected: 5000

VARIABLES
    eofEmitted,
    idlePolls,
    consecutiveErrors,
    backoffMs,
    lastAction

vars == <<eofEmitted, idlePolls, consecutiveErrors, backoffMs, lastAction>>

Min(a, b) ==
    IF a < b THEN a ELSE b

IncIdle(i) ==
    IF i < MaxIdle THEN i + 1 ELSE MaxIdle

IncErrors(e) ==
    IF e < MaxErrors THEN e + 1 ELSE MaxErrors

RECURSIVE Pow2(_)
Pow2(n) ==
    IF n = 0 THEN 1 ELSE 2 * Pow2(n - 1)

BackoffDelay(errors) ==
    IF errors = 0 THEN
        0
    ELSE
        Min(MaxBackoff, InitialBackoff * Pow2(Min(errors - 1, 6)))

Init ==
    /\ eofEmitted = FALSE
    /\ idlePolls = 0
    /\ consecutiveErrors = 0
    /\ backoffMs = 0
    /\ lastAction = "Init"

DataStep ==
    /\ eofEmitted' = FALSE
    /\ idlePolls' = 0
    /\ UNCHANGED <<consecutiveErrors, backoffMs>>
    /\ lastAction' = "Data"

NoDataEmit ==
    LET nextIdle == IncIdle(idlePolls) IN
    /\ ~eofEmitted
    /\ nextIdle >= Threshold
    /\ eofEmitted' = TRUE
    /\ idlePolls' = nextIdle
    /\ UNCHANGED <<consecutiveErrors, backoffMs>>
    /\ lastAction' = "NoDataEmit"

NoDataNoEmit ==
    LET nextIdle == IncIdle(idlePolls) IN
    /\ eofEmitted \/ nextIdle < Threshold
    /\ eofEmitted' = eofEmitted
    /\ idlePolls' = nextIdle
    /\ UNCHANGED <<consecutiveErrors, backoffMs>>
    /\ lastAction' = "NoDataNoEmit"

ErrorStep ==
    LET nextErrors == IncErrors(consecutiveErrors) IN
    /\ consecutiveErrors' = nextErrors
    /\ backoffMs' = BackoffDelay(nextErrors)
    /\ UNCHANGED <<eofEmitted, idlePolls>>
    /\ lastAction' = "Error"

CleanStep ==
    /\ consecutiveErrors > 0
    /\ consecutiveErrors' = 0
    /\ backoffMs' = 0
    /\ UNCHANGED <<eofEmitted, idlePolls>>
    /\ lastAction' = "Clean"

Next ==
    \/ DataStep
    \/ NoDataEmit
    \/ NoDataNoEmit
    \/ ErrorStep
    \/ CleanStep

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ eofEmitted \in BOOLEAN
    /\ idlePolls \in 0..MaxIdle
    /\ consecutiveErrors \in 0..MaxErrors
    /\ backoffMs \in 0..MaxBackoff
    /\ lastAction \in {"Init", "Data", "NoDataEmit", "NoDataNoEmit", "Error", "Clean"}

EofEmissionRequiresThreshold ==
    lastAction = "NoDataEmit" =>
    /\ eofEmitted
    /\ idlePolls >= Threshold

DataResetsEofState ==
    lastAction = "Data" =>
    /\ ~eofEmitted
    /\ idlePolls = 0

BackoffZeroIffNoErrors ==
    (consecutiveErrors = 0) <=> (backoffMs = 0)

BackoffDelayConsistent ==
    consecutiveErrors > 0 => backoffMs = BackoffDelay(consecutiveErrors)

=============================================================================
