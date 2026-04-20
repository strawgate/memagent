package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/anishathalye/porcupine"
)

type operationInput struct {
	Op        string `json:"op"`
	BatchID   uint64 `json:"batch_id"`
	SubmitSeq uint64 `json:"submit_seq"`
	SourceID  uint64 `json:"source_id"`
	Offset    uint64 `json:"offset"`
}

type operationOutput struct {
	Outcome    string  `json:"outcome"`
	Ackable    bool    `json:"ackable"`
	AdvancedTo *uint64 `json:"advanced_to"`
	Success    *bool   `json:"success"`
	Durable    *uint64 `json:"durable"`
}

type operation struct {
	ClientID int             `json:"client_id"`
	Call     int64           `json:"call"`
	Return   int64           `json:"return"`
	Input    operationInput  `json:"input"`
	Output   operationOutput `json:"output"`
}

type historyDocument struct {
	Operations []operation `json:"operations"`
}

type batchState struct {
	SubmitSeq uint64
	SourceID  uint64
	Offset    uint64
	Resolved  bool
	Ackable   bool
}

type modelState struct {
	Committed    uint64
	HasCommitted bool
	Durable      *uint64
	SourceID     *uint64
	Order        []uint64
	Batches      map[uint64]batchState
}

func cloneState(s modelState) modelState {
	orderCopy := make([]uint64, len(s.Order))
	copy(orderCopy, s.Order)
	batchesCopy := make(map[uint64]batchState, len(s.Batches))
	for k, v := range s.Batches {
		batchesCopy[k] = v
	}
	var durableCopy *uint64
	if s.Durable != nil {
		d := *s.Durable
		durableCopy = &d
	}
	var sourceCopy *uint64
	if s.SourceID != nil {
		source := *s.SourceID
		sourceCopy = &source
	}
	return modelState{
		Committed:    s.Committed,
		HasCommitted: s.HasCommitted,
		Durable:      durableCopy,
		SourceID:     sourceCopy,
		Order:        orderCopy,
		Batches:      batchesCopy,
	}
}

func toPorcupineOps(doc historyDocument) []porcupine.Operation {
	ops := make([]porcupine.Operation, 0, len(doc.Operations))
	for _, op := range doc.Operations {
		ops = append(ops, porcupine.Operation{
			ClientId: op.ClientID,
			Call:     op.Call,
			Return:   op.Return,
			Input:    op.Input,
			Output:   op.Output,
		})
	}
	return ops
}

func buildModel() porcupine.Model {
	return porcupine.Model{
		Init: func() any {
			return modelState{
				Committed:    0,
				HasCommitted: false,
				Durable:      nil,
				SourceID:     nil,
				Order:        make([]uint64, 0),
				Batches:      make(map[uint64]batchState),
			}
		},
		Step: func(stateAny, inputAny, outputAny any) (bool, any) {
			state := cloneState(stateAny.(modelState))
			input := inputAny.(operationInput)
			output := outputAny.(operationOutput)
			switch input.Op {
			case "submit":
				if _, exists := state.Batches[input.BatchID]; exists {
					return false, state
				}
				if state.SourceID == nil {
					source := input.SourceID
					state.SourceID = &source
				} else if input.SourceID != *state.SourceID {
					return false, state
				}
				state.Batches[input.BatchID] = batchState{
					SubmitSeq: input.SubmitSeq,
					SourceID:  input.SourceID,
					Offset:    input.Offset,
					Resolved:  false,
					Ackable:   false,
				}
				state.Order = append(state.Order, input.BatchID)
				sort.SliceStable(state.Order, func(i, j int) bool {
					l := state.Batches[state.Order[i]].SubmitSeq
					r := state.Batches[state.Order[j]].SubmitSeq
					return l < r
				})
				return true, state
			case "resolve":
				batch, exists := state.Batches[input.BatchID]
				if !exists || batch.Resolved {
					return false, state
				}
				if input.SourceID != batch.SourceID {
					return false, state
				}
				expectedAckable := output.Outcome == "delivered" || output.Outcome == "rejected"
				switch output.Outcome {
				case "delivered", "rejected", "retry_exhausted", "timed_out", "pool_closed", "worker_channel_closed", "no_workers_available", "internal_failure":
				default:
					return false, state
				}
				if output.Ackable != expectedAckable {
					return false, state
				}
				batch.Resolved = true
				batch.Ackable = expectedAckable
				state.Batches[input.BatchID] = batch

				newCommitted := uint64(0)
				haveCommitted := false
				for _, batchID := range state.Order {
					b := state.Batches[batchID]
					if !b.Resolved || !b.Ackable {
						break
					}
					newCommitted = b.Offset
					haveCommitted = true
				}
				if !haveCommitted {
					newCommitted = state.Committed
				}
				if newCommitted < state.Committed {
					return false, state
				}

				if newCommitted > state.Committed {
					if output.AdvancedTo == nil || *output.AdvancedTo != newCommitted {
						return false, state
					}
				} else if output.AdvancedTo != nil {
					return false, state
				}
				state.Committed = newCommitted
				if haveCommitted {
					state.HasCommitted = true
				}
				return true, state
			case "flush":
				if output.Success == nil {
					return false, state
				}
				if *output.Success {
					if state.HasCommitted {
						d := state.Committed
						state.Durable = &d
					} else {
						state.Durable = nil
					}
				}
				return true, state
			case "read_durable":
				if state.SourceID == nil || input.SourceID != *state.SourceID {
					return false, state
				}
				if state.Durable == nil {
					return output.Durable == nil, state
				}
				return output.Durable != nil && *output.Durable == *state.Durable, state
			default:
				return false, state
			}
		},
		Equal: func(a, b any) bool {
			left := a.(modelState)
			right := b.(modelState)
			if left.Committed != right.Committed || left.HasCommitted != right.HasCommitted || len(left.Order) != len(right.Order) || len(left.Batches) != len(right.Batches) {
				return false
			}
			if (left.Durable == nil) != (right.Durable == nil) {
				return false
			}
			if left.Durable != nil && *left.Durable != *right.Durable {
				return false
			}
			if (left.SourceID == nil) != (right.SourceID == nil) {
				return false
			}
			if left.SourceID != nil && *left.SourceID != *right.SourceID {
				return false
			}
			for i := range left.Order {
				if left.Order[i] != right.Order[i] {
					return false
				}
			}
			for k, lv := range left.Batches {
				rv, ok := right.Batches[k]
				if !ok || rv != lv {
					return false
				}
			}
			return true
		},
		DescribeOperation: func(inputAny, outputAny any) string {
			input := inputAny.(operationInput)
			output := outputAny.(operationOutput)
			return fmt.Sprintf("op=%s batch(id=%d,seq=%d,offset=%d)->{outcome=%s,ackable=%t,advanced_to=%v,success=%v,durable=%v}",
				input.Op, input.BatchID, input.SubmitSeq, input.Offset, output.Outcome, output.Ackable, output.AdvancedTo, output.Success, output.Durable)
		},
	}
}

func main() {
	historyPath := flag.String("history", "", "path to JSON history file")
	flag.Parse()

	if *historyPath == "" {
		fmt.Fprintln(os.Stderr, "--history is required")
		os.Exit(2)
	}

	f, err := os.Open(*historyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open history: %v\n", err)
		os.Exit(2)
	}
	defer f.Close()

	var doc historyDocument
	if err := json.NewDecoder(f).Decode(&doc); err != nil {
		fmt.Fprintf(os.Stderr, "decode history: %v\n", err)
		os.Exit(2)
	}

	ops := toPorcupineOps(doc)
	if len(ops) == 0 {
		fmt.Fprintln(os.Stderr, "history has no operations")
		os.Exit(2)
	}

	model := buildModel()
	if ok := porcupine.CheckOperations(model, ops); !ok {
		fmt.Fprintln(os.Stderr, "history is not linearizable")
		os.Exit(1)
	}

	fmt.Printf("linearizable: %d operations\n", len(ops))
}
