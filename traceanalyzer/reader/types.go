package reader

import (
	"fmt"
	"log"
)

// TraceRow represents a single row from the traces table.
type TraceRow struct {
	RunID              int64
	SeqNum             int64
	NodeID             int64
	Step               int32
	FunctionName       string
	TraceKind          string // "Enter" or "Exit"
	Payload            string
	SchedulableCount   int64
	TraceID            int64
	CausalOperationID  *int64 // nullable
}

// ExecutionRow represents a single row from the executions table.
type ExecutionRow struct {
	RunID    int64
	SeqNum   int64
	UniqueID int64
	ClientID int64
	Kind     string // "Invocation", "Response", "Crash", "Recover"
	Action   string
	Payload  string
}

// Invocation represents a paired Enter+Exit for one trace_id.
type Invocation struct {
	RunID              int64
	TraceID            int64
	NodeID             int64
	FunctionName       string
	EnterStep          int32
	ExitStep           int32
	EnterPayload       string
	ExitPayload        string
	EnterSchedCount    int64
	ExitSchedCount     int64
	CausalOperationID  *int64
	Unpaired           bool // true if Exit was not found
}

// PairInvocations groups TraceRows by (run_id, trace_id) and pairs Enter/Exit events.
// Warns on unpaired entries (crash-interrupted).
func PairInvocations(rows []TraceRow) []Invocation {
	type key struct {
		RunID   int64
		TraceID int64
	}

	enters := make(map[key]*Invocation)
	var invocations []Invocation

	for i := range rows {
		r := &rows[i]
		k := key{RunID: r.RunID, TraceID: r.TraceID}

		switch r.TraceKind {
		case "Enter":
			inv := &Invocation{
				RunID:             r.RunID,
				TraceID:           r.TraceID,
				NodeID:            r.NodeID,
				FunctionName:      r.FunctionName,
				EnterStep:         r.Step,
				EnterPayload:      r.Payload,
				EnterSchedCount:   r.SchedulableCount,
				CausalOperationID: r.CausalOperationID,
				Unpaired:          true,
			}
			enters[k] = inv

		case "Exit":
			if inv, ok := enters[k]; ok {
				inv.ExitStep = r.Step
				inv.ExitPayload = r.Payload
				inv.ExitSchedCount = r.SchedulableCount
				inv.Unpaired = false
				invocations = append(invocations, *inv)
				delete(enters, k)
			} else {
				log.Printf("Warning: Exit without matching Enter for run_id=%d trace_id=%d", r.RunID, r.TraceID)
			}

		default:
			log.Printf("Warning: unknown trace_kind %q for run_id=%d trace_id=%d", r.TraceKind, r.RunID, r.TraceID)
		}
	}

	// Collect unpaired Enter events
	for _, inv := range enters {
		invocations = append(invocations, *inv)
	}

	unpairedCount := 0
	for _, inv := range invocations {
		if inv.Unpaired {
			unpairedCount++
		}
	}
	if unpairedCount > 0 {
		fmt.Printf("Warning: %d unpaired Enter events (crash-interrupted)\n", unpairedCount)
	}

	return invocations
}
