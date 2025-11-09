package main

import (
	"fmt"
	"strings"

	"github.com/anishathalye/porcupine"
)

type qInput struct {
	Op  string // "ENQ" or "DEQ"
	Val string // for ENQ: the string to enqueue; for DEQ: unused
}

// We’ll store outputs as strings. For ENQ we don't check the output.
// For DEQ we require it equals the head element (or "<empty>" if queue empty).

func queueModel() porcupine.Model {
	return porcupine.Model{
		// State is a FIFO queue of strings
		Init: func() interface{} { return []string{} },

		Step: func(state, input, output interface{}) (bool, interface{}) {
			q := append([]string{}, state.([]string)...) // copy
			in := input.(qInput)
			out := ""
			out, _ = output.(string)

			switch strings.ToUpper(in.Op) {
			case "ENQ":
				// Enqueue: accept any output (often empty)
				q = append(q, in.Val)
				return true, q

			case "DEQ":
				if len(q) == 0 {
					// Must have recorded "<empty>" for empty dequeue
					return out == "<empty>", q
				}
				hd := q[0]
				if out != hd {
					return false, state
				}
				return true, q[1:]

			default:
				// Unknown operation this should not happen
				fmt.Println("Debug: Unknown Ops")
				return false, state
			}
		},

		// compare each char
		Equal: func(a, b interface{}) bool {
			x := a.([]string)
			y := b.([]string)
			if len(x) != len(y) {
				return false
			}
			for i := range x {
				if x[i] != y[i] {
					return false
				}
			}
			return true
		},

		// better labels in visualization
		DescribeOperation: func(input, output interface{}) string {
			in := input.(qInput)
			switch strings.ToUpper(in.Op) {
			case "ENQ":
				return fmt.Sprintf("ENQ(%q)", in.Val)
			case "DEQ":
				if output == nil {
					return "DEQ(?)"
				}
				return fmt.Sprintf("DEQ→%q", output.(string))
			default:
				return "??"
			}
		},

		DescribeState: func(state interface{}) string {
			return fmt.Sprintf("%v", state.([]string))
		},
	}
}
