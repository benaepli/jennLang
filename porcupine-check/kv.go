package main

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

type kvInput struct {
	Op  string // "Get" / "Put" / "Delete"
	Key string // the key
	Val string // for "Add" Ops
}

// We’ll store outputs as strings. For ENQ we don't check the output.
// For DEQ we require it equals the head element (or "<empty>" if queue empty).

func kvModel() porcupine.Model {
	return porcupine.Model{
		// State is a FIFO queue of strings
		Init: func() interface{} { return map[string]string{} },

		Step: func(state, input, output interface{}) (bool, interface{}) {
			q := maps.Clone(state.(map[string]string)) // copy
			in := input.(kvInput)
			out := ""
			out, _ = output.(string)

			switch strings.ToUpper(in.Op) {
			case "PUT":
				q[in.Key] = in.Val
				// Enqueue: accept any output (often empty)
				return true, q

			case "GET":
				v, ok := q[in.Key]

				if !ok {
					return out == "NotFound", q //if key doesn't exist
				}

				return out == v, q
			case "DELETE":
				delete(q, in.Key)
				return true, q
			default:
				// Unknown operation this should not happen
				fmt.Println("Debug: Unknown Ops")
				return false, state
			}
		},

		// compare each name space
		Equal: func(a, b interface{}) bool {
			return maps.Equal(a.(map[string]string), b.(map[string]string))
		},

		// better labels in visualization
		DescribeOperation: func(input, output interface{}) string {
			in := input.(kvInput)
			switch strings.ToUpper(in.Op) {
			case "PUT":
				return fmt.Sprintf("PUT <(%q), (%q)>", in.Key, in.Val)
			case "GET":
				if output == nil {
					return "DEQ(?)"
				}
				return fmt.Sprintf("GET (%q)→%q", in.Key, output.(string))
			case "DELETE":
				return fmt.Sprintf("DELETE <(%q), (%q)>", in.Key, in.Val)
			default:
				return "??"
			}
		},

		DescribeState: func(state interface{}) string {
			// pretty print map with sorted keys (stable viz)
			m := state.(map[string]string)
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var b strings.Builder
			b.WriteString("{")
			for i, k := range keys {
				if i > 0 {
					b.WriteString(", ")
				}
				fmt.Fprintf(&b, "%s:%s", k, m[k])
			}
			b.WriteString("}")
			return b.String()
		},
	}
}
