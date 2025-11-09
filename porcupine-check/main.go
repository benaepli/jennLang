package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
)

func mustAtoi(s string) int {
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		log.Fatalf("bad int %q: %v", s, err)
	}
	return v
}

func mustAtoi64(s string) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		log.Fatalf("bad int64 %q: %v", s, err)
	}
	return v
}

func main() {
	// 1) Open CSV modify the name for different workload

	specname := "kv"                         //the spec
	hname := "kv_history_large_linearizable" //the workload

	fname := "./histories/" + specname + "/" + hname + ".csv"
	if len(os.Args) > 1 {
		fname = os.Args[1]
	}
	f, err := os.Open(fname)
	if err != nil {
		log.Fatalf("open %s: %v", fname, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // allow variable; we’ll pick by header

	// 2) Read header
	header, err := r.Read()
	if err != nil {
		log.Fatalf("read header: %v", err)
	}
	index := func(name string) int {
		name = strings.ToLower(name)
		for i, h := range header {
			if strings.ToLower(strings.TrimSpace(h)) == name {
				return i
			}
		}
		log.Fatalf("missing required column %q in header: %v", name, header)
		return -1
	}
	iCall := index("call_ns")
	iRet := index("return_ns")
	iClient := index("client_id")
	iOp := index("op")
	iVal := index("value")
	iOut := index("output")

	// 3) Parse rows → porcupine.Operations
	var ops []porcupine.Operation
	rowNum := 1
	for {
		row, err := r.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatalf("read row %d: %v", rowNum, err)
		}
		rowNum++

		call := mustAtoi64(row[iCall])
		ret := mustAtoi64(row[iRet])
		client := mustAtoi(row[iClient])
		op := strings.TrimSpace(row[iOp])
		val := strings.TrimSpace(row[iVal])
		out := strings.TrimSpace(row[iOut])

		// Normalize empty output to nil so DescribeOperation can show "DEQ(?)" if missing
		var outAny interface{}
		if out != "" {
			outAny = out
		}

		//Jeffery: handle different applications
		switch specname {
		case "queue":
			ops = append(ops, porcupine.Operation{
				Input:    qInput{Op: op, Val: val},
				Output:   outAny,
				Call:     call,
				Return:   ret,
				ClientId: client,
			})
		case "kv":
			ops = append(ops, porcupine.Operation{
				Input:    kvInput{Op: op, Key: strings.TrimSpace(row[index("key")]), Val: val},
				Output:   outAny,
				Call:     call,
				Return:   ret,
				ClientId: client,
			})
		default:
			fmt.Print("Debug:No Application name")
			os.Exit(1)
		}
	}

	// 4) Check linearizability + visualize
	var model porcupine.Model
	switch specname {
	case "kv":
		model = kvModel()
	case "queue":
		model = queueModel()
	default:
		log.Fatalf("unknown spec %q (use kv|queue)", specname)
	}
	// New API: needs a timeout (time.Duration) and returns (CheckResult, LinearizationInfo)
	res, info := porcupine.CheckOperationsVerbose(model, ops, 0) // 0 = no timeout
	fmt.Println("Linearizable?", res == porcupine.Ok)

	// Always produce a visualization so you can inspect even passing runs
	outHTML := "./visualization/" + specname + "/" + hname + ".html"

	// Arg order changed: either write to a path...
	if err := porcupine.VisualizePath(model, info, outHTML); err != nil {
		log.Fatalf("visualize: %v", err)
	}
	fmt.Printf("Wrote %s (open it in a browser)\n", outHTML)

	// CheckResult is a string enum (Ok/Illegal/Unknown), not bool
	if res != porcupine.Ok {
		fmt.Println("Jeffery: exit with 2")
		os.Exit(2) // nonzero exit for CI
	}
}
