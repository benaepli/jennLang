package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/benaepli/turnpike-traceanalyzer/metrics"
	"github.com/benaepli/turnpike-traceanalyzer/reader"
	"github.com/benaepli/turnpike-traceanalyzer/report"
)

func main() {
	inputPath := flag.String("input", "", "Path to DuckDB file or Parquet output directory (required)")
	runID := flag.Int64("run", -1, "Run ID to analyze (-1 for all runs)")
	format := flag.String("format", "table", "Output format: table or json")
	flag.Parse()

	if *inputPath == "" {
		flag.Usage()
		log.Fatalln("Error: -input flag is required.")
	}

	formatNorm := strings.ToLower(*format)
	if formatNorm != "table" && formatNorm != "json" {
		log.Fatalf("invalid format %q (use table|json)", *format)
	}

	// Verify we can list run IDs
	runIDs, err := reader.ListRunIDs(*inputPath)
	if err != nil {
		log.Fatalf("failed to list run IDs: %v", err)
	}

	if *runID >= 0 {
		found := false
		for _, id := range runIDs {
			if id == *runID {
				found = true
				break
			}
		}
		if !found {
			log.Fatalf("run_id %d not found. Available: %v", *runID, runIDs)
		}
		fmt.Fprintf(os.Stderr, "Analyzing run %d...\n", *runID)
	} else {
		fmt.Fprintf(os.Stderr, "Analyzing all %d runs...\n", len(runIDs))
	}

	r := &report.FullReport{RunID: *runID}

	// Duration metrics
	fmt.Fprintln(os.Stderr, "  Computing duration metrics...")
	r.Duration, err = metrics.ComputeDuration(*inputPath, *runID)
	if err != nil {
		log.Printf("Warning: duration metrics failed: %v", err)
	}

	// Dispatch latency metrics
	fmt.Fprintln(os.Stderr, "  Computing dispatch metrics...")
	r.Dispatch, err = metrics.ComputeDispatch(*inputPath, *runID)
	if err != nil {
		log.Printf("Warning: dispatch metrics failed: %v", err)
	}

	// Interleaving metrics
	fmt.Fprintln(os.Stderr, "  Computing interleaving metrics...")
	r.Interleaving, err = metrics.ComputeInterleaving(*inputPath, *runID)
	if err != nil {
		log.Printf("Warning: interleaving metrics failed: %v", err)
	}

	// Fault metrics
	fmt.Fprintln(os.Stderr, "  Computing fault metrics...")
	r.Fault, err = metrics.ComputeFault(*inputPath, *runID)
	if err != nil {
		log.Printf("Warning: fault metrics failed: %v", err)
	}

	// Fingerprint metrics
	fmt.Fprintln(os.Stderr, "  Computing fingerprint metrics...")
	r.Fingerprint, err = metrics.ComputeFingerprint(*inputPath, *runID)
	if err != nil {
		log.Printf("Warning: fingerprint metrics failed: %v", err)
	}

	// Output
	switch formatNorm {
	case "json":
		if err := report.WriteJSON(os.Stdout, r); err != nil {
			log.Fatalf("failed to write JSON: %v", err)
		}
	case "table":
		report.WriteTable(os.Stdout, r)
	}
}
