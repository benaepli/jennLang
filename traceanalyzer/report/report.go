package report

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/benaepli/jennlang-traceanalyzer/metrics"
)

// FullReport is the top-level structure for all metrics output.
type FullReport struct {
	RunID        int64                       `json:"run_id"` // -1 means all runs
	Duration     *metrics.DurationResult     `json:"duration,omitempty"`
	Dispatch     *metrics.DispatchResult     `json:"dispatch,omitempty"`
	Interleaving *metrics.InterleavingResult `json:"interleaving,omitempty"`
	Fault        *metrics.FaultResult        `json:"fault,omitempty"`
	Fingerprint  *metrics.FingerprintResult  `json:"fingerprint,omitempty"`
}

// WriteJSON writes the report as JSON to the given writer.
func WriteJSON(w io.Writer, r *FullReport) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteTable writes the report as human-readable tables to the given writer.
func WriteTable(w io.Writer, r *FullReport) {
	if r.RunID >= 0 {
		fmt.Fprintf(w, "=== Trace Analysis for Run %d ===\n\n", r.RunID)
	} else {
		fmt.Fprintf(w, "=== Trace Analysis (All Runs) ===\n\n")
	}

	writeDurationTable(w, r.Duration)
	writeDispatchTable(w, r.Dispatch)
	writeInterleavingTable(w, r.Interleaving)
	writeFaultTable(w, r.Fault)
	writeFingerprintTable(w, r.Fingerprint)
}

func writeDurationTable(w io.Writer, d *metrics.DurationResult) {
	if d == nil || len(d.Functions) == 0 {
		fmt.Fprintf(w, "## Function Duration\nNo data.\n\n")
		return
	}

	fmt.Fprintf(w, "## Function Duration (step-distance)\n")
	fmt.Fprintf(w, "%-35s %6s %6s %6s %8s %8s %6s %6s %6s %6s %s\n",
		"Function", "Count", "Unp.", "Unp%", "Min", "Max", "Mean", "P50", "P95", "P99", "Var?")
	fmt.Fprintf(w, "%s\n", strings.Repeat("-", 115))

	for _, f := range d.Functions {
		flag := ""
		if f.HighVariance {
			flag = "HIGH"
		}
		fmt.Fprintf(w, "%-35s %6d %6d %5.1f%% %8d %8d %6.1f %6d %6d %6d %s\n",
			truncate(f.FunctionName, 35),
			f.Count, f.UnpairedCount, f.UnpairedRatio*100,
			f.Min, f.Max, f.Mean,
			f.P50, f.P95, f.P99,
			flag)
	}
	fmt.Fprintln(w)
}

func writeDispatchTable(w io.Writer, d *metrics.DispatchResult) {
	if d == nil || len(d.Functions) == 0 {
		return // Silently skip if no dispatch data
	}

	fmt.Fprintf(w, "## Dispatch Queueing Latency (Enter - Dispatch)\n")
	fmt.Fprintf(w, "%-35s %6s %8s %8s %6s %6s %6s %6s\n",
		"Function", "Count", "Min", "Max", "Mean", "P50", "P95", "P99")
	fmt.Fprintf(w, "%s\n", strings.Repeat("-", 90))

	for _, f := range d.Functions {
		fmt.Fprintf(w, "%-35s %6d %8d %8d %6.1f %6d %6d %6d\n",
			truncate(f.FunctionName, 35),
			f.Count, f.MinLatency, f.MaxLatency, f.MeanLatency,
			f.P50Latency, f.P95Latency, f.P99Latency)
	}
	fmt.Fprintln(w)
}

func writeInterleavingTable(w io.Writer, il *metrics.InterleavingResult) {
	if il == nil {
		fmt.Fprintf(w, "## Interleaving\nNo data.\n\n")
		return
	}

	if len(il.Depth) > 0 {
		fmt.Fprintf(w, "## Interleaving Depth (cross-node events during invocation)\n")
		fmt.Fprintf(w, "%-35s %6s %6s %6s %8s %6s %6s\n",
			"Function", "Count", "Min", "Max", "Mean", "P50", "P95")
		fmt.Fprintf(w, "%s\n", strings.Repeat("-", 85))
		for _, d := range il.Depth {
			fmt.Fprintf(w, "%-35s %6d %6d %6d %8.1f %6d %6d\n",
				truncate(d.FunctionName, 35),
				d.Count, d.MinDepth, d.MaxDepth, d.MeanDepth,
				d.P50Depth, d.P95Depth)
		}
		fmt.Fprintln(w)
	}

	if len(il.Overlap) > 0 {
		fmt.Fprintf(w, "## Concurrent Same-Function Overlap\n")
		fmt.Fprintf(w, "%-35s %10s %10s %10s\n",
			"Function", "Pairs", "Overlap", "Fraction")
		fmt.Fprintf(w, "%s\n", strings.Repeat("-", 70))
		for _, o := range il.Overlap {
			fmt.Fprintf(w, "%-35s %10d %10d %9.3f\n",
				truncate(o.FunctionName, 35),
				o.TotalPairs, o.OverlapCount, o.OverlapFraction)
		}
		fmt.Fprintln(w)
	}

	if len(il.SchedDelta) > 0 {
		fmt.Fprintf(w, "## Schedulable Count Delta (exit - enter)\n")
		fmt.Fprintf(w, "%-35s %6s %8s %8s %8s\n",
			"Function", "Count", "Min", "Max", "Mean")
		fmt.Fprintf(w, "%s\n", strings.Repeat("-", 70))
		for _, s := range il.SchedDelta {
			fmt.Fprintf(w, "%-35s %6d %8d %8d %8.1f\n",
				truncate(s.FunctionName, 35),
				s.Count, s.MinDelta, s.MaxDelta, s.MeanDelta)
		}
		fmt.Fprintln(w)
	}
}

func writeFaultTable(w io.Writer, f *metrics.FaultResult) {
	if f == nil {
		fmt.Fprintf(w, "## Fault Analysis\nNo data.\n\n")
		return
	}

	if len(f.CrashDuringFunc) > 0 {
		fmt.Fprintf(w, "## Functions Active During Crash\n")
		fmt.Fprintf(w, "%-35s %10s\n", "Function", "Interrupts")
		fmt.Fprintf(w, "%s\n", strings.Repeat("-", 48))
		for _, c := range f.CrashDuringFunc {
			fmt.Fprintf(w, "%-35s %10d\n",
				truncate(c.FunctionName, 35), c.InterruptCount)
		}
		fmt.Fprintln(w)
	}

	if f.CrashDistance != nil {
		fmt.Fprintf(w, "## Crash-to-Trace Distance\n")
		fmt.Fprintf(w, "  Crashes: %d, Min dist: %d, Max dist: %d, Mean dist: %.1f\n\n",
			f.CrashDistance.CrashCount,
			f.CrashDistance.MinDistance,
			f.CrashDistance.MaxDistance,
			f.CrashDistance.MeanDistance)
	}

	if len(f.CrashCoverage) > 0 {
		fmt.Fprintf(w, "## Per-Function Crash Coverage\n")
		fmt.Fprintf(w, "%-35s %8s %8s %10s\n", "Function", "CrRuns", "Total", "Coverage")
		fmt.Fprintf(w, "%s\n", strings.Repeat("-", 65))
		for _, c := range f.CrashCoverage {
			fmt.Fprintf(w, "%-35s %8d %8d %9.3f\n",
				truncate(c.FunctionName, 35),
				c.RunsWithCrash, c.TotalRuns, c.CoverageFraction)
		}
		fmt.Fprintln(w)
	}
}

func writeFingerprintTable(w io.Writer, fp *metrics.FingerprintResult) {
	if fp == nil {
		fmt.Fprintf(w, "## Exploration Diversity\nNo data.\n\n")
		return
	}

	fmt.Fprintf(w, "## Exploration Diversity\n")
	fmt.Fprintf(w, "  Total runs:           %d\n", fp.TotalRuns)
	fmt.Fprintf(w, "  Unique fingerprints:  %d\n", fp.UniqueFingerprints)
	fmt.Fprintf(w, "  Diversity ratio:      %.4f\n", fp.DiversityRatio)
	fmt.Fprintf(w, "  Unique node profiles: %d\n", fp.UniqueNodeProfiles)
	fmt.Fprintln(w)

	if len(fp.CausalChains) > 0 {
		fmt.Fprintf(w, "## Causal Chain Diversity\n")
		fmt.Fprintf(w, "%-35s %10s %12s\n", "Function", "Chains", "Invocations")
		fmt.Fprintf(w, "%s\n", strings.Repeat("-", 60))
		for _, c := range fp.CausalChains {
			fmt.Fprintf(w, "%-35s %10d %12d\n",
				truncate(c.FunctionName, 35),
				c.DistinctChains, c.TotalInvocations)
		}
		fmt.Fprintln(w)
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-2] + ".."
}
