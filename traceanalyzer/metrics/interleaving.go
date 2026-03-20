package metrics

import (
	"database/sql"
	"fmt"

	"github.com/benaepli/turnpike-traceanalyzer/reader"
)

// InterleavingDepthStats holds per-function interleaving depth statistics.
type InterleavingDepthStats struct {
	FunctionName string  `json:"function_name"`
	Count        int     `json:"count"`
	MinDepth     int     `json:"min_depth"`
	MaxDepth     int     `json:"max_depth"`
	MeanDepth    float64 `json:"mean_depth"`
	P50Depth     int     `json:"p50_depth"`
	P95Depth     int     `json:"p95_depth"`
}

// OverlapStats holds same-function concurrent overlap statistics.
type OverlapStats struct {
	FunctionName    string  `json:"function_name"`
	TotalPairs      int     `json:"total_pairs"`
	OverlapCount    int     `json:"overlap_count"`
	OverlapFraction float64 `json:"overlap_fraction"`
}

// SchedDeltaStats holds schedulable count delta statistics per function.
type SchedDeltaStats struct {
	FunctionName string  `json:"function_name"`
	Count        int     `json:"count"`
	MinDelta     int64   `json:"min_delta"`
	MaxDelta     int64   `json:"max_delta"`
	MeanDelta    float64 `json:"mean_delta"`
}

// InterleavingResult holds the complete interleaving analysis.
type InterleavingResult struct {
	Depth      []InterleavingDepthStats `json:"interleaving_depth"`
	Overlap    []OverlapStats           `json:"concurrent_overlap"`
	SchedDelta []SchedDeltaStats        `json:"schedulable_count_delta"`
}

// ComputeInterleaving computes concurrency and overlap metrics using DuckDB SQL.
func ComputeInterleaving(dbPath string, runID int64) (*InterleavingResult, error) {
	db, err := reader.OpenDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	result := &InterleavingResult{}

	if err := computeDepth(db, dbPath, runID, result); err != nil {
		return nil, err
	}
	if err := computeOverlap(db, dbPath, runID, result); err != nil {
		return nil, err
	}
	if err := computeSchedDelta(db, dbPath, runID, result); err != nil {
		return nil, err
	}

	return result, nil
}

func computeDepth(db *sql.DB, dbPath string, runID int64, result *InterleavingResult) error {
	src := reader.TracesSource(dbPath)
	filter := runIDFilter(runID)

	// For each paired invocation, count trace events from OTHER nodes in [enter_step, exit_step]
	query := fmt.Sprintf(`
		WITH latest_enter AS (
			SELECT run_id, node_id, trace_id, function_name, step
			FROM %[1]s
			WHERE trace_kind = 'Enter'
			QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id, trace_id ORDER BY step DESC) = 1
		),
		paired AS (
			SELECT
				e.run_id,
				e.node_id,
				e.function_name,
				e.step AS enter_step,
				x.step AS exit_step
			FROM latest_enter e
			JOIN %[1]s x
			  ON e.run_id = x.run_id
			  AND e.trace_id = x.trace_id
			  AND x.trace_kind = 'Exit'
			WHERE 1=1 %[2]s
		),
		depth_per_inv AS (
			SELECT
				p.function_name,
				(SELECT COUNT(*)
				 FROM %[1]s t
				 WHERE t.run_id = p.run_id
				   AND t.node_id != p.node_id
				   AND t.step >= p.enter_step
				   AND t.step <= p.exit_step
				) AS depth
			FROM paired p
		)
		SELECT
			function_name,
			COUNT(*) AS cnt,
			MIN(depth) AS min_depth,
			MAX(depth) AS max_depth,
			AVG(depth) AS mean_depth,
			PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY depth) AS p50,
			PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY depth) AS p95
		FROM depth_per_inv
		GROUP BY function_name
		ORDER BY function_name
	`, src, filter)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query interleaving depth: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var s InterleavingDepthStats
		var meanDepth sql.NullFloat64
		if err := rows.Scan(&s.FunctionName, &s.Count, &s.MinDepth, &s.MaxDepth, &meanDepth, &s.P50Depth, &s.P95Depth); err != nil {
			return fmt.Errorf("failed to scan depth row: %w", err)
		}
		if meanDepth.Valid {
			s.MeanDepth = meanDepth.Float64
		}
		result.Depth = append(result.Depth, s)
	}
	return rows.Err()
}

func computeOverlap(db *sql.DB, dbPath string, runID int64, result *InterleavingResult) error {
	src := reader.TracesSource(dbPath)
	filter := runIDFilter(runID)

	// For pairs of invocations of the same function on different nodes in the same run,
	// check if their [enter_step, exit_step] ranges overlap.
	query := fmt.Sprintf(`
		WITH latest_enter AS (
			SELECT run_id, node_id, trace_id, function_name, step
			FROM %[1]s
			WHERE trace_kind = 'Enter'
			QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id, trace_id ORDER BY step DESC) = 1
		),
		paired AS (
			SELECT
				e.run_id,
				e.node_id,
				e.function_name,
				e.step AS enter_step,
				x.step AS exit_step
			FROM latest_enter e
			JOIN %[1]s x
			  ON e.run_id = x.run_id
			  AND e.trace_id = x.trace_id
			  AND x.trace_kind = 'Exit'
			WHERE 1=1 %[2]s
		),
		cross_pairs AS (
			SELECT
				a.function_name,
				COUNT(*) AS total_pairs,
				SUM(CASE
					WHEN a.enter_step <= b.exit_step AND b.enter_step <= a.exit_step
					THEN 1 ELSE 0
				END) AS overlap_count
			FROM paired a
			JOIN paired b
			  ON a.run_id = b.run_id
			  AND a.function_name = b.function_name
			  AND a.node_id < b.node_id
			GROUP BY a.function_name
		)
		SELECT function_name, total_pairs, overlap_count,
		       CASE WHEN total_pairs > 0 THEN CAST(overlap_count AS DOUBLE) / total_pairs ELSE 0 END
		FROM cross_pairs
		ORDER BY function_name
	`, src, filter)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query overlap: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var s OverlapStats
		if err := rows.Scan(&s.FunctionName, &s.TotalPairs, &s.OverlapCount, &s.OverlapFraction); err != nil {
			return fmt.Errorf("failed to scan overlap row: %w", err)
		}
		result.Overlap = append(result.Overlap, s)
	}
	return rows.Err()
}

func computeSchedDelta(db *sql.DB, dbPath string, runID int64, result *InterleavingResult) error {
	src := reader.TracesSource(dbPath)
	filter := runIDFilter(runID)

	query := fmt.Sprintf(`
		WITH latest_enter AS (
			SELECT run_id, trace_id, function_name, schedulable_count
			FROM %[1]s
			WHERE trace_kind = 'Enter'
			QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id, trace_id ORDER BY step DESC) = 1
		),
		paired AS (
			SELECT
				e.function_name,
				x.schedulable_count - e.schedulable_count AS delta
			FROM latest_enter e
			JOIN %[1]s x
			  ON e.run_id = x.run_id
			  AND e.trace_id = x.trace_id
			  AND x.trace_kind = 'Exit'
			WHERE 1=1 %[2]s
		)
		SELECT
			function_name,
			COUNT(*) AS cnt,
			MIN(delta) AS min_delta,
			MAX(delta) AS max_delta,
			AVG(delta) AS mean_delta
		FROM paired
		GROUP BY function_name
		ORDER BY function_name
	`, src, filter)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query sched delta: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var s SchedDeltaStats
		var meanDelta sql.NullFloat64
		if err := rows.Scan(&s.FunctionName, &s.Count, &s.MinDelta, &s.MaxDelta, &meanDelta); err != nil {
			return fmt.Errorf("failed to scan sched delta row: %w", err)
		}
		if meanDelta.Valid {
			s.MeanDelta = meanDelta.Float64
		}
		result.SchedDelta = append(result.SchedDelta, s)
	}
	return rows.Err()
}
