package metrics

import (
	"database/sql"
	"fmt"

	"github.com/benaepli/jennlang-traceanalyzer/reader"
)

// CrashDuringFunction shows which functions were active when a crash occurred.
type CrashDuringFunction struct {
	FunctionName   string `json:"function_name"`
	InterruptCount int    `json:"interrupt_count"`
}

// CrashDistanceStats shows the minimum distance between crash events and trace events.
type CrashDistanceStats struct {
	MinDistance  int     `json:"min_distance"`
	MaxDistance  int     `json:"max_distance"`
	MeanDistance float64 `json:"mean_distance"`
	CrashCount   int     `json:"crash_count"`
}

// FunctionCrashCoverage shows the fraction of runs where a function was active during a crash.
type FunctionCrashCoverage struct {
	FunctionName     string  `json:"function_name"`
	RunsWithCrash    int     `json:"runs_with_crash"`
	TotalRuns        int     `json:"total_runs"`
	CoverageFraction float64 `json:"coverage_fraction"`
}

// FaultResult holds the complete fault/crash analysis.
type FaultResult struct {
	CrashDuringFunc []CrashDuringFunction   `json:"crash_during_function"`
	CrashDistance   *CrashDistanceStats     `json:"crash_distance"`
	CrashCoverage   []FunctionCrashCoverage `json:"crash_coverage"`
}

// ComputeFault computes crash proximity metrics by joining traces and executions.
func ComputeFault(dbPath string, runID int64) (*FaultResult, error) {
	db, err := reader.OpenDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	result := &FaultResult{}

	if err := computeCrashDuringFunc(db, dbPath, runID, result); err != nil {
		return nil, err
	}
	if err := computeCrashDistance(db, dbPath, runID, result); err != nil {
		return nil, err
	}
	if err := computeCrashCoverage(db, dbPath, runID, result); err != nil {
		return nil, err
	}

	return result, nil
}

func computeCrashDuringFunc(db *sql.DB, dbPath string, runID int64, result *FaultResult) error {
	tSrc := reader.TracesSource(dbPath)
	eSrc := reader.ExecutionsSource(dbPath)
	filter := runIDFilter(runID)

	// Find invocations that were active when a crash happened on the same node.
	// Crash events in executions have action ending with 'System.Crash'.
	// We use seq_num from executions as the ordering proxy for crash timing.
	query := fmt.Sprintf(`
		WITH paired AS (
			SELECT
				e.run_id,
				e.node_id,
				e.function_name,
				e.step AS enter_step,
				x.step AS exit_step
			FROM %[1]s e
			JOIN %[1]s x
			  ON e.run_id = x.run_id
			  AND e.trace_id = x.trace_id
			  AND e.trace_kind = 'Enter'
			  AND x.trace_kind = 'Exit'
			WHERE 1=1 %[3]s
		),
		crashes AS (
			SELECT run_id, seq_num
			FROM %[2]s
			WHERE kind = 'Invocation' AND action LIKE '%%System.Crash'
			%[3]s
		)
		SELECT
			p.function_name,
			COUNT(*) AS interrupt_count
		FROM paired p
		JOIN crashes c
		  ON p.run_id = c.run_id
		  AND c.seq_num >= p.enter_step
		  AND c.seq_num <= p.exit_step
		GROUP BY p.function_name
		ORDER BY interrupt_count DESC
	`, tSrc, eSrc, filter)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query crash-during-function: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var s CrashDuringFunction
		if err := rows.Scan(&s.FunctionName, &s.InterruptCount); err != nil {
			return fmt.Errorf("failed to scan crash-during-function row: %w", err)
		}
		result.CrashDuringFunc = append(result.CrashDuringFunc, s)
	}
	return rows.Err()
}

func computeCrashDistance(db *sql.DB, dbPath string, runID int64, result *FaultResult) error {
	tSrc := reader.TracesSource(dbPath)
	eSrc := reader.ExecutionsSource(dbPath)
	filter := runIDFilter(runID)

	// For each crash event, find the minimum distance to any trace event on the same node/run.
	query := fmt.Sprintf(`
		WITH crashes AS (
			SELECT run_id, seq_num
			FROM %[2]s
			WHERE kind = 'Invocation' AND action LIKE '%%System.Crash'
			%[3]s
		),
		distances AS (
			SELECT
				ABS(c.seq_num - t.step) AS dist
			FROM crashes c
			JOIN %[1]s t
			  ON c.run_id = t.run_id
		)
		SELECT
			COUNT(DISTINCT c.seq_num || '-' || c.run_id) AS crash_count,
			MIN(d.min_dist) AS min_distance,
			MAX(d.min_dist) AS max_distance,
			AVG(d.min_dist) AS mean_distance
		FROM crashes c
		CROSS JOIN LATERAL (
			SELECT MIN(ABS(c.seq_num - t.step)) AS min_dist
			FROM %[1]s t
			WHERE t.run_id = c.run_id
		) d
	`, tSrc, eSrc, filter)

	// Try the LATERAL join version first; fall back to a simpler query if not supported
	var s CrashDistanceStats
	var meanDist sql.NullFloat64
	var minDist, maxDist sql.NullInt64
	err := db.QueryRow(query).Scan(&s.CrashCount, &minDist, &maxDist, &meanDist)
	if err != nil {
		// Fallback: simpler approach without LATERAL
		query = fmt.Sprintf(`
			WITH crashes AS (
				SELECT run_id, seq_num
				FROM %[2]s
				WHERE kind = 'Invocation' AND action LIKE '%%System.Crash'
				%[3]s
			),
			crash_trace_dist AS (
				SELECT
					c.run_id,
					c.seq_num AS crash_seq,
					MIN(ABS(c.seq_num - t.step)) AS min_dist
				FROM crashes c
				JOIN %[1]s t ON c.run_id = t.run_id
				GROUP BY c.run_id, c.seq_num
			)
			SELECT
				COUNT(*) AS crash_count,
				MIN(min_dist) AS min_distance,
				MAX(min_dist) AS max_distance,
				AVG(min_dist) AS mean_distance
			FROM crash_trace_dist
		`, tSrc, eSrc, filter)

		err = db.QueryRow(query).Scan(&s.CrashCount, &minDist, &maxDist, &meanDist)
		if err != nil {
			if err == sql.ErrNoRows {
				result.CrashDistance = nil
				return nil
			}
			return fmt.Errorf("failed to query crash distance: %w", err)
		}
	}

	if minDist.Valid {
		s.MinDistance = int(minDist.Int64)
	}
	if maxDist.Valid {
		s.MaxDistance = int(maxDist.Int64)
	}
	if meanDist.Valid {
		s.MeanDistance = meanDist.Float64
	}
	if s.CrashCount > 0 {
		result.CrashDistance = &s
	}
	return nil
}

func computeCrashCoverage(db *sql.DB, dbPath string, runID int64, result *FaultResult) error {
	tSrc := reader.TracesSource(dbPath)
	eSrc := reader.ExecutionsSource(dbPath)
	filter := runIDFilter(runID)

	query := fmt.Sprintf(`
		WITH paired AS (
			SELECT
				e.run_id,
				e.node_id,
				e.function_name,
				e.step AS enter_step,
				x.step AS exit_step
			FROM %[1]s e
			JOIN %[1]s x
			  ON e.run_id = x.run_id
			  AND e.trace_id = x.trace_id
			  AND e.trace_kind = 'Enter'
			  AND x.trace_kind = 'Exit'
			WHERE 1=1 %[3]s
		),
		crashes AS (
			SELECT run_id, seq_num
			FROM %[2]s
			WHERE kind = 'Invocation' AND action LIKE '%%System.Crash'
			%[3]s
		),
		func_crash_runs AS (
			SELECT DISTINCT
				p.function_name,
				p.run_id
			FROM paired p
			JOIN crashes c
			  ON p.run_id = c.run_id
			  AND c.seq_num >= p.enter_step
			  AND c.seq_num <= p.exit_step
		),
		total_runs AS (
			SELECT COUNT(DISTINCT run_id) AS cnt FROM %[1]s WHERE 1=1 %[3]s
		)
		SELECT
			fcr.function_name,
			COUNT(DISTINCT fcr.run_id) AS runs_with_crash,
			tr.cnt AS total_runs,
			CASE WHEN tr.cnt > 0
				THEN CAST(COUNT(DISTINCT fcr.run_id) AS DOUBLE) / tr.cnt
				ELSE 0
			END AS coverage_fraction
		FROM func_crash_runs fcr
		CROSS JOIN total_runs tr
		GROUP BY fcr.function_name, tr.cnt
		ORDER BY coverage_fraction DESC
	`, tSrc, eSrc, filter)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query crash coverage: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var s FunctionCrashCoverage
		if err := rows.Scan(&s.FunctionName, &s.RunsWithCrash, &s.TotalRuns, &s.CoverageFraction); err != nil {
			return fmt.Errorf("failed to scan crash coverage row: %w", err)
		}
		result.CrashCoverage = append(result.CrashCoverage, s)
	}
	return rows.Err()
}
