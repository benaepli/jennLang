package metrics

import (
	"database/sql"
	"fmt"
	"math"

	"github.com/benaepli/jennlang-traceanalyzer/reader"
)

// FunctionDurationStats holds per-function duration statistics.
type FunctionDurationStats struct {
	FunctionName  string  `json:"function_name"`
	Count         int     `json:"count"`
	UnpairedCount int     `json:"unpaired_count"`
	UnpairedRatio float64 `json:"unpaired_ratio"`
	Min           int     `json:"min"`
	Max           int     `json:"max"`
	Mean          float64 `json:"mean"`
	Stddev        float64 `json:"stddev"`
	P50           int     `json:"p50"`
	P95           int     `json:"p95"`
	P99           int     `json:"p99"`
	CV            float64 `json:"cv"` // coefficient of variation
	HighVariance  bool    `json:"high_variance"`
}

// DurationResult holds the complete duration analysis.
type DurationResult struct {
	Functions []FunctionDurationStats `json:"functions"`
}

// ComputeDuration computes function duration distributions using DuckDB SQL.
func ComputeDuration(dbPath string, runID int64) (*DurationResult, error) {
	db, err := reader.OpenDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := reader.TracesSource(dbPath)

	// Build the paired invocations CTE via self-join on (run_id, trace_id)
	whereClause := ""
	if runID >= 0 {
		whereClause = fmt.Sprintf("WHERE e.run_id = %d", runID)
	}

	query := fmt.Sprintf(`
		WITH paired AS (
			SELECT
				e.function_name,
				x.step - e.step AS duration
			FROM %[1]s e
			JOIN %[1]s x
			  ON e.run_id = x.run_id
			  AND e.trace_id = x.trace_id
			  AND e.trace_kind = 'Enter'
			  AND x.trace_kind = 'Exit'
			%[2]s
		),
		enter_counts AS (
			SELECT function_name, COUNT(*) AS total_enters
			FROM %[1]s
			WHERE trace_kind = 'Enter'
			%[3]s
			GROUP BY function_name
		),
		exit_counts AS (
			SELECT function_name, COUNT(*) AS total_exits
			FROM %[1]s
			WHERE trace_kind = 'Exit'
			%[3]s
			GROUP BY function_name
		)
		SELECT
			p.function_name,
			COUNT(*) AS cnt,
			COALESCE(ec.total_enters, 0) - COUNT(*) AS unpaired_count,
			MIN(p.duration) AS min_dur,
			MAX(p.duration) AS max_dur,
			AVG(p.duration) AS mean_dur,
			STDDEV_POP(p.duration) AS stddev_dur,
			PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY p.duration) AS p50,
			PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY p.duration) AS p95,
			PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY p.duration) AS p99,
			COALESCE(ec.total_enters, 0) AS total_enters
		FROM paired p
		LEFT JOIN enter_counts ec ON p.function_name = ec.function_name
		GROUP BY p.function_name, ec.total_enters
		ORDER BY p.function_name
	`, src, whereClause, runIDFilter(runID))

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query duration stats: %w", err)
	}
	defer rows.Close()

	var result DurationResult
	for rows.Next() {
		var s FunctionDurationStats
		var meanDur, stddevDur sql.NullFloat64
		var totalEnters int

		if err := rows.Scan(
			&s.FunctionName, &s.Count, &s.UnpairedCount,
			&s.Min, &s.Max, &meanDur, &stddevDur,
			&s.P50, &s.P95, &s.P99, &totalEnters,
		); err != nil {
			return nil, fmt.Errorf("failed to scan duration row: %w", err)
		}

		if meanDur.Valid {
			s.Mean = meanDur.Float64
		}
		if stddevDur.Valid {
			s.Stddev = stddevDur.Float64
		}
		if totalEnters > 0 {
			s.UnpairedRatio = float64(s.UnpairedCount) / float64(totalEnters)
		}
		if s.Mean > 0 {
			s.CV = s.Stddev / s.Mean
		}
		s.HighVariance = s.CV > 1.0

		result.Functions = append(result.Functions, s)
	}

	return &result, rows.Err()
}

// runIDFilter returns a SQL AND clause for filtering by run_id, or empty string.
func runIDFilter(runID int64) string {
	if runID >= 0 {
		return fmt.Sprintf("AND run_id = %d", runID)
	}
	return ""
}

// Round helper for display.
func roundFloat(val float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}
