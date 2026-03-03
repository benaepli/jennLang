package metrics

import (
	"database/sql"
	"fmt"

	"github.com/benaepli/jennlang-traceanalyzer/reader"
)

// DispatchFunctionStats holds dispatch latency statistics for a single function.
type DispatchFunctionStats struct {
	FunctionName string  `json:"function_name"`
	Count        int     `json:"count"`
	MinLatency   int     `json:"min_latency"`
	MaxLatency   int     `json:"max_latency"`
	MeanLatency  float64 `json:"mean_latency"`
	P50Latency   int     `json:"p50_latency"`
	P95Latency   int     `json:"p95_latency"`
	P99Latency   int     `json:"p99_latency"`
}

// DispatchResult holds the dispatch latency results for all traced functions.
type DispatchResult struct {
	Functions []DispatchFunctionStats `json:"functions"`
}

// ComputeDispatch computes dispatch queueing latency distributions using DuckDB SQL.
func ComputeDispatch(dbPath string, runID int64) (*DispatchResult, error) {
	db, err := reader.OpenDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := reader.TracesSource(dbPath)

	whereClause := ""
	if runID >= 0 {
		whereClause = fmt.Sprintf("WHERE d.run_id = %d", runID)
	}

	query := fmt.Sprintf(`
		WITH latest_enter AS (
			SELECT run_id, trace_id, step
			FROM %[1]s
			WHERE trace_kind = 'Enter'
			QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id, trace_id ORDER BY step DESC) = 1
		),
		paired AS (
			SELECT
				d.function_name,
				e.step - d.step AS latency
			FROM %[1]s d
			JOIN latest_enter e
			  ON d.run_id = e.run_id
			  AND d.trace_id = e.trace_id
			  AND d.trace_kind = 'Dispatch'
			%[2]s
		)
		SELECT
			function_name,
			COUNT(*) AS cnt,
			MIN(latency) AS min_lat,
			MAX(latency) AS max_lat,
			AVG(latency) AS mean_lat,
			PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY latency) AS p50,
			PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY latency) AS p95,
			PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY latency) AS p99
		FROM paired
		GROUP BY function_name
		ORDER BY function_name
	`, src, whereClause)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query dispatch stats: %w", err)
	}
	defer rows.Close()

	var result DispatchResult
	for rows.Next() {
		var s DispatchFunctionStats
		var meanLat sql.NullFloat64

		if err := rows.Scan(
			&s.FunctionName, &s.Count,
			&s.MinLatency, &s.MaxLatency, &meanLat,
			&s.P50Latency, &s.P95Latency, &s.P99Latency,
		); err != nil {
			return nil, fmt.Errorf("failed to scan dispatch row: %w", err)
		}

		if meanLat.Valid {
			s.MeanLatency = meanLat.Float64
		}

		result.Functions = append(result.Functions, s)
	}

	return &result, rows.Err()
}
