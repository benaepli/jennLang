package metrics

import (
	"database/sql"
	"fmt"

	"github.com/benaepli/jennlang-traceanalyzer/reader"
)

// FingerprintResult holds the complete exploration diversity analysis.
type FingerprintResult struct {
	TotalRuns          int     `json:"total_runs"`
	UniqueFingerprints int     `json:"unique_fingerprints"`
	DiversityRatio     float64 `json:"diversity_ratio"`
	UniqueNodeProfiles int     `json:"unique_node_profiles"`
	CausalChains       []CausalChainDiversity `json:"causal_chains,omitempty"`
}

// CausalChainDiversity shows how many distinct causal operation ID sequences
// appear per function across runs.
type CausalChainDiversity struct {
	FunctionName       string `json:"function_name"`
	DistinctChains     int    `json:"distinct_chains"`
	TotalInvocations   int    `json:"total_invocations"`
}

// ComputeFingerprint computes exploration diversity metrics using DuckDB SQL.
func ComputeFingerprint(dbPath string, runID int64) (*FingerprintResult, error) {
	db, err := reader.OpenDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	result := &FingerprintResult{}

	if err := computeTraceFingerprint(db, dbPath, runID, result); err != nil {
		return nil, err
	}
	if err := computeNodeProfiles(db, dbPath, runID, result); err != nil {
		return nil, err
	}
	if err := computeCausalChains(db, dbPath, runID, result); err != nil {
		return nil, err
	}

	return result, nil
}

func computeTraceFingerprint(db *sql.DB, dbPath string, runID int64, result *FingerprintResult) error {
	src := reader.TracesSource(dbPath)
	filter := runIDFilter(runID)

	// Per run, hash the ordered (function_name, trace_kind) sequence.
	// Count distinct fingerprints across all runs.
	query := fmt.Sprintf(`
		WITH per_run_seq AS (
			SELECT
				run_id,
				MD5(STRING_AGG(function_name || ':' || trace_kind, ',' ORDER BY seq_num)) AS fingerprint
			FROM %s
			WHERE 1=1 %s
			GROUP BY run_id
		)
		SELECT
			COUNT(*) AS total_runs,
			COUNT(DISTINCT fingerprint) AS unique_fingerprints
		FROM per_run_seq
	`, src, filter)

	err := db.QueryRow(query).Scan(&result.TotalRuns, &result.UniqueFingerprints)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("failed to query trace fingerprints: %w", err)
	}

	if result.TotalRuns > 0 {
		result.DiversityRatio = float64(result.UniqueFingerprints) / float64(result.TotalRuns)
	}
	return nil
}

func computeNodeProfiles(db *sql.DB, dbPath string, runID int64, result *FingerprintResult) error {
	src := reader.TracesSource(dbPath)
	filter := runIDFilter(runID)

	// Per (run, node), build function_name -> call_count vector, then count distinct profiles.
	query := fmt.Sprintf(`
		WITH per_node_profile AS (
			SELECT
				run_id,
				node_id,
				MD5(STRING_AGG(function_name || ':' || CAST(cnt AS VARCHAR), ',' ORDER BY function_name)) AS profile
			FROM (
				SELECT run_id, node_id, function_name, COUNT(*) AS cnt
				FROM %s
				WHERE trace_kind = 'Enter' %s
				GROUP BY run_id, node_id, function_name
			) sub
			GROUP BY run_id, node_id
		)
		SELECT COUNT(DISTINCT profile) FROM per_node_profile
	`, src, filter)

	err := db.QueryRow(query).Scan(&result.UniqueNodeProfiles)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to query node profiles: %w", err)
	}
	return nil
}

func computeCausalChains(db *sql.DB, dbPath string, runID int64, result *FingerprintResult) error {
	src := reader.TracesSource(dbPath)
	filter := runIDFilter(runID)

	// Count distinct causal_operation_id values per function across all runs.
	query := fmt.Sprintf(`
		SELECT
			function_name,
			COUNT(DISTINCT causal_operation_id) AS distinct_chains,
			COUNT(*) AS total_invocations
		FROM %s
		WHERE trace_kind = 'Enter'
		  AND causal_operation_id IS NOT NULL
		  %s
		GROUP BY function_name
		ORDER BY function_name
	`, src, filter)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query causal chains: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var c CausalChainDiversity
		if err := rows.Scan(&c.FunctionName, &c.DistinctChains, &c.TotalInvocations); err != nil {
			return fmt.Errorf("failed to scan causal chain row: %w", err)
		}
		result.CausalChains = append(result.CausalChains, c)
	}
	return rows.Err()
}
