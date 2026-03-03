package reader

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

// isParquetDir returns true if the given path is a Parquet output directory.
// It handles the root output dir (containing "traces" and/or "executions" subdirs),
// or the "traces" / "executions" dir itself.
func isParquetDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return false
	}

	// Check if this is the root dir (has "traces" or "executions" subdirectory)
	for _, sub := range []string{"traces", "executions"} {
		subInfo, err := os.Stat(filepath.Join(path, sub))
		if err == nil && subInfo.IsDir() {
			return true
		}
	}

	// Check if this directory itself contains .parquet files
	files, err := filepath.Glob(filepath.Join(path, "*.parquet"))
	return err == nil && len(files) > 0
}

// openDB opens an in-memory DuckDB when path is a Parquet directory, or opens
// the DuckDB file directly otherwise.
func openDB(path string) (*sql.DB, error) {
	if isParquetDir(path) {
		return sql.Open("duckdb", "")
	}
	return sql.Open("duckdb", path)
}

// OpenDB opens the database for external use (metrics queries).
func OpenDB(path string) (*sql.DB, error) {
	return openDB(path)
}

// IsParquetDir is the exported version of isParquetDir.
func IsParquetDir(path string) bool {
	return isParquetDir(path)
}

// TracesSource returns the SQL table expression for the traces relation.
func TracesSource(path string) string {
	if isParquetDir(path) {
		base := filepath.Base(path)
		if base == "traces" {
			return fmt.Sprintf("read_parquet('%s', union_by_name=true)", filepath.Join(path, "*.parquet"))
		}
		return fmt.Sprintf("read_parquet('%s', union_by_name=true)", filepath.Join(path, "traces", "*.parquet"))
	}
	return "traces"
}

// ExecutionsSource returns the SQL table expression for the executions relation.
func ExecutionsSource(path string) string {
	if isParquetDir(path) {
		base := filepath.Base(path)
		if base == "executions" {
			return fmt.Sprintf("read_parquet('%s', union_by_name=true)", filepath.Join(path, "*.parquet"))
		}
		return fmt.Sprintf("read_parquet('%s', union_by_name=true)", filepath.Join(path, "executions", "*.parquet"))
	}
	return "executions"
}

// ListRunIDs returns all available run IDs from traces (works for both DuckDB and Parquet).
func ListRunIDs(dbPath string) ([]int64, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := TracesSource(dbPath)
	query := fmt.Sprintf(`SELECT DISTINCT run_id FROM %s ORDER BY run_id ASC`, src)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query run IDs: %w", err)
	}
	defer rows.Close()

	var runIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan run_id: %w", err)
		}
		runIDs = append(runIDs, id)
	}
	return runIDs, rows.Err()
}

// ReadTraces reads all trace rows for a given run_id (or all runs if runID < 0).
func ReadTraces(dbPath string, runID int64) ([]TraceRow, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := TracesSource(dbPath)
	var query string
	var rows *sql.Rows

	if runID >= 0 {
		query = fmt.Sprintf(`
			SELECT run_id, seq_num, node_id, step, function_name, trace_kind,
			       payload, schedulable_count, trace_id, causal_operation_id
			FROM %s
			WHERE run_id = ?
			ORDER BY run_id, seq_num ASC
		`, src)
		rows, err = db.Query(query, runID)
	} else {
		query = fmt.Sprintf(`
			SELECT run_id, seq_num, node_id, step, function_name, trace_kind,
			       payload, schedulable_count, trace_id, causal_operation_id
			FROM %s
			ORDER BY run_id, seq_num ASC
		`, src)
		rows, err = db.Query(query)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query traces: %w", err)
	}
	defer rows.Close()

	var result []TraceRow
	for rows.Next() {
		var t TraceRow
		if err := rows.Scan(
			&t.RunID, &t.SeqNum, &t.NodeID, &t.Step, &t.FunctionName,
			&t.TraceKind, &t.Payload, &t.SchedulableCount, &t.TraceID,
			&t.CausalOperationID,
		); err != nil {
			return nil, fmt.Errorf("failed to scan trace row: %w", err)
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// ReadExecutions reads all execution rows for a given run_id (or all runs if runID < 0).
func ReadExecutions(dbPath string, runID int64) ([]ExecutionRow, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := ExecutionsSource(dbPath)
	var query string
	var rows *sql.Rows

	if runID >= 0 {
		query = fmt.Sprintf(`
			SELECT run_id, seq_num, unique_id, client_id, kind, action, payload
			FROM %s
			WHERE run_id = ?
			ORDER BY run_id, seq_num ASC
		`, src)
		rows, err = db.Query(query, runID)
	} else {
		query = fmt.Sprintf(`
			SELECT run_id, seq_num, unique_id, client_id, kind, action, payload
			FROM %s
			ORDER BY run_id, seq_num ASC
		`, src)
		rows, err = db.Query(query)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query executions: %w", err)
	}
	defer rows.Close()

	var result []ExecutionRow
	for rows.Next() {
		var e ExecutionRow
		if err := rows.Scan(
			&e.RunID, &e.SeqNum, &e.UniqueID, &e.ClientID,
			&e.Kind, &e.Action, &e.Payload,
		); err != nil {
			return nil, fmt.Errorf("failed to scan execution row: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}
