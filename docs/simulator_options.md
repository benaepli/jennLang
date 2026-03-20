# Simulator Options

The Spur simulator is highly configurable via CLI flags and JSON configuration files. This document details the parameters available for tweaking simulation runs.

## CLI Subcommands & Flags

The primary entry point operates via subcommands on the main `spur` cargo project:

```bash
cargo run --release --manifest-path spur/Cargo.toml -- [SUBCOMMAND] [OPTIONS...]
```

### `explore`

Runs the main execution explorer over a configuration space, compiling the spec internally.

- `-c, --config [FILE]`: The JSON configuration file defining exploration parameters, including the scheduler policy, diversity rates, and bounded executions (e.g. number of nodes, crashes, etc.).
- `-o, --output-dir [DIR]`: Directory to emit traces and graph visualizations.
- `-e, --explorer [TYPE]`: The exploration strategy. Options:
  - `standard` (Default): Exhaustive or randomly sampled bounded execution.
  - `genetic`: Genetic algorithm-based exploration for finding edge cases.
- `--log-backend [BACKEND]`: Determines the format for execution history persistence.
  - `parquet` (Default): High-performance structured logging utilizing Apache Parquet.
  - `duckdb`: SQLite-like backend using DuckDB.

### `run-plan`

Executes a fixed, deterministic DAG schedule of events instead of exploring random schedules.

- `-p, --plan [FILE]`: The plan configuration JSON file.
- `-o, --output-dir [DIR]`: Output directory for results.
- `--log-backend [BACKEND]`: Same log backend options as `explore`.

## Logging & Output Formats

By utilizing the `HistoryWriter` trait, Spur can decouple execution logic from persistence.

### Structured Logging

Depending on the chosen backend, the simulator emits files encompassing several distinct data schemas generated per run:

1. `executions`: Logs client operations (`Invocation`, `Response`, `Crash`, `Recover`). Used heavily for linearizability checking.
2. `logs`: Captures standard print statements and application-level debug output.
3. `traces`: Structured trace events from the `@trace` annotations (see the tracing documentation).

## Porcupine Integration

Porcupine is the linearizability checker that integrates natively with the `executions` output of the Spur simulator.

By running `porcupine/main` on the resulting SQLite/Parquet files, developers can ascertain if a generated schedule violated the guarantees of the protocol (e.g. key-value constraints). Porcupine also yields a useful HTML visualization that diagrams the execution interleavings of node invocations, facilitating debugging when a simulation trace violates linearizability.
