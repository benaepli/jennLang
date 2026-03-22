# Spur

Spur is a domain-specific language for specifying and testing distributed protocols. The toolchain compiles `.spur` specifications, explores execution schedules via a simulator, and checks linearizability.

## Toolchain

1. **Spur compiler + simulator** (Rust) — compiles `.spur` specs and explores execution schedules
2. **Porcupine** (Go) — linearizability checker, verifies Read/Write operations from `ClientInterface`
3. **Traceanalyzer** (Go) — computes trace metrics (duration, dispatch latency, interleaving, faults)

## Key Commands

All commands run from the project root.

### Explore (run simulator)

```bash
cargo run --release --manifest-path spur/Cargo.toml --bin spur -- explore -e standard --config CONFIG.json -y --output-dir output SPEC.spur
```

- `-y` auto-confirms output directory deletion
- `-e standard` for exhaustive/random exploration, `-e genetic` for genetic algorithm

### Debug logs

```bash
cargo run --release --manifest-path spur/Cargo.toml --bin spur -- debug logs --db output --run-id N
cargo run --release --manifest-path spur/Cargo.toml --bin spur -- debug traces --db output --run-id N
cargo run --release --manifest-path spur/Cargo.toml --bin spur -- debug combined --db output --run-id N
```

### Trace analysis

```bash
cd traceanalyzer && go build -o main main.go && cd ..
./traceanalyzer/main -input output
```

### Porcupine (linearizability checker)

```bash
cd porcupine && go build -o main main.go && cd ..
./porcupine/main -input output -type duckdb -model kv -output-dir output
```

- **Exit code 0** = all runs linearizable
- **Exit code 2** = linearizability violations found

## Project Layout

- `bin/spur/` — specification files (`.spur`)
- `scheduler_configs/` — explorer configuration JSONs
- `spur/` — Rust workspace (compiler, simulator, CLI, LSP)
- `spur/design/language.md` — full language grammar and reference
- `docs/` — simulator semantics, options, tracing documentation
- `porcupine/` — Go linearizability checker
- `traceanalyzer/` — Go trace analysis tool
- `scripts/` — helper scripts (`porcupine.sh`, `trace.sh`)

## Important Notes

- Go tools (`porcupine/main`, `traceanalyzer/main`) need `go build` before first run
- The simulator uses DuckDB as its default log backend
- Porcupine checks linearizability by analyzing `ClientInterface` `Read`/`Write` call-response pairs
- Every spec must have a `ClientInterface` with `Read` and `Write` functions for linearizability verification to work
