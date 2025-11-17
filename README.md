# Turnpike

A distributed system execution explorer and linearizability checker built with OCaml, Rust, and Go.

## Architecture

The repository is organized as follows:

```
bin/                    # Main execution explorer (main.ml, benchmark.ml) and execution modules
lib/                    # OCaml libraries (lexer, parser, AST, simulator)
spur/                   # Spur specification compiler (Rust submodule)
porcupine/              # Linearizability checker (Go submodule)
scheduler_configs/      # JSON configuration files for execution exploration
output/                 # Generated execution traces (CSV files)
README.md
jennLang.opam
```

## Workflow Overview

1. **Compile Protocol Specification**: Use Spur (Rust) to compile a `.spur` specification to JSON
2. **Generate Execution Traces**: Run the OCaml execution explorer to generate CSV traces
3. **Check Linearizability**: Use Porcupine (Go) to verify linearizability and generate visualizations

## Installing Dependencies

The following instructions were tested on Ubuntu 20.04 and Fedora.

### Install OCaml and Dune
```bash
bash -c "sh <(curl -fsSL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh)" # install OCaml
opam init
opam install ocaml-lsp-server odoc ocamlformat utop dune yojson # install Dune and dependencies
```

### Install Rust (for Spur)
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Install Go (for Porcupine)
```bash
# Ubuntu/Debian
sudo apt install golang-go

# Fedora
sudo dnf install golang

# Or download from https://go.dev/dl/
```

### Clone Repository with Submodules
```bash
git clone --recurse-submodules https://github.com/jl3953/jennLang
cd jennLang
```

If you already cloned without submodules:
```bash
git submodule update --init --recursive
```

## Quick Start

### Step 1: Compile a Spur Specification

Spur is a domain-specific language for specifying distributed protocols. Use the `compile.sh` script to compile a specification to JSON:

```bash
./compile.sh spur/specs/simple.spur
```

This generates `output.json`, which contains the compiled control flow graph.

**Alternatively**, compile manually from the spur directory:
```bash
cd spur
cargo run --release -- specs/simple.spur ../output.json
cd ..
```

**Available specifications:**
- `spur/specs/simple.spur` - Simple key-value store
- `spur/specs/locks.spur` - Distributed locks
- `spur/specs/concurrency.spur` - Concurrency examples
- `spur/specs/sync.spur` - Synchronization primitives

### Step 2: Generate Execution Traces

The execution explorer generates multiple execution traces by exploring different parameter configurations.

**Basic usage:**
```bash
eval $(opam env --switch=default)  # Run once per session
dune build
dune exec main output.json scheduler_configs/vr_comprehensive.json output/
```

**Arguments:**
- `output.json` - Compiled program from Spur
- `scheduler_configs/vr_comprehensive.json` - Configuration file defining exploration parameters
- `output/` - Directory where CSV trace files will be generated

**Configuration parameters** (in scheduler config JSON):
- `num_servers` - Range of server counts to explore
- `num_clients` - Range of client counts
- `num_write_ops` / `num_read_ops` - Number of operations
- `num_timeouts` - Timeout scenarios
- `num_crashes` - Server crash scenarios
- `dependency_density` - Operation dependency density values
- `randomly_delay_msgs` - Whether to randomly delay messages
- `num_runs_per_config` - Number of runs per configuration
- `max_iterations` - Maximum simulation steps per run

**Example configuration:** See `scheduler_configs/vr_comprehensive.json`

### Step 3: Check Linearizability with Porcupine

Porcupine checks if the generated execution traces are linearizable.

**Build Porcupine:**
```bash
cd porcupine
go build -o main main.go
cd ..
```

**Check a single trace:**
```bash
./porcupine/main -input output/0001_s3_c1_w5_r3_t0_crash0_d0.00_run1.csv \
                  -output output.html \
                  -model kv
```

**Arguments:**
- `-input` - CSV trace file from execution explorer
- `-output` - HTML visualization file
- `-model` - Consistency model (`kv` for key-value, `queue` for queue)

**Output:**
- Prints whether the trace is linearizable
- Generates an HTML visualization showing the execution history
- Exit code 2 if not linearizable, 0 if linearizable

## Benchmark Mode

Run benchmarks with timing statistics:

```bash
dune exec main benchmark output.json scheduler_configs/scheduler_config.json \
  -n 20 \
  -i 5000 \
  -o ./benchmark_results
```

**Options:**
- `-n, --num-runs N` - Number of benchmark runs (default: 10)
- `-i, --iterations N` - Max iterations per run (default: 1000)
- `-o, --output-dir DIR` - Output directory (default: current directory)

**Output:** Displays timing statistics including mean, min, max, standard deviation, and coefficient of variation.
