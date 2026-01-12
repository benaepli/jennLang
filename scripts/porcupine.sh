#!/bin/bash
set -e

# Change to the project root directory (in case the script is run from elsewhere)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

# 1. Compile Porcupine
echo "Compiling Porcupine..."
cd porcupine
go build -o main main.go
cd ..

# 2. Run Porcupine on output/results.db
echo "Running Porcupine on output/results.db..."
./porcupine/main -input output/results.db -type duckdb -model kv -output-dir output
