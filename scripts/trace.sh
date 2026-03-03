#!/bin/bash
set -e

# Change to the project root directory (in case the script is run from elsewhere)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

# 1. Compile traceanalyzer
echo "Compiling traceanalyzer..."
cd traceanalyzer
go build -o main main.go
cd ..

# 2. Run traceanalyzer on output/ 
echo "Running traceanalyzer on output/..."
./traceanalyzer/main -input output "$@"
