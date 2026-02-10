#!/usr/bin/bash

# Check if a spec file was provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <spec_file>"
    echo "Example: $0 bin/spur/simple.spur"
    exit 1
fi

SPEC_FILE="$1"

# Check if the spec file exists
if [ ! -f "$SPEC_FILE" ]; then
    echo "Error: Spec file '$SPEC_FILE' not found"
    exit 1
fi

# Run spur compiler from the spur subdirectory
echo "Compiling $SPEC_FILE using spur..."
cd spur && cargo run --release -- compile "../$SPEC_FILE" --output ../output.json

# Check if compilation was successful
if [ $? -eq 0 ]; then
    echo "Successfully compiled to output.json"
else
    echo "Compilation failed"
    exit 1
fi
