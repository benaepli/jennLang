---
name: debug-protocol
description: Run the debug loop for a Spur protocol specification — explore, analyze traces, check linearizability, diagnose, and fix.
user-invocable: true
---

# Debug Protocol Specification

You are debugging a distributed protocol specification written in the Spur language. Your job is to iteratively run the simulator, check for linearizability violations and deadlocks, diagnose issues, and propose fixes.

## Arguments

- `$1`: Path to the `.spur` spec file (required)
- `$2`: Path to the scheduler config JSON (required)
- `$3`: Optional path to a pseudocode/reference file from the paper. If provided, read it first and use it as ground truth when diagnosing bugs.

## Pre-flight

1. **Verify files exist**: Check that the spec file (`$1`) and config file (`$2`) exist. If not, stop and report.

2. **Check ClientInterface contract**: Read the spec file and verify it has a `ClientInterface` block containing `Read` and `Write` functions. These are required for linearizability checking. If missing, stop and tell the user.

3. **Read pseudocode reference** (if `$3` provided): Read the pseudocode file. Keep it as context for diagnosing protocol logic bugs. Cross-reference the spec against it when looking for errors.

4. **Build Go tools** (once):
```bash
cd traceanalyzer && go build -o main main.go && cd .. && cd porcupine && go build -o main main.go && cd ..
```

## Debug Loop

Repeat up to **5 iterations**. Track the iteration count.

### Step 1: Run Explorer

```bash
timeout 300 cargo run --release --manifest-path spur/Cargo.toml --bin spur -- explore -e standard --config $2 -y --output-dir output $1 2>&1
```

Handle exit codes:
- **Exit 0**: Explorer completed successfully. Proceed to Step 2.
- **Exit 124**: Explorer timed out (5 min limit). Report this to the user and suggest reducing config parameters (fewer `max_iterations`, `num_runs_per_config`, or narrower ranges). Stop the loop.
- **Other non-zero**: Likely a compilation error. Read the error output, fix the spec file automatically, and re-run this step. This does NOT count as an iteration.

### Step 2: Run Trace Analysis

```bash
./traceanalyzer/main -input output
```

Note any anomalies: runs with 0 completed operations, unusually short durations, or other red flags.

### Step 3: Run Porcupine

```bash
./porcupine/main -input output -type duckdb -model kv -output-dir output 2>&1 | tee /tmp/porcupine_output.txt
```

Capture the exit code. The output is also saved to `/tmp/porcupine_output.txt` for parsing.

### Step 4: Diagnose

**If exit code is 2 (linearizability violations found):**
- Extract failing run IDs by grepping for non-linearizable runs:
  ```bash
  grep 'Linearizable? false' /tmp/porcupine_output.txt
  ```
  Each matching line has the format `Run N: Linearizable? false` — extract the run number N from each line.
- For the first 2-3 failing runs, query the combined debug view:
  ```bash
  cargo run --release --manifest-path spur/Cargo.toml --bin spur -- debug combined --db output --run-id N
  ```
- Analyze the execution timeline: look for stale reads, lost writes, split-brain scenarios, incorrect commit ordering
- If pseudocode was provided, cross-reference the spec logic against the paper's algorithm

**If exit code is 0 (all runs linearizable):**
- Do NOT assume everything is fine. Spot-check 2-3 runs with `debug combined` to look for deadlocks:
  ```bash
  cargo run --release --manifest-path spur/Cargo.toml --bin spur -- debug combined --db output --run-id N
  ```
- Look for: runs where client operations never completed, nodes stuck waiting, no progress after a certain point
- Check the traceanalyzer output for runs with very few completed operations relative to what the config specifies
- If everything looks good, declare success and stop the loop

### Step 5: Fix or Report

**Compile errors**: Fix automatically and re-run (not counted as iteration).

**Protocol logic bugs**:
1. Present your diagnosis clearly:
   - Which runs failed and why
   - What the root cause appears to be
   - What the correct behavior should be (reference pseudocode if available)
2. Propose a specific fix (show the code change)
3. **Wait for user approval before applying the edit**
4. After the user approves, apply the fix

**Same bug persists after 2 fix attempts**: Stop the loop. Explain what you've tried, what the bug appears to be, and why your fixes haven't resolved it. Ask the user for guidance.

### Step 6: Iterate

After applying a fix, go back to Step 1. Increment the iteration counter.

## Iteration Summary

After each iteration, output a summary:

```
## Iteration N
- Explorer: X runs completed in Ys
- Porcupine: X/Y runs linearizable
- Failing runs: [run IDs]
- Diagnosis: [brief description of the bug]
- Proposed fix: [description of the change]
- Status: [awaiting approval / applied / success]
```

## Stopping Conditions

Stop the loop when any of these are true:
- **Success**: All porcupine runs pass AND spot-checks show no deadlocks
- **Max iterations**: 5 iterations reached — summarize remaining issues
- **Stuck**: Same error pattern after 2 fix attempts — explain and ask for guidance
- **Timeout**: Explorer timed out — suggest config adjustments

## Important Reminders

- The Spur language reference is in `spur/design/language.md`
- Simulator semantics are in `docs/simulator_semantics.md`
- Always use `-y` flag with explore to auto-confirm output dir deletion
- Always use `timeout 300` to cap explorer runtime at 5 minutes
- `ClientInterface` must have `Read` and `Write` — these are what Porcupine checks
- Read/Write in ClientInterface must not return until the operation truly completes (retry loops on redirect are normal)
