---
name: implement-protocol
description: Generate a Spur protocol specification from pseudocode, with support for incremental implementation and optional paper PDF reference.
user-invocable: true
---

# Implement Protocol Specification

You are implementing a distributed protocol as a Spur specification from pseudocode. You will generate a `.spur` file, then test it with the debug workflow.

## Arguments

Parse the arguments from the input:

- `$1`: Path to pseudocode file (required) — text or markdown with the algorithm
- `$2`: Scope description (optional) — natural language describing what to implement. Examples:
  - `"full"` or omitted: implement everything
  - `"core replication only, no view changes or recovery"`: subset
  - `"add crash recovery"`: incremental addition to existing spec
- `--pdf PATH`: Optional paper PDF for additional context
- `--spec PATH`: Optional existing `.spur` file to modify instead of creating from scratch
- `--config PATH`: Scheduler config for testing (optional, will ask user if not provided)

### Example invocations

```
/implement-protocol pseudocode/vr.md
/implement-protocol pseudocode/vr.md "core replication, no view changes"
/implement-protocol pseudocode/vr.md --pdf papers/vr-revisited.pdf
/implement-protocol pseudocode/vr.md "add crash recovery" --spec bin/spur/VR.spur
```

## Phase 1: Understand

1. **Read the pseudocode file**. Identify all procedures, message types, state variables, and invariants.

2. **If PDF provided**: Read the paper (focus on system model, algorithm sections, and correctness arguments — skip proofs, evaluation, and related work). Extract:
   - Fault model (how many faults tolerated, crash vs Byzantine)
   - Message delivery assumptions (reliable, FIFO, etc.)
   - Consistency guarantee (linearizability, sequential consistency, etc.)

3. **If existing spec provided** (`--spec`): Read it thoroughly. Understand what's already implemented and what's missing.

4. **Read existing specs for reference**: Look at specs in `bin/spur/` to understand idiomatic Spur patterns and conventions. Use these as style guides.

5. **Parse scope**: Determine what subset of the protocol to implement. Common scopes:
   - **Full**: all features including crash recovery, view changes, etc.
   - **Core only**: basic replication/consensus without fault tolerance
   - **No persistence**: skip `persist_data`/`retrieve_data`/`RecoverInit`
   - **Add feature X**: modify existing spec to add a specific feature

## Phase 2: Design

Present a design to the user for approval before writing any code. Include:

1. **Message types** needed (structs/enums)
2. **Node state variables** and their roles
3. **Function mapping**: which pseudocode procedures map to which Spur functions, and whether each is sync or async
4. **ClientInterface design**: how Read and Write will work (which node they contact, retry logic)
5. **What's in scope vs out of scope** based on the scope argument
6. **Crash recovery strategy** (if in scope): what state to persist, how RecoverInit works

Wait for user approval before proceeding.

## Phase 3: Implement

### If creating a new spec:

Write the spec file to `bin/spur/<ProtocolName>.spur`. Follow this structure:

1. **Type definitions** — message types, log entry types, response types
2. **`role Node`** block:
   - State variables with initial values
   - `fn Init(me: int, peers: list<Node>)` — required, sync, sets up initial state
   - `async fn RecoverInit(me: int, peers: list<Node>)` — if crash recovery is in scope
   - Protocol handlers (async functions for message processing)
   - Timeout monitors if needed (`async fn monitor_timeouts()`)
   - Node-local RPC handlers that the ClientInterface calls (e.g., `async fn Write(key, value)`, `async fn Read(key)`)
3. **`ClientInterface`** block:
   - `async fn Write(dest: Node, key: string, value: string)` — must retry until committed
   - `async fn Read(dest: Node, key: string): string?` — must retry until read completes
   - Both functions must handle redirects (e.g., if contacted node is not the leader)
   - Both must NOT return until the operation truly completes

### If modifying an existing spec (`--spec`):

- Edit the existing file in place
- Preserve existing structure and working code
- Add new functions, state variables, and types as needed
- Mark any TODO items for features not yet implemented

### Implementation guidelines

- Use `@trace` on key protocol functions (message handlers, state transitions)
- Mark out-of-scope features with `// TODO: <feature>` comments
- Use `persist_data()` before yield points if crash recovery is in scope
- Remember: `f()` returns `(n-1)/2` for fault tolerance threshold
- Collections are immutable — use `:=` for updates
- Sync functions are atomic and cannot use channel ops
- Calling an async function spawns a background task — don't await if you want it to run concurrently

## Phase 4: Review

Before moving to testing:

1. Verify every pseudocode procedure has a corresponding Spur function
2. Verify ClientInterface `Read`/`Write` don't return prematurely
3. Verify `Init` sets up all required state and spawns background tasks (timeout monitors, etc.)
4. If crash recovery is in scope: verify `persist_data` is called for critical state before yield points
5. Check quorum calculations use `f()` correctly
6. Check all message type match arms are handled

Present the complete spec to the user for final review.

## Phase 5: Test

1. If no `--config` was provided, ask the user which scheduler config to use. Suggest starting with a small config for initial testing.

2. Chain into the debug workflow by invoking `/debug-protocol` with:
   - The generated/modified spec file
   - The selected scheduler config
   - The pseudocode file as the reference (3rd argument to debug-protocol)

## Important Reminders

- The Spur language reference is in `spur/design/language.md` and `.claude/rules/language.md`
- Simulator semantics are in `docs/simulator_semantics.md`
- Debugging heuristics are in `.claude/rules/debugging.md`
- Existing specs in `bin/spur/` are the best style reference
- ClientInterface MUST have Read and Write — Porcupine checks linearizability through these
- New specs go in `bin/spur/`
