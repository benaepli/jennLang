# Debugging Spur Specifications

## Linearizability Violations (Porcupine exit code 2)

Common root causes:

- **Stale reads**: Node returns a value from its local state without ensuring it has the latest committed data. Often happens when a non-primary handles reads without forwarding or checking commit status.
- **Lost writes**: A write is acknowledged to the client but never gets replicated to a quorum, so it's lost on leader failure. Check that Write in ClientInterface only returns after commit.
- **Split-brain**: Two nodes both believe they are primary after a view change. Usually caused by incorrect view number comparison or quorum counting.
- **Incorrect commit ordering**: Operations are applied to the state machine in different orders on different replicas. Check log indexing and commit advancement logic.
- **Premature ClientInterface return**: `Read` or `Write` in ClientInterface returns before the operation is truly committed (e.g., returning on a redirect response instead of retrying).

## Deadlock Patterns

These don't cause porcupine failures but result in runs that never complete:

- **Missing timeout handler**: Node waits for a response that never comes (crashed peer) but has no timeout to trigger recovery/view change.
- **recv in sync function**: Channel operations are only allowed in async functions. If this compiles, it will block the node.
- **Recovery that never completes**: `RecoverInit` enters an infinite wait. Remember that the node starts receiving messages after the first yield point in `RecoverInit`.
- **Circular RPC waits**: Two nodes each waiting for the other's response with no timeout.

## Common Spur Bugs

- **Not persisting state before yield points**: After a yield point (channel recv, RPC await), the node could crash. Any state not saved via `persist_data()` will be lost. Critical state like view number, log, and commit number should be persisted before yield points.
- **Quorum errors**: `f()` returns `(n-1)/2`. A quorum typically requires `f()+1` responses. Off-by-one errors here are very common.
- **View change state not reset**: After transitioning from view-change back to normal operation, leftover view-change state (vote counts, collected logs) can interfere with the next view change.
- **Missing match arms**: When matching on message types, forgetting to handle a variant can silently drop important messages.
- **Incorrect log replay during recovery**: When rebuilding state from a recovered log, ensure operations are applied in order and the commit number is correctly restored.
- **Buffered messages after view change**: Messages received during a view change may need to be re-processed after the new view is established. Dropping them can cause lost operations.
- **ClientInterface Read returning uncommitted data**: Reading from log entries that haven't been committed to a quorum. The read should only return values from committed entries.

## Reading Debug Output

### `debug combined` output

Shows a unified timeline of all events for a run. Key things to look for:

- **Invocation → Response pairs**: Each client operation should have a matching response. Missing responses indicate the operation never completed (possible deadlock).
- **Gaps in a node's timeline**: A gap where a node has no events usually indicates it was crashed during that period.
- **Message ordering**: Check if messages are being processed in the expected order, especially after recovery.
- **Timeout events**: Look for whether timeouts are firing and triggering the expected recovery logic.

### Porcupine results

- Exit code 2 = at least one run violates linearizability
- The output identifies which runs failed
- HTML visualization files (`run_N.html`) in the output directory show the operation timeline with crossing lines indicating violations

## Debugging Strategy

1. Start with the porcupine output to identify failing runs
2. Use `debug combined` on failing runs to understand the execution timeline
3. Look for the specific operation that violated linearizability
4. Trace backwards from that operation to find where the protocol diverged from correct behavior
5. Cross-reference with the paper's pseudocode if available
6. For deadlocks: look at runs where operations never completed, check for missing timeouts
