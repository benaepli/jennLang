# Simulator Semantics

This document details the expected execution behavior of the Spur simulator, specifically around node failures and process initialization.

## Node Initialization & Recovery

All nodes in the simulator have a lifecycle that handles startup and potential crash-recovery cycles.

### Normal Initialization

Each node is required to have an `Init` function.

- **Synchronous Execution:** The `Init` function is executed synchronously at node startup.
- **Required:** Every node explicitly requires an `Init` block to specify its starting state.

### Recovery Initialization

Nodes can experience simulated crashes. When the simulator revives a node from a crashed state, it must run a recovery routine. In Spur, this is achieved by specifying an optional function, typically named `RecoverInit`.

- **Optional Implementation:** Unlike `Init`, `RecoverInit` is entirely optional.
- **Behavior After Recovery:** On recovery, the user's node starts receiving messages from other nodes in the network _immediately_ after the **first yield point** (the first blocking call or channel receive) of `RecoverInit`. Prior to that yield point, incoming messages will not be processed, ensuring the node can safely reinitialize critical state.

## Message Delivery During Crashes

During a crash, a node is offline and unable to process its message queue. This raises a question regarding the delivery semantics of messages dispatched to the crashed node by other active participants.

Spur handles this transparently:

- Messages sent _to_ a crashed node (from an alive origin) are **not dropped**.
- Instead, the simulator identifies the recipient is offline and buffers these records.
- Upon recovery, all buffered incoming messages are automatically re-injected into the node's runnable tasks queue.

This mechanism simulates a network where packets sent during a temporary outage are eventually delivered upon the target's return, preventing silent message loss.

## Timeouts

Spur models timeouts with the `set_timer()` built-in. It takes no arguments and returns a `chan<()>`.

```
var timeout_ch: chan<()> = set_timer();
<- timeout_ch;
// timeout has fired
```

The key design decision is that `set_timer` has **no duration parameter**. The simulator decides when the timer fires, allowing it to explore different timeout orderings across executions. This is essential for finding bugs that depend on the relative timing of timeouts, message deliveries, and other events.

The typical pattern is to receive from the timer channel in a loop, checking conditions after each timeout:

```
async fn monitor_timeouts() {
    for ;; {
        <- set_timer();
        // check conditions and act
    }
}
```

### Timers and Crashes

- When a node crashes, all of its pending timers are **dropped**.
- If a timer fires while its node is crashed, it is silently discarded.

## Persistence

Spur provides three built-in functions for data that must survive crash/recovery cycles:

- **`persist_data(value)`** — Stores a value in durable storage for the current node. Each node has a single persistence slot; calling `persist_data` overwrites any previously stored value.
- **`retrieve_data<T>()`** — Returns `T?`. Yields the stored value if one exists, or `nil` otherwise. A runtime error occurs if the stored value's type does not match `T`.
- **`discard_data()`** — Removes the persisted value for the current node.

Persisted data lives outside the node's regular state. When a node crashes, all of its in-memory state (variables, channels, continuations) is wiped, but persisted data remains intact. This allows `RecoverInit` to restore critical state:

```
fn RecoverInit() {
    var saved = retrieve_data<MyState>();
    if saved != nil {
        // restore fields from saved!
    }
}
```
