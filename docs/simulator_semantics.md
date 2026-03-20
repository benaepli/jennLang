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
