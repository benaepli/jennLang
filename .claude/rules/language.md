# Language: Spur

Full grammar and reference: `spur/design/language.md`

## Program Structure

A Spur program consists of top-level definitions: `role` blocks, a `ClientInterface` block, `type` definitions, and standalone functions.

```
role Node {
    var state: int = 0;         // state variables with initial values

    fn Init(me: int, peers: list<Node>) { ... }          // required, sync
    async fn RecoverInit(me: int, peers: list<Node>) { ... }  // optional, for crash recovery

    @trace
    async fn HandleRequest(...) { ... }   // protocol handlers
}

ClientInterface {
    async fn Write(dest: Node, key: string, value: string) { ... }
    async fn Read(dest: Node, key: string): string? { ... }
}
```

## ClientInterface Contract (Linearizability)

The simulator verifies linearizability by feeding `ClientInterface` `Read`/`Write` call-response pairs to Porcupine. These functions are **required**:

- `async fn Write(dest: Node, key: string, value: string)` — must return `()` only after the write is committed
- `async fn Read(dest: Node, key: string): string?` — must return the value (or nil) only after the read completes

If these are missing, have wrong signatures, or return prematurely, linearizability results are meaningless. Retry loops (e.g., redirect to primary) are common — the function must not return until the operation truly succeeds.

## Type System

- **Primitives**: `int`, `string`, `bool`
- **Tuples/unit**: `()`, `(T, U)`, ...
- **Collections**: `list<T>`, `map<K, V>` (all immutable)
- **Channels**: `chan<T>`
- **Optionals**: `T?` — either a value of type `T` or `nil`
- **Structs**: `type Name { field: Type; ... };`
- **Enums**: `type Name enum { Variant1, Variant2(T), ... };`

## Key Syntax

### RPCs

```
var result_chan: chan<Response> = other_node->some_handler(arg1, arg2);
var result: Response = <- result_chan;   // blocks until response
```

### Channels

- Create: `var ch = make();`
- Send: `value >- ch;` or `send(ch, value);`
- Receive: `var v = <- ch;` or `var v = recv(ch);`
- Channel ops only allowed in `async` functions

### Sync vs Async

- **sync** (default): blocking, atomic, cannot use channel ops
- **async**: returns `chan<T>` immediately, caller must `<-` to get result
- Calling an async function **spawns a new background task** (record). If you don't await the returned channel, the task runs concurrently in the background while the caller continues. This is how you spawn background work like timeout monitors or replication handlers.

### Immutable Updates (`:=`)

```
var updated = record.field := new_value;       // struct field update
var updated = my_map["key"] := new_value;      // map update
var updated = my_list[0] := new_value;         // list update
```

Desugars to `store(x, key, value)`. Original is unchanged.

### Safe Navigation

```
var val = optional_thing?.field;        // nil if optional_thing is nil
var val = optional_map?["key"];         // nil-safe index
var val = thing?.field ?? "default";    // with fallback
```

### Unwrap

`optional!` — unwraps the optional or panics if nil.

### Pattern Matching

```
match msg {
    MessageType.Prepare(data) => { ... },
    MessageType.Commit(data) => { ... },
    _ => { ... },
}
```

### Loops

```
for ;; { ... }                         // infinite loop
for i = 0; i < n; i = i + 1 { ... }   // C-style
for item in my_list { ... }            // for-in
for (key, val) in my_map { ... }       // destructuring
```

## Built-in Functions

- `println(s)` — print a string (shows in debug logs)
- `int_to_string(n)` — convert int to string
- `head(list)`, `tail(list)`, `len(collection)` — list operations
- `append(list, elem)`, `prepend(list, elem)` — return new list
- `store(collection, key, value)` — immutable update (usually via `:=` syntax)
- `exists(map, key)` — check if key exists in map
- `erase(map, key)` — return map without key
- `min(a, b)` — minimum of two values

## Persistence API

For data that survives crash/recovery:

- `persist_data(value)` — store to durable storage (one slot per node, overwrites)
- `retrieve_data<T>()` — returns `T?`, the stored value or nil
- `discard_data()` — remove persisted value

## Timers

```
var timeout_ch: chan<()> = set_timer();
<- timeout_ch;    // blocks until simulator fires the timer
```

No duration parameter — the simulator controls when timers fire to explore different orderings.

## Simulator Semantics

- `Init` runs synchronously at startup (required)
- `RecoverInit` runs on crash recovery (optional); node starts receiving messages after first yield point
- Messages to crashed nodes are buffered and re-delivered on recovery
- Crashed nodes lose all in-memory state; only `persist_data` survives
- Timers are dropped on crash
