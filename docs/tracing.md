# Tracing and Telemetry

The Spur simulator includes tracing capabilities to inspect the internal state and execution flow of your protocol without relying purely on standard logs.

## The `@trace` Annotation

In Spur, you can tag any function with the `@trace` annotation to automatically capture execution telemetry.

```text
@trace
async func handle_request(req: Request) -> Response {
  // ...
}
```

## Trace Events

When a function is traced, the simulator engine automatically captures a sequence of structured events:

1. **`Dispatch`**: Recorded at the exact moment the simulator schedules the traced function to run.
2. **`Enter`**: Recorded when the execution of the traced function officially begins. This event records the function's incoming parameters.
3. **`Exit`**: Recorded when the traced function completes execution. This event captures the function's return value.

## Trace Payloads

Trace events capture rich contextual metadata beyond just function names:

- **Trace IDs**: Every invocation of a traced function receives a unique identifier.
- **Causal Operation IDs**: Traces are causally linked back to the original client invocation that triggered them, creating traceable request paths across the system.
- **Schedulable Counts**: Traces capture the exact `schedulable_count` of the simulator's runnable queue at the time of the event. This metric is incredibly useful for quantifying the "greediness" of the scheduler and identifying bottlenecks.
