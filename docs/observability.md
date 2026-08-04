# Observability

Sage exposes two integration points, for two different jobs:

- **`SageListener`** is an asynchronous observer of `SageEvent`s (command completions, connection transitions, cache outcomes, topology changes), called off the command path. Use it for metrics and operational logging.
- **`CommandTracer`** produces distributed-tracing spans synchronously on the command path, so each Redis command appears as a client span nested under the surrounding request in an APM such as Datadog or Jaeger. Use it for distributed tracing: see [Distributed tracing](#distributed-tracing).

## Events

Register one or more `SageListener` instances on `SageConfig`. Each listener receives every `SageEvent`: command completions, connection transitions, cache outcomes, and topology changes. You can use these events for metrics or logging.

| Event | Reported when |
| --- | --- |
| `CommandCompleted(name, node, duration, outcome)` | One command completed. `duration` is measured by the client and includes any cluster redirects or retries. `outcome` is `Succeeded` or `Failed(error)`. A cached read served locally does not produce this event. |
| `Connection.Connected(node)` | The multiplexed connection connected, on the initial connect and on every reconnect. |
| `Connection.Disconnected(node)` | A live connection was lost and the runtime began reconnecting. Graceful close is not reported. |
| `Connection.ConnectFailed(node, error)` | A connection could not be established, or the node could not be qualified during topology discovery. Names the address and cause, whether the failure is handled internally or also returned to the caller. |
| `Connection.ReconnectFailed(node, error)` | A reconnect attempt failed to establish, carrying its cause. Backoff and retries continue. Not reported for the initial connect. |
| `Cache.Hit(command)` / `Cache.Miss(command)` | A `cached` read was served locally, or had to fetch from the server. |
| `TopologyChanged(masters)` | The cluster's slot-owning master set changed (a failover, or scaling a shard in or out). |

Events omit command arguments and payloads. This keeps secrets such as `AUTH` credentials and user values out of listeners. Where an event carries `node`, it is `Some` for cluster and master-replica clients and `None` for a standalone client. A node that keeps failing to connect produces one `ConnectFailed` rather than one event per attempt.

### Registering a listener

A `SageListener` has one synchronous, `Unit`-returning method. Match on the event you care about and forward it to your metrics system:

```scala
import sage.{SageEvent, SageListener}
import sage.SageEvent.*

val metrics = new SageListener {
  def onEvent(event: SageEvent): Unit = event match {
    case CommandCompleted(name, _, duration, _) => // record latency
    case Connection.Disconnected(_)             => // bump a gauge
    case Connection.ConnectFailed(node, error)  => // log the unreachable address
    case Connection.ReconnectFailed(_, error)   => // log the cause
    case Cache.Hit(_)                           => // count a hit
    case Cache.Miss(_)                          => // count a miss
    case _                                      => ()
  }
}

val config = SageConfig(
  topology = Topology.Standalone(Endpoint("localhost", 6379)),
  listeners = Vector(metrics)
)
```

This listener example works on every backend.

### Delivery guarantees

Listeners run separately from command execution. A slow or throwing listener may delay or lose events, but it does not affect commands.

::: warning
Delivery is best-effort: a thrown exception is swallowed, and events are dropped once the internal dispatch queue fills. Listeners suit metrics, sampling, and operational logging, not anything that must be a complete record.
:::

## Distributed tracing

Use a `CommandTracer` rather than a listener for distributed tracing. It runs while Sage processes the command, which makes each Redis span a child of the active span. A listener runs later and cannot preserve this parent-child relationship.

Set one on `SageConfig.tracer`. The `sage-opentelemetry` module provides an OpenTelemetry implementation:

```scala
"com.github.ghostdogpr" %% "sage-opentelemetry" % "@VERSION@"
```

```scala
import sage.opentelemetry.OpenTelemetryCommandTracer

val config = SageConfig(
  topology = Topology.Standalone(Endpoint("localhost", 6379)),
  // reads the globally-registered OpenTelemetry
  tracer   = Some(OpenTelemetryCommandTracer.global())
)
```

It emits one `CLIENT` span per command, named for the command (`GET`, `SET`, ...), carrying `db.system`, `db.operation.name`, `peer.service` (default `redis`, configurable), `component`, and the server address; a failure sets an error status with the exception. The tracer records the command name but omits arguments and keys, keeping secrets and user values out of traces. Spans follow the ambient sampling decision.

Sage emits one span for each command sent to the server, including each command in a pipeline (cluster redirects remain part of the command's span). A `cached` read served locally does not produce a span. A cache miss is traced like any other command. In a `transaction`, Sage traces the watch-phase reads individually and creates one span named `MULTI` for the `MULTI`/`EXEC` body. This span represents the round trip, not whether the transaction committed.

The tracer reads the active span from `Context.current()` when a fiber submits a command. If an APM agent propagates that context, the Redis span becomes a child of the active request span without additional configuration. This works for ZIO under the Datadog Java agent; see the agent's documentation for its own setup.

### Context on a fiber runtime without an agent

When you use the OpenTelemetry SDK without an agent, you need to configure context storage. On a fiber runtime, the active span lives in fiber-local state (a ZIO `FiberRef` or Cats Effect `IOLocal`), but the tracer reads the current OpenTelemetry context. Configure context storage so that `Context.current()` can see the active span:

- **ZIO**: with `zio-telemetry`, wire OpenTelemetry through the `OpenTelemetry.contextJVM` and `OpenTelemetry.global` layers (rather than `OpenTelemetry.contextZIO` and `OpenTelemetry.custom`), which back tracing with OpenTelemetry's native context so the SDK reads the active span. See zio-telemetry's auto-instrumentation interop documentation.
- **cats-effect**: with `otel4s` on Cats Effect 3.6+, add the `otel4s-oteljava-context-storage` dependency, enable the `cats.effect.trackFiberContext` system property, and provide `IOLocalContextStorage.localProvider[IO]`. This keeps the Java `Context` and the otel4s fiber context aligned so the SDK reads the active span. Note that the stock OpenTelemetry Java agent does not keep Cats Effect context in sync; otel4s ships a dedicated agent distribution for the agent case.

`OpenTelemetryCommandTracer.withContextProvider` lets you supply a custom `() => Context` for a context source that is thread-local but non-default.
