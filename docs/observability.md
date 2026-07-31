# Observability

Sage exposes two integration points, for two different jobs:

- **`SageListener`** is an asynchronous observer of `SageEvent`s (command completions, connection transitions, cache outcomes, topology changes), called off the command path. Use it for metrics and operational logging.
- **`CommandTracer`** produces distributed-tracing spans synchronously on the command path, so each Redis command appears as a client span nested under the surrounding request in an APM such as Datadog or Jaeger. Use it for distributed tracing: see [Distributed tracing](#distributed-tracing).

## Events

Register one or more `SageListener` on `SageConfig`, and each receives every `SageEvent`: command completions, connection transitions, cache outcomes, and topology changes. This is how you wire Sage into your metrics or logging.

| Event | Reported when |
| --- | --- |
| `CommandCompleted(name, node, duration, outcome)` | One logical command settled. `duration` is client-observed (including any cluster redirects/retries); `outcome` is `Succeeded` or `Failed(error)`. A cached read served locally yields no `CommandCompleted`. |
| `Connection.Connected(node)` | The multiplexed connection connected, on the initial connect and on every reconnect. |
| `Connection.Disconnected(node)` | A live connection was lost and the runtime began reconnecting. Graceful close is not reported. |
| `Connection.ConnectFailed(node, error)` | A connection could not be established, or the node could not be qualified during topology discovery. Names the address and cause, whether the failure is handled internally or also returned to the caller. |
| `Connection.ReconnectFailed(node, error)` | A reconnect attempt failed to establish, carrying its cause. Backoff and retries continue. Not reported for the initial connect. |
| `Cache.Hit(command)` / `Cache.Miss(command)` | A `cached` read was served locally, or had to fetch from the server. |
| `TopologyChanged(masters)` | The cluster's slot-owning master set changed (a failover, or scaling a shard in or out). |

Events carry no command arguments or payloads, so secrets such as `AUTH` credentials and user values never reach a listener. Where an event carries `node`, it is `Some` for cluster and master-replica clients and `None` for a standalone client. A node that keeps failing to connect produces one `ConnectFailed`, not one per attempt.

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

`SageListener` lives in the core and is the same on every backend, so this snippet is backend-independent.

### Delivery guarantees

Listeners are invoked off the command path, so a slow or throwing listener cannot block or break command execution. It only delays or loses events.

::: warning
Delivery is best-effort: a thrown exception is swallowed, and events are dropped once the internal dispatch queue fills. Listeners suit metrics, sampling, and operational logging, not anything that must be a complete record.
:::

## Distributed tracing

Use a `CommandTracer` rather than a listener here: it runs synchronously on the command path, so each Redis span is a child of whatever span is active when the command is issued. A listener runs too late, off that path, and its spans would be orphaned.

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

It emits one `CLIENT` span per command, named for the command (`GET`, `SET`, ...), carrying `db.system`, `db.operation.name`, `peer.service` (default `redis`, configurable), `component`, and the server address; a failure sets an error status with the exception. Only the command name is recorded, never arguments or keys, so secrets and user values stay out of your traces. Spans follow the ambient sampling decision.

One span is emitted per command that reaches the server, including each command in a pipeline (cluster redirects fold into the command's own span). A `cached` read served locally reaches no server and produces no span; one that misses is traced like any other command. In a `transaction`, the watch-phase reads are traced individually and the `MULTI`/`EXEC` body gets a single span named `MULTI`, which reflects the round trip rather than whether the transaction committed.

The tracer reads the active span from OpenTelemetry's thread-local current context (`Context.current()`) on the fiber that submits the command, and the module depends only on the OpenTelemetry API. So under an APM agent that instruments your runtime and propagates context across its threads, the Redis span nests under the active request span with no further wiring. That is the case for ZIO under the Datadog Java agent; configuring the agent itself is covered by its own documentation.

### Context on a fiber runtime without an agent

Running a bare OpenTelemetry SDK with no agent is the case that needs attention: on a fiber runtime the active span lives in fiber-local state (a ZIO `FiberRef`, a Cats Effect `IOLocal`), which is not the current context the tracer reads, so spans would be orphaned. Configure context storage so that `Context.current()` sees the active span:

- **ZIO**: with `zio-telemetry`, wire OpenTelemetry through the `OpenTelemetry.contextJVM` and `OpenTelemetry.global` layers (rather than `OpenTelemetry.contextZIO` and `OpenTelemetry.custom`), which back tracing with OpenTelemetry's native context so the SDK reads the active span. See zio-telemetry's auto-instrumentation interop documentation.
- **cats-effect**: with `otel4s` on Cats Effect 3.6+, add the `otel4s-oteljava-context-storage` dependency, enable the `cats.effect.trackFiberContext` system property, and provide `IOLocalContextStorage.localProvider[IO]`. This keeps the Java `Context` and the otel4s fiber context aligned so the SDK reads the active span. Note that the stock OpenTelemetry Java agent does not keep Cats Effect context in sync; otel4s ships a dedicated agent distribution for the agent case.

`OpenTelemetryCommandTracer.withContextProvider` lets you supply a custom `() => Context` for a context source that is thread-local but non-default.
