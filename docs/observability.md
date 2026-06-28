# Observability

Sage exposes two integration points, for two different jobs:

- **`SageListener`** is an asynchronous observer of `SageEvent`s (command completions, connection transitions, cache outcomes, topology changes), called off the command path. Use it for metrics and operational logging.
- **`CommandTracer`** produces distributed-tracing spans synchronously on the command path, so each Redis command appears as a client span nested under the surrounding request in an APM such as Datadog or Jaeger. Use it for distributed tracing: see [Distributed tracing](#distributed-tracing).

They are separate types because their constraints differ: a listener runs after the fact and may drop events under load, so it cannot produce spans that nest under the active request; a tracer runs on the caller's fiber as the command is submitted, so it can.

## Events

Register one or more `SageListener` on `SageConfig`, and each receives every `SageEvent`: command completions, connection transitions, cache outcomes, and topology changes. This is how you wire sage into your metrics or logging.

| Event | Reported when |
| --- | --- |
| `CommandCompleted(name, node, duration, outcome)` | One logical command settled. `duration` is client-observed (including any cluster redirects/retries); `outcome` is `Succeeded` or `Failed(error)`. A cached read served locally yields no `CommandCompleted`. |
| `Connection.Connected(node)` | The multiplexed connection connected, on the initial connect and on every reconnect. |
| `Connection.Disconnected(node)` | A live connection was lost and the runtime began reconnecting. Graceful close is not reported. |
| `Cache.Hit(command)` / `Cache.Miss(command)` | A `cached` read was served locally, or had to fetch from the server. |
| `TopologyChanged(masters)` | The cluster's slot-owning master set changed (a failover, or scaling a shard in or out). |

Events carry no command arguments or payloads, so secrets such as `AUTH` credentials and user values never reach a listener. `node` is `Some` in a cluster (the relevant master) and `None` on a standalone server.

### Registering a listener

A `SageListener` has one synchronous, `Unit`-returning method. Match on the event you care about and forward it to your metrics system:

```scala
import sage.{SageEvent, SageListener}
import sage.SageEvent.*

val metrics = new SageListener {
  def onEvent(event: SageEvent): Unit = event match {
    case CommandCompleted(name, _, duration, _) => // record latency
    case Connection.Disconnected(_)             => // bump a gauge
    case Cache.Hit(_)                           => // count a hit
    case Cache.Miss(_)                          => // count a miss
    case _                                       => ()
  }
}

val config = SageConfig(
  topology = Topology.Standalone(Endpoint("localhost", 6379)),
  listeners = Vector(metrics)
)
```

`SageListener` lives in the core and is the same on every backend, so this snippet is backend-independent.

### Delivery guarantees

Listeners are invoked off the command path, so they cannot block or break command execution:

- The callback must not block. A slow listener only delays event delivery.
- A thrown exception is swallowed.
- Events are shed once the internal dispatch queue fills, so delivery is lossy under load.

::: warning
Delivery is best-effort: events are dropped once the dispatch queue fills, and a throwing listener is swallowed. Listeners suit metrics, sampling, and operational logging, not anything that must be a complete or lossless record.
:::

## Distributed tracing

A `SageListener` is the wrong tool for distributed tracing: by the time it runs on the dispatcher thread the caller's trace context is gone, so its spans would not nest under the in-flight request, and a dropped event would orphan a span. A `CommandTracer` instead runs synchronously on the command path, so each Redis span is a child of whatever span is active when the command is issued.

Set one on `SageConfig.tracer`. The `sage-opentelemetry` module provides an OpenTelemetry implementation:

```scala
libraryDependencies += "com.github.ghostdogpr" %% "sage-opentelemetry" % "@VERSION@"
```

```scala
import sage.opentelemetry.OpenTelemetryCommandTracer

val config = SageConfig(
  topology = Topology.Standalone(Endpoint("localhost", 6379)),
  tracer   = Some(OpenTelemetryCommandTracer.global()) // reads the globally-registered OpenTelemetry
)
```

It emits one `CLIENT` span per command, named for the command (`GET`, `SET`, ...), with `db.system=redis`, `db.operation.name`, `peer.service` (default `redis`, configurable), `component=redis-client`, and the server address (the configured endpoint for a standalone server, the routed node for a cluster or master/replica); a failure sets an error status carrying the exception. Only the command name is recorded, never arguments or keys, so secrets and user values stay out of your traces. Spans follow the ambient sampling decision.

One span is emitted per command that reaches the server: an ordinary command, a blocking command, and each command in a pipeline (cluster redirects fold into the command's own span). A `cached` read served from the local cache reaches no server and produces no span; one that misses and fetches from the server is traced like any other command.

In a `transaction`, each read during the watch phase and the `WATCH` itself are traced like ordinary commands, and the atomic `MULTI`/`EXEC` body gets a single span named `MULTI`. That span reflects the round trip, not whether the transaction committed, so a `WATCH` abort or an error inside `EXEC` still settles it successfully. Transaction commands are traced but emit no `CommandCompleted`, so the listener contract is unchanged.

The tracer reads the active span from OpenTelemetry's thread-local current context (`Context.current()`) on the fiber that submits the command. The module depends only on the OpenTelemetry **API**, so an APM agent supplies the implementation: when the agent instruments your runtime and propagates its context across that runtime's threads, the Redis span nests under the active request span with no further wiring. This is the case for ZIO under the Datadog Java agent. Configuring the agent itself (for Datadog, enabling its OpenTelemetry support) is covered by the agent's own documentation.

### Context on a fiber runtime without an agent

Running a bare OpenTelemetry SDK with no agent is the case that needs attention: on a fiber runtime the active span lives in fiber-local state (a ZIO `FiberRef`, a Cats Effect `IOLocal`), which is not the current context the tracer reads, so spans would be orphaned. Configure context storage so that `Context.current()` sees the active span:

- **ZIO**: with `zio-telemetry`, wire OpenTelemetry through the `OpenTelemetry.contextJVM` and `OpenTelemetry.global` layers (rather than `OpenTelemetry.contextZIO` and `OpenTelemetry.custom`), which back tracing with OpenTelemetry's native context so the SDK reads the active span. See zio-telemetry's auto-instrumentation interop documentation.
- **cats-effect**: with `otel4s` on Cats Effect 3.6+, add the `otel4s-oteljava-context-storage` dependency, enable the `cats.effect.trackFiberContext` system property, and provide `IOLocalContextStorage.localProvider[IO]`. This keeps the Java `Context` and the otel4s fiber context aligned so the SDK reads the active span. Note that the stock OpenTelemetry Java agent does not keep Cats Effect context in sync; otel4s ships a dedicated agent distribution for the agent case.

`OpenTelemetryCommandTracer.withContextProvider` lets you supply a custom `() => Context` for a context source that is thread-local but non-default.
