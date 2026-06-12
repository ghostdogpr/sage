# Observability

Sage reports what its runtime is doing through events. Register one or more `SageListener` on `SageConfig`, and each receives every `SageEvent`: command completions, connection transitions, cache outcomes, and topology changes. This is how you wire sage into your metrics, tracing, or logging.

## Events

| Event | Reported when |
| --- | --- |
| `CommandCompleted(name, node, duration, outcome)` | One logical command settled. `duration` is client-observed (including any cluster redirects/retries); `outcome` is `Succeeded` or `Failed(error)`. A cached read served locally yields no `CommandCompleted`. |
| `Connection.Connected(node)` | The multiplexed connection connected, on the initial connect and on every reconnect. |
| `Connection.Disconnected(node)` | A live connection was lost and the runtime began reconnecting. Graceful close is not reported. |
| `Cache.Hit(command)` / `Cache.Miss(command)` | A `cached` read was served locally, or had to fetch from the server. |
| `TopologyChanged(masters)` | The cluster's slot-owning master set changed (a failover, or scaling a shard in or out). |

Events carry no command arguments or payloads, so secrets such as `AUTH` credentials and user values never reach a listener. `node` is `Some` in a cluster (the relevant master) and `None` on a standalone server.

## Registering a listener

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

## Delivery guarantees

Listeners are invoked off the command path, so they cannot block or break command execution:

- The callback must not block. A slow listener only delays event delivery.
- A thrown exception is swallowed.
- Events are shed once the internal dispatch queue fills, so delivery is lossy under load.

::: warning
Delivery is best-effort: events are dropped once the dispatch queue fills, and a throwing listener is swallowed. Listeners suit metrics, sampling, and operational logging, not anything that must be a complete or lossless record.
:::
