# Configuration

Everything about how a client connects is set on one value, `SageConfig`. The command surface never changes: the same `SageClient` talks to a standalone server, a cluster, or a master-replica deployment, and the only difference is configuration. Standalone, cluster, and master-replica are choices here, not different code.

```scala
val config = SageConfig(
  topology = Topology.Standalone(Endpoint("localhost", 6379))
)
```

Every field has a sensible default, so `SageConfig()` connects to a local standalone server. The sections below cover the fields that shape connectivity; [Connection tuning](#connection-tuning) summarizes the operational knobs.

## Standalone

The default topology. A single endpoint, and optionally a logical database:

```scala
val config = SageConfig(
  topology = Topology.Standalone(Endpoint("localhost", 6379)),
  database = 0
)
```

The `database` is selected once at connection setup and fixed for the client's lifetime, re-applied on every reconnect. It is never changed by a runtime command, because that would move the keyspace under every fiber sharing the connection. Valkey 9+ also supports numbered databases in cluster mode; Redis Cluster and older Valkey versions reject a non-zero database during connection setup.

## Cluster

Give the cluster seeds. Sage discovers the full topology from them, routes each command to the node owning its key's slot, and follows `MOVED`/`ASK` redirects transparently:

```scala
val config = SageConfig(
  topology = Topology.Cluster(
    Vector(Endpoint("localhost", 7000), Endpoint("localhost", 7001))
  ),
  database = 0
)
```

Seeds bootstrap discovery only. Once the topology is known, sage routes to the nodes the cluster reports; any one seed answering is enough.
Set `database` to a non-zero value for a Valkey 9+ cluster configured with a sufficiently large `cluster-databases` setting. Sage issues `SELECT` while establishing every node connection, including after reconnects and redirects. Servers without numbered cluster database support reject the connection.

### Hash tags

Redis Cluster hashes the bytes inside the first non-empty `{...}` pair instead of the whole key. Use the same tag in every key that must live
in one slot, for example `user:{42}:profile` and `user:{42}:settings`. Transactions require all keys to share one slot.

## Master-replica

Select `Topology.MasterReplica` with seed endpoints. Sage asks each its role, discovers the master and its replicas, sends writes to the master, and routes reads per the read policy:

```scala
val config = SageConfig(
  topology = Topology.MasterReplica(
    Vector(Endpoint("localhost", 6379), Endpoint("localhost", 6380))
  ),
  readFrom = ReadFrom.ReplicaPreferred
)
```

## Read routing

`readFrom` governs which node a read-only command may run on, the same setting for both cluster and master-replica deployments:

| `ReadFrom` | Reads go to |
| --- | --- |
| `Master` (default) | the master, always |
| `MasterPreferred` | the master, falling back to a replica |
| `Replica` | a replica, failing if none is reachable |
| `ReplicaPreferred` | a replica, falling back to the master |

Only read-only commands are eligible. Writes, and any command not marked read-only, always go to the master regardless of the policy. Reads served by a replica may lag the master; that staleness is the policy's accepted contract, not a fault.

## TLS and ACL

Both are configuration on top of the same client. `tls` selects the trust source; `auth` carries the ACL user:

```scala
val config = SageConfig(
  topology = Topology.Standalone(Endpoint("localhost", 6380)),
  tls = Some(TlsConfig(TrustSource.System)),
  auth = Some(AuthConfig(username = "app", password = "app-secret"))
)
```

`TrustSource.System` uses the system trust store. Use `TrustSource.Pem` or `TrustSource.TrustStore` for a private CA, or `TrustSource.Custom(sslContext)` to supply your own `SSLContext` (the path to mutual TLS). `AuthConfig` redacts its password in logs and in any printed `SageConfig`.

::: warning
`TrustSource.Insecure` is for local development only. It trusts every certificate and skips hostname verification, leaving the connection open to machine-in-the-middle attacks. Never use it in production.
:::

## Connection tuning

The remaining fields tune connection lifecycle, pooling, and observability. Each is its own config type with its own defaults, so you set only what you need:

| Field | Tunes | Defaults |
| --- | --- | --- |
| `connectTimeout` | per socket connect/TLS step and per bootstrap command (`HELLO 3`, identification, optional `SELECT`/`CLIENT SETNAME`, cache setup) | `10.seconds` |
| `reconnect` (`BackoffConfig`) | exponential reconnect backoff with full jitter | `50.millis` to `5.seconds`, ×2 |
| `watchdog` (`WatchdogConfig`) | idle-connection liveness ping (death detector) | ping every `60.seconds`, `30.seconds` timeout |
| `closeTimeout` | how long `close` waits for in-flight commands on the multiplexed connection to drain (blocking commands and transactions on the dedicated pool are force-closed at once) | `5.seconds` |
| `dedicatedPool` (`DedicatedPoolConfig`) | the pool behind blocking commands and transactions | max `8`, acquire `5.seconds`, idle `30.seconds` |
| `pubsub` (`PubSubConfig`) | per-subscription message buffer size | `128` |
| `clientCache` (`CacheConfig`) | client-side caching on/off and size cap | enabled, `64 MB` |
| `clientName` | `CLIENT SETNAME`, shown in `CLIENT LIST` / `CLIENT INFO` | none |
| `listeners` | observers of runtime events (`SageListener`) | none |

For example, a cluster client with a shorter connect timeout, a larger blocking-command pool, a more frequent watchdog, and a name:

```scala
import scala.concurrent.duration.*

val config = SageConfig(
  topology = Topology.Cluster(Vector(Endpoint("localhost", 7000))),
  connectTimeout = 5.seconds,
  dedicatedPool = DedicatedPoolConfig(maxConnections = 16),
  watchdog = WatchdogConfig(pingInterval = 30.seconds),
  clientName = Some("orders-service")
)
```

Disable client-side caching where the server permits ordinary commands but denies `CLIENT TRACKING` (some proxies and ACL setups); `cached` reads then run without caching, keeping the call portable:

```scala
val config = SageConfig(
  topology = Topology.Standalone(Endpoint("localhost", 6379)),
  clientCache = CacheConfig(enabled = false)
)
```

## From a connection URI

For the common cases you can parse a `redis://` or `rediss://` URI instead of assembling the config by hand. `rediss` selects TLS with system trust, userinfo becomes the ACL auth, a `/<db>` path sets the database, and comma-separated hosts yield cluster seeds. It returns the problem as a `Left` rather than throwing, and there is intentionally no way to select insecure TLS from a URI:

```scala
// fromUri returns Either: a Left describes the problem, a Right is the config
val parsed = SageConfig.fromUri("rediss://app:app-secret@localhost:6380/0")
// further tuning stays programmatic:
//   SageConfig.fromUri(uri).map(_.copy(readFrom = ReadFrom.ReplicaPreferred))
```
