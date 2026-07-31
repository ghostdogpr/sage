# Configuration

Everything about how a client connects is set on one value, `SageConfig`. The command surface never changes: the same `SageClient` talks to a standalone server, a cluster, or a master-replica deployment, and the only difference is configuration.

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

The `database` is selected at connection setup and fixed for the client's lifetime. There is no runtime `SELECT`, because it would move the keyspace under every fiber sharing the connection.

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

Seeds bootstrap discovery only. Once the topology is known, Sage routes to the nodes the cluster reports; any one seed answering is enough.

A non-zero `database` in cluster mode needs Valkey 9+ with a large enough `cluster-databases` setting. Redis Cluster and older Valkey versions reject the connection.

### Hash tags

Redis Cluster hashes the bytes inside the first non-empty `{...}` pair instead of the whole key. Use the same tag in every key that must live
in one slot, for example `user:{42}:profile` and `user:{42}:settings`. Transactions require all keys to share one slot.

### Supported cross-slot commands

`mGet`, `mSet`, `exists`, `del`, `unlink`, and `touch` may span slots. Sage groups their keys by slot, sends one subcommand per slot, and merges the
replies back into one: `mGet` restores request order (keeping missing and repeated positions), `exists`, `del`, `unlink`, and `touch` sum their
counts, and `mSet` succeeds only if every group returns `OK`. This works inside a pipeline too.

Each slot's subcommand is atomic on its own, but the call as a whole is not. A cross-slot `mGet` is not a point-in-time snapshot, and a failing
`mSet`, `del`, or `unlink` may already have written to the groups that succeeded. If any group fails, the whole call fails. Use a common hash tag
when the operation must be atomic.

`mSetNx` is never split, since that would break its all-or-nothing condition, and no cross-slot command is allowed inside a transaction, which must
stay pinned to one slot.

### Commands that run on every master

No node sees the whole keyspace, and a cluster replicates neither the script nor the function cache. So `scriptLoad`, `scriptExists`, `scriptFlush`,
the `function*` mutations, `flushAll`, `flushDb`, `keys`, `dbSize`, `waitReplicas`, and `waitAof` run on every slot-owning master, and their replies
are folded into one. There is no partial result: if one master fails, the call fails.

This is also the one case where Sage does not retry a `-CLUSTERDOWN` for you; see
[Refusals Sage retries for you](/error-handling#refusals-sage-retries-for-you).

### Topology refresh

Sage re-reads the slot map whenever a command shows it is out of date: a `MOVED` or `ASK` redirect, a slot no known node covers, a node that is
unreachable or no longer a master. Reshardings and failovers therefore need no configuration.

Adding a replica breaks nothing, so nothing tells Sage to look again, and reads under `ReadFrom.Replica` or `ReadFrom.ReplicaPreferred` keep
using the replicas already known. Set `topologyRefreshInterval` to check for new ones on a timer:

```scala
val config = SageConfig(
  topology = Topology.Cluster(
    Vector(Endpoint("localhost", 7000)),
    ClusterConfig(topologyRefreshInterval = Some(30.seconds))
  ),
  readFrom = ReadFrom.Replica
)
```

There is no timer unless you set one. Each refresh costs one `CLUSTER SLOTS`, and ticks arriving within `minRefreshInterval` of the last refresh are
skipped, so a short interval cannot flood the cluster. `MasterReplicaConfig` has the same setting.

## Master-replica

Select `Topology.MasterReplica` with seed endpoints. Sage discovers the nodes' roles, sends writes to the master, and routes reads per the read policy:

```scala
val config = SageConfig(
  topology = Topology.MasterReplica(
    Vector(Endpoint("localhost", 6379), Endpoint("localhost", 6380))
  ),
  readFrom = ReadFrom.ReplicaPreferred
)
```

The number of endpoints decides where Sage may connect:

| Seeds | Nodes Sage dials |
| --- | --- |
| Several | only the supplied endpoints, each classified by its own `ROLE`. Addresses that `ROLE` advertises are ignored, and a replica is used once it reports `connected` |
| One | the supplied endpoint and the master or replicas discovered from its `ROLE` reply |

Use several endpoints for managed deployments whose stable primary and reader names differ from the per-node addresses Redis advertises.

An endpoint that is unreachable, or a replica that is still synchronizing, is left out at connect time so a partially available deployment still
connects. Sage picks it up again on its own once traffic reveals the topology has changed, with no polling. Adding a *second* replica is the case
nothing signals, so set `topologyRefreshInterval` if you scale readers out; see [Topology refresh](#topology-refresh).

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
| `connectTimeout` | each socket connect, TLS handshake, and connection-setup command | `10.seconds` |
| `reconnect` (`BackoffConfig`) | exponential reconnect backoff with full jitter | `50.millis` to `5.seconds`, ×2 |
| `watchdog` (`WatchdogConfig`) | idle-connection liveness ping (death detector) | ping every `60.seconds`, `30.seconds` timeout |
| `closeTimeout` | how long `close` waits for in-flight commands to drain (blocking commands and transactions are closed at once) | `5.seconds` |
| `dedicatedPool` (`DedicatedPoolConfig`) | the pool behind blocking commands and transactions, per node | max `8`, acquire `5.seconds`, idle `30.seconds` |
| `pubsub` (`PubSubConfig`) | per-subscription message buffer size | `128` |
| `clientCache` (`CacheConfig`) | client-side caching on/off and size cap | enabled, `64 MB` |
| `clientName` | `CLIENT SETNAME`, shown in `CLIENT LIST` / `CLIENT INFO` | none |
| `listeners` | observers of runtime events (`SageListener`) | none |
| `tracer` | [distributed-tracing](/observability#distributed-tracing) spans on the command path (`CommandTracer`) | none |

`dedicatedPool.maxConnections` is a ceiling per node, not per client: a blocking command runs on the node holding its keys, so every node gets its own pool. Connections open on demand and idle ones are evicted, so the ceiling is what a burst can reach, but size it against `maxclients` with your node count in mind.

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
