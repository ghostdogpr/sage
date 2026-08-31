# Client-side caching

A **cached read** enables client-side caching for one call. The first read fetches the result from the server and caches it locally. Later reads of the same command use that local copy until the server invalidates it or the TTL expires.

Caching is opt-in per call. An ordinary `get` always goes to the server; only `cached` consults the local cache.

::: code-group

```scala [Ox]
client.set("cached:key", "v1")
// first fetches and caches; second is a local hit
val v1 = client.cached(Commands.get[String, String]("cached:key"), 1.minute)
val v2 = client.cached(Commands.get[String, String]("cached:key"), 1.minute)
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _  <- client.set("cached:key", "v1")
  v1 <- client.cached(Commands.get[String, String]("cached:key"), 1.minute)
  v2 <- client.cached(Commands.get[String, String]("cached:key"), 1.minute)
} yield (v1, v2)
```

:::

## How entries are kept fresh

Sage enables server-assisted client tracking. When a cached key changes, the server sends an invalidation and Sage removes the entry. The TTL provides a second limit. Sage evicts an expired entry even if no invalidation arrives. Until either event occurs, a cached read returns the local value without a round trip.

## What can be cached

A read is cacheable when its result depends only on the current value of its keys. The server can then invalidate the result whenever one of those keys changes. Reads that vary with time (`TTL`, `OBJECT IDLETIME`) or are non-deterministic (`SRANDMEMBER`) are read-only but not cacheable because a key change cannot reliably invalidate them.

::: warning
`cached` rejects writes and keyless reads with `NotCacheable`. The server cannot invalidate a keyless read when data changes, which could leave a stale value in the cache.
:::

Tune cache sizing and behavior through `clientCache` on [`SageConfig`](/configuration).

## Topology

Caching works with every topology. A cached read always runs against a master, regardless of the read policy, because the tracked cache belongs to that master. In a cluster, Sage routes the read to the master that owns the key's slot. Each slot-owning master keeps a separate cache.

## Limitations

- The cache budget applies to each master. `clientCache.maxBytes` sets the size of every master's cache, so the cluster-wide limit is `maxBytes` multiplied by the number of masters.
- Replacing a connection clears its cache. A reconnect, a cluster failover, or a resharding that moves the slot to another master starts with an empty cache.
- During a live slot migration, a cached read of a migrating key runs uncached and follows the `ASK` redirect once. Caching resumes after the migration.
- A server can support RESP3 but reject `CLIENT TRACKING` because of an ACL or proxy restriction. The client still connects, but cached reads run uncached as if caching were disabled.
