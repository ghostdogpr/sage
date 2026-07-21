# Client-side caching

A **cached read** opts into client-side caching for one call. The first read fetches from the server and caches the result locally; later reads of the same command are served from that local copy until the server invalidates it or the TTL expires.

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

Sage uses the server's tracking so that when a key you have cached changes, the server pushes an invalidation and the entry is dropped. The TTL you pass is a second bound: an entry is evicted once it expires even if no invalidation arrives. Between those two, a cached read returns the local value without a round-trip.

## What can be cached

Only reads whose result is a pure function of the named keys' current state are cacheable, because a server invalidation push covers every way such a result can change. Reads that vary with time (`TTL`, `OBJECT IDLETIME`) or are non-deterministic (`SRANDMEMBER`) are read-only but not cacheable: nothing would ever fire to invalidate them. Passing such a command to `cached` is not how caching is meant to be used.

::: warning
`cached` rejects a write or a keyless read with `NotCacheable`. A keyless read could only ever be evicted by its TTL, never by an invalidation, so it is refused rather than allowed to go silently stale.
:::

Tune cache sizing and behavior through `clientCache` on [`SageConfig`](/configuration).

## Topology

Caching works on every topology. On a standalone or master-replica client, cached reads run on the master, which holds the tracking-backed cache. On a cluster client, each slot-owning master owns its own cache, and a cached read is routed to the master owning the key's slot (never a replica, whatever the read policy). The same call is therefore portable across all three topologies; the result is always correct, you only forgo the local hit where caching is not active.

## Limitations

- The cache budget is per master, not global: `clientCache.maxBytes` sizes each master's cache, so a cluster's effective ceiling is `maxBytes` times the number of masters.
- A cache goes cold whenever its connection is replaced or its master stops owning the slot: a reconnect, a cluster failover, or a resharding that moves the slot off its master all start it fresh. Steady-state hit rates are unaffected; only these (rare) events reset it.
- During a live slot migration a cached read of a migrating key runs uncached (following the `ASK` redirect once) until the migration completes.
- A server that speaks RESP3 but rejects `CLIENT TRACKING` (an ACL or proxy restriction) still connects; cached reads there run uncached, exactly as when caching is disabled.
