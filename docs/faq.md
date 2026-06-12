# FAQ

## Why a native protocol implementation rather than wrapping an existing Java client?

Implementing RESP3, the commands, and the codecs directly in Scala keeps the core free of any Java dependency and gives sage full control over allocation and decoding. It is the difference behind "native Redis protocol": there is no foreign client to adapt, configure, or work around.

## Which backend artifact should I use?

One per effect system, all sharing the same runtime:

- ZIO: `sage-client-zio`
- cats-effect: `sage-client-ce`
- Kyo: `sage-client-kyo`
- Ox: `sage-client-ox`

`sage-core` comes in transitively, so you depend on the backend artifact only. See [Getting started](/getting-started).

## Does every command open or borrow a connection?

No. Ordinary commands are auto-pipelined onto one multiplexed connection per node, shared by every fiber, with replies matched in order. Only commands that hold per-connection state or block (`WATCH`/`MULTI`/`EXEC`, `BLPOP`, and the like) lease a dedicated connection, and pub/sub uses its own subscription connection. The [Getting started](/getting-started) "how it works" aside covers this.

## Redis or Valkey? Which versions?

Both. Sage targets RESP3 and modern Redis 8+ and Valkey 8+.

## What Scala and JDK versions are required?

Scala 3.3.x LTS and later, on JDK 21 or newer.

## Is Scala.js or Scala Native supported?

No. The core is JVM-only.

## Is Redis Sentinel supported?

No, Sentinel is out of scope. Sage supports standalone, cluster, and master-replica deployments; see [Configuration](/configuration).

## Can I run Lua scripts or server-side functions?

Yes. `client.eval` (with `client.scriptLoad` / `client.evalSha` for cached scripts) runs Lua; `client.functionLoad`, `client.fCall`, `client.functionList`, and `client.functionDelete` manage and call server-side functions grouped into libraries. Read-only `*Ro` variants exist for the eligible commands. In a cluster, script and function management is routed to every master automatically. These replies come back as a raw `Frame`, which you decode with the strict helpers (for example `reply.asLong`).

## What happens when the connection drops?

Sage fails fast and reconnects in the background with exponential backoff. There is no offline queue: commands are not buffered while disconnected. A command in flight when the connection drops fails with `ConnectionLost`, whose `mayHaveExecuted` flag tells you whether retrying is safe (see [Error handling](/error-handling)). A watchdog detects connections that have gone silently dead so they are replaced. Reconnect and watchdog behavior are tunable on [`SageConfig`](/configuration).
