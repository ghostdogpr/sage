# FAQ

## How does Sage perform?

Commands from every fiber share one auto-pipelined connection per node. Sage can combine concurrent commands into a single socket write and one round trip instead of sending each one separately. I/O runs on virtual threads with blocking reads and writes. Reading replies does not block new commands from being written, and the RESP3 parser and codecs decode replies directly into your types.

Run [the benchmarks](https://github.com/ghostdogpr/sage/tree/main/benchmarks) against a real server to compare concurrent workloads. Results depend on the client versions and hardware, so the repository does not publish fixed numbers.

## Which backend artifact should I use?

One per Scala stack, all sharing the same runtime:

- ZIO: `sage-client-zio`
- Cats Effect: `sage-client-ce`
- Kyo: `sage-client-kyo`
- Ox: `sage-client-ox`
- Pekko: `sage-client-pekko`

`sage-core` comes in transitively, so you depend on the backend artifact only. See [Getting started](/getting-started).

## Does every command open or borrow a connection?

No. Ordinary commands are auto-pipelined onto one multiplexed connection per node, shared by every fiber, with replies matched in order. Commands that hold per-connection state or block (`WATCH`/`MULTI`/`EXEC`, `BLPOP`, and the like) temporarily use a dedicated connection. Pub/sub uses its own subscription connection. The [Getting started](/getting-started) "how it works" aside covers this.

## Redis or Valkey? Which versions?

Both. Sage targets Redis 8+ and Valkey 8+, where every command it exposes is available. It can connect to any RESP3 server, including Redis 6.0 or later. An older server supports only the commands available in that version. For example, hash-field TTL requires 7.4, while `HGETEX`, `HGETDEL`, and `HSETEX` require 8.0. An older server returns an error for these commands.

## What Scala and JDK versions are required?

Scala 3.9.x LTS and later, on JDK 21 or newer.

## Is Scala.js or Scala Native supported?

No. The core is JVM-only.

## Is Redis Sentinel supported?

No, Sentinel is out of scope. Sage supports standalone, cluster, and master-replica deployments; see [Configuration](/configuration).

## Can I run Lua scripts or server-side functions?

Yes. `client.eval` runs Lua. Use `client.scriptLoad` and `client.evalSha` to cache scripts. `client.functionLoad`, `client.fCall`, `client.functionList`, and `client.functionDelete` manage and call server-side function libraries. Eligible commands also have read-only `*Ro` variants. In a cluster, Sage routes script and function management to every master. These commands return a raw `Frame`. Decode it with a strict helper such as `reply.asLong`.

## What happens when the connection drops?

Sage fails fast and reconnects in the background with exponential backoff. It does not buffer commands while disconnected. A command in flight when the connection drops fails with `ConnectionLost`, whose `mayHaveExecuted` flag tells you whether retrying is safe (see [Error handling](/error-handling)). A watchdog detects and replaces connections that stop responding. You can configure reconnect and watchdog behavior on [`SageConfig`](/configuration).
